import time
import threading
import subprocess as sp
import concurrent.futures as cf


def worker(x):
    # A thread pool allows you to run either IO-bound Python bytecode
    # concurrently (which means the IO actions themselves will happen in
    # parallel), or CPU-bound external subprocesses in parallel.
    sp.run(["sleep", f"{x}s"])
    return threading.get_ident(), x


def log(tid, x):
    ts = str(int(time.time()))[-2:]
    print(f"Worker {tid}, job {x} done at ...{ts}.")


FOO = []


def worker_accessing_global_mutable_resource(x):
    if x not in FOO:
        # this sleeps the current thread, not the entire process
        time.sleep(1)
        FOO.append(x)
    return threading.get_ident(), id(FOO), FOO


def main():
    # As of 3.8, the default for 'max_workers' with 'ThreadPoolExecutor'
    # is 'min(os.cpu_count() + 4, 32)', which is a three-way compromise
    # between using the pool for IO-bound tasks (where more workers than
    # CPUs is what you want, so that you can wait in parallel),
    # CPU-bound tasks (where you want roughly as many workers as you've
    # got CPUs), and not implicitly hogging all of the resources of
    # machines with many (> 32) cores.
    #
    # Still, if you have a more precise idea of the workload you're
    # submitting and the machine you'll be running it on, it's worth
    # tweaking this. E.g. if you want to paralellize subprocesses, which
    # is CPU-bound, something like 'int(os.cpu_count() * 0.9)' is
    # reasonable if you can afford to use more than 32 cores on big
    # machines, while still leaving a bit of headroom.
    with cf.ThreadPoolExecutor(max_workers=2) as ex:
        tasks = [5, 0, 4, 3, 2, 1]

        # The '.map()' method yields results as they come in and
        # preserves the ordering. This means that if the tasks have
        # uneven execution times, getting the results from quicker tasks
        # might be blocked behind slower tasks, as demonstrated by the
        # code below, which prints the first three results
        # simultaneously, once the first (longest-running) task is done:
        #
        # 	Worker 139625394366208, job 5 done at ...90.
        # 	Worker 139625385973504, job 0 done at ...90.
        # 	Worker 139625385973504, job 4 done at ...90.
        # 	Worker 139625385973504, job 3 done at ...92.
        # 	Worker 139625394366208, job 2 done at ...92.
        # 	Worker 139625394366208, job 1 done at ...93.
        #
        # Remember also that the 'chunksize' argument is ignored with
        # threads -- its purpose is to alleviate IPC cost, but there's
        # no IPC with threads, so it clearly makes no sense to chunk
        # tasks when using 'ThreadPoolExecutor'.
        #
        # Exceptions which occurred while performing a task are raised
        # at the point that its result should be yielded.
        for tid, x in ex.map(worker, tasks):
            log(tid, x)
        print()

        # Receiving results out of order, as soon as they're completed,
        # is slightly more complicated but possible. Note that
        # 'as_completed()' has the advantage of being able to juggle
        # futures driven to completion by different executors at the
        # same time. The output of the code below is the following:
        #
        # 	Worker 139625377580800, job 0 done at ...88.
        # 	Worker 139625377580800, job 4 done at ...92.
        # 	Worker 139625385973504, job 5 done at ...93.
        # 	Worker 139625385973504, job 2 done at ...95.
        # 	Worker 139625377580800, job 3 done at ...95.
        # 	Worker 139625385973504, job 1 done at ...96.
        for fut in cf.as_completed(ex.submit(worker, t) for t in tasks):
            # Accessing 'fut.result()' might raise an exception if
            # something went wrong while computing the future.
            tid, x = fut.result()
            log(tid, x)
        print()

        # Threads are part of the same process, which means they share
        # the same address space, which in turn means all worker threads
        # and parent threads have access to the same globals. So the
        # same instance of the global list 'FOO' is actually shared by
        # all threads, which race to update it (workers) or print it
        # (main thread).
        #
        # Now, in practice (at least in CPython), the GIL saves you from
        # memory corruption because only one thread is allowed to run at
        # a time. But you can still have logic bugs -- e.g. here, the
        # intent is to append to 'FOO' without creating duplicates, but
        # since there's no atomic check-and-append for lists, you can
        # end up with duplicates anyway.
        #
        # 	Worker 6138818560 has FOO at address 4354886336 with contents [5, 0]
        # 	Worker 6155644928 has FOO at address 4354886336 with contents [5, 0]
        # 	Worker 6138818560 has FOO at address 4354886336 with contents [5, 0, 4, 3]
        # 	Worker 6155644928 has FOO at address 4354886336 with contents [5, 0, 4, 3]
        # 	Worker 6138818560 has FOO at address 4354886336 with contents [5, 0, 4, 3, 2, 1]
        # 	Worker 6155644928 has FOO at address 4354886336 with contents [5, 0, 4, 3, 2, 1]
        # 	Worker 6138818560 has FOO at address 4354886336 with contents [5, 0, 4, 3, 2, 1, 1]
        #
        # The right way to do this is to either use atomic operations
        # (e.g. maybe your elements are hashable, in which case 'FOO'
        # might be a set, and 'set.add()' is atomic) or a lock. But
        # locking is tricky, in real-world code, you can easily end up
        # with a deadlock, or write a very complicated and brittle
        # threaded version of your program which executes fully
        # sequentially, much like the original non-threaded one, except
        # possibly slower due to the context-switching overhead, because
        # the locks are too coarse-grained to allow any parallelism.
        #
        # So a better approach than locks is to have resources managed
        # by dedicated daemon threads and send them requests via atomic
        # message queues. See Raymond Hettinger's excellent talk for a
        # sketch of how this is done and why locks should be avoided:
        # https://youtu.be/9zinZmE3Ogk?t=2889
        #
        # It might also be the case that what you need is not one global
        # 'FOO', but for each thread to have *its own* global 'FOO'
        # (which is incidentally what happens when using process-based
        # parallelism, see 02-concurrent_futures_processes.py). That can
        # be achieved in a threading context using thread locals -- see
        # 'threading.local()'.
        for fut in cf.as_completed(
            ex.submit(worker_accessing_global_mutable_resource, t) for t in tasks + [1]
        ):
            tid, foo_id, foo = fut.result()
            print(f"Worker {tid} has FOO at address {foo_id} with contents {foo}")

        # Finally, if you'd like to process potentially unbounded
        # streams of data in real time, it looks like you'll have to use
        # queues and manage them manually, which can be a bit finicky:
        # https://stackoverflow.com/questions/41648103/how-would-i-go-about-using-concurrent-futures-and-queues-for-a-real-time-scenari
        #
        # See also https://bugs.python.org/issue29842.
        #
        # This is one case where you might want to consider using
        # 'multiprocessing' instead, which provides this ability out of
        # the box in its high-level API. That is, if you don't mind the
        # overhead associated with spawning multiple processes that you
        # don't technically need.


if __name__ == "__main__":
    main()
