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
