import os
import time
import multiprocessing as mp
import concurrent.futures as cf


def worker(x):
    # A process pool allows you to run CPU-intensive Python bytecode in
    # parallel.
    time.sleep(x)
    return os.getpid(), x


def log(tid, x):
    ts = str(int(time.time()))[-2:]
    print(f"Worker {tid}, job {x} done at ...{ts}.")


FOO = set()


def worker_accessing_global_mutable_resource(x):
    FOO.add(x)
    return os.getpid(), id(FOO), FOO


def main():
    # The multiprocessing context allows you to specify how processes
    # are started:
    #
    # - "forkserver" is safe to mix with threading and fast, but only
    #   works on Unix platforms which support passing file descriptors
    #   over Unix pipes
    # - "spawn" is safe w.r.t. to threading but slow; it's the default
    #   on macOS and Windows
    # - "fork" is fast but can lead to problems with multithreaded code,
    #   cf. https://pythonspeed.com/articles/python-multiprocessing/;
    #   only available and default on Unix
    ctx = mp.get_context("fork")

    # The default for 'max_workers' with 'ProcessPoolExecutor' is
    # 'os.cpu_count()', because it expects CPU-bound tasks, but if you
    # want to make it easier for your computer to do other stuff at the
    # same time, then something like 'int(os.cpu_count() * 0.9)' seems
    # reasonable.
    with cf.ProcessPoolExecutor(max_workers=2, mp_context=ctx) as ex:
        tasks = [5, 0, 4, 3, 2, 1]

        # The '.map()' method yields results as they come in and
        # preserves the ordering. This means that if the tasks have
        # uneven execution times, getting the results from quicker tasks
        # might be blocked behind slower tasks, as demonstrated by the
        # code below, which prints the first three results
        # simultaneously, once the first (longest-running) task is done:
        #
        # 	Worker 3264662, job 5 done at ...62.
        # 	Worker 3264659, job 0 done at ...62.
        # 	Worker 3264659, job 4 done at ...62.
        # 	Worker 3264659, job 3 done at ...64.
        # 	Worker 3264662, job 2 done at ...64.
        # 	Worker 3264662, job 1 done at ...65.
        #
        # Beware of the 'chunksize' argument, it can be tricky. On the
        # surface, it seems simple: if I have 2 workers, 6 tasks and a
        # chunk size of 3, then the first 3 tasks will (probably) be
        # submitted to the first worker and the rest to the second,
        # right?
        #
        # Right, but since the purpose of 'chunksize' is to limit IPC
        # overhead, this also means that you *won't get any results
        # until an entire chunk is done running*. That's not a bug, it's
        # the whole raison d'etre for 'chunksize': instead of sending
        # tiny amounts of data between processes all the time, you send
        # larger amounts of data (= chunks) much less often. However,
        # that also means that if the chunks take a long time to
        # compute, you'll be stuck for a long time without any results
        # to pass on to downstream tasks or log (to make sure things are
        # running as expected), even though some tasks have already
        # completed in the child processes.
        #
        # So be careful with 'chunksize'. Rule of thumb: with
        # longer-running tasks and/or more data per task to send via
        # IPC, the default 'chunksize=1' is probably preferable,
        # especially if you want to log progress as results are
        # computed. With shorter-running tasks, especially with a small
        # amount of IPC data per task, it definitely makes sense to
        # chunk them to increase performance. When in doubt, benchmark.
        #
        # If you want all the gory details on why 'chunksize' is tricky
        # and parallel scheduling is hard, there's an excellent answer
        # over on SO: https://stackoverflow.com/a/54032744/1826241
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
        # 	Worker 3264662, job 0 done at ...65.
        # 	Worker 3264662, job 4 done at ...69.
        # 	Worker 3264659, job 5 done at ...70.
        # 	Worker 3264659, job 2 done at ...72.
        # 	Worker 3264662, job 3 done at ...72.
        # 	Worker 3264659, job 1 done at ...73.
        for fut in cf.as_completed(ex.submit(worker, t) for t in tasks):
            # Accessing 'fut.result()' might raise an exception if
            # something went wrong while computing the future.
            tid, x = fut.result()
            log(tid, x)
        print()

        # If you'd like to share state between processes... Well, you
        # can't, that's what process separation is for. You can pass
        # around data via queues and the like, which is how you get back
        # results from workers into your main process, but that data
        # must be pickled first, which involves overhead and
        # restrictions (you can't pickle everything).
        #
        # If you just naively create a mutable global and try to update
        # it from multiple workers, it won't work as intended -- each
        # worker gets its own separate copy of the global. That's
        # because a worker process receives a copy of everything in the
        # parent process when it's created.
        #
        # Note that there's a subtle gotcha which might make it look
        # like your workers are actually sharing the same global: if you
        # inspect the memory address of the global via 'id()', the
        # number might be the same across multiple workers, even though
        # they're actually different objects and different memory
        # locations under the hood.
        #
        # This only happens when the method of starting child processes
        # involves forking, for two reasons:
        #
        # 1. Forking creates a perfect clone of the parent process,
        #    which continues execution from the same point. That means
        #    that all the resources are laid out the same way as in the
        #    parent process.
        # 2. 'id()' doesn't show you actual physical memory locations,
        #    it shows you addresses which the OS presents to your
        #    process, which is affected by virtual addressing. And since
        #    a parent process and its fork are clones of each other, the
        #    virtual address memory layout is the same and all objects
        #    live at the same (virtual) addresses in both processes, all
        #    the while being physically separate.
        #
        # This potentially confusing state of things is demonstrated in
        # the output below: the addresses of the 'FOO' global are
        # identical, in both workers, but the contents are clearly
        # separate.
        #
        # 	Worker 31397 has FOO at address 4376767392 with contents {5}
        # 	Worker 31398 has FOO at address 4376767392 with contents {0}
        # 	Worker 31397 has FOO at address 4376767392 with contents {4, 5}
        # 	Worker 31398 has FOO at address 4376767392 with contents {0, 3}
        # 	Worker 31397 has FOO at address 4376767392 with contents {2, 4, 5}
        # 	Worker 31398 has FOO at address 4376767392 with contents {0, 1, 3}
        #
        # But if you change the multiprocessing context to 'spawn'
        # above, then instead of forking, workers will be spawned as new
        # processes (which is slower) and you should get different
        # addresses in the output:
        #
        # 	Worker 32171 has FOO at address 4349491904 with contents {5}
        # 	Worker 32172 has FOO at address 4388403904 with contents {0}
        # 	Worker 32171 has FOO at address 4349491904 with contents {4, 5}
        # 	Worker 32172 has FOO at address 4388403904 with contents {0, 3}
        # 	Worker 32171 has FOO at address 4349491904 with contents {2, 4, 5}
        # 	Worker 32172 has FOO at address 4388403904 with contents {0, 1, 3}
        #
        # For more on the gotchas of virtual addressing as related to
        # Python multiprocessing, see: https://kaushikghose.wordpress.com/2016/08/26/python-global-state-multiprocessing-and-other-ways-to-hang-yourself/
        for fut in cf.as_completed(
            ex.submit(worker_accessing_global_mutable_resource, t) for t in tasks
        ):
            pid, foo_id, foo = fut.result()
            print(f"Worker {pid} has FOO at address {foo_id} with contents {foo}")

        # Finally, if you'd like to process potentially unbounded
        # streams of data in real time, it looks like you'll have to use
        # queues and manage them manually, which can be a bit finicky:
        # https://stackoverflow.com/questions/41648103/how-would-i-go-about-using-concurrent-futures-and-queues-for-a-real-time-scenari
        #
        # See also https://bugs.python.org/issue29842.
        #
        # This is one case where you might want to consider using
        # 'multiprocessing' instead, which provides this ability out of
        # the box in its high-level API.


if __name__ == "__main__":
    main()
