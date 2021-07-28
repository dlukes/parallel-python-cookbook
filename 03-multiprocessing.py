import os
import time
import random
import multiprocessing as mp

# Cf. the 'ProcessPoolExecutor' snippet for details, this just shows how
# to achieve equivalent behavior by directly using the 'multiprocessing'
# module. 'concurrent.futures' is newer and it makes it easier to switch
# between thread- and process-based parallelism, so it's probably better
# to use that by default, but it's useful to have a reminder of how the
# features of the two modules map onto each other, because it can be a
# bit tricky.
#
# Also, some use cases are simpler with 'multiprocessing', e.g. handling
# unbounded / real-time streams as they come in is easily done with
# '.imap()' or '.imap_unordered()' (see below) because they're lazy,
# whereas with 'concurrent.futures', you'd have to resort to a more
# complicated setup involving manual management of queues.


def worker(x):
    time.sleep(x)
    return os.getpid(), x


def log(pid, x):
    ts = str(int(time.time()))[-2:]
    pid = str(pid)[-2:]
    print(f"Worker {pid}, job {x} done at {ts}.")


# Since the underlying mechanism is the same, the handling of (mutable)
# globals is the same as with process-based parallelism using
# 'concurrent.futures' -- each child process gets its own copy. See
# there for more detailed comments.
FOO = set()


def worker_accessing_global_mutable_resource(x):
    FOO.add(x)
    time.sleep(1)
    return os.getpid(), id(FOO), FOO


def main():
    ctx = mp.get_context("forkserver")
    with ctx.Pool(processes=2) as pool:
        tasks = [5, 0, 4, 3, 2, 1]

        # Beware! Unlike in 'concurrent.futures', the '.map()' method
        # waits for all of the results to be done *and only then* yields
        # them, all at the same time. Sort of like the builtin 'map()'
        # in Python 2.x.
        #
        # 	Worker 46, job 5 done at 65.
        # 	Worker 47, job 0 done at 65.
        # 	Worker 47, job 4 done at 65.
        # 	Worker 47, job 3 done at 65.
        # 	Worker 46, job 2 done at 65.
        # 	Worker 46, job 1 done at 65.
        for pid, x in pool.map(worker, tasks):
            log(pid, x)
        print()

        # The counterpart of '.map()' from 'concurrent.futures' is
        # '.imap()', which yields results as they're available but
        # preserves ordering, which means results from quicker tasks can
        # be blocked behind slower ones.
        #
        # 	Worker 47, job 5 done at 70.
        # 	Worker 46, job 0 done at 70.
        # 	Worker 46, job 4 done at 70.
        # 	Worker 46, job 3 done at 72.
        # 	Worker 47, job 2 done at 72.
        # 	Worker 47, job 1 done at 73.
        for pid, x in pool.imap(worker, tasks):
            log(pid, x)
        print()

        # To get results immediately in the order that they become
        # available, use '.imap_unordered()'. In 'concurrent.futures',
        # this is not expressed with a '.map()' method variant, but with
        # the 'as_completed()' function.
        #
        # 	Worker 47, job 0 done at 73.
        # 	Worker 47, job 4 done at 77.
        # 	Worker 46, job 5 done at 78.
        # 	Worker 46, job 2 done at 80.
        # 	Worker 47, job 3 done at 80.
        # 	Worker 46, job 1 done at 81.
        for pid, x in pool.imap_unordered(worker, tasks):
            log(pid, x)
        print()

        # As with 'ProcessPoolExecutor' in 'concurrent.futures', note
        # that "getting results immediately" might exhibit unintuitive
        # behavior in practice: both '.imap()' and '.imap_unordered()'
        # accept 'chunksize' arguments, which means all of the caveats
        # about this tricky parameter apply here as well. In particular,
        # it means that a task's result is yielded not as soon as the
        # task is done, but only once the entire chunk that the task is
        # part of is done.

        # Each worker ends up getting its own private copy of the global
        # FOO defined in the parent process.
        #
        # 	Worker 47 has FOO at address 4492265952 with contents {0}
        # 	Worker 46 has FOO at address 4492265952 with contents {5}
        # 	Worker 47 has FOO at address 4492265952 with contents {0, 4}
        # 	Worker 46 has FOO at address 4492265952 with contents {3, 5}
        # 	Worker 47 has FOO at address 4492265952 with contents {0, 2, 4}
        # 	Worker 46 has FOO at address 4492265952 with contents {1, 3, 5}
        for pid, foo_id, foo in pool.imap_unordered(
            worker_accessing_global_mutable_resource, tasks
        ):
            print(f"Worker {pid} has FOO at address {foo_id} with contents {foo}")
            print(FOO is foo)
        print()

        # And finally, the one thing that requires a much more involved
        # setup with 'concurrent.futures', as mentioned above:
        # processing (potentially) unbounded streams in real time:
        def infinite():
            while True:
                yield random.randint(0, 3)

        for pid, x in pool.imap_unordered(worker, infinite()):
            log(pid, x)


if __name__ == "__main__":
    main()
