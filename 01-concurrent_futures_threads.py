import time
import threading
import subprocess as sp
import concurrent.futures as cf
from copy import deepcopy


def worker(x):
    # A thread pool allows you to run either IO-bound Python bytecode
    # concurrently (which means the IO actions themselves will happen in
    # parallel), or CPU-bound external subprocesses in parallel.
    sp.run(["sleep", f"{x}s"])
    return threading.get_ident(), x


def log(tid, x):
    ts = str(int(time.time()))[-2:]
    tid = str(tid)[-2:]
    print(f"Worker {tid}, job {x} done at ...{ts}.")


# NOT A GOOD IDEA (data race); locks?
STATE = set()


def worker_accessing_global_mutable_state(x):
    STATE.add(x)
    time.sleep(1)
    return threading.get_ident(), id(STATE), STATE


TLOCAL = threading.local()
TLOCAL.STATE = set()


def worker_accessing_threadlocal_state(x):
    # When the worker first runs, LOCAL will provide access to a fresh
    # empty thread local, i.e. we can't access the set stored under the
    # STATE attribute in the main thread above (that's the entire
    # purpose of thread locals).
    TLOCAL.__dict__.setdefault("STATE", set()).add(x)
    time.sleep(1)
    return threading.get_ident(), id(TLOCAL.STATE), TLOCAL.STATE


def worker_accessing_threadlocal_state_returning_copy(x):
    TLOCAL.__dict__.setdefault("STATE", set()).add(x)
    time.sleep(1)
    return threading.get_ident(), id(TLOCAL.STATE), deepcopy(TLOCAL.STATE)


def log_state(tid, state_id, state, parent_state):
    tid = str(tid)[-2:]
    print(
        f"Worker {tid}, state contains {state}, "
        f"worker state and parent state have same address: {id(parent_state) == state_id}, "
        f"worker state is parent state: {parent_state is state}"
    )


def main():
    # Will be used below to reset thread-local state, so that each
    # example starts with empty thread local state.
    global TLOCAL

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
        # 	Worker 08, job 5 done at 90.
        # 	Worker 04, job 0 done at 90.
        # 	Worker 04, job 4 done at 90.
        # 	Worker 04, job 3 done at 92.
        # 	Worker 08, job 2 done at 92.
        # 	Worker 08, job 1 done at 93.
        #
        # Remember also that the 'chunksize' argument is ignored with
        # threads -- its purpose is to alleviate IPC cost, but there's
        # no IPC with threads, so it clearly makes no sense to chunk
        # tasks when using 'ThreadPoolExecutor'.
        #
        # Exceptions which occurred while performing a task are raised
        # at the point that its result should be yielded.
        # for tid, x in ex.map(worker, tasks):
        #     log(tid, x)
        # print()

        # Receiving results out of order, as soon as they're completed,
        # is slightly more complicated but possible. Note that
        # 'as_completed()' has the advantage of being able to juggle
        # futures driven to completion by different executors at the
        # same time. The output of the code below is the following:
        #
        # 	Worker 00, job 0 done at 88.
        # 	Worker 00, job 4 done at 92.
        # 	Worker 04, job 5 done at 93.
        # 	Worker 04, job 2 done at 95.
        # 	Worker 00, job 3 done at 95.
        # 	Worker 04, job 1 done at 96.
        # for fut in cf.as_completed(ex.submit(worker, t) for t in tasks):
        #     # Accessing 'fut.result()' might raise an exception if
        #     # something went wrong while computing the future.
        #     tid, x = fut.result()
        #     log(tid, x)
        # print()

        # When a thread-based worker accesses a global variable, it
        # accesses shared global state, so careful when mutating it.
        # Technically, there's a data race -- e.g. one thread could be
        # writing and another one reading at the same time, and it would
        # read partially written (i.e. invalid) data. In practice, this
        # won't happen in CPython thanks to the GIL.
        #
        # As you can see in the results below though, there's still a
        # race condition. The workers add the numbers to the STATE set
        # one by one, so how come that the size of the set printed by
        # log_state doesn't also increase on number at a time? It's
        # because the logger in the main thread and the worker threads
        # all race together, and by the time the first log line gets
        # printed, the worker threads have already managed to add three
        # numbers to STATE.
        #
        # To put it another way: when a worker is done with a task,
        # nothing says that it has to wait for the logger in the main
        # thread to print the result before getting started with another
        # task. In theory, the Python thread scheduler could first run
        # all worker tasks, and only then resume the main thread, so all
        # six logger calls would log a set with six elements. Although a
        # scheduler that doesn't allow threads to take turns more or
        # less equally is of course a bad scheduler.
        #
        # 	Worker 08, state contains {0, 4, 5}, worker state and parent state have same address: True, worker state is parent state: True
        # 	Worker 12, state contains {0, 3, 4, 5}, worker state and parent state have same address: True, worker state is parent state: True
        # 	Worker 12, state contains {0, 1, 2, 3, 4, 5}, worker state and parent state have same address: True, worker state is parent state: True
        # 	Worker 08, state contains {0, 1, 2, 3, 4, 5}, worker state and parent state have same address: True, worker state is parent state: True
        # 	Worker 12, state contains {0, 1, 2, 3, 4, 5}, worker state and parent state have same address: True, worker state is parent state: True
        # 	Worker 08, state contains {0, 1, 2, 3, 4, 5}, worker state and parent state have same address: True, worker state is parent state: True
        for fut in cf.as_completed(
            ex.submit(worker_accessing_global_mutable_state, t) for t in tasks
        ):
            tid, state_id, state = fut.result()
            log_state(tid, state_id, state, STATE)
        print()

        # With thread locals, each worker gets its own state, but each
        # still runs its own separate race with the logger in the main
        # thread. So it's entirely possible that a worker will manage to
        # add two numbers to its state before the logger manages to log
        # it for the first time, as seen below.
        #
        # 	Worker 12, state contains {4, 5}, worker state and parent state have same address: False, worker state is parent state: False
        # 	Worker 08, state contains {0, 3}, worker state and parent state have same address: False, worker state is parent state: False
        # 	Worker 12, state contains {2, 4, 5}, worker state and parent state have same address: False, worker state is parent state: False
        # 	Worker 08, state contains {0, 1, 3}, worker state and parent state have same address: False, worker state is parent state: False
        # 	Worker 12, state contains {2, 4, 5}, worker state and parent state have same address: False, worker state is parent state: False
        # 	Worker 08, state contains {0, 1, 3}, worker state and parent state have same address: False, worker state is parent state: False
        for fut in cf.as_completed(
            ex.submit(worker_accessing_threadlocal_state, t) for t in tasks
        ):
            tid, state_id, state = fut.result()
            log_state(tid, state_id, state, TLOCAL.STATE)
        print()

        # Reset thread local state (otherwise we'd continue using the
        # sets from the previous for-loop).
        TLOCAL = threading.local()
        TLOCAL.STATE = set()

        # If you want the logger to log the state as it was when the
        # worker was about to return, then your first intuition might be
        # locks. However, remember Raymond Hettinger's advice:
        #
        # 1. (Unlike in Rust), locks (in Python) don't actually lock
        #    anything. It's entirely up to the programmer to remember to
        #    use them when accessing a resource which is associated with
        #    a lock.
        # 2. They're tricky to get right. You're likely to end up with a
        #    deadlock instead, or possibly with a program which
        #    actually executes sequentially because the locks are so
        #    coarse-grained that they prevent any parallelism
        #
        # So create a snapshot with deepcopy instead.
        for fut in cf.as_completed(
            ex.submit(worker_accessing_threadlocal_state_returning_copy, t)
            for t in tasks
        ):
            tid, state_id, state = fut.result()
            log_state(tid, state_id, state, TLOCAL.STATE)

        # But there's still a race condition. Maybe it's fine for your
        # use case, but if not, you need to add a lock. Remember though
        # that unlike in Rust, locks don't really lock anything in
        # Python -- it's up to the programmer to remember to use them
        # whenever accessing a resource, access to which should be
        # coordinated by a lock.

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
