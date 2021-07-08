import time
import random
import threading
import subprocess as sp
import concurrent.futures as cf
from queue import Queue

# TODO: an advantage in theory of using the 'ThreadPool' in addition to
# most of the machinery we already had in place for doing this with just
# vanilla threads: threads spin up on demand by default, whereas our
# vanilla solution starts them up front (and doing on demand would
# require additional non-trivial manual work). So if you have a high
# maximum number of workers, but you happen to only ever have a low
# number of tasks to handle at the same time, you won't pay the cost of
# the threads you don't actually need. In practice though, threads are
# relatively cheap, so you might not notice the difference unless your
# use case warrants allowing an extremely high max number of threads
# (thousands?) while also sometimes exhibiting runs which end up needing
# very few, in which case dynamic allocation of threads might spare a
# tangible amount of resources.

# One possible way of handling unbounded streams of data in real time
# with 'ThreadPoolExecutor' and 'Queue'. You might also want to try and
# split the work into chunks on the main thread as suggested here:
# https://bugs.python.org/issue34168#msg322075
#
# That's probably easier to read and less tricky to get right, but the
# performance might be worse if you end up in a situation where some of
# the tasks in the chunk take a long time to complete while the rest of
# your workers sit idle, because no more work can come in until the
# chunk is done.

# The size of the Queue can be tricky to get right -- you need to apply
# some backpressure, but if your system is distributed, too much of it
# might mean that requests to this component start timing out.
CONSUMER_WORKERS = 2
INPUTQ = Queue(10)
OUTPUTQ = Queue(10)
EXIT = threading.Event()

# Can't create the thread pool and submit tasks on the main thread, we
# might actually lose input values that we've accepted for processing.
# Imagine we're leading lines from standard input: the keyboard
# interrupt might come after the next line is read in, but before it's
# put on the 'INPUTQ' for processing.
#
# By contrast in 08, the keyboard interrupt just sets the 'EXIT' event,
# which can't interrupt the producer loop whenever it likes (unlike an
# exception): it lets the full cycle complete, and only then is the loop
# condition re-checked and the loop exits.
#
# Also, we could very easily interrupt the program after a new consumer
# task has been submitted to the thread pool, but before its
# corresponding input value is put on the 'INPUTQ'. In that case, that
# last consumer will get stuck waiting on 'INPUTQ.get()' and the program
# will hang, as there's no safe way to shut down a thread pool which
# still has futures running (for the same reason that you shouldn't just
# kill a thread from the outside -- it might be holding a lock and
# killing it would mean the lock will never be released, leading to
# deadlock).


def threadpool():
    with cf.ThreadPoolExecutor(max_workers=CONSUMER_WORKERS) as ex:
        job_id = 0
        while not EXIT.is_set():
            # for each input value we put on the queue, we also submit a
            # consumer task to the thread pool to handle it (it's not
            # like this particular value will necessarily be handled by
            # this particular task, it depends on the order in which the
            # threads get to run, but the number of input values put on
            # the queue must match the number of submitted tasks). we
            # ignore the returned future, meaning it's a useless
            # abstraction that we need to pay the price for
            ex.submit(consumer)
            # sending the input data to the consumer task via the
            # 'INPUTQ' instead of just passing it along as arguments
            # when submitting the task seems gratuitous. But it's not:
            # without our own bounded 'INPUTQ', no backpressure would be
            # applied, because thread pool's work queue is unbounded,
            # see https://github.com/python/cpython/blob/main/Lib/concurrent/futures/thread.py
            # (search for 'self._work_queue'). again, an unbounded queue
            # makes sense in the setup that submitting tasks and
            # retrieving results happens sequentially on the same
            # thread: if the queue had a maximum size of 10, then trying
            # to submit 11 or more tasks to it would block, which means
            # you'd never get to the next stage where you can start
            # retrieving results (and thus freeing up space on the work
            # queue for additional tasks to be submitted).
            INPUTQ.put((job_id, random.randint(0, 3)))
            job_id += 1


def consumer():
    job_id, x = INPUTQ.get()
    sp.run(["sleep", f"{x}s"])
    OUTPUTQ.put((threading.get_ident(), job_id, x))
    # TODO: tricky
    INPUTQ.task_done()


def logger():
    while True:
        thread_id, job_id, x = OUTPUTQ.get()
        ts = str(int(time.time()))[-2:]
        print(f"Worker {thread_id}, job {job_id} (sleep {x}s) done at ...{ts}.")
        OUTPUTQ.task_done()


def main():
    tp = threading.Thread(target=threadpool)
    tp.start()
    # logger must be on a separate thread; if it's in the thread pool,
    # it might get kicked off its thread and everything will get stuck,
    # because all available threads will be occupied by consumer tasks,
    # which can't make progress without logger emptying out the OUTPUTQ
    # at the same time
    threading.Thread(target=logger, daemon=True).start()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("caught keyboard interrupt")

    print("signaling threadpool to stop adding new tasks")
    EXIT.set()

    print("waiting for outstanding tasks to complete")
    tp.join()

    # No need to wait on INPUTQ here: the threadpool won't close down
    # until all consumer tasks have completed, i.e. until 'INPUTQ' is
    # empty, so this is already taken care of by 'tp.join()' above. We
    # still have to wait on 'OUTPUTQ' though, because those values are
    # consumed by a daemon thread that might get killed as the program
    # is exiting, leaving some of the items in the queue unhandled.
    print("waiting for OUTPUTQ to empty")
    OUTPUTQ.join()

    print("exiting gracefully")


if __name__ == "__main__":
    main()
