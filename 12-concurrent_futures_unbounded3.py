# This is finally an example of processing an unbounded stream of tasks
# with 'concurrent.futures' that works and is robust. But in order to do
# that, it still needs 'threading' and 'queue', and doesn't use any
# fancy bells and whistles provided by the futures abstraction. The
# 'ThreadPoolExecutor' handles just the parallel consumer stage, we
# submit tasks to it and collect theSo why might we want to do this,
# what can it buy us?
#
# An advantage, at least in theory, of using the 'ThreadPoolExecutor' in
# addition to most of the machinery we already had in place for doing
# this with just vanilla threads, is that threads in a thread pool spin
# up on demand by default, whereas our vanilla solution starts them up
# front (and doing on demand would require additional non-trivial manual
# work). So if you have a high maximum number of workers, but you happen
# to only ever have a low number of tasks to handle at the same time,
# you won't pay the cost of the threads you don't actually need.
#
# In practice though, threads are relatively cheap, so you might not
# notice the difference unless your use case warrants allowing an
# extremely high max number of threads (thousands?) while also sometimes
# exhibiting runs which end up needing very few, in which case dynamic
# allocation of threads might spare a tangible amount of resources.
#
# Another potential advantage would be if 'ThreadPoolExecutor' provided
# something akin to the 'maxtasksperchild' option of
# 'multiprocessing.Pool', which can help limit the risk of resource
# leaks in extremely long-lived programs. Each worker thread would only
# process a limited amount of tasks before being replaced by a fresh new
# worker. However, there's no such option.

import time
import random
import threading
import subprocess as sp
import concurrent.futures as cf
from queue import Queue

CONSUMER_WORKERS = 2
INPUTQ = Queue(10)
OUTPUTQ = Queue(10)
EXIT = threading.Event()

# As we've seen previously, we can't just create the thread pool and
# submit tasks on the main thread, we might actually lose input values
# that we've accepted for processing. Imagine we're reading lines from
# standard input: the keyboard interrupt might come after the next line
# is read in, but before it's put on the 'INPUTQ' for processing.
#
# By contrast, in 08-threading_unbounded_graceful_exit.py, the keyboard
# interrupt just sets the 'EXIT' event, which can't interrupt the
# producer loop whenever it likes (unlike an exception): it lets the
# full cycle complete, and only then is the loop condition re-checked
# and the loop exits.


def threadpool():
    with cf.ThreadPoolExecutor(max_workers=CONSUMER_WORKERS) as ex:
        job_id = 0
        while not EXIT.is_set():
            # For each input value we put on the 'INPUTQ' (see below),
            # we also submit a consumer task to the thread pool to
            # handle it (it's not like a given value will necessarily be
            # handled by the task which was submitted in the same loop
            # iteration, it depends on the order in which the threads
            # ultimately get to run, but the number of input values put
            # on the queue must match the number of submitted tasks).
            #
            # We ignore the returned future, it's useless in this
            # context. No idea what the overhead for these future
            # objects is, maybe it's negligible, but in any case,
            # there's no way around paying it.
            ex.submit(consumer)
            # Sending the input data to the consumer task via the
            # 'INPUTQ' instead of just passing it along as arguments
            # when submitting the task seems gratuitous. But it's not:
            # without our own bounded 'INPUTQ', no backpressure would be
            # applied, because the thread pool's work queue is unbounded.
            #
            # An unbounded queue makes sense in a setup where submitting
            # tasks and retrieving results happens sequentially on the
            # same thread, which is what we've previously established
            # the whole futures abstraction is meant for. If the queue
            # had a maximum size of 10, then trying to submit 11 or more
            # tasks to it would block, which means you'd never get to
            # the next stage where you can start retrieving results (and
            # thus freeing up space on the work queue for additional
            # tasks to be submitted).
            INPUTQ.put((job_id, random.randint(0, 3)))
            job_id += 1


# Without the indirection of the 'EXIT' event, we could also very easily
# interrupt the program after a new consumer task has been submitted to
# the thread pool, but before its corresponding input value is put on
# the 'INPUTQ'. In that case, that last consumer will get stuck waiting
# on 'INPUTQ.get()' and the program will hang, as there's no safe way to
# shut down a thread pool which still has futures running (for the same
# reason that you shouldn't just kill a thread from the outside -- it
# might be holding a lock and killing it would mean the lock will never
# be released, leading to deadlock).


def consumer():
    job_id, x = INPUTQ.get()
    sp.run(["sleep", f"{x}s"])
    OUTPUTQ.put((threading.get_ident(), job_id, x))
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
    # The logger must be on a thread outside the pool. If it's inside,
    # it might get kicked off its thread at some point and everything
    # will get stuck, because all available threads will be occupied by
    # consumer tasks, which can't make progress without the logger
    # taking stuff out of the 'OUTPUTQ' every now and then.
    threading.Thread(target=logger, daemon=True).start()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print("caught keyboard interrupt")

    print("signaling threadpool to stop adding new tasks")
    EXIT.set()

    print("waiting for outstanding tasks to complete")
    tp.join()

    # No need to wait on the 'INPUTQ' here: the thread pool won't close
    # down until all consumer tasks have completed, which itself
    # requires 'INPUTQ' being empty, so this is already taken care of by
    # 'tp.join()' above. We still have to wait on 'OUTPUTQ' though,
    # because those values are getting consumed by the logger, which is
    # a daemon thread that might get killed as the program is exiting,
    # leaving some of the items in the queue unhandled.
    print("waiting for OUTPUTQ to empty")
    OUTPUTQ.join()

    print("exiting gracefully")


if __name__ == "__main__":
    main()
