# NOTE: This is all very complicated and very probably wrong in subtle
# or not-so-subtle ways. The machinery to submit new tasks and retrieve
# results is absurdly brittle and hard to navigate. I'll try and revisit
# this if I ever have a breakthrough on how to best achieve this with
# 'concurrent.futures', but maybe it's just not the right tool for
# unbounded streams of data.
#
# For the time being, if you need threaded processing of unbounded
# streams, you're probably better off using the vanilla 'threading'
# module and daemon threads, which make it unnecessary to worry about a
# thread pool, because you never need to assign new tasks to the
# threads. See 07-vanilla_threading_unbounded.py.

import time
import random
import threading
import subprocess as sp
import concurrent.futures as cf
from queue import Queue

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
QUEUE = Queue(10)

# Note that the producer should run in a separate thread, *not* a
# separate process, because that would create a copy of the queue
# instead of sharing it and the main process wouldn't be able to read
# from it. So if you're running your consumers in a
# 'ProcessPoolExecutor', you still need to have a 'ThreadPoolExecutor'
# to run the producer in.
def producer():
    while True:
        QUEUE.put(random.randint(0, 3))


def consumer(x):
    sp.run(["sleep", f"{x}s"])
    return threading.get_ident(), x


def log(tid, job_id, x):
    ts = str(int(time.time()))[-2:]
    print(f"Worker {tid}, job {job_id} (sleep {x}s) done at ...{ts}.")


def main():
    # Remember to set aside a worker for your producer! So if you want
    # 2 consumer workers, you need 3 workers total.
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        future2job_id = {}
        future2job_id[ex.submit(producer)] = None

        job_id = 0
        while future2job_id:
            done, _ = cf.wait(
                future2job_id, timeout=0.25, return_when=cf.FIRST_COMPLETED
            )

            # This condition is probably too simplistic -- in theory,
            # this while-loop could keep switching back and forth with
            # the producer thread, so the queue would never be empty and
            # tasks would just keep being submitted to the thread pool
            # and never being retrieved.
            while not QUEUE.empty():
                t = QUEUE.get()
                future2job_id[ex.submit(consumer, t)] = job_id
                job_id += 1

            for fut in done:
                jid = future2job_id[fut]
                del future2job_id[fut]
                if jid is not None:
                    tid, x = fut.result()
                    log(tid, jid, x)


if __name__ == "__main__":
    main()
