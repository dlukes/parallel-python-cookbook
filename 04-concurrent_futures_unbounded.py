# NOTE: Starting from here, many of the examples are variations on a
# theme -- how to process (potentially) unbounded streams of data when
# you can't or don't want to use 'multiprocessing' and the convenient
# abstraction that is 'Pool.imap_unordered()'. The general structure
# that all of these examples demonstrate consists of the following
# stages:
#
#                      .--> consumer 1 -->.
#                     /                    \
#                    /----> consumer 2 -->--\
#                   /                        \
# --> producer -->--------> consumer 3 -->--------> logger
#                   \                        /
#                    \----> {  ....  } -->--/
#                     \                    /
#                      `--> consumer X -->´
#
# 1. The *producer* stage is sequential. It's the single point where
#    values to process enter the program. In the examples, it's just a
#    random number generator, but realistically, it could read data from
#    stdin, from a socket, get it over the network etc.
# 2. The *consumer* stage is where the values get processed. It's the
#    only stage we aim to run concurrently, because we assume the tasks
#    are independent and implemented in such a way that they can at
#    least partially run in parallel (e.g. because they involve waiting
#    for I/O or CPU-intensive computations in C extensions, both of
#    which release the GIL).
# 3. The *logger* stage is again sequential. It's a simple stand-in for
#    any kind of stage where the individual results need to interact
#    with a mutable global resource (in this case, the non-threadsafe
#    print buffer, but it could also be a global counter, some kind of
#    aggregation step etc.) and/or where a stricter ordering needs to be
#    enforced between operations once more.
#
# In practice, there are probably many cases where you might get away
# with getting rid of a stage like stage 3. But it can be useful, so
# we'll keep it in the interest of focusing on the most generally
# applicable solutions. It can also be tricky to implement correctly, so
# it gives occasion to discuss some interesting pitfalls.
#
# ----------------------------------------------------------------------

# WARNING: The approach presented in this particular example, using the
# 'concurrent.futures' module, is stupid. It's the makes-you-want-to-
# shoot-your-brains-out way to process an unbounded stream of tasks
# using threads. It's extremely brittle and complicated and wrong in
# both subtle and less subtle ways.
#
# I'm keeping it around for historical reasons, and also to illustrate
# that in general, futures are the wrong abstraction for this. Futures
# simplify things when you have two separate stages -- task submission
# and collection of results -- that you want to perform sequentially on
# the same thread. But in this case, there's an unbounded stream of
# tasks, which means that in order to keep both of these operations on
# the same thread, you need to alternate between them, which is hard to
# get right and wrap your head around. It's easy to get it wrong and end
# up looping forever in one of these stages, never getting any work
# done. It's easy to mess up backpressure. Etc.
#
# Probably the simplest way to do it right is to split the tasks into
# batches and make sure you alternate between submitting batches and
# getting the results out of them, but that's less than optimal because
# most of your workers end up being idle as the batch is finishing up --
# they have to wait for the next batch to get more work. But see
# <https://bugs.python.org/issue34168#msg322075> for an example.
#
# Batching also applies some backpressure, which you need to do,
# otherwise you might end up running out of memory if you keep accepting
# tasks faster than you can handle them. A more sophisticated way to
# apply backpressure (continuous, as opposed to chunked) is to use a
# bounded queue, which is what this example attempted to do, but at that
# point, you might as well ditch 'concurrent.futures' entirely, because
# it was intended as a higher-level abstraction to avoid having to
# manually set up threads with the vanilla 'threading' module and
# connect them with queues from the 'queue' module.
#
# But as we'll see eventually, there is *no reasonable way* to process
# unbounded streams of data using 'concurrent.futures' without reaching
# for 'threading' and 'queue' as well, while the reverse is completely
# possible. So the latter is always simpler, while in the former, you
# keep tripping up over futures, which are a useless abstraction in this
# context. In some cases, there may be reasons for doing it anyway, see
# 12-concurrent_futures_unbounded3.py, but it probably shouldn't be the
# first thing you think of.
#
# (Note that this assessment might change in the future, pending some
# changes to 'Executor.map' which would make it lazier. For more
# details, see <https://bugs.python.org/issue29842>.)
#
# Examples 06--10 discuss various ways of processing unbounded streams
# with just the 'threading' and 'queue' modules, and issues around this.
# Let's not pretend there's no learning curve or gotchas, but sane and
# correct code can in principle be written for this purpose using these
# tools.
#
# 11-concurrent_futures_unbounded2.py shows an abortive attempt at a
# simpler and more obviously correct approach using 'concurrent.futures'
# while avoiding messing with manually creating your own threads and
# queues. It's a cautionary tale of good intentions gone south, and
# ultimately fails miserably.
#
# Finally, 12-concurrent_futures_unbounded3.py shows the complicated,
# Frankensteinian setup using all three of 'concurrent.futures',
# 'threading' and 'queue', explains why you need the latter two for
# everything to be correct and relatively sane, and discusses why and
# when you might want to do this -- as in, why you might want to pull
# 'concurrent.futures' into the mix, even though futures are useless in
# this scenario and you won't be using any futures-related
# functionality. (Hint: the executor might still come in handy.)

raise RuntimeError("Don't do this. Seriously.")

import time
import random
import threading
import subprocess as sp
import concurrent.futures as cf
from queue import Queue

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
        # WARNING: Having the producer on the same thread pool as the
        # consumers is a really bad idea™: if it gets kicked off its
        # thread, then the pool will deadlock with all threads occupied
        # by consumers waiting on someone to provide them with input
        # values.
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
