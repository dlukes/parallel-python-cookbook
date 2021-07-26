# WARNING: Like 04-concurrent_futures_unbounded.py, this is a second
# attempt at processing an unbounded stream of tasks using just
# 'concurrent.futures', without manually creating your own threads and
# queues, leveraging callbacks this time around. It doesn't work either.
# I'm only keeping it to document the pitfalls.

raise RuntimeError("Don't do this. Seriously.")

import time
import random
import threading
import subprocess as sp
import concurrent.futures as cf


def consumer(job_id, x):
    sp.run(["sleep", f"{x}s"])
    return threading.get_ident(), job_id, x


def log(fut):
    thread_id, job_id, x = fut.result()
    ts = str(int(time.time()))[-2:]
    print(f"Worker {thread_id}, job {job_id} (sleep {x}s) done at ...{ts}.")


def main():
    print(
        """\
Press Ctrl-C after a while, the thread pool will first complete any
outstanding futures and only then exit. However, depending on where the
interrupt falls, a task might be accepted but not processed, or
processed but not logged (see comments below).
------------------------------------------------------------------------"""
    )
    with cf.ThreadPoolExecutor(max_workers=2) as ex:
        job_id = 0

        # Conceptually, we want to do something like this, using
        # callbacks to chain subsequent operations, instead of
        # communicating via queues:
        # while True:
        #     seconds = random.randint(0, 3)
        #     fut = ex.submit(consumer, job_id, seconds)
        #     fut.add_done_callback(log)
        #     job_id += 1

        # However, we can't just do that. There would be no backpressure
        # and we could end up eating all available memory, accepting
        # tasks faster than we can handle them. Also, a loop this busy
        # might make it hard for the consumer threads to get scheduled
        # regularly.
        #
        # So, let's apply some backpressure. Ideally, you'd want to set
        # a bound on the executor's internal task queue, so that
        # 'ex.submit(...)' would just block if the queue is full. But
        # there's no way to do that, the task queue is always unbounded,
        # end of story.
        #
        # So the next best thing is a clunky workaround: reach into the
        # executor's internals, check the queue size, and if it's more
        # than a given threshold, sleep for a bit instead of submitting
        # a new task. This is of course ugly and suboptimal -- if the
        # tasks are processed faster than your chosen sleep time, you
        # will end up idling; if they're slower, the loop will still be
        # a busy one, just less so.
        while True:
            if ex._work_queue.qsize() <= 10:
                seconds = random.randint(0, 3)
                # There's another problem though: there's no way to do a
                # graceful shutdown in this approach. E.g. if we receive
                # the keyboard interrupt here, then a job has been
                # accepted but won't be submitted for processing...
                fut = ex.submit(consumer, job_id, seconds)
                # ... and if we get it here, then the job has been
                # processed but won't be logged.
                fut.add_done_callback(log)
                job_id += 1
            else:
                time.sleep(0.1)

    # It seems like the second gotcha can be worked around by putting
    # everything including the logging into the consumer function.
    # However, there's a catch: that would mean printing simultaneously
    # from multiple threads, which as we've seen previously is a big
    # no-no.
    #
    # If your response is, "then just get rid of logging altogether,
    # it's just a cosmetic detail!", then consider there might be a
    # mandatory processing stage with similar properties, i.e. that it
    # accesses a global mutable resource. For instance, some kind of
    # aggregation step like updating a global counter or similar, which
    # should not be happening in parallel.
    #
    # But if you look closely, it turns out we're actually *already*
    # violating that requirement! When we add the done callback to the
    # future, the only guarantee on where it will run according to the
    # docs is it will be "in a thread belonging to the process that
    # added [it]". So there's nothing preventing two callbacks from
    # running in two different threads at the same time.
    #
    # This is the final nail in the coffin of this approach. To recap:
    #
    # 1. It requires clunky and potentially inefficient workarounds
    #    (which rely on the private internals of the executor) to avoid
    #    a busy loop and apply some backpressure, so that we don't end
    #    up eating all the RAM.
    # 2. It doesn't guarantee that all accepted tasks will be fully
    #    processed on interrupt. (Technically, this can be solved at the
    #    cost of adding a bit more complexity with a custom signal
    #    handler, see 09-threading_unbounded_signal_handler.py.)
    # 3. It can potentially run the logging in parallel, which is bad.


if __name__ == "__main__":
    main()
