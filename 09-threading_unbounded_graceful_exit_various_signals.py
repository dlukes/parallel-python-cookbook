import os
import time
import random
import signal
import threading
import subprocess as sp
from queue import Queue

from util import print_box

# If you want the application to exit gracefully in response to
# different types of signals (not only SIGINT, but also e.g. SIGTERM),
# you need to set up a custom signal handler, which involves some fairly
# low-level (though not excessively complicated) tinkering. On the other
# hand, no need to reach for 'threading.Event' in this scenario.
#
# For more practical examples on how to set up signal handlers, see
# <https://stackoverflow.com/q/18499497> (the official docs on this
# topic can be a bit forbidding).

CONSUMER_WORKERS = 2
INPUTQ = Queue(10)
OUTPUTQ = Queue(10)
EXIT = False

# Signal handlers always run in the main thread, as documented here:
# <https://docs.python.org/3/library/signal.html#signals-and-threads>.
# That means that the 'EXIT' flag will be set in the main thread.
# Depending on the setup, it might be checked in a loop condition in
# another thread, which might look like a data race at first sight, but
# it's actually OK because 'Simple assignment to simple variables is
# "atomic" AKA threadsafe' (see <https://stackoverflow.com/q/2291069>).


def handler(signum, _):
    print_box(f"caught signal {signal.strsignal(signum)}")
    global EXIT
    EXIT = True


signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

# NOTE: By setting such a primitive custom handler for SIGINT, you lose
# the ability to force an abrupt shutdown by repeatedly mashing Ctrl-C.
# Depending on your requirements, that may be a limitation or an
# advantage.
#
# Or you can install the handler just for SIGTERM, designating it as the
# signal intended for graceful shutdown, and leave the behavior of
# SIGINT unmodified (abrupt shutdown).


def consumer():
    while True:
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
    print_box(
        "wait for a few jobs to complete, then press "
        f"Ctrl-C or run 'kill -15 {os.getpid()}'"
    )

    for _ in range(CONSUMER_WORKERS):
        threading.Thread(target=consumer, daemon=True).start()
    threading.Thread(target=logger, daemon=True).start()

    # One advantage of using a custom signal handler is that we can have
    # the producer on the main thread: when the application receives a
    # SIGINT or SIGTERM, the main thread will switch to the handler at
    # the earliest opportunity, execute it, and then *switch back to
    # where it was*, i.e. into the loop below, finish executing a full
    # cycle, check the loop condition and exit the loop in an orderly
    # manner.
    #
    # As a reminder, in 08-threading_unbounded_graceful_exit.py, SIGINT
    # is delivered to our program as an exception. If the producer were
    # on the main thread in that case, there would be no way to resume
    # execution at the point where the exception happened, which is why
    # we had to put an extra layer of indirection in there, using a
    # dedicated thread which we notify to exit via a 'threading.Event'.
    job_id = 0
    while not EXIT:
        INPUTQ.put((job_id, random.randint(0, 3)))
        job_id += 1

    print_box("waiting for queues to empty")
    INPUTQ.join()
    OUTPUTQ.join()

    print_box("exiting gracefully")


if __name__ == "__main__":
    main()
