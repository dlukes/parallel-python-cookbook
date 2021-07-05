import time
import random
import threading
import subprocess as sp
from queue import Queue

# This is a simple example of processing a (potentially) unbounded
# stream of tasks using just the vanilla 'threading' module. It uses
# daemon threads to avoid having to worry about how to join them, but
# this also means that when the application receives a signal to exit,
# it drops everything it's doing and immediately exits, even though that
# very probably means dropping some tasks it's already accepted on the
# floor. If that's unacceptable, see the graceful shutdown alternative
# in 08-threading_unbounded_graceful_exit.py.

CONSUMER_WORKERS = 2
INPUTQ = Queue(10)
OUTPUTQ = Queue(10)


def producer():
    job_id = 0
    while True:
        INPUTQ.put((job_id, random.randint(0, 3)))
        job_id += 1


def consumer():
    while True:
        job_id, x = INPUTQ.get()
        sp.run(["sleep", f"{x}s"])
        INPUTQ.task_done()
        OUTPUTQ.put((threading.get_ident(), job_id, x))


def main():
    # We don't need to hang onto the join handles of the threads,
    # they'll be automatically destroyed at program exit.
    threading.Thread(target=producer, daemon=True).start()
    for _ in range(CONSUMER_WORKERS):
        threading.Thread(target=consumer, daemon=True).start()

    while True:
        thread_id, job_id, x = OUTPUTQ.get()
        ts = str(int(time.time()))[-2:]
        print(f"Worker {thread_id}, job {job_id} (sleep {x}s) done at ...{ts}.")
        OUTPUTQ.task_done()


if __name__ == "__main__":
    main()
