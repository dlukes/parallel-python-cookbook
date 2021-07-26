import time
import random
import threading
import subprocess as sp
from queue import Queue

from util import print_box

# This is a variant of 07-threading_unbounded_abrupt_exit.py with a more
# graceful exit (as if it weren't obvious from the name). In other
# words, all tasks that were submitted to the 'INPUTQ' prior to exit
# being requested will complete instead of being dropped on the floor.
# This way, whatever source is submitting those tasks can be sure that
# all the tasks that were accepted by the program have actually been
# handled.

CONSUMER_WORKERS = 2
INPUTQ = Queue(10)
OUTPUTQ = Queue(10)
EXIT = threading.Event()


# In this setup, the producer will be a non-daemon thread which we can
# notify to exit via an 'Event' in its loop condition. When the 'Event'
# is set, the producer stops producing. Imagine e.g. it's accepting
# lines from stdin -- once the 'EXIT' event is set, it stops enqueueing
# them for processing by the consumer and exits. But the exit is
# orderly: we make sure that all the lines we've accepted from stdin end
# up enqueued and subsequently processed.
#
# (Technically, there's nothing stopping us from letting the producer
# remain a daemon thread, but we want to semantically signal our intent
# that Python is not allowed to just kill this thread willy-nilly when
# it's otherwise ready to exit. This might even help us to catch bugs:
# if we're expecting Python to exit, but it doesn't, it means we didn't
# actually join the producer thread even though we might think we did,
# and it's still running somewhere in the background. Whereas if it were
# a daemon thread, Python would just summarily kill it and exit, and we
# wouldn't notice that something's amiss.)


def producer():
    job_id = 0
    while not EXIT.is_set():
        INPUTQ.put((job_id, random.randint(0, 3)))
        job_id += 1
    print_box("exiting producer")


# As long as we don't need the consumer thread to do some kind of
# cleanup on shutdown (see towards the end), it can remain a daemon
# thread: it just processes tasks from the 'INPUTQ' and puts results on
# the 'OUTPUTQ' until it's killed off at program exit.


def consumer():
    while True:
        job_id, x = INPUTQ.get()
        sp.run(["sleep", f"{x}s"])
        OUTPUTQ.put((threading.get_ident(), job_id, x))
        # NOTE: The placement of '.task_done()' calls can be tricky to
        # get right, leading to subtle bugs. E.g. here, if we swap it
        # with the preceding '.put()', we might end up in a state where
        # both queues will be momentarily empty and their joins in the
        # main thread will unblock, and *only after that* will the
        # consumer put the next item on 'OUTPUTQ'. However, by then, the
        # program will be in the process of exiting and the logger might
        # get killed before it gets a chance to handle that last item.
        # See 10-task_done_placement.py for more details.
        INPUTQ.task_done()


# We now also have a separate logger thread, which does what our main
# thread did before. That's because our main thread now has a different
# job (see below).


def logger():
    while True:
        thread_id, job_id, x = OUTPUTQ.get()
        ts = str(int(time.time()))[-2:]
        print(f"Worker {thread_id}, job {job_id} (sleep {x}s) done at ...{ts}.")
        OUTPUTQ.task_done()


def main():
    print_box("wait for a few jobs to complete, then press Ctrl-C")

    # We only need to hang onto the join handle of the producer thread,
    # as it's the only one we need to join at a specific point in the
    # execution of our program.
    pt = threading.Thread(target=producer)
    pt.start()
    for _ in range(CONSUMER_WORKERS):
        threading.Thread(target=consumer, daemon=True).start()
    threading.Thread(target=logger, daemon=True).start()

    # The job of the main thread now is to just wait for a signal that
    # the application should exit. That's why we've moved the logging to
    # a different thread, because the signal would terminate that loop,
    # whereas we actually want the logging to continue as the program is
    # winding down in an orderly fashion (which is orchestrated by the
    # main thread, see below).
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        print_box("caught keyboard interrupt")

    # Technically, prints in the main thread are now subject to a race
    # condition, because we're also printing from the logger thread at
    # the same time. Stdout is a resource that should be managed by a
    # single thread, see Hettinger's talk on concurrency. The stdout
    # buffer is not threadsafe and you can easily get garbled output
    # when multiple threads try printing at the same time, or worse, see
    # <https://stackoverflow.com/q/40356200>.
    #
    # But occasionally seeing that garbled output (the boxes of messages
    # printed with 'print_box()' can get interspersed with lines from
    # the logger thread) is a good reminder that you shouldn't do this,
    # so let's leave it as is.
    #
    # NOTE: In practice, you could use the logging module instead, which
    # is threadsafe.

    # Tell the producer thread to stop accepting/submitting new tasks to
    # the 'INPUTQ'.
    print_box("signaling producer to stop producing")
    EXIT.set()

    # Now, we need to wait for the producer to actually exit. This
    # matters because the next step will be to wait for the queues to
    # empty, which only makes sense if we know there aren't any more
    # tasks incoming.
    #
    # What would happen without this '.join()'? Imagine our producer is
    # (potentially) slow at creating tasks. Maybe they're coming in over
    # the network, which involves wait times. It gets a signal to exit
    # while it's waiting for a task which will eventually become the
    # final one. In the meantime, the consumer and logger process the
    # contents of the queues, emptying them out, and as soon as the
    # queues are empty, the program is poised to exit, but it's actually
    # still blocked by that producer thread (which is non-daemon and
    # still running). Eventually, the final task gets submitted to the
    # 'INPUTQ', but the trouble is that it might not get processed,
    # because the consumer and logger daemon threads might get killed
    # before they get a chance to do so, as there's nothing stopping the
    # program from exiting now.
    #
    # So that's why it's a good idea to check that the producer is done
    # at this point, before waiting on the queues to be emptied out.
    print_box("making sure producer is done producing")
    pt.join()

    # Our trusty daemon threads are continuing to spin, handling tasks
    # as they get them via the two queues. Given enough time, they'll
    # get to the end of our unbounded stream of input (the stream is
    # unbounded in theory, but we've just told the producer to stop
    # producing, so it has an end in practice).
    #
    # Imagine the queues as pipes, the producer as a faucet, stdout as
    # the basin where all the logging messages a.k.a. water is supposed
    # to end up. If you close the faucet, the rest of the water will
    # eventually trickle down to the basin and the pipes will end up
    # empty, unless you obliterate the pipes before it's had a chance to
    # do so.
    #
    # Since our threads are daemon threads, Python is happy to
    # obliterate them whenever, even if they're in the middle of doing
    # something. That's not what we want. Luckily, our pipes have
    # '.join()' methods, which will make our program wait until they're
    # emptied out.
    #
    # It's important to realize that it's the loops in the two
    # daemon threads that ensure the water gets drained out, not the
    # calls to '.join()' themselves. Their purpose is just to say, don't
    # move past this point unless both queues are empty, and we know
    # they'll both empty out eventually.
    #
    # Still, the ordering of those '.join()' calls matters: they should
    # be in the same order as data flows through the pipeline.
    # Intuitively, in order to be sure that a later pipe is empty *for
    # good* (as opposed to just happening to be empty *at the moment*),
    # you need to be sure that there isn't any more stuff incoming
    # through the earlier stages of the pipeline. This is also discussed
    # in more detail in 10-task_done_placement.py, which is even more of
    # a toy example so that you can switch joins around and observe how
    # that affects behavior.
    print_box("waiting for queues to empty")
    INPUTQ.join()
    OUTPUTQ.join()

    # Finally, it's conceivable that you might want the consumer and/or
    # logger threads to perform some more complicated cleanup before
    # exiting. How to achieve that?
    #
    # Well in such a case, you wouldn't make them daemon threads, they'd
    # be regular threads, and instead of a 'while True' loop, they'd
    # also have an event as their loop condition -- a separate one from
    # the one used by the producer thread, though! And this is the point
    # where you'd '.set()' that event -- after both queues have been
    # emptied, you can exit the threads' processing loop and perform any
    # required cleanup.
    #
    # You could then leave it up to Python to join those threads as it's
    # exiting, because nothing else really happens beyond this point, so
    # we don't really care at which exact point the threads terminate.
    # Which means we don't have to hang onto their join handles when we
    # create them at the beginning of main, and explicitly call
    # '.join()' on them, unlike for the producer thread, whose shutdown
    # must happen before waiting for the queues to empty, as explained
    # above.

    print_box("exiting gracefully")


if __name__ == "__main__":
    main()
