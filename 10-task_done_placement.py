import sys
import time
import queue
import threading

from util import print_box

q = queue.Queue()
r = queue.Queue()
x = 42

# This example demonstrates how the placement of '.task_done()' calls
# for 'queue.Queue' instances can be tricky, so you should think about
# it carefully. The goal of this program is to move the value which
# starts out in the 'x' variable from the main thread through 'first'
# and into 'second', where it's logged.
#
# We use 'q.join()' and 'r.join()' as a means to ensure that the value
# has made it through this pipeline by a certain point in our program.
# As we'll see, whether this strategy actually works depends on the
# correct placement of '.task_done()' calls.
#
# Try running the script in the following ways, and observe the
# different points at which the value from 'x' is printed, if at all,
# relative to the message which notifies us that we've joined on both
# queues:
#
# - without any arguments: the 'first' and 'second' threads are run as
#   non-daemon, placement of '.task_done()' calls is correct, so
#   everything works fine, i.e. the joins unblock only after 'x' has
#   made it through the pipeline;
# - passing 'daemon' as an argument: threads are run as daemon, but
#   otherwise it's the same;
# - passing 'buggy' as an argument: threads are run as non-daemon but
#   placement if '.task_done()' calls is buggy, which means the joins
#   unblock *before* 'x' has made it through the pipeline; however,
#   since the threads are non-daemon, Python waits for them to complete
#   before exiting, so 'x' is still allowed to make it through the
#   pipeline, but at a later point than the one we think we've enforced
#   via the joins;
# - passing both 'daemon' and 'buggy' as arguments: with daemon threads,
#   when the joins don't work as intended (because the placement of
#   '.task_done()' calls is buggy), 'x' might not make it through the
#   entire pipeline, because the program will exit when the joins
#   unblock, abruptly killing those daemon threads.

buggy = "buggy" in sys.argv
daemon = "daemon" in sys.argv

print("setup:")
print("  - placement of .task_done() calls is buggy:", buggy)
print("  - using daemon threads:", daemon)

if buggy:

    def first():
        x = q.get()
        # Here, we call '.task_done()' as soon as we've received the
        # value from 'q'. This means that past this point, both 'q' and
        # 'r' will be empty with no pending tasks, which means both
        # calls to '.join()' below can unblock -- even though 'x' is
        # nowhere near having made it through the entire pipeline.
        q.task_done()
        # The sleep simulates some more work that this thread has to
        # perform on 'x' before sending it on to the second thread. It
        # exacerbates the issue caused by the wrong placement of the
        # '.task_done()' call -- it makes it less likely that the
        # outcome will vary depending on how the threads happen to get
        # scheduled on each particular run, making the results more
        # predictable.
        time.sleep(1)
        print_box(f"first: {x = } (potentially after queues have joined)")
        r.put(x)

    def second():
        # Similar comments apply as in the first thread: '.task_done()'
        # is called too soon, so the joins can unblock, even though this
        # thread is nowhere near done yet. The sleep again serves to
        # exacerbate the issue.
        x = r.get()
        r.task_done()
        time.sleep(1)
        print_box(f"second: {x = } (potentially after queues have joined)")


else:

    def first():
        x = q.get()
        time.sleep(1)
        r.put(x)
        print_box(f"first: {x = } (reliably always before queues join)")
        # And this is the correct alternative. How to think about it
        # intuitively, so that you don't make the mistake in a larger,
        # more complicated program?
        #
        # Conceptually, the "task" here is not only to get a value from
        # 'q', but to also do some stuff with it (well, sleep, in our
        # case) and then *hand it off to 'r'*. So we can't consider the
        # task "done" until the hand-off has actually happened. At that
        # point, we've passed 'x' to the next part of the pipeline, so
        # we can unblock this one, as we know 'r' will take over
        # blocking until that next part is done too.
        #
        # Visually, there needs to be an inter(b)locking (see what I did
        # there?) pattern between the different parts of the pipeline:
        #
        # ┌    join on thread 1 blocks
        # │
        # │ ┐  join on thread 2 blocks
        # │ │
        # └ │  join on thread 1 unblocks
        #   │
        #   ┘  join on thread 2 unblocks
        q.task_done()

    def second():
        x = r.get()
        time.sleep(1)
        print_box(f"second: {x = } (reliably always before queues join)")
        r.task_done()
        # At this point, the second join unblocks and we can be
        # confident we're at a point in our program where the pipeline
        # has completed and 'x' has been printed.


threading.Thread(target=second, daemon=daemon).start()
threading.Thread(target=first, daemon=daemon).start()

print(f"sending {x = } down the pipeline")
q.put(x)

# Now we '.join()' on both queues, which we intend to mean "only move
# beyond this point if both threads have run to completion". As we've
# seen above, this doesn't always work, depending on whether the
# '.task_done()' calls have been placed correctly or not.
#
# WARNING: Another lurking gotcha is the ordering of those '.join()'
# calls -- they should be in the order that 'x' moves through the
# pipeline. Try switching them around and see what happens. 'r' is now
# first and will very probably unblock immediately, because 'x' has just
# been put on 'q', but 'r' is still empty. And subsequently, 'q.join()'
# will unblock as soon as '.task_done()' is called on 'q', and execution
# in the main thread will move past this checkpoint, even though 'r' has
# only just received 'x' and 'second' is not done handling it. So that's
# another way that we can fail to enforce the synchronization we think
# we're enforcing.
print("waiting to join on both queues")
q.join()
r.join()

print("joined, so the first and second threads must have printed by now, right?")

# Finally, let's stress that this is a toy example designed to
# demonstrate how thread synchronization via queue joins works and what
# the gotchas are. In the real world, you probably wouldn't have a
# pipeline consisting of two threads that you want to send a single
# value through, but if you had something roughly structurally
# analogous, here's what you would really do:
#
# - The target functions ('first' and 'second') don't contain any
#   infinite processing loops and we want them to run to completion, so
#   they should definitely be run on regular threads, not daemon
#   threads.
# - From that follows that the simplest way to wait for their completion
#   is not by joining on the queues they use to communicate, which can
#   be tricky -- as we've shown above, '.task_done()' calls must be
#   correctly placed and the joins themselves are order-sensitive.
#
#   Instead, it's much easier to join on the threads themselves. You
#   don't have to muck about with '.task_done()' calls at all, and you
#   can do it in any order you like and still be confident that you
#   won't get past those two joins until both of the threads have
#   actually run to completion.
#
# So a recommendation: join on threads if you can, there's no simpler
# way to know that a thread's done, that's literally what joining on
# threads is for. Joining on queues can be tricky, the ordering of
# '.task_done()' and '.join()' calls matters. But sometimes, there's no
# way around it -- e.g. with daemon threads (which typically involve
# infinite loops, so they don't terminate and can't therefore be
# joined), or if your thread does lots of stuff and you want to
# synchronize with a point in the middle of all this, instead of waiting
# all the way to the end.
