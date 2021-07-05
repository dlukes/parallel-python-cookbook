import queue
import threading


print_queue = queue.Queue()


def print_manager():
    while True:
        job = print_queue.get()
        print(job)
        # IMPORTANT: Notify 'print_queue' that the given task has been
        # processed. Without this, the 'print_queue.join()' at the end
        # will hang forever, because the queue will think that all the
        # tasks are still waiting to be processed.
        print_queue.task_done()


# Regular threads are implicitly joined at the end of the program, which
# means the following would hang:
# t = threading.Thread(target=print_manager)
# t.start()

# By contrast, *daemon* threads are "abruptly stopped" at shutdown, see
# https://docs.python.org/3/library/threading.html#thread-objects. So
# this works fine (as in, the script doesn't hang):
t = threading.Thread(target=print_manager, daemon=True)
t.start()

# If needed, you can also daemonize a thread after the fact:
# t = threading.Thread(target=print_manager)
# t.daemon = True
# t.start()

# A common purpose of daemon threads is to have them manage some kind of
# mutable resource. Other threads send them messages via an atomic
# message queue with actions to perform on the resource on their behalf.
# Careful however that "abruptly stopping" a daemon thread may lead to
# any persistent resources that the thread is holding -- open files,
# database transactions etc. -- not being released properly.

for i in range(5):
    print_queue.put(i)

# IMPORTANT: Wait for the 'print_queue' to empty out. Otherwise Python
# will very probably terminate the daemon thread and exit before the
# daemon thread has had a chance to print a single item.
print_queue.join()
