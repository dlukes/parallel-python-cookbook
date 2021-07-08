import sys
import time
import queue
import threading

buggy = "buggy" in sys.argv
daemon = "daemon" in sys.argv

print("setup:")
print("  - placement of .task_done() calls is potentially buggy:", buggy)
print("  - using daemon threads:", daemon)

q = queue.Queue()
r = queue.Queue()
x = 1

# 'x' should move between the threads: main -> first -> second (which is
# where it's logged).

if buggy:

    def first():
        x = q.get()
        q.task_done()
        time.sleep(1)
        r.put(x)

    def second():
        x = r.get()
        r.task_done()
        time.sleep(1)
        print(f"got {x = } out of the pipeline (potentially after queues have joined)")


else:

    def first():
        x = q.get()
        r.put(x)
        time.sleep(1)
        q.task_done()

    def second():
        x = r.get()
        print(f"got {x = } out of the pipeline (safely always before queues join)")
        time.sleep(1)
        r.task_done()


threading.Thread(target=second, daemon=daemon).start()
threading.Thread(target=first, daemon=daemon).start()

print(f"sending {x = } into the pipeline")
q.put(x)

print("waiting to join")
q.join()
r.join()

print("joined")
