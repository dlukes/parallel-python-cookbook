import time
import trio

# 'trio' is an alternative async runtime for Python. That means that
# like threads in Python, it can be used to parallelize IO-bound tasks
# or CPU-bound tasks run in subprocesses, but not CPU-bound Python code.
#
# It has tons of great ideas, some of which the stdlib async runtime
# 'asyncio' has been trying to incorporate of late. Its docs are a great
# resource (highly informative yet accessible) to learn about the
# pitfalls of async, its nuts and bolts, and how to do it right (or at
# least, less wrong): https://trio.readthedocs.io/
#
# In addition to the 'trio' docs, I cannot recommend enough the
# following articles on concurrency by the library's original creator:
#
# - https://vorpus.org/blog/notes-on-structured-concurrency-or-go-statement-considered-harmful/
# - https://vorpus.org/blog/timeouts-and-cancellation-for-humans/
# - https://vorpus.org/blog/control-c-handling-in-python-and-trio/
#
# The example below is very lightly adapted from
# https://trio.readthedocs.io/en/stable/reference-core.html#managing-multiple-producers-and-or-multiple-consumers


async def producer(producer_send_channel, tasks):
    # 'async with' makes sure that the other end of the channel gets
    # properly notified when this end gets closed.
    async with producer_send_channel:
        # 'tasks' could easily be an unbounded stream of data to process
        # on the fly.
        for x in tasks:
            await producer_send_channel.send(x)


async def consumer(worker_id, consumer_receive_channel, consumer_send_channel):
    async with consumer_receive_channel, consumer_send_channel:
        async for x in consumer_receive_channel:
            await trio.run_process(["sleep", f"{x}s"])
            await consumer_send_channel.send((worker_id, x))


async def main():
    num_workers = 2
    tasks = [5, 0, 4, 3, 2, 1]

    # Nurseries allow you to run a tree of child tasks concurrently.
    async with trio.open_nursery() as nursery:
        producer_send_channel, consumer_receive_channel = trio.open_memory_channel(0)
        consumer_send_channel, main_receive_channel = trio.open_memory_channel(0)

        nursery.start_soon(producer, producer_send_channel, tasks)

        async with consumer_receive_channel, consumer_send_channel:
            for worker_id in range(num_workers):
                nursery.start_soon(
                    consumer,
                    worker_id,
                    # By cloning, we let each party have their own
                    # handle on the channel, and only when all handles
                    # are closed does the channel close and notify the
                    # other end.
                    consumer_receive_channel.clone(),
                    consumer_send_channel.clone(),
                )

        async with main_receive_channel:
            async for worker_id, x in main_receive_channel:
                ts = str(int(time.time()))[-2:]
                print(f"Worker {worker_id} slept for {x}s, woke up at ...{ts}.")


if __name__ == "__main__":
    trio.run(main)
