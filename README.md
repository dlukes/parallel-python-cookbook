Python parallel cookbook
========================

A set of recipes on how to use [`concurrent.futures`][cf],
[`threading`][thread], [`multiprocessing`][mp] and the [`trio`][trio]
async runtime (an alternative to stdlib's `asyncio`) to run stuff in
parallel with Python.

[cf]: https://docs.python.org/3/library/concurrent.futures.html
[thread]: https://docs.python.org/3/library/threading.html
[mp]: https://docs.python.org/3/library/multiprocessing.html
[trio]: https://trio.readthedocs.io/

The examples cover common gotchas which might result in your code
behaving differently than you intended, and show idiomatic and concise
ways to solve similar tasks across the libraries so as to highlight what
is and isn't (easily) achievable with each of their APIs. Some of the
comments are repeated in more than one file to make each file more
self-contained.

Prerequisites
-------------

To make the most of the information contained in this repo, you should
already have a general idea about:

- IO-bound vs. CPU-bound tasks
- the *Global Interpreter Lock* (GIL), which forces Python code in
  threads to run sequentially, but subprocesses spawned by Python code
  in threads *can* run in parallel
- async, which is another multitasking model alongside threads, so like
  threads, it allows you to wait on subprocesses spawned by Python in
  parallel (unlike threads, the task-switching is cooperative, not
  pre-emptive, so it's sequential by design, GIL or no GIL)

If you'd like an introduction and/or refresher, I recommend watching
[this talk by Raymond Hettinger][rh], which covers all of the above
topics, and reading the [Trio tutorial][tt], which delves specifically
into async in a very accessible way. There's also a [great talk on
concurrency, async and Trio by Nathaniel J. Smith][njs].

[rh]: https://youtu.be/9zinZmE3Ogk
[tt]: https://trio.readthedocs.io/en/stable/tutorial.html
[njs]: https://youtu.be/oLkfnc_UMcE
