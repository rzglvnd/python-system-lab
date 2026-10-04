# A queued worker's shutdown contract

Run `python queued_worker.py`. One worker consumes unique string job identifiers
in FIFO order. `start()` creates the owned task; synchronous `submit()` accepts
work only while the worker is running and open. `close()` immediately rejects new
submissions and adds a stop marker after accepted jobs. `shutdown()` closes and
awaits the worker. Repeated close and completed shutdown calls are harmless.

The admission check and enqueue contain no await, so another coroutine on the same
event loop cannot close the worker between those operations. This is not a
thread-safe interface. Calling an async shutdown method only creates a coroutine;
use `close()` when admission must stop immediately, before awaiting shutdown.

Tests hold the first handler at an event, close admission, and verify that both
accepted jobs still complete in order. Empty shutdown and invalid lifecycle calls
are also covered. Handler exceptions currently propagate to the shutdown caller.

This is an in-memory experiment with an unbounded queue and retained job IDs and
outcomes. It has no persistence, retry, backpressure, restart, or process-signal
integration. Process failure loses queued work. A returned handler is considered
successful; no claim is made about external transactions or exactly-once effects.
