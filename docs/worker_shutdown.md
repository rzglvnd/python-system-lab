# A queued worker's shutdown contract

Run `python queued_worker.py`. One worker consumes string job identifiers
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
are also covered. Ordinary handler exceptions are recorded as failed jobs; the
worker continues to the next job while draining. Cancellation propagates instead.

This is an in-memory experiment with retained job IDs and outcomes.
It has no persistence, retry, restart, or process-signal
integration. Process failure loses queued work. A returned handler is considered
successful; no claim is made about external transactions or exactly-once effects.

## Grace deadline and cancellation

`await worker.shutdown(grace_period=seconds)` allows accepted work to drain for a
finite, nonnegative period, then cancels and awaits the worker. `None` waits without
a deadline. The report separates completed, interrupted, and unstarted jobs and
records whether the grace period expired. Pending jobs are discarded explicitly;
they are not reported as completed. Reports contain immutable snapshots.

The deadline uses `asyncio.wait`, which does not itself cancel the worker. The
supervisor performs cancellation explicitly and joins the owned task. Cancelling
the shutdown caller also cancels and joins the worker before propagating cancellation.
Only one shutdown call may be active; repeated completed calls return the outcomes.

Tests use an already-started blocked job and a zero grace period to trigger expiry
without sleep-based races. Another test holds asynchronous cleanup at an event and
proves that shutdown remains pending until cleanup is released. A grace period
limits draining, not cleanup: a handler that ignores cancellation or blocks the
event loop can prevent shutdown indefinitely. Repeated supervisor cancellation is
outside this contract. Interrupted jobs may already have external side effects;
this report cannot tell you whether retrying them would duplicate those effects.

## Failure policy and accounting

Ordinary `Exception` failures are recorded by job ID, exception type name, and
message, then draining continues. The report's `failed` entries do not retain
tracebacks. `CancelledError` remains a separate interrupted outcome. If a handler
raises a cleanup error while cancellation is unwinding, that job is failed instead
of interrupted. After an abort request, no next job starts even if the current
handler replaces or suppresses cancellation. A handler that suppresses cancellation
and returns is classified as completed; the worker cannot infer its business result.

Tests combine a completed job, a failure, an interrupted job, and an unstarted job
and verify complete accounting with the default unlimited retention policy.
They also check queue accounting after failures and discarded work. `task_done()`
is called for every retrieved item (including the stop marker and discarded jobs),
so queue `join()` completion alone does not mean successful processing. The tests'
one-second join guard only detects a bookkeeping hang; no timing result is asserted.

This policy treats jobs as independent. It is inappropriate for a workflow where
one failure should invalidate subsequent jobs. `BaseException` subclasses other
than cancellation are not converted into ordinary failed-job reports. Error messages
may include handler-provided data and need review before publishing or logging.

Interview checkpoint: explain the synchronous admission boundary, grace period
versus a hard execution limit, why queue accounting is separate from job outcomes,
and why retrying interrupted work requires an idempotency policy. A useful next
experiment is bounding retained outcome history; durable recovery remains separate.

## Bounded admission

`QueuedWorker(handler, max_pending=N)` allows at most N waiting jobs plus the one
currently running. The default zero preserves unbounded admission. Negative limits
are rejected. `submit()` raises `asyncio.QueueFull` at capacity without recording
that identifier as accepted. The producer can retry the same identifier later.

One extra queue slot is reserved for the stop marker. Producers cannot use it;
the synchronous admission check and enqueue contain no await. Thus `close()` can
always stop admission even when waiting jobs fill their capacity. Tests hold the
running job at an event, fill the backlog, reject excess work, and close the full
worker while checking FIFO completion and rejection accounting.

This bounds the count of waiting jobs. Accepted identifiers, outcome history,
job size, and producer task count still contribute to memory use.

## Waiting producers

`await worker.submit_wait(job)` waits when capacity is full, while `submit(job)`
still rejects immediately. Acceptance happens when the job is enqueued, before
the method returns; waiting alone does not accept a job or reserve its identifier.
Duplicate identifiers are checked again when a producer wakes.

A broadcast event signals capacity changes, closure, or worker termination.
Every awakened producer rechecks admission and capacity before enqueuing, with no
await in that final check-and-enqueue operation. Capturing the event after a full
check also contains no await, avoiding a missed notification. Closing wakes all
waiters with an admission error. Cancelling a producer suspended on capacity does
not enqueue its job. The interface adds no helper tasks requiring separate cleanup.

Wake-up ordering and producer fairness are unspecified: synchronous producers may
take capacity before a waiting producer resumes. Producers should await submissions
in sequence; spawning unlimited waiting tasks would still permit unbounded memory
use. Callers can impose their own timeout on admission, separate from job execution.

## Admission races during shutdown

Tests coordinate two producers competing for a single free slot, including two
producers requesting the same identifier. Exactly one is accepted. They also
force closure or producer cancellation after a dequeue signals free capacity but
before a waiting producer resumes. No new job enters in either case. A worker
that terminates through cancellation wakes capacity waiters with an admission
error, rather than leaving them indefinitely blocked.

The ordering comes from event gates and synchronous handler actions. Five-second
watchdogs detect hangs in these race tests; they are not performance measurements.
A capacity notification is only permission to retry, not a reserved slot. Acceptance
is the enqueue operation. Cancellation after acceptance cannot retract that job;
applications needing stronger acknowledgement or retry guarantees require a
separate protocol. The next useful experiment is choosing how much accepted-ID
and outcome history to retain while preserving the report's meaning.

## Retained outcome details and lifetime counts

`history_limit=N` keeps the latest N outcomes across all four categories in one
shared deque. `None` (the default) retains every outcome; zero keeps only counts.
The report's completed, failed, interrupted, and unstarted tuples are the retained
details, ordered within each category by when their outcomes were recorded. They
are not a complete audit trail when retention is limited. Discarding queued work
during shutdown records new unstarted outcomes and can evict earlier failures.

`report.counts` records lifetime accepted submissions and each terminal outcome
independently of detail retention. `omitted_outcomes` states how many details were
omitted. Counts and details are immutable snapshots. For supported shutdown paths,
accepted equals completed + failed + interrupted + unstarted. No count includes
the stop marker or a rejected submission.

## Identifier eviction and duplicate detection

With limited history, duplicate detection covers queued jobs, the running job,
and retained outcomes. When an outcome is evicted, its identifier is removed too.
That identifier can then be submitted again, counting as another accepted
submission. A zero limit forgets an identifier as soon as its outcome is recorded.
The default unlimited history preserves lifetime duplicate rejection.

Completed and failed jobs have the same eviction policy. No active identifier is
forgotten before its outcome. This is a bounded duplicate-detection window by
outcome count, not durable idempotency or a time-based expiry policy. Reusing an
evicted identifier can repeat external side effects. Applications requiring
durable duplicate protection need a separate persistent design.

With finite history N and waiting capacity M, stored job identifiers are limited
to at most N + M + 1 (retained outcomes, waiting jobs, and the running job).
This bounds retained job references, not bytes: job/error strings, Python integer
counters, caller-held reports, and waiting producer tasks have separate costs.

For example, three successful jobs with `history_limit=2` produce two completed
details, a completed count of three, and `omitted_outcomes=1`. A one-entry history
can end with only the last discarded job after a deadline abort, while the counts
still include earlier successes, failures, and interrupted work.

The retention tests exercise all outcome categories with unlimited, zero, one,
and three-entry histories. They verify queue accounting, repeatable immutable
reports, and exclusion of rejected or cancelled submissions from accepted counts.
A 300-job experiment also checks stored identifier counts during processing with
waiting capacity one and history three. It measures the retained-reference bound;
it does not measure byte allocation or process RSS.

Interview checkpoint: explain why a recent-history report differs from a lifetime
total, why bounded duplicate detection permits ID reuse, and which memory costs
remain outside this limit. A useful next task is a reproducible allocation
experiment comparing full and bounded retention as the processed job count grows.
