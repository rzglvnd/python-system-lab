# Python System Lab

Small executable experiments exploring Python behavior, with tests and short
explanations. Experiments study generator execution, failure timing, and resource ownership.
This is a learning laboratory.

## Run locally

Requires Python 3.11 or newer. The experiments use only the standard library:

```sh
python generator_execution.py
python generator_cleanup.py
python streaming_csv.py
```

For tests and development tools, create a virtual environment:

```sh
python -m venv .venv
```

Activate it with `.venv\Scripts\Activate.ps1` in PowerShell, or
`source .venv/bin/activate` on Linux/macOS. Then run:

```sh
python -m pip install --upgrade pip
python -m pip install --group dev
python -m pytest -q
python -m ruff check .
python -m ruff format --check .
python -m mypy
```

CI is configured to run these checks on Python 3.11 and 3.13. Development
dependency ranges are bounded but not locked to exact versions.

## Experiment: when does a generator execute?

Before running the script, predict which rows have been parsed immediately after
calling each function, after one `next()`, and after encountering `"bad"`.

Both functions convert strings to integers and record attempted conversions.
`eager_numbers` finishes the loop before returning a list. `lazy_numbers` contains
`yield`, so calling it creates a generator; its body starts when it is advanced.
Each successful `next()` resumes execution up to the next yield.

Argument expressions still execute before a generator function is called.
Deferring its body does not defer work used to construct its arguments.

With input `["10", "20", "bad"]`, the trace is:

```text
eager call: ValueError (no result returned)
eager events: ["parse '10'", "parse '20'", "parse 'bad'"]
lazy created: []
first next: 10
events after first next: ["parse '10'"]
second next: 20
third next: ValueError
lazy events: ["parse '10'", "parse '20'", "parse 'bad'"]
remaining after failure: []
```

The tests cover incremental source consumption, empty input, normal exhaustion,
recreation from reusable input, and failure timing. An unhandled exception leaving
this generator terminates it; another `next()` raises `StopIteration`.

## Why this matters for backend work

A consumer processing database rows or ingestion records may receive valid items
before a later item fails. Exception handling must cover iteration, not just the
call that creates the generator. Earlier side effects are not rolled back by a
later parsing error. The eager function also performs work before failure, even
though it returns no result; eager evaluation is not a transaction.

A list supports repeated iteration. A generator is a one-pass iterator. Repeating
the computation requires creating another generator and having a reusable or
reopened source; an already exhausted source iterator cannot replay itself.

Lazy conversion avoids accumulating a result list, but this example does not
establish bounded memory: it retains the source, and the diagnostic events list
grows with consumption. Calling `list()` on a generator also materializes its
remaining results. No timing or memory benchmark is included.

## Questions to defend

- Where does the generator pause, and what resumes it?
- Why does catching errors around creation miss the malformed row?
- What happens to already processed records when a later conversion fails?
- Why can a list be iterated twice while this generator cannot?
- What allocations would need to change before measuring streaming memory use?

## Experiment: who closes a file when iteration stops early?

`generator_cleanup.py` reads integers from a temporary UTF-8 file. The generator
owns the file and holds its context open across yields. The caller owns the
generator's lifetime. Predict the trace before running it:

```text
created: []
first number: 10
after bare break: ['opened']
after explicit close: ['opened', 'closed: True']
after break inside closing: ['opened']
after closing context: ['opened', 'closed: True']
consumer failed: ['opened', 'closed: True']
```

The closure event records the actual file object's `closed` property after its
context exits. Tests use real temporary files, retain generator references, and
explicitly close partially consumed generators; they do not force garbage
collection or depend on object destruction.

| Exit path | File lifetime in this experiment |
| --- | --- |
| Full consumption, including empty input | Closed when the generator finishes |
| Malformed row inside the generator | Closed as the exception unwinds its file context |
| Consumer uses `break` | Remains open while the generator is suspended and retained |
| Consumer raises an exception | Remains open unless the caller also closes the generator |
| Caller invokes `close()` on a started generator | File context exits before `close()` returns |
| Caller exits `with closing(generator)` | Generator is closed at context exit, including after consumer failure |
| Generator is closed before it starts | Body never executes; no file is opened |

Reading the final value is not the same as exhausting the generator: it remains
suspended at that yield until advanced again or closed. Likewise, `break` inside
a `closing()` block does not close the generator until the block itself exits.

`close()` raises `GeneratorExit` at a started generator's suspension point,
allowing its contexts and `finally` blocks to unwind. A consumer exception is
outside that generator; a normal `for` loop does not forward it into the producer.
In this example, `closing()` performs cleanup while preserving the consumer error.
Cleanup that itself raises could replace the propagating error, and yielding
while handling `GeneratorExit` is invalid. Neither behavior is implemented here.

### Ownership decision

The path-taking generator is convenient, but callers must honor its closure
contract whenever consumption might stop early. `contextlib.closing()` expresses
that contract around the consumer; `try/finally` with an explicit `close()` is an
equivalent option when a larger scope requires it.

For an application API, accepting an already-open stream is often simpler:

```python
with path.open(encoding="utf-8") as stream:
    for number in lazy_numbers(stream, []):
        process(number)
        if should_stop(number):
            break
```

This is illustrative consumer code: `process` and `should_stop` are application
callbacks. Here the caller's `with` owns file closure, independent of the parsing
generator's lifetime. The trade-off is that callers manage resource acquisition
and must not consume the iterator beyond that context.

### What to defend

- Why does a producer error trigger cleanup while a consumer error alone does not?
- Why can a generator still hold a file after yielding its last value?
- Where should resource ownership live in a reusable streaming API?
- Why is relying on garbage collection insufficient for timely cleanup?

An open file consumes an operating-system handle. Similar ownership questions
arise with database cursors, but file closure does not demonstrate transaction
rollback, connection-pool behavior, async cancellation, or process-crash recovery.
The trace list grows with events; no memory or throughput benchmark is claimed.

References: [generator methods](https://docs.python.org/3/reference/expressions.html#generator-iterator-methods)
and [contextlib.closing](https://docs.python.org/3/library/contextlib.html#contextlib.closing).

## Application: validated streaming CSV records

`streaming_csv.read_records(stream)` applies caller-owned resource management to
CSV ingestion. It yields dictionaries of strings using the standard-library CSV
parser. The caller opens and closes the text stream:

```python
from pathlib import Path
from streaming_csv import read_records

path = Path("records.csv")
with path.open(encoding="utf-8", newline="") as stream:
    for record in read_records(stream):
        print(record)
        break
```

The file closes when the caller's context exits, including on consumer failure.
Closing the parsing generator does **not** close the supplied stream. Consume the
iterator within the resource's context; a retained iterator can otherwise try to
read a closed file. The reader uses the stream's current position and never seeks.

### Validation contract

- The first logical record is the header. Empty input is an error; a header-only
  file is valid and yields no data records.
- Header names must be nonblank and exactly unique. Whitespace and case are
  preserved; `id`, `ID`, and ` id` are distinct. There is no required domain schema.
- Every data record must have the header's field count. Blank physical rows
  outside quoted fields are rejected as zero-field records, not silently skipped.
- Empty field values are valid strings. Values are not stripped or converted.
- The parser uses the default Excel dialect with `strict=True`. It handles quoted
  commas, doubled quotes, and embedded newlines. Strict mode reports the parser's
  recognized syntax errors; it is not a comprehensive RFC-conformance validator.
- Parsing failures (including field-size limits) and structure failures raise
  `CsvValidationError`, with `record_number`
  and `line_number` attributes. Records count from 1, including the header. The
  physical line is the last line consumed by the parser for that record or error,
  relative to this reader; it is 0 for empty input. It is not a byte offset or a
  field column. Underlying CSV errors are preserved as exception causes.
- Validation is deferred until iteration. Valid earlier records have already been
  delivered when a later record fails. Failure terminates this iterator; it does
  not roll back consumer side effects or offer a skip-and-resume mode.
- I/O errors and decoding errors propagate unchanged. Callers choose encoding;
  UTF-8 with a BOM requires an appropriate encoding such as `utf-8-sig` if the BOM
  should be removed. No encoding or dialect detection is performed.

Run `python streaming_csv.py` for a self-contained example:

```text
{'id': '1', 'note': 'hello, world'}
{'id': '2', 'note': 'two\nlines'}
record 4, physical line 5: expected 2 fields, got 1
inside caller context, closed: False
after caller context, closed: True
```

The tests include real files with CRLF and non-ASCII content, invalid headers,
unequal row widths, parser errors, quoted multiline fields, and early consumer
exit. An in-memory stream-position check verifies that requesting one record consumes only its
header and required physical lines, leaving later records unread by the parser.
Text I/O may still buffer bytes beneath that interface.

### Decisions and limits

`csv.reader` plus explicit width checks prevents silent truncation when building
dictionaries. `DictReader` normally represents missing or surplus fields rather
than rejecting their rows; this experiment chooses fail-fast validation instead.
Header validation prevents duplicate names from silently overwriting values.

The iterator retains the header and current record rather than accumulating all
records. A single large record or a consumer collecting results can still use
substantial memory, and the CSV parser's field-size limit still applies. A measured
comparison for a fixed-width dataset is linked below; throughput is unmeasured.
The iterator is intended for one
consumer; it does not introduce parallel processing, retries, or database writes.

Be ready to explain who closes the stream, why record numbers differ from physical
line numbers, and how partial success changes retry or transaction design.

Reference: [Python CSV documentation](https://docs.python.org/3/library/csv.html).

## Measurement: streaming versus eager CSV loading

Run isolated allocation measurements with:

```sh
python benchmark_csv.py run --sizes 1000 10000 100000 --repetitions 3
```

Each trial runs in a fresh interpreter and uses the same parser and aggregation.
The runner validates every output count and checksum. It reports peak traced
Python allocations, not total process RAM, and does not measure throughput.

See the [measured report](reports/csv-memory-2026-09-22.md) for the methodology,
environment, actual results, and limitations, plus [all raw trials](reports/csv-memory-2026-09-22.json).
At 100,000 fixed-width records, the measured peak was 64,046 bytes for streaming
and 36,133,599 bytes for eager loading. These observations apply to the tested
input and consumer; they are not general memory guarantees.

## Measurement: individual CSV record width

Run the width matrix with:

```sh
python benchmark_csv.py run --sizes 1000 --widths 16 1024 16384 65536 --repetitions 3
```

The [width report](reports/csv-width-2026-09-25.md) records actual results and
limitations, with [raw trials](reports/csv-width-2026-09-25.json). At 1,000 rows,
streaming peaked at 47,411 bytes for a 16-character note and 659,665 bytes for a
65,536-character note; eager loading peaked at 375,993 and 66,196,346 bytes. This
shows that streaming avoids cross-row retention but still pays for the current
logical record, parser buffers, and checksum aggregation. Here characters mean
Unicode code points; UTF-8 byte length differs for non-ASCII text. These peak
measurements do not isolate the allocation cost of each stage.

The benchmark keeps widths below the default CSV field-size limit. Boundary and
failure behavior are covered by the following tests.

## Failure contract: oversized CSV fields

Run the focused tests with:

```sh
python -m pytest tests/test_streaming_csv.py -q -k "limit or oversized"
```

Tests use a controlled limit of eight decoded Unicode code points per field.
They cover lengths seven, eight, and nine for ASCII, `é`, and quoted multiline
values. Eight is accepted; nine raises `CsvValidationError` with the original
`csv.Error` preserved in `__cause__`. UTF-8 byte length differs from this decoded
field length. Embedded newlines count toward the field; CSV quoting syntax does
not. The same limit applies to header fields.

The wrapper now says `CSV parsing error` rather than `CSV syntax error`, because
a well-formed field can exceed the configured limit. The exception class and
location attributes are unchanged. Callers should not depend on exact underlying
CSV error text, which comes from the Python runtime.

An oversized multiline field after a valid record demonstrates partial success:
the valid record remains delivered, the failed iterator terminates, and no later
record is emitted. `record_number` identifies the logical record including the
header; `line_number` identifies the last physical line supplied to the parser
when it detects failure, not necessarily the end of the rejected record. There
is no automatic retry, resynchronization, or rollback of consumer side effects.

The reader still leaves the input stream open. A caller's surrounding `with`
closes it when an error propagates, as verified with a real temporary UTF-8 file.

### Configuration ownership

`csv.field_size_limit()` is shared configuration for the CSV module in the Python
interpreter. It is not a constructor option for an individual reader. A test
starts two readers, changes the limit, and confirms that both observe the new
value when reading their next fields.

The application should choose a limit before parsing starts and keep it stable
while readers are active. A reusable generator should not temporarily change it
across yields: unrelated readers may run while that generator is suspended.
Restoring a setting later does not provide isolation for concurrent readers.
Different ingestion policies may require separate worker processes.

The test fixture saves the original value and restores it in `finally`, including
after failures. Tests using it assume serial execution within each interpreter;
they do not change application defaults or introduce a per-reader limit API.

### What this limit does not guarantee

A record can contain many individually valid fields. A test accepts a record
whose combined values exceed the per-field limit. Text I/O buffers and the
physical line supplied to the parser also consume memory before field validation,
and consumers can retain arbitrary numbers of records. This limit alone therefore
does not bound total record size, file size, process memory, or ingestion work.

The previous memory reports remain unchanged: these are deterministic boundary
tests, not new allocation measurements. This completes the CSV sequence's current
scope: execution, cleanup, validation, measured retention, and size-limit failures.

Reference: [csv.field_size_limit](https://docs.python.org/3/library/csv.html#csv.field_size_limit).

## Async experiment: TaskGroup failure propagation and cleanup

`taskgroup_failure.py` coordinates two workers with `asyncio.Event` objects, so
the experiment does not depend on sleeps or race-prone timing. Both workers begin
and wait for the same release event. The failing worker raises a `RuntimeError`;
`asyncio.TaskGroup` cancels the sibling, waits for its cleanup, and raises the
non-cancellation failure as an exception group.

Run it with:

```sh
python taskgroup_failure.py
```

The lifecycle is observable through events:

```text
failing:started, sibling:started
failing:raising, sibling:working
sibling:cancelled, sibling:cleanup, failing:cleanup
failures: ['failing failed']
```

The precise interleaving of the two initial workers may vary, but these facts are
invariants: both start before release; the sibling receives cancellation; both
cleanup blocks run; the cancellation is re-raised; and the original failure is
reported. `CancelledError` is a control signal used by structured concurrency,
not a normal success result. Swallowing it would prevent reliable cancellation
and can make a task group hang or misreport its state.

The tests use `asyncio.run`, coordination events, and a direct caller cancellation.
They do not use sleep-based timing or assert wall-clock behavior. The failing task
does not undo work already performed by its sibling; cancellation is cooperative
and reaches the sibling at its next await point. `finally` is the right place for
resource cleanup, but cleanup itself must be cancellation-aware.

This experiment does not demonstrate retries, timeouts, process termination,
thread safety, or transactional rollback. The next async experiment should test
caller cancellation of an entire task group.

## Caller cancellation of a TaskGroup

Run `python taskgroup_cancellation.py`. The coordinator waits until both workers
are running before cancelling their parent. Both children propagate cancellation
and complete their `finally` blocks before the caller observes `CancelledError`.
A normal-completion test also verifies that releasing the workers does not cancel
them. Cancellation requests shutdown; awaiting the parent observes its completion.
This models request or job cancellation, not process termination or rollback.

## TaskGroup deadlines

Run `python taskgroup_deadline.py`. After both workers start, the experiment
reschedules an `asyncio.timeout` deadline into the past. Expiration is delivered
on a subsequent event-loop iteration, without a sleep or a narrow timing window.
The deadline cancels its current task; cancellation propagates through the awaited
parent to the children. The caller catches `TimeoutError` outside the timeout
context only after the workers finish cleanup. The success test releases both
workers under a distant deadline. No elapsed-time assertion is involved.

The local suppression of the child's `CancelledError` in the ownership fallback
only drains that child; cancellation of the coordinator continues to propagate.
This experiment covers one cancellation request, not repeated cancellation during
cleanup or simultaneous worker failure and external cancellation.

Reference: [Python timeout contexts](https://docs.python.org/3.11/library/asyncio-task.html#timeouts).

## Asynchronous cleanup delays completion

`tests/test_taskgroup_async_cleanup.py` holds a worker inside an awaited cleanup
operation. For both caller cancellation and deadline expiration, the test proves
that the parent is still pending after cleanup begins. Only releasing the cleanup
gate lets the parent report `CancelledError` or `TimeoutError`. The gate is released
in a `finally` block so an assertion failure does not leave the worker blocked.

A timeout triggers cooperative cancellation; it is not a hard upper bound on
execution time. A stuck cleanup operation can delay shutdown indefinitely. Real
services must choose a cleanup policy and account for external resources and
process-level termination. These tests do not implement that policy or promise
that cleanup survives a second cancellation request.

Interview checkpoint: explain why cancelling a task and awaiting its termination
are separate steps, why cancellation should propagate after cleanup, and why a
request deadline alone cannot guarantee a bounded shutdown. A useful next experiment
would examine a second cancellation arriving while asynchronous cleanup is pending.

## Repeated cancellation during cleanup

Run `python repeated_cancellation.py`. The first direct cancellation enters the
worker's `finally` block. An event confirms the worker is awaiting cleanup before
the coordinator directly cancels that same worker again. The second cancellation
interrupts cleanup; entering `finally` does not guarantee finishing it. A control
test releases cleanup after only one cancellation and observes completion followed
by the original `CancelledError`. Both paths use event coordination, not sleeps.

This deliberately targets the worker itself. It does not claim that cancelling a
TaskGroup's parent twice is equivalent to cancelling a child twice. No external
resource is modified, and no rollback or process-termination guarantee is implied.

## Shielding and explicit task ownership

Run `python shielded_cleanup.py` to compare a plain await with `asyncio.shield`.
Cancelling the caller interrupts cleanup through a plain await. With shielding,
the caller still receives `CancelledError`, but cleanup remains pending. A separate
supervisor retains the cleanup task, releases it, and awaits its result. A failure
in cleanup reaches that supervisor rather than becoming an unobserved exception.

Shielding changes cancellation propagation, not ownership. It does not protect
against direct cancellation of the cleanup task, guarantee successful cleanup,
or bound shutdown time. This example assumes a supervisor that lives long enough
to join the task; it is not a general solution for repeated supervisor cancellation.
Retaining the task and observing its result are essential parts of the design.

Reference: [Shielding from cancellation](https://docs.python.org/3.11/library/asyncio-task.html#shielding-from-cancellation).

## Cleanup can fail too

`tests/test_taskgroup_cleanup_failure.py` starts three workers before releasing a
work failure. One cancelled sibling optionally raises `ValueError` in cleanup;
the other records successful cleanup. Tests verify that the group reports both
the original `RuntimeError` and the cleanup error, and waits for the remaining
sibling. The control case reports only the work error. Assertions compare exception
types and messages without depending on sibling execution or exception ordering.

The cleanup exception replaces cancellation as that sibling's outcome. TaskGroup
collects the non-cancellation failures into an `ExceptionGroup`; cancellation alone
is not added to that group. This preserves the failure from a different worker,
but does not prove that a cleanup exception can never mask an earlier exception
inside the same worker. The experiment covers a flat group and synchronous cleanup
failure, not nested groups or simultaneous external cancellation.

Interview checkpoint: distinguish entering cleanup from completing it; explain
who owns a shielded task, who observes its failure, and which errors a TaskGroup
reports. The next practical step is choosing an explicit shutdown policy for a
small background worker, including what to do when cleanup cannot finish.

## Queued worker shutdown

`python queued_worker.py` applies these cancellation concepts to one in-memory
worker. See [the shutdown contract](docs/worker_shutdown.md) for admission rules,
tests, and limitations.

`benchmark_worker.py` compares unlimited, bounded, and counts-only retention in
fresh processes. The [measured retention report](reports/worker-retention-2026-10-06.md)
includes reproduction steps, raw trials, current/peak traced allocations, and
measurement limitations.

The [payload-width report](reports/worker-payload-width-2026-10-08.md) varies
successful-job ID size and failed-job message size independently, showing why
limiting retained entry counts does not bound payload bytes.
