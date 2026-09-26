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
- Syntax and structure failures raise `CsvValidationError`, with `record_number`
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

The benchmark keeps widths below the default CSV field-size limit. Next experiment:
test that limit and document the failure contract for oversized fields.
