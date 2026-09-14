# Python System Lab

Small executable experiments exploring Python behavior, with tests and short
explanations. Experiments study generator execution, failure timing, and resource ownership.
This is a learning laboratory.

## Run locally

Requires Python 3.11 or newer. The experiments use only the standard library:

```sh
python generator_execution.py
python generator_cleanup.py
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

Next experiment: apply explicit resource ownership to a streaming CSV reader.
