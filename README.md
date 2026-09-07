# Python System Lab

Small executable experiments exploring Python behavior, with tests and short
explanations. The first experiment studies generator execution and failure timing.
This is a learning laboratory.

## Run locally

Requires Python 3.11 or newer. The experiment uses only the standard library:

```sh
python generator_execution.py
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

Next experiment: resource cleanup when a consumer stops iteration early. This
experiment opens no database connections or files and makes no claims about
concurrency, database transactions, or resource cleanup.
