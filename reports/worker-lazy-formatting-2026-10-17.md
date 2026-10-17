# Diagnostic construction timing and worker retention

Measured on 2026-10-17 in Asia/Tehran. In counts-only mode, lazy exceptions
generated no diagnostic text across 1,000 failing jobs. Prebuilt exceptions with
4,096-character messages generated 4,096,000 diagnostic characters, despite
neither mode's `__str__` being called. Median peak traced allocations were 19,241
bytes for lazy construction and 27,092 for prebuilt construction at that width,
under each tested cap. These are results for an instrumented synthetic workload,
not a general throughput or total-memory guarantee.

With retained history, both modes formatted all 1,000 failures, including with a
zero-character cap. Deferring construction avoided work only when the consumer
did not request the diagnostic text.

## Reproduce

```sh
python benchmark_error_formatting.py run --jobs 1000 --widths 13 128 4096 --caps none 0 128 --repetitions 4 --history-limit 100 --max-pending 16 --output reports/worker-lazy-formatting-local.json
```

For a single isolated trial:

```sh
python benchmark_error_formatting.py worker 1000 lazy counts-only --width 4096 --cap 0 --history-limit 100 --max-pending 16
```

The [raw results](worker-lazy-formatting-2026-10-17.json) contain all 216 trials,
source hashes, environment metadata, formatting-call and generated-character
counts, retained-text verification counts, and current/peak traced bytes. The
formatting report uses its own schema version 1 with nested measurement records.
Source hashes cover local file bytes, including their line endings.

This experiment is separate from `benchmark_worker.py` and
`compare_worker_versions.py`. Those files and the worker were left unchanged;
the earlier committed-source comparison remains reproducible. This run compares
two message-construction modes using the current worker, not two worker versions.

## Construction model

Both modes use the same `DiagnosticError` class and attribute shape, with empty
exception arguments. Each error holds the job ID, requested width, a shared
counter object, and either a prebuilt string or `None`.

- Prebuilt mode constructs its string in the exception constructor. `__str__`
  returns that string without constructing it again.
- Lazy mode constructs its string only when `__str__` runs. It does not cache the
  result; the worker requests it once for each failure whose details it retains
  at recording time.
- Both modes build the same ASCII content: a unique 13-character job prefix plus
  `e` padding. Counters record `__str__` calls and cumulative characters returned
  by workload message construction. Generated characters are not simultaneous
  live bytes, allocation counts, or an estimate of process memory.

At width 4,096, every trial produced these counts, independently of cap:

| Retention policy | Formatting calls, either mode | Prebuilt generated characters | Lazy generated characters |
| :--- | ---: | ---: | ---: |
| All outcomes | 1,000 | 4,096,000 | 4,096,000 |
| Latest 100 | 1,000 | 4,096,000 | 4,096,000 |
| Counts only | 0 | 4,096,000 | 0 |

Finite history formats a failure before its eventual eviction. Retaining only
the latest 100 outcomes therefore does not reduce construction to 100 messages.
A zero-character cap still needs formatting to determine diagnostic flags when
details are retained. Counts-only retention avoids that request entirely.

## Method and validation boundaries

- CPython 3.13.7, 64-bit Windows 11 build 26300, `ProactorEventLoop`;
  Intel64 Family 6 Model 154 Stepping 3.
- Each trial has 1,000 jobs, one producer, one worker, and capacity for 16 waiting
  IDs. Widths are 13, 128, and 4,096 ASCII characters; caps are unrestricted,
  zero, and 128. Policies retain all outcomes, the latest 100, or counts only.
- Every mode/case runs in a fresh process. Modes run in adjacent pairs and reverse
  order on alternate repetitions. Each mode runs first twice across four
  repetitions. Cap and policy order rotate too.
- Each handler hashes the compact descriptor `job-ID|requested-width` plus newline,
  then raises its exception. The parent independently hashes that input sequence
  without constructing diagnostic payloads. This checksum verifies ordered inputs,
  not the content of diagnostic strings that were never generated.
- All 216 trials processed, accepted, and failed all 1,000 jobs. The parent
  validates counts, checksum, retained IDs/details, omitted outcomes, retained
  character totals, truncation counts, formatting calls, generated characters,
  and the number of retained messages checked.
- After sampling, the child verifies each retained failure's job ID, exception
  type, expected prefix, and absence of a formatting-failure flag. It constructs
  expected text only for retained records and does not invoke exception methods.
  Counts-only mode performs no such construction. Tests replace the builder with
  a function that fails if called to enforce that property. Other tests reject
  corrupt retained text and forged formatting telemetry.
- Imports and event-loop creation precede tracing. The measured region includes
  the counter object and its integer updates, worker creation, compact-descriptor
  hashing, message/exception construction, processing, bookkeeping, shutdown, and
  report creation. Sampling keeps stopped worker and report alive without forced
  garbage collection. Retained-text verification, summary construction, and JSON
  serialization follow sampling and are excluded from recorded current/peak
  values. Verification's expected-text allocations are also excluded from the
  workload's generated-character counter.

Unlike the older payload-width workload, this handler does not hash full messages.
Direct numeric comparisons to those older reports would conflate different
validation work and exception models.

## Counts-only allocations

Values are median traced bytes across four trials per mode and case. Raw trials
preserve all ranges. These values are observations, not test thresholds.

| Source width | Cap | Prebuilt current | Lazy current | Prebuilt peak | Lazy peak |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 13 | none | 15,618 | 15,074 | 19,273 | 19,241 |
| 13 | 0 | 15,618 | 15,586 | 19,273 | 19,241 |
| 13 | 128 | 15,106 | 15,586 | 18,249 | 19,177 |
| 128 | none | 15,362 | 15,330 | 18,848 | 19,177 |
| 128 | 0 | 15,618 | 15,586 | 19,360 | 19,241 |
| 128 | 128 | 15,618 | 15,842 | 19,360 | 19,241 |
| 4,096 | none | 15,618 | 15,074 | 27,092 | 19,241 |
| 4,096 | 0 | 15,362 | 16,098 | 27,092 | 19,241 |
| 4,096 | 128 | 16,130 | 15,842 | 27,092 | 19,241 |

For unrestricted 4,096-character messages, prebuilt peak bytes ranged from 26,068
to 27,092 and lazy peak bytes from 19,241 to 19,423. The median gap was 7,851 bytes.
At width 13 with cap 128, however, lazy median peak bytes were higher by 928. Small
cases do not establish a uniform reduction. Current medians also move in both
directions; this run does not establish consistently lower retained allocations.

## Retained-history allocations at width 4,096

Current values below matched between modes in every trial. Both modes generated
and formatted all 1,000 messages. Peak columns show medians.

| Policy | Cap | Current, either mode | Prebuilt peak | Lazy peak |
| :--- | ---: | ---: | ---: | ---: |
| All outcomes | none | 4,468,850 | 4,469,121 | 4,469,121 |
| All outcomes | 0 | 331,850 | 332,246 | 332,350 |
| All outcomes | 128 | 500,850 | 501,121 | 501,181 |
| Latest 100 | none | 464,734 | 479,728 | 479,832 |
| Latest 100 | 0 | 51,034 | 66,028 | 66,132 |
| Latest 100 | 128 | 67,934 | 82,928 | 83,032 |

Deferral alone did not remove payload generation when diagnostics were consumed.
The 100-outcome zero-cap case generated 4,096,000 characters in either mode while
retaining no message characters. A retention policy and a truncation policy govern
different stages of that work.

## Interpretation and limits

The construction counters establish that lazy counts-only jobs did not build
their diagnostic messages. Allocation measurements describe the effect of that
choice alongside the rest of this instrumented workload. The exception object,
IDs, queue, descriptor hashing, counters, and worker bookkeeping still allocate.
Counter updates themselves have costs; these results do not isolate a pure
formatting cost or imply zero allocation in lazy mode.

The four repetitions and balanced mode order do not establish statistical
confidence or a general performance formula. `tracemalloc` does not establish
process RSS, all native allocations, tracer overhead, throughput, or allocations
made before tracing. No elapsed-time or throughput metric was collected. The
experiment covers ASCII payloads, synchronous handlers, one interpreter, and a
synthetic formatter without I/O, caching, or external side effects.

The engineering decision is whether diagnostic details are required. Lazy
construction can avoid their cost when the chosen consumer never requests them;
retaining even an empty diagnostic still requires the worker's formatting checks.
Durable audit requirements and recovery from process failure remain separate from
these in-memory retention choices.
