# Worker allocations versus retained string width

Measured on 2026-10-08 in Asia/Tehran (the raw timestamps use UTC).
With 100 retained outcomes, increasing successful-job ID width from 13 to 4,096
ASCII characters raised median current traced allocations from 39,410 to 447,710
bytes. Increasing failed-job message width over the same range raised them from
59,510 to 467,810 bytes. A fixed history count therefore still permits larger
retained allocations when its strings grow.

## Reproduce

```sh
python benchmark_worker.py run --sizes 1000 --id-widths 13 128 1024 4096 --repetitions 3 --history-limit 100 --max-pending 16 --output reports/worker-id-width-local.json
python benchmark_worker.py run --sizes 1000 --error-widths 13 128 1024 4096 --repetitions 3 --history-limit 100 --max-pending 16 --output reports/worker-error-width-local.json
```

Preserved results: [ID series](worker-id-width-2026-10-08.json) and
[error-message series](worker-error-width-2026-10-08.json). Each contains 36
fresh-process trials, source hashes, environment metadata, checksums, counts,
retained character counts, and current/peak traced bytes. Policy order rotates
between repetitions. Report schema version 2 adds workload widths and retained
character counts; the earlier version 1 results remain unchanged. The original
command without width options still runs successful jobs with 13-character IDs.

## Method

- CPython 3.13.7, 64-bit, Windows 11 build 26200, `ProactorEventLoop`.
  Processor identifier: Intel64 Family 6 Model 154 Stepping 3.
- Every trial has 1,000 jobs, one producer, one worker, and capacity for 16
  waiting jobs. Policies retain all outcomes, the latest 100, or counts only.
- The ID series contains successful jobs. IDs start with `job-000000000` and
  their unique nine-digit index, then use `x` padding to reach the requested width.
- The error series uses 13-character IDs. Every handler invocation raises
  `ValueError` with a newly constructed message: the unique 13-character ID prefix
  followed by `e` padding. Messages are generated inside the handler, not shared
  from a preconstructed string. Even the 13-character message is a separate
  string from its ID. Tests verify distinct construction and exact lengths.
- Widths count ASCII characters, which also equal UTF-8 encoded bytes here.
  They exclude Python string-object overhead and say nothing about Unicode
  storage for other character sets.
- The handler hashes each ID plus newline. Failed workloads also hash the
  generated message plus newline. The controller independently hashes the source
  sequence; each trial checks processed count, checksum, successful/failed counts,
  retention counts, and retained ID/message character totals.
- Imports and event-loop creation precede tracing. The measured region includes
  worker/digest creation, ID generation, admission, processing, message generation,
  exception creation/catching, bookkeeping, shutdown, and report creation.
- Sampling occurs after shutdown while both worker and report remain alive.
  Current bytes include live measured-region allocations; peak covers temporary
  allocations up to sampling. Summary metadata and JSON serialization follow
  sampling. No forced garbage collection occurs.

## Successful-job ID series

Values are median traced bytes across three trials per pair. Full ranges are
available in the raw results; these values are observations rather than test
thresholds.

| ID width | Unlimited current | Latest 100 current | Counts-only current | Unlimited peak | Latest 100 peak | Counts-only peak |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 13 | 216,690 | 39,410 | 15,226 | 216,961 | 46,941 | 18,577 |
| 128 | 331,690 | 50,910 | 15,226 | 331,961 | 59,713 | 20,417 |
| 1,024 | 1,227,690 | 140,510 | 15,226 | 1,227,961 | 161,434 | 37,326 |
| 4,096 | 4,299,690 | 447,710 | 14,714 | 4,299,961 | 527,002 | 95,694 |

All 36 trials completed all 1,000 jobs. Retained outcome and identifier counts were
1,000, 100, and zero for unlimited, bounded, and counts-only policies respectively.

## Failed-job message series

IDs remain 13 characters; all 1,000 jobs fail normally and draining continues.
Outcome and identifier retention counts match the same three policies. Retained
message characters are respectively 1,000 times width, 100 times width, and zero.

| Message width | Unlimited current | Latest 100 current | Counts-only current | Unlimited peak | Latest 100 peak | Counts-only peak |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 13 | 420,834 | 59,510 | 15,226 | 421,105 | 67,313 | 18,849 |
| 128 | 535,834 | 71,010 | 14,714 | 536,105 | 78,651 | 18,891 |
| 1,024 | 1,431,834 | 160,610 | 14,714 | 1,432,105 | 170,820 | 21,476 |
| 4,096 | 4,503,834 | 467,810 | 15,226 | 4,504,105 | 487,236 | 30,692 |

## Interpretation and limits

Across both series, the 13-to-4,096 width increase adds 4,083,000 current traced
bytes to unlimited retention and 408,300 bytes to the 100-outcome policy in this
run. Those deltas match the extra ASCII payload retained in 1,000 and 100 strings.
Failed outcomes also retain `JobFailure` objects and error-type metadata, making
their baseline different from successful outcomes. These are observations for
this workload, not an isolated byte-size formula for the worker.

Counts-only current medians range from 14,714 to 15,226 bytes. Peaks grow because
strings must still be generated, queued or processed, encoded for hashing, and
temporarily held while exceptions unwind. In the ID series the bounded queue may
hold up to 16 large waiting IDs; in the error series messages are generated one
handler invocation at a time. The series therefore have different transient
allocation patterns. Small trial differences are preserved without assigning a
cause.

`tracemalloc` reports Python allocations traced after its start. It does not
establish total process memory, RSS, all native allocations, tracer overhead,
throughput, or allocations made before tracing. The benchmark uses only ASCII,
unique padded payloads, synchronous handler bodies, one producer, one interpreter,
and one final report. It does not measure traceback retention: the worker stores
exception type/message details rather than exception objects.

The engineering implication is that count limits and payload-size policies solve
different problems. A byte-oriented policy must also define how to measure input
size, reject oversized IDs before acceptance, and preserve useful failure details
when error messages are too large. No size restriction is introduced here. The
next step is deciding an explicit payload-size contract based on these results.

Reference: [Python tracemalloc documentation](https://docs.python.org/3.11/library/tracemalloc.html).
