# Worker allocations with retained error-message caps

Measured on 2026-10-14 in Asia/Tehran (the raw timestamp uses UTC).
For 4,096-character failure messages and a history of 100 outcomes, a
128-character cap reduced median current traced allocations from 468,626 to
71,826 bytes. Median peak allocations fell from 487,860 to 91,060 bytes.
The cap reduced retained text, but larger source messages still raised the peak:
with that same cap, 128-character source messages had a peak median of 80,007
bytes while retaining the same 71,826 current bytes.

## Reproduce

```sh
python benchmark_worker.py run --sizes 1000 --error-widths 13 128 4096 --error-message-caps none 0 128 --repetitions 3 --history-limit 100 --max-pending 16 --output reports/worker-error-caps-local.json
```

The [raw results](worker-error-caps-2026-10-14.json) preserve all 81 trials,
including environment metadata, source hashes, processing checksums, counts,
configured caps, retained message lengths, retained truncation counts, and
current/peak traced bytes. Schema version 3 adds the cap matrix and per-trial cap
and truncation metadata. Existing benchmark commands still default to unrestricted
messages. Earlier result files remain unchanged.

For a single isolated trial:

```sh
python benchmark_worker.py worker 1000 bounded --error-width 4096 --max-error-message-chars 128 --history-limit 100 --max-pending 16
```

## Method and correctness

- CPython 3.13.7, 64-bit, Windows 11 build 26300, `ProactorEventLoop`.
  Processor identifier: Intel64 Family 6 Model 154 Stepping 3.
- Every trial processes 1,000 jobs with 13-character IDs, one producer, one worker,
  and capacity for 16 waiting jobs. Retention policies keep all outcomes, the
  latest 100, or counts only. Each handler invocation raises `ValueError`; failures
  do not stop draining.
- Messages have 13, 128, or 4,096 ASCII characters. Each is separately constructed
  inside the handler from the unique ID prefix plus padding, rather than sharing
  a preconstructed payload. Caps are unrestricted (`none`), zero, and 128.
  The character policy also equals payload-byte length for these ASCII strings;
  it does not establish Unicode object sizes.
- One fresh interpreter runs each trial. Cap order and retention-policy order
  rotate between three repetitions. The unrestricted baseline and capped cases
  use the same current source and environment; these comparisons do not use the
  older payload-width report's baseline.
- The handler hashes each ID and its full generated message before raising.
  The parent independently computes the expected source checksum, which is
  unchanged by the diagnostic cap. Every trial validates processed count,
  lifetime outcome counts, retained IDs/details, omitted outcomes, retained
  message characters, and retained truncation counts.
- All 81 trials accepted and failed all 1,000 jobs. Retained outcomes and IDs
  numbered 1,000, 100, and zero respectively. For a finite cap C, retained message
  characters equal retained failures times `min(source width, C)`. Flags are set
  only when source width exceeds C: a width of 128 at cap 128 is not truncated.
  Counts-only reports contain neither message details nor truncation flags;
  their truncation count is zero, not a lifetime count of shortened messages.
- Imports and event-loop creation precede tracing. The traced region includes
  worker creation, ID/message generation, admission, processing, full-message
  hashing, exception handling, retention bookkeeping, shutdown, and report
  creation. Sampling keeps both stopped worker and report alive, with no forced
  garbage collection. Retained-text/flag summaries and JSON serialization happen
  after sampling. Source hashes identify raw local file bytes; line endings
  affect these hashes.

## Current allocations

Values are median traced bytes across three trials per case. Full ranges are
preserved in the raw results; these observations are not test thresholds.

| Source message characters | Cap | All outcomes | Latest 100 | Counts only |
| ---: | ---: | ---: | ---: | ---: |
| 13 | none | 428,642 | 60,326 | 15,242 |
| 13 | 0 | 374,642 | 54,926 | 14,730 |
| 13 | 128 | 428,642 | 60,326 | 16,266 |
| 128 | none | 543,642 | 71,826 | 14,730 |
| 128 | 0 | 374,642 | 54,926 | 14,730 |
| 128 | 128 | 543,642 | 71,826 | 15,242 |
| 4,096 | none | 4,511,642 | 468,626 | 15,242 |
| 4,096 | 0 | 374,642 | 54,926 | 15,242 |
| 4,096 | 128 | 543,642 | 71,826 | 15,242 |

## Peak allocations

Values are median traced bytes over the same trials.

| Source message characters | Cap | All outcomes | Latest 100 | Counts only |
| ---: | ---: | ---: | ---: | ---: |
| 13 | none | 428,913 | 68,091 | 18,935 |
| 13 | 0 | 374,913 | 62,745 | 17,857 |
| 13 | 128 | 428,913 | 68,199 | 18,935 |
| 128 | none | 543,913 | 80,007 | 19,089 |
| 128 | 0 | 374,913 | 62,637 | 18,931 |
| 128 | 128 | 543,913 | 80,007 | 19,104 |
| 4,096 | none | 4,511,913 | 487,860 | 34,861 |
| 4,096 | 0 | 379,003 | 74,160 | 30,724 |
| 4,096 | 128 | 547,834 | 91,060 | 30,893 |

## Interpretation and limits

At cap 128, increasing message width from 128 to 4,096 left current allocations
unchanged for both full and 100-outcome retention. Keeping fewer characters does
not eliminate failure objects, IDs, exception-type names, history, or bookkeeping.
Even the zero cap retained 54,926 current bytes in the 100-outcome case. Full
history still grows with job count despite a message cap; combining finite history
and a payload policy addresses separate sources of retained allocation.

Peak allocations remain sensitive to the source message. In the 100-outcome case,
the 128 cap held current bytes fixed but peak bytes rose from 80,007 to 91,060
between source widths 128 and 4,096. With counts only and cap 128, the same width
change raised median peak bytes from 19,104 to 30,893. Full messages are still
generated, hashed with temporary concatenation/encoding, and carried by exceptions
before the worker slices retained details. This workload does not isolate the
allocation cost of `str(error)` or truncation alone.

Counts-only current medians range from 14,730 to 16,266 bytes. Small trial
differences remain in the raw data without assigning them a cause. The three
repetitions and rotating order are useful controls, not statistical confidence
or proof of a universal memory formula.

`tracemalloc` records Python allocations traced after its start. These results do
not establish process RSS, all native allocations, tracer overhead, throughput,
or total memory. They cover ASCII payloads, synchronous handler bodies, one
producer, one interpreter version, and one final report. They do not measure
traceback retention or pathological exception formatting: the worker stores
type/message details rather than exception objects, and `__str__` failures remain
outside its reporting contract.

Interview checkpoint: explain why a cap can reduce both current and peak bytes
without making either a total-memory guarantee, why the baseline must share the
same source, and why retained truncation counts differ from lifetime failure
counts. A useful follow-up is a concise case study connecting admission,
retention, and payload policies to their measured limits.
