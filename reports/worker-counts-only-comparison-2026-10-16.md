# Before and after counts-only diagnostic removal

Measured on 2026-10-16 in Asia/Tehran. In counts-only mode with 4,096-character
messages and no message cap, median peak traced allocations fell from 34,893 to
30,756 bytes after skipping failure formatting and `JobFailure` construction.
With a zero-character cap at the same source width, median peak bytes stayed at
30,756. Current traced allocations showed no consistent reduction across cases.
Removing work therefore produced a workload-dependent peak change in this run,
not a general memory or throughput guarantee.

## Reproduce

```sh
python compare_worker_versions.py --before 9fd869a3d7d036b3cf3c76825a9f2abfcab50e81 --after ad05adc71e180e31e04064350d4a373a6d9dfb37 --jobs 1000 --error-widths 13 128 4096 --message-caps none 0 128 --repetitions 4 --history-limit 100 --max-pending 16 --output reports/worker-counts-only-comparison-local.json
```

Both commits must exist in the local Git object database; a shallow checkout may
need additional history fetched before running this command. The controller
exports only `benchmark_worker.py` and `queued_worker.py` into newly created
temporary directories. It does not switch branches or copy uncommitted changes.
Those directories are removed after the comparison, including on failure.

The [raw results](worker-counts-only-comparison-2026-10-16.json) preserve all 216
trials, full commit IDs, exported source hashes, controller/dependency hashes,
environment metadata, and per-trial correctness checks. Comparison schema 1 wraps
the existing measurement schema 3. Exported source hashes cover exact Git blob
bytes; controller hashes cover local file bytes, whose line endings can differ.
The controller requires identical committed benchmark code in both revisions,
matching its local validator source after newline decoding.

## Controlled comparison

The worker source differs by only these three lines, before rendering a failure:

```python
if self._history.maxlen == 0:
    self._record("failed", job)
    continue
```

The benchmark source has the same SHA-256 in both exports:
`5a0bd3c628886de7125f9492d14794aa75028913efa479da53a3444c8d9caccd`.
Full and 100-outcome histories exercise the retained-diagnostic path as controls.
The baseline was rerun in this experiment; older October 14 measurements were
not used as its before values.

- Environment: CPython 3.13.7, 64-bit Windows 11 build 26300,
  `ProactorEventLoop`; Intel64 Family 6 Model 154 Stepping 3.
- Every trial processes 1,000 failing jobs with 13-character IDs, one producer,
  one worker, and capacity for 16 waiting jobs. Message widths are 13, 128, and
  4,096 ASCII characters; caps are unrestricted, zero, and 128 characters.
- Each workload/policy/cap pair runs before and after in adjacent fresh Python
  processes. Version order reverses on alternate repetitions: each version runs
  first twice across four repetitions. Cap and policy order also rotate.
- All 216 trials accepted and failed all 1,000 jobs. Both versions matched the
  full generated-source checksum, processed count, lifetime outcome counts,
  retained ID/detail counts, omitted outcomes, retained text length, and retained
  truncation count. Counts-only trials retained zero details and omitted 1,000.
- Each handler constructs its full message and hashes the ID and message before
  raising `ValueError`. The after version skips diagnostics only inside the
  worker; source generation and checksum work are unchanged.
- Imports and event-loop creation precede tracing. Sampling follows shutdown
  with both stopped worker and report alive and no forced garbage collection.
  The measured region includes generation, hashing, exception handling,
  bookkeeping, and report creation. Summary metadata and serialization follow
  sampling. This experiment measures current/peak traced Python bytes, not RSS.

## Counts-only results

Values are median traced bytes across four trials per version and case. All
individual values and ranges remain in the raw results. No timing metric was
collected, and no measurement is used as a test threshold.

| Source message characters | Cap | Before current | After current | Before peak | After peak |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 13 | none | 14,746 | 15,258 | 18,967 | 18,913 |
| 13 | 0 | 15,002 | 15,002 | 18,913 | 18,913 |
| 13 | 128 | 15,514 | 15,002 | 18,967 | 18,913 |
| 128 | none | 14,746 | 15,258 | 18,609 | 18,952 |
| 128 | 0 | 15,258 | 15,002 | 18,971 | 18,952 |
| 128 | 128 | 15,258 | 15,258 | 19,121 | 18,952 |
| 4,096 | none | 15,002 | 15,514 | 34,893 | 30,756 |
| 4,096 | 0 | 15,258 | 15,770 | 30,756 | 30,756 |
| 4,096 | 128 | 16,282 | 15,002 | 30,925 | 30,756 |

For unrestricted 4,096-character messages, before peak bytes were 34,893 in all
four trials. After peak bytes ranged from 29,732 to 30,756. The median difference
was 4,137 bytes. At the same width, cap zero had identical before/after peak
values, and cap 128 had a median difference of 169 bytes.

The effect was not uniformly favorable. At width 128 without a cap, median peak
bytes increased by 343; before/after ranges overlapped. Counts-only current
allocations ranged from 14,490 to 16,282 across both versions, with median changes
in both directions. These observations do not support a consistent reduction in
post-shutdown retained allocations.

## Retained-history controls

Current medians and individual current values matched between versions for every
full-history and 100-outcome case. Full-history peak values also matched in every
case. The 100-outcome peak medians differed by at most 181 bytes at source widths
13 and 128; at width 4,096, all peak values matched. The selected controls below
show that large-message case. Values are identical before and after.

| Policy | Cap | Current bytes | Peak bytes |
| :--- | ---: | ---: | ---: |
| All outcomes | none | 4,519,458 | 4,519,729 |
| All outcomes | 0 | 382,458 | 386,827 |
| All outcomes | 128 | 551,458 | 555,658 |
| Latest 100 | none | 469,442 | 488,492 |
| Latest 100 | 0 | 55,742 | 74,792 |
| Latest 100 | 128 | 72,642 | 91,692 |

## Interpretation and next experiment

The implementation and regression tests establish that counts-only failures no
longer invoke exception formatting or construct `JobFailure` details. The matched
experiment establishes the observed allocation differences for this workload.
These are separate claims: removing an operation does not establish how much it
contributed to the peak, or whether it improved execution time.

Before the change, the worker's local `message` could remain bound between loop
iterations. That is a plausible contributor to the larger unrestricted-message
peak, since the next handler constructs and hashes another full message before
the worker replaces the previous diagnostic text. This is an inference from the
source, not a stage-by-stage allocation measurement. The data does not attribute
the difference solely to formatting or to `JobFailure` objects.

Four repetitions and balanced version order do not establish statistical
confidence or remove all environmental variation. `tracemalloc` excludes
allocations made before tracing, does not cover all native memory or tracer
overhead, and does not measure process RSS. The experiment uses one interpreter,
ASCII payloads, synchronous handler bodies, and preconstructed exception messages.
It does not measure expensive custom `__str__` implementations or throughput.

A useful follow-up is a separate lazy-formatting workload that constructs a message
inside `__str__`, with formatting-call counts and validation that does not itself
force discarded diagnostic text to be generated. That would explore a different
failure workload without treating these results as a universal optimization claim.
