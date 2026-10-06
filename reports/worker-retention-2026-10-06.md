# Queued worker allocations by retention policy

Measured on 2026-10-06. As successful job count increased from 1,000 to 50,000,
unlimited history's median current traced allocations rose from 216,690 to
10,426,466 bytes. With history limited to 100 outcomes, the corresponding values
were 39,410 and 38,882 bytes. Counts-only retention used 15,346 and 14,306 bytes.
This supports the retention design for this workload and interpreter; it is
not a measurement of total process memory or proof of a universal byte bound.

## Reproduce

```sh
python benchmark_worker.py run --sizes 1000 10000 50000 --repetitions 3 --history-limit 100 --max-pending 16 --output reports/worker-retention-local.json
```

The [raw results](worker-retention-2026-10-06.json) preserve all 27 trials,
checksums, lifetime counts, retention counts, process IDs, source-file SHA-256
hashes, timestamps, and environment metadata. Each trial launches a fresh
interpreter. Policy order rotates between repetitions.

## Method

- CPython 3.13.7, 64-bit, Windows 11 build 26200; `ProactorEventLoop`.
  The recorded processor identifier is Intel64 Family 6 Model 154 Stepping 3.
- One producer, one worker, maximum 16 waiting jobs. Policies: unlimited history,
  100 retained outcomes, and counts-only (`history_limit=0`).
- IDs are generated incrementally as `job-000000000` through the final index.
  These tested IDs are 13 ASCII characters. No input list is retained.
- The handler hashes each ID followed by an ASCII newline and increments a
  processing count. It performs no I/O, waits, or external side effects.
- Expected checksums are independently computed over the source sequence in
  the controller, outside the measured child process.
- Imports and event-loop creation precede `tracemalloc.start(1)`. Tracing includes
  digest and worker creation, ID generation, admission, event notifications,
  processing, bookkeeping, shutdown, and report construction.
- Current and peak bytes are sampled after shutdown while **both the stopped
  worker and its report remain alive**, along with the digest and local variables.
  The measurement record and JSON serialization happen after sampling. No forced
  garbage collection is performed.
- Every trial validates processed count, checksum, all outcome counts, retained
  IDs and details, omitted details, and consistency of current versus peak bytes.
  All accepted jobs completed; no trial failed, interrupted, or discarded work.

## Results

Bytes below are traced Python allocations. Median is across three fresh-process
trials; ranges show minimum to maximum. These are observations, not CI thresholds.

| Jobs | History policy | Current median | Current range | Peak median | Peak range |
| ---: | :--- | ---: | :--- | ---: | :--- |
| 1,000 | Unlimited | 216,690 | 216,690–216,690 | 216,961 | 216,961–216,961 |
| 1,000 | Latest 100 | 39,410 | 39,410–39,410 | 46,995 | 46,833–47,303 |
| 1,000 | Counts only | 15,346 | 14,834–15,346 | 18,577 | 18,577–18,577 |
| 10,000 | Unlimited | 2,203,602 | 2,203,602–2,203,602 | 2,222,825 | 2,222,825–2,222,825 |
| 10,000 | Latest 100 | 38,882 | 38,882–38,882 | 47,303 | 47,303–47,303 |
| 10,000 | Counts only | 14,818 | 14,306–15,842 | 18,759 | 18,759–18,759 |
| 50,000 | Unlimited | 10,426,466 | 10,426,466–10,426,466 | 10,501,553 | 10,501,553–10,501,553 |
| 50,000 | Latest 100 | 38,882 | 38,882–38,882 | 47,303 | 47,303–47,303 |
| 50,000 | Counts only | 14,306 | 14,306–14,818 | 18,759 | 18,759–18,759 |

Unlimited retention kept one detail and identifier per processed job. The bounded
policy retained exactly 100 of each; counts-only retained zero. Lifetime accepted
and completed counts matched 1,000, 10,000, and 50,000 in every policy.

## Interpretation and limits

Unlimited history retains IDs, outcome objects, the duplicate-detection set, and
the report's references to all completed IDs. The observations are consistent
with memory growing with job count. Bounded and counts-only measurements stayed
near their initial scale over the tested range. Current traced bytes include
other live workload allocations, so they are not an isolated worker-object size.
Small differences across counts/trials are recorded without attributing a cause.

`tracemalloc` tracks Python allocations made after tracing starts. These results
do not report RSS, working set, all native allocations, tracer overhead, or
memory allocated before tracing. They do not measure throughput or compare
untraced runtimes. This benchmark keeps a single final report alive; applications
that retain many reports have a different memory profile.

Only successful jobs with short IDs are measured. Large IDs, error messages,
blocked handlers, multiple producers, different queue capacities, and other
interpreters may behave differently. The fixed retention bounds reference counts;
it does not bound payload bytes, producer task counts, or the size of lifetime
integer counters.

Bounded history also changes duplicate protection: evicted IDs may be accepted
again. It is appropriate for limited diagnostics, not a durable audit or
idempotency guarantee. The next measurement should vary ID and error-message
length while holding job count and retention policy fixed.

Reference: [Python tracemalloc documentation](https://docs.python.org/3.11/library/tracemalloc.html).
