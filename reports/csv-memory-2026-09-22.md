# CSV streaming versus eager loading: traced allocation peaks

Measured on 2026-09-22. Across 1,000 to 100,000 records, streaming peak traced
allocations ranged from 55,714 to 64,046 bytes; eager loading grew from 394,467 to
36,133,599 bytes. This result supports incremental consumption for this workload.
It is not a measurement of total process RAM or a general CSV performance claim.

## Reproduce

From the repository root, with Python 3.11 or newer:

```sh
python benchmark_csv.py run --sizes 1000 10000 100000 --repetitions 3
```

The command prints JSON. Add `--output reports/csv-memory-local.json` to save a
new result without overwriting this run. Temporary CSV files are removed after
the trials. The runner uses only the standard library and the existing parser.

The preserved [raw results](csv-memory-2026-09-22.json) contain all 18 trials,
input file hashes, expected output checksums, worker process IDs, environment
details, and the UTC completion timestamp. The parsing implementation is the one
published in commit `cd1c4b6`; the benchmark driver is committed with this report.

## Environment and input

- CPython 3.13.7, 64-bit, MSC v.1944, AMD64.
- Windows 11, build 26200.
- Intel Core i7-12700H, 20 logical processors.
- OS-reported physical memory: 68,430,585,856 bytes (about 63.73 GiB).
- Three columns: a zero-padded nine-digit ID, a three-digit amount cycling from
  000 to 999, and the repeated note `alpha, "beta"\ncafé` (with a real embedded
  newline). The note exercises quoting, non-ASCII text, and multiline records.
- UTF-8, LF record separators; generated files are 38,015, 380,015, and 3,800,015
  bytes respectively. Field widths are fixed over these tested sizes.

## Method

1. The parent writes each dataset to disk and calculates its expected record count
   and SHA-256 output checksum from the source records, outside measurement.
2. Each trial launches a new Python interpreter. Module imports complete before
   tracing starts. Workers run sequentially, with a 180-second timeout per trial.
3. `tracemalloc.start(1)` begins immediately before opening the input. Its peak
   includes file opening, decoding, parsing, dictionary creation, checksum
   aggregation, and closure. Process startup and dataset generation are excluded.
4. Streaming passes `read_records(stream)` directly to the aggregation function.
   Eager mode first builds `list(read_records(stream))` and retains that list
   through aggregation and peak collection. Both process the complete file.
5. The same aggregation counts records and hashes every field in record order,
   using compact UTF-8 JSON records separated by LF. The parent rejects a trial
   whose count or checksum differs from the independently generated expectation.
6. Each size/mode pair runs three times. Order alternates between streaming-first
   and eager-first across repetitions. Fresh processes isolate Python allocation
   state; they do not clear the operating system's file cache.

## Results

Values below are medians of three fresh-process trials. In this run, the minimum,
median, and maximum were identical for each size/mode pair; raw values are retained
so that this observation can be checked rather than inferred from rounding.

| Records | Input bytes | Streaming peak bytes | Eager peak bytes | Streaming KiB | Eager MiB |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 38,015 | 55,714 | 394,467 | 54.41 | 0.376 |
| 10,000 | 380,015 | 64,044 | 3,647,789 | 62.54 | 3.479 |
| 100,000 | 3,800,015 | 64,046 | 36,133,599 | 62.54 | 34.460 |

KiB = 1,024 bytes; MiB = 1,048,576 bytes. All 18 counts and checksums matched the
expected values. At 100,000 records, eager peak traced allocations were about 564
times streaming's peak for this particular input and consumer.

## Interpretation and limits

The eager consumer retains every parsed dictionary and its values. The streaming
consumer retains the header, current parsing/aggregation state, and transient
records rather than a collection of all rows. Between 10,000 and 100,000 records,
the measured streaming peak changed by two bytes while eager allocations grew
roughly with record count. The smaller-input difference has not been attributed
to specific allocations; this experiment records peaks, not allocation snapshots.

The useful conclusion is about **consumer retention**: a generator API provides
no memory benefit if the caller immediately collects all its results. Both modes
here use the exact same parser and validation logic.

These numbers come from `tracemalloc`, which tracks Python memory allocations.
They are not RSS, working set, committed memory, or total native memory. Existing
allocations before tracing, interpreter baseline, operating-system caches, and
tracer bookkeeping are outside the reported metric. No timings were recorded;
the tracing and JSON checksum work would also complicate throughput comparisons.

The synthetic data repeats a short payload and uses a fixed number of fields.
Wider records, more columns, different text, huge fields, another Python version,
or a retaining consumer can change the result. The parser's field-size limit is
unchanged. This is evidence of nearly flat traced allocations over these tested
row counts, not proof of bounded memory for arbitrary CSV input. All data is valid;
this benchmark does not measure error recovery or database ingestion.

Tests verify deterministic data generation, full-field/order-sensitive checksums,
equivalent results, report structure, CLI validation, and cleanup of tracing after
parser failure. CI runs only small correctness trials; it has no machine-dependent
memory-ratio or timing assertions.

Before using these results in an interview, explain why equal work and fresh
workers matter, what the traced peak excludes, and why fixed row width is a key
assumption. Next, vary individual record width to test the limit of the current
finding.

Reference: [Python tracemalloc documentation](https://docs.python.org/3/library/tracemalloc.html).
