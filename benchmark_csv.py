"""Compare peak traced allocations in fresh streaming and eager CSV workers."""

import argparse
import csv
import hashlib
import json
import os
import platform
import subprocess
import sys
import tracemalloc
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Literal

from streaming_csv import read_records

Mode = Literal["streaming", "eager"]


@dataclass(frozen=True)
class Summary:
    count: int
    checksum: str


@dataclass(frozen=True)
class Measurement:
    mode: Mode
    count: int
    checksum: str
    peak_bytes: int
    pid: int


def summarize(records: Iterable[dict[str, str]]) -> Summary:
    """Hash every field in record order without retaining the records."""
    digest = hashlib.sha256()
    count = 0
    for record in records:
        encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )
        digest.update(encoded + b"\n")
        count += 1
    return Summary(count, digest.hexdigest())


def generate_dataset(path: Path, rows: int) -> Summary:
    """Write deterministic UTF-8 CSV, outside workers' measurement windows."""
    if rows < 0:
        raise ValueError("rows must be nonnegative")

    def records() -> Iterable[dict[str, str]]:
        for index in range(rows):
            yield {
                "id": f"{index:09d}",
                "amount": f"{index % 1000:03d}",
                "note": 'alpha, "beta"\ncafé',
            }

    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["id", "amount", "note"], lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(records())
    # Compute an expected result from source records, independently of CSV parsing.
    return summarize(records())


def measure(path: Path, mode: Mode) -> Measurement:
    """Trace file opening, parsing, aggregation and closure, excluding imports."""
    if mode not in ("streaming", "eager"):
        raise ValueError("unknown measurement mode")
    if tracemalloc.is_tracing():
        raise RuntimeError("measurement requires tracing to be initially disabled")
    tracemalloc.start(1)
    try:
        with path.open(encoding="utf-8", newline="") as stream:
            records: Iterable[dict[str, str]] = read_records(stream)
            if mode == "eager":
                records = list(records)
            summary = summarize(records)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    return Measurement(mode, summary.count, summary.checksum, peak, os.getpid())


def run_benchmark(sizes: list[int], repetitions: int) -> dict[str, object]:
    """Generate inputs once, then launch one fresh interpreter per trial."""
    if not sizes or any(size <= 0 for size in sizes) or repetitions <= 0:
        raise ValueError("sizes and repetitions must be positive")
    trials: list[dict[str, object]] = []
    datasets: list[dict[str, object]] = []
    with TemporaryDirectory(prefix="csv-memory-") as directory:
        for rows in sizes:
            path = Path(directory) / f"{rows}.csv"
            expected = generate_dataset(path, rows)
            with path.open("rb") as stream:
                file_hash = hashlib.file_digest(stream, "sha256").hexdigest()
            datasets.append(
                {
                    "rows": rows,
                    "file_bytes": path.stat().st_size,
                    "sha256": file_hash,
                    "expected_checksum": expected.checksum,
                }
            )
            for repetition in range(1, repetitions + 1):
                modes: tuple[Mode, Mode] = ("streaming", "eager")
                if repetition % 2 == 0:
                    modes = ("eager", "streaming")
                for mode in modes:
                    completed = subprocess.run(
                        [
                            sys.executable,
                            str(Path(__file__).resolve()),
                            "worker",
                            str(path),
                            mode,
                        ],
                        check=True,
                        capture_output=True,
                        text=True,
                        encoding="utf-8",
                        timeout=180,
                    )
                    result = Measurement(**json.loads(completed.stdout))
                    if (
                        result.mode != mode
                        or result.count != expected.count
                        or result.checksum != expected.checksum
                        or result.peak_bytes < 0
                    ):
                        raise RuntimeError(
                            "worker result failed correctness validation"
                        )
                    trials.append({"repetition": repetition, **asdict(result)})
    return {
        "schema_version": 1,
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "environment": {
            "python": sys.version,
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "metric": "peak bytes traced by tracemalloc; not process RSS",
        "repetitions": repetitions,
        "datasets": datasets,
        "trials": trials,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run isolated trials")
    run.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 100000])
    run.add_argument("--repetitions", type=int, default=3)
    run.add_argument("--output", type=Path, help="JSON file; otherwise print to stdout")
    worker = commands.add_parser("worker", help="measure one trial in this process")
    worker.add_argument("path", type=Path)
    worker.add_argument("mode", choices=("streaming", "eager"))
    args = parser.parse_args()
    if args.command == "worker":
        print(json.dumps(asdict(measure(args.path, args.mode))))
        return
    if any(size <= 0 for size in args.sizes) or args.repetitions <= 0:
        parser.error("sizes and repetitions must be positive")
    report = json.dumps(run_benchmark(args.sizes, args.repetitions), indent=2) + "\n"
    if args.output is None:
        print(report, end="")
    else:
        args.output.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
