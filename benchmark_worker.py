"""Measure worker retention allocations in fresh Python processes."""

import argparse
import asyncio
import hashlib
import json
import os
import platform
import subprocess
import sys
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from itertools import product
from pathlib import Path
from typing import Literal

from queued_worker import OutcomeCounts, QueuedWorker

Policy = Literal["unlimited", "bounded", "counts-only"]
POLICIES: tuple[Policy, ...] = ("unlimited", "bounded", "counts-only")


@dataclass(frozen=True)
class Measurement:
    jobs: int
    policy: Policy
    history_limit: int | None
    processed: int
    checksum: str
    counts: OutcomeCounts
    retained_details: int
    retained_ids: int
    omitted_outcomes: int
    current_bytes: int
    peak_bytes: int
    pid: int
    event_loop: str
    id_width: int = 13
    error_width: int | None = None
    retained_id_chars: int = 0
    retained_message_chars: int = 0


def validate_workload(jobs: int, id_width: int, error_width: int | None) -> None:
    if not 0 <= jobs <= 1_000_000_000:
        raise ValueError("jobs must be nonnegative and fit nine-digit identifiers")
    if id_width < 13 or (error_width is not None and error_width < 13):
        raise ValueError("ID and error widths must be at least 13 ASCII characters")


def job_id(index: int, width: int = 13) -> str:
    return f"job-{index:09d}" + "x" * (width - 13)


def error_message(job: str, width: int) -> str:
    # join constructs a new message even at width 13; the unique prefix prevents
    # repeated failures sharing one preconstructed payload string.
    return "".join((job[:13], "e" * (width - 13)))


def expected_checksum(
    jobs: int, id_width: int = 13, error_width: int | None = None
) -> str:
    """Hash the source sequence without a worker or retained input list."""
    digest = hashlib.sha256()
    for index in range(jobs):
        job = job_id(index, id_width)
        digest.update((job + "\n").encode("ascii"))
        if error_width is not None:
            digest.update((error_message(job, error_width) + "\n").encode("ascii"))
    return digest.hexdigest()


def retention_limit(policy: Policy, history_limit: int) -> int | None:
    if policy not in POLICIES or history_limit <= 0:
        raise ValueError("unknown policy or nonpositive bounded history limit")
    return {"unlimited": None, "bounded": history_limit, "counts-only": 0}[policy]


async def _measure(
    jobs: int,
    policy: Policy,
    history_limit: int | None,
    max_pending: int,
    id_width: int,
    error_width: int | None,
) -> Measurement:
    # The event loop and module imports exist before tracing begins.
    tracemalloc.start(1)
    try:
        digest = hashlib.sha256()
        processed = 0

        async def handle(job: str) -> None:
            nonlocal processed
            digest.update((job + "\n").encode("ascii"))
            processed += 1
            if error_width is not None:
                message = error_message(job, error_width)
                digest.update((message + "\n").encode("ascii"))
                raise ValueError(message)

        worker = QueuedWorker(
            handle, max_pending=max_pending, history_limit=history_limit
        )
        worker.start()
        try:
            for index in range(jobs):
                await worker.submit_wait(job_id(index, id_width))
        finally:
            report = await worker.shutdown()
        checksum = digest.hexdigest()
        # Keep BOTH the stopped worker and the shutdown report alive at sampling.
        current, peak = tracemalloc.get_traced_memory()
        return Measurement(
            jobs,
            policy,
            history_limit,
            processed,
            checksum,
            report.counts,
            len(report.completed)
            + len(report.failed)
            + len(report.interrupted)
            + len(report.unstarted),
            len(worker._known_ids),
            report.omitted_outcomes,
            current,
            peak,
            os.getpid(),
            type(asyncio.get_running_loop()).__name__,
            id_width,
            error_width,
            sum(len(job) for job in report.completed)
            + sum(len(failure.job) for failure in report.failed),
            sum(len(failure.message) for failure in report.failed),
        )
    finally:
        tracemalloc.stop()


def measure(
    jobs: int,
    policy: Policy,
    history_limit: int = 100,
    max_pending: int = 16,
    *,
    id_width: int = 13,
    error_width: int | None = None,
) -> Measurement:
    """Trace worker creation through shutdown, sampling before releasing ownership."""
    limit = retention_limit(policy, history_limit)
    validate_workload(jobs, id_width, error_width)
    if jobs < 0 or max_pending <= 0:
        raise ValueError("jobs must be nonnegative and waiting capacity positive")
    if tracemalloc.is_tracing():
        raise RuntimeError("measurement requires tracing to be initially disabled")
    return asyncio.run(
        _measure(jobs, policy, limit, max_pending, id_width, error_width)
    )


def validate_result(
    result: Measurement,
    jobs: int,
    policy: Policy,
    history_limit: int,
    checksum: str,
    *,
    id_width: int = 13,
    error_width: int | None = None,
) -> None:
    limit = retention_limit(policy, history_limit)
    retained = jobs if limit is None else min(jobs, limit)
    counts = (
        OutcomeCounts(jobs, jobs, 0, 0, 0)
        if error_width is None
        else OutcomeCounts(jobs, 0, jobs, 0, 0)
    )
    if (
        result.jobs != jobs
        or result.policy != policy
        or result.history_limit != limit
        or result.processed != jobs
        or result.checksum != checksum
        or result.counts != counts
        or result.id_width != id_width
        or result.error_width != error_width
        or result.retained_id_chars != retained * id_width
        or result.retained_message_chars != retained * (error_width or 0)
        or result.retained_details != retained
        or result.retained_ids != retained
        or result.omitted_outcomes != jobs - retained
        or not 0 <= result.current_bytes <= result.peak_bytes
        or result.pid <= 0
        or result.pid == os.getpid()
    ):
        raise RuntimeError("worker result failed correctness validation")


def run_benchmark(
    sizes: list[int],
    repetitions: int,
    history_limit: int = 100,
    max_pending: int = 16,
    *,
    id_widths: list[int] | None = None,
    error_widths: list[int] | None = None,
) -> dict[str, object]:
    """Launch one fresh interpreter per policy, job count, and repetition."""
    if (
        not sizes
        or any(size <= 0 for size in sizes)
        or repetitions <= 0
        or history_limit <= 0
        or max_pending <= 0
    ):
        raise ValueError(
            "sizes, repetitions, history limit, and capacity must be positive"
        )
    script = Path(__file__).resolve()
    id_widths = [13] if id_widths is None else id_widths
    error_cases: list[int | None] = (
        [None] if error_widths is None else [width for width in error_widths]
    )
    if not id_widths or not error_cases:
        raise ValueError("width lists must not be empty")
    for jobs, id_width, error_width in product(sizes, id_widths, error_cases):
        validate_workload(jobs, id_width, error_width)
    trials: list[dict[str, object]] = []
    for jobs, id_width, error_width in product(sizes, id_widths, error_cases):
        expected = expected_checksum(jobs, id_width, error_width)
        for repetition in range(1, repetitions + 1):
            # Rotate policy order to avoid always measuring one policy first.
            offset = (repetition - 1) % len(POLICIES)
            for policy in POLICIES[offset:] + POLICIES[:offset]:
                command = [
                    sys.executable,
                    str(script),
                    "worker",
                    str(jobs),
                    policy,
                    "--history-limit",
                    str(history_limit),
                    "--max-pending",
                    str(max_pending),
                    "--id-width",
                    str(id_width),
                ]
                if error_width is not None:
                    command.extend(["--error-width", str(error_width)])
                completed = subprocess.run(
                    command,
                    check=True,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    timeout=180,
                )
                raw = json.loads(completed.stdout)
                raw["counts"] = OutcomeCounts(**raw["counts"])
                result = Measurement(**raw)
                validate_result(
                    result,
                    jobs,
                    policy,
                    history_limit,
                    expected,
                    id_width=id_width,
                    error_width=error_width,
                )
                trials.append({"repetition": repetition, **asdict(result)})
    return {
        "schema_version": 2,
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "environment": {
            "python": sys.version,
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "source_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (script, script.with_name("queued_worker.py"))
        },
        "metric": "current and peak Python allocations traced by tracemalloc, not RSS",
        "sampling": "after shutdown; stopped worker and report alive; no forced GC",
        "sizes": sizes,
        "repetitions": repetitions,
        "bounded_history_limit": history_limit,
        "max_pending": max_pending,
        "id_widths": id_widths,
        "error_widths": error_cases,
        "width_unit": "ASCII characters (also UTF-8 bytes)",
        "trials": trials,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run isolated trials")
    run.add_argument("--sizes", type=int, nargs="+", default=[1000, 10000, 50000])
    run.add_argument("--repetitions", type=int, default=3)
    run.add_argument("--output", type=Path)
    run.add_argument("--id-widths", type=int, nargs="+", default=[13])
    run.add_argument("--error-widths", type=int, nargs="+")
    worker = commands.add_parser("worker", help="measure one trial")
    worker.add_argument("jobs", type=int)
    worker.add_argument("policy", choices=POLICIES)
    worker.add_argument("--id-width", type=int, default=13)
    worker.add_argument("--error-width", type=int)
    for command in (run, worker):
        command.add_argument("--history-limit", type=int, default=100)
        command.add_argument("--max-pending", type=int, default=16)
    args = parser.parse_args()
    if args.history_limit <= 0 or args.max_pending <= 0:
        parser.error("history limit and waiting capacity must be positive")
    if args.command == "worker":
        if args.jobs < 0:
            parser.error("jobs must be nonnegative")
        try:
            validate_workload(args.jobs, args.id_width, args.error_width)
        except ValueError as error:
            parser.error(str(error))
        print(
            json.dumps(
                asdict(
                    measure(
                        args.jobs,
                        args.policy,
                        args.history_limit,
                        args.max_pending,
                        id_width=args.id_width,
                        error_width=args.error_width,
                    )
                )
            )
        )
        return
    if any(size <= 0 for size in args.sizes) or args.repetitions <= 0:
        parser.error("sizes and repetitions must be positive")
    try:
        for jobs, id_width, error_width in product(
            args.sizes,
            args.id_widths,
            [None] if args.error_widths is None else args.error_widths,
        ):
            validate_workload(jobs, id_width, error_width)
    except ValueError as error:
        parser.error(str(error))
    output = (
        json.dumps(
            run_benchmark(
                args.sizes,
                args.repetitions,
                args.history_limit,
                args.max_pending,
                id_widths=args.id_widths,
                error_widths=args.error_widths,
            ),
            indent=2,
        )
        + "\n"
    )
    if args.output is None:
        print(output, end="")
    else:
        args.output.write_text(output, encoding="utf-8")


if __name__ == "__main__":
    main()
