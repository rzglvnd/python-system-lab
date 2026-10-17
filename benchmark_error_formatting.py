"""Measure prebuilt and lazy diagnostic messages across worker retention policies."""

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
from pathlib import Path
from typing import Literal

from benchmark_worker import (
    POLICIES,
    Measurement,
    Policy,
    error_message,
    job_id,
    parse_message_cap,
    retention_limit,
    validate_message_cap,
    validate_result,
    validate_workload,
)
from queued_worker import OutcomeCounts, QueuedWorker

Mode = Literal["prebuilt", "lazy"]
MODES: tuple[Mode, ...] = ("prebuilt", "lazy")


@dataclass
class FormattingCounters:
    calls: int = 0
    generated_chars: int = 0


class DiagnosticError(Exception):
    """Same exception shape in both modes; lazy messages are never cached."""

    def __init__(
        self, job: str, width: int, mode: Mode, counters: FormattingCounters
    ) -> None:
        super().__init__()
        self.job = job
        self.width = width
        self.counters = counters
        self.prebuilt = self._build() if mode == "prebuilt" else None

    def _build(self) -> str:
        message = error_message(self.job, self.width)
        self.counters.generated_chars += len(message)
        return message

    def __str__(self) -> str:
        self.counters.calls += 1
        return self.prebuilt if self.prebuilt is not None else self._build()


@dataclass(frozen=True)
class FormattingMeasurement:
    mode: Mode
    measurement: Measurement
    formatting_calls: int
    generated_message_chars: int
    retained_messages_verified: int


def source_checksum(jobs: int, width: int) -> str:
    """Hash compact input descriptors without constructing diagnostic payloads."""
    digest = hashlib.sha256()
    for index in range(jobs):
        digest.update(f"job-{index:09d}|{width}\n".encode("ascii"))
    return digest.hexdigest()


async def _measure(
    jobs: int,
    mode: Mode,
    policy: Policy,
    history_limit: int | None,
    max_pending: int,
    width: int,
    cap: int | None,
) -> FormattingMeasurement:
    tracemalloc.start(1)
    try:
        digest = hashlib.sha256()
        counters = FormattingCounters()
        processed = 0

        async def handle(job: str) -> None:
            nonlocal processed
            digest.update(f"{job}|{width}\n".encode("ascii"))
            processed += 1
            raise DiagnosticError(job, width, mode, counters)

        worker = QueuedWorker(
            handle,
            history_limit=history_limit,
            max_pending=max_pending,
            max_error_message_chars=cap,
        )
        worker.start()
        try:
            for index in range(jobs):
                await worker.submit_wait(job_id(index))
        finally:
            report = await worker.shutdown()
        checksum = digest.hexdigest()
        # Both worker and report remain owned; validation and summaries follow sampling.
        current, peak = tracemalloc.get_traced_memory()
        observed_counters = (counters.calls, counters.generated_chars)
        measurement = Measurement(
            jobs=jobs,
            policy=policy,
            history_limit=history_limit,
            processed=processed,
            checksum=checksum,
            counts=report.counts,
            retained_details=len(report.failed),
            retained_ids=len(worker._known_ids),
            omitted_outcomes=report.omitted_outcomes,
            current_bytes=current,
            peak_bytes=peak,
            pid=os.getpid(),
            event_loop=type(asyncio.get_running_loop()).__name__,
            error_width=width,
            retained_id_chars=sum(len(failure.job) for failure in report.failed),
            retained_message_chars=sum(
                len(failure.message) for failure in report.failed
            ),
            max_error_message_chars=cap,
            retained_truncated_messages=sum(f.message_truncated for f in report.failed),
        )
        for index, failure in enumerate(report.failed, start=jobs - len(report.failed)):
            expected_job = job_id(index)
            # Construct expected text only for retained records, after sampling.
            expected_text = error_message(expected_job, width)
            if cap is not None:
                expected_text = expected_text[:cap]
            if (
                failure.job != expected_job
                or failure.error_type != "DiagnosticError"
                or failure.message != expected_text
                or failure.message_format_failed
            ):
                raise RuntimeError("retained diagnostic validation failed")
        if observed_counters != (counters.calls, counters.generated_chars):
            raise RuntimeError("validation changed diagnostic counters")
        return FormattingMeasurement(
            mode, measurement, *observed_counters, len(report.failed)
        )
    finally:
        tracemalloc.stop()


def measure_formatting(
    jobs: int,
    mode: Mode,
    policy: Policy,
    *,
    width: int = 4096,
    cap: int | None = None,
    history_limit: int = 100,
    max_pending: int = 16,
) -> FormattingMeasurement:
    validate_workload(jobs, 13, width)
    validate_message_cap(cap)
    limit = retention_limit(policy, history_limit)
    if mode not in MODES or max_pending <= 0:
        raise ValueError("unknown mode or nonpositive waiting capacity")
    if tracemalloc.is_tracing():
        raise RuntimeError("measurement requires tracing to be initially disabled")
    return asyncio.run(_measure(jobs, mode, policy, limit, max_pending, width, cap))


def validate_formatting_result(
    result: FormattingMeasurement,
    jobs: int,
    mode: Mode,
    policy: Policy,
    history_limit: int,
    width: int,
    cap: int | None,
) -> None:
    limit = retention_limit(policy, history_limit)
    retained = jobs if limit is None else min(jobs, limit)
    expected_calls = 0 if limit == 0 else jobs
    expected_chars = jobs * width if mode == "prebuilt" else expected_calls * width
    validate_result(
        result.measurement,
        jobs,
        policy,
        history_limit,
        source_checksum(jobs, width),
        error_width=width,
        max_error_message_chars=cap,
    )
    if (
        result.mode != mode
        or result.formatting_calls != expected_calls
        or result.generated_message_chars != expected_chars
        or result.retained_messages_verified != retained
    ):
        raise RuntimeError("formatting workload failed correctness validation")


def run_formatting_benchmark(
    jobs: int = 1000,
    *,
    widths: list[int] | None = None,
    caps: list[int | None] | None = None,
    repetitions: int = 4,
    history_limit: int = 100,
    max_pending: int = 16,
) -> dict[str, object]:
    widths = [13, 128, 4096] if widths is None else widths
    caps = [None, 0, 128] if caps is None else caps
    if (
        min(jobs, repetitions, history_limit, max_pending) <= 0
        or not widths
        or not caps
    ):
        raise ValueError("positive counts and nonempty width/cap lists required")
    for width in widths:
        validate_workload(jobs, 13, width)
    for cap in caps:
        validate_message_cap(cap)
    script = Path(__file__).resolve()
    trials: list[dict[str, object]] = []
    for width in widths:
        for repetition in range(1, repetitions + 1):
            cap_offset = (repetition - 1) % len(caps)
            policy_offset = (repetition - 1) % len(POLICIES)
            mode_order = MODES if repetition % 2 else MODES[::-1]
            for cap in caps[cap_offset:] + caps[:cap_offset]:
                for policy in POLICIES[policy_offset:] + POLICIES[:policy_offset]:
                    for mode in mode_order:
                        command = [
                            sys.executable,
                            str(script),
                            "worker",
                            str(jobs),
                            mode,
                            policy,
                            "--width",
                            str(width),
                            "--history-limit",
                            str(history_limit),
                            "--max-pending",
                            str(max_pending),
                        ]
                        if cap is not None:
                            command.extend(["--cap", str(cap)])
                        completed = subprocess.run(
                            command,
                            check=True,
                            capture_output=True,
                            text=True,
                            encoding="utf-8",
                            timeout=180,
                        )
                        raw = json.loads(completed.stdout)
                        raw["measurement"]["counts"] = OutcomeCounts(
                            **raw["measurement"]["counts"]
                        )
                        raw["measurement"] = Measurement(**raw["measurement"])
                        result = FormattingMeasurement(**raw)
                        validate_formatting_result(
                            result, jobs, mode, policy, history_limit, width, cap
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
        "source_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                script,
                script.with_name("benchmark_worker.py"),
                script.with_name("queued_worker.py"),
            )
        },
        "sampling": "after shutdown; stopped worker and report alive; no forced GC",
        "metric": "current and peak Python allocations traced by tracemalloc, not RSS",
        "checksum": (
            "ordered job ID and requested width descriptors, not diagnostic payloads"
        ),
        "order": (
            "adjacent modes; prebuilt first on odd repetitions, lazy on even; "
            "rotated caps/policies"
        ),
        "jobs": jobs,
        "widths": widths,
        "caps": caps,
        "repetitions": repetitions,
        "bounded_history_limit": history_limit,
        "max_pending": max_pending,
        "trials": trials,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--jobs", type=int, default=1000)
    run.add_argument("--widths", type=int, nargs="+", default=[13, 128, 4096])
    run.add_argument(
        "--caps", type=parse_message_cap, nargs="+", default=[None, 0, 128]
    )
    run.add_argument("--repetitions", type=int, default=4)
    run.add_argument("--output", type=Path, required=True)
    worker = commands.add_parser("worker")
    worker.add_argument("jobs", type=int)
    worker.add_argument("mode", choices=MODES)
    worker.add_argument("policy", choices=POLICIES)
    worker.add_argument("--width", type=int, default=4096)
    worker.add_argument("--cap", type=parse_message_cap)
    for command in (run, worker):
        command.add_argument("--history-limit", type=int, default=100)
        command.add_argument("--max-pending", type=int, default=16)
    args = parser.parse_args()
    try:
        if args.command == "worker":
            result = measure_formatting(
                args.jobs,
                args.mode,
                args.policy,
                width=args.width,
                cap=args.cap,
                history_limit=args.history_limit,
                max_pending=args.max_pending,
            )
            print(json.dumps(asdict(result)))
        else:
            report = run_formatting_benchmark(
                args.jobs,
                widths=args.widths,
                caps=args.caps,
                repetitions=args.repetitions,
                history_limit=args.history_limit,
                max_pending=args.max_pending,
            )
            args.output.write_text(
                json.dumps(report, indent=2) + "\n", encoding="utf-8"
            )
            print(f"Saved formatting trials to {args.output}")
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
