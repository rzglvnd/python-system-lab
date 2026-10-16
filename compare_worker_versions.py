"""Compare two committed workers using identical exported benchmark source."""

import argparse
import hashlib
import json
import platform
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from benchmark_worker import (
    POLICIES,
    Measurement,
    expected_checksum,
    parse_message_cap,
    validate_message_cap,
    validate_result,
    validate_workload,
)
from queued_worker import OutcomeCounts

SOURCE_FILES = ("benchmark_worker.py", "queued_worker.py")
BEFORE = "9fd869a3d7d036b3cf3c76825a9f2abfcab50e81"
AFTER = "ad05adc71e180e31e04064350d4a373a6d9dfb37"


@dataclass(frozen=True)
class ExportedVersion:
    revision: str
    directory: Path
    source_sha256: dict[str, str]


def git_bytes(repository: Path, *arguments: str) -> bytes:
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout


def export_revision(
    repository: Path, revision: str, directory: Path
) -> ExportedVersion:
    commit = (
        git_bytes(
            repository,
            "rev-parse",
            "--verify",
            "--end-of-options",
            f"{revision}^{{commit}}",
        )
        .decode("ascii")
        .strip()
    )
    directory.mkdir()
    hashes: dict[str, str] = {}
    for name in SOURCE_FILES:
        source = git_bytes(repository, "show", f"{commit}:{name}")
        (directory / name).write_bytes(source)
        hashes[name] = hashlib.sha256(source).hexdigest()
    return ExportedVersion(commit, directory, hashes)


def compare_versions(
    before: str = BEFORE,
    after: str = AFTER,
    *,
    repository: Path | None = None,
    jobs: int = 1000,
    error_widths: list[int] | None = None,
    message_caps: list[int | None] | None = None,
    repetitions: int = 4,
    history_limit: int = 100,
    max_pending: int = 16,
) -> dict[str, object]:
    if min(jobs, repetitions, history_limit, max_pending) <= 0:
        raise ValueError(
            "jobs, repetitions, history limit and capacity must be positive"
        )
    widths = [13, 128, 4096] if error_widths is None else error_widths
    caps = [None, 0, 128] if message_caps is None else message_caps
    if not widths or not caps:
        raise ValueError("width and cap lists must not be empty")
    for width in widths:
        validate_workload(jobs, 13, width)
    for cap in caps:
        validate_message_cap(cap)
    controller = Path(__file__).resolve()
    repository = controller.parent if repository is None else repository.resolve()
    trials: list[dict[str, object]] = []
    # Only files written by this controller live in this newly created temp root.
    with TemporaryDirectory(prefix="worker-versions-") as temporary:
        root = Path(temporary).resolve()
        versions = {
            "before": export_revision(repository, before, root / "before"),
            "after": export_revision(repository, after, root / "after"),
        }
        if versions["before"].revision == versions["after"].revision:
            raise ValueError("comparison requires two different commits")
        benchmark_hashes = {
            v.source_sha256["benchmark_worker.py"] for v in versions.values()
        }
        if len(benchmark_hashes) != 1:
            raise ValueError("comparison requires identical committed benchmark source")
        exported_benchmark = (
            versions["before"].directory / "benchmark_worker.py"
        ).read_text(encoding="utf-8")
        if exported_benchmark != controller.with_name("benchmark_worker.py").read_text(
            encoding="utf-8"
        ):
            raise ValueError(
                "exported benchmark must match the controller's validator source"
            )
        for width in widths:
            checksum = expected_checksum(jobs, error_width=width)
            for repetition in range(1, repetitions + 1):
                version_order = (
                    ("before", "after") if repetition % 2 else ("after", "before")
                )
                cap_offset = (repetition - 1) % len(caps)
                policy_offset = (repetition - 1) % len(POLICIES)
                for cap in caps[cap_offset:] + caps[:cap_offset]:
                    for policy in POLICIES[policy_offset:] + POLICIES[:policy_offset]:
                        for label in version_order:
                            version = versions[label]
                            command = [
                                sys.executable,
                                str(version.directory / "benchmark_worker.py"),
                                "worker",
                                str(jobs),
                                policy,
                                "--error-width",
                                str(width),
                                "--history-limit",
                                str(history_limit),
                                "--max-pending",
                                str(max_pending),
                            ]
                            if cap is not None:
                                command.extend(["--max-error-message-chars", str(cap)])
                            completed = subprocess.run(
                                command,
                                cwd=version.directory,
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
                                checksum,
                                error_width=width,
                                max_error_message_chars=cap,
                            )
                            trials.append(
                                {
                                    "version": label,
                                    "revision": version.revision,
                                    "repetition": repetition,
                                    **asdict(result),
                                }
                            )
        version_metadata = {
            label: {
                "revision": version.revision,
                "source_sha256": version.source_sha256,
            }
            for label, version in versions.items()
        }
    return {
        "schema_version": 1,
        "measurement_schema_version": 3,
        "recorded_at_utc": datetime.now(UTC).isoformat(),
        "environment": {
            "python": sys.version,
            "implementation": platform.python_implementation(),
            "platform": platform.platform(),
            "machine": platform.machine(),
            "processor": platform.processor(),
        },
        "versions": version_metadata,
        "controller_source_sha256": {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (
                controller,
                *(controller.with_name(name) for name in SOURCE_FILES),
            )
        },
        "sampling": "after shutdown; stopped worker and report alive; no forced GC",
        "metric": "current and peak Python allocations traced by tracemalloc, not RSS",
        "order": (
            "adjacent version pairs; before first on odd repetitions, after on even; "
            "rotated caps and policies"
        ),
        "jobs": jobs,
        "id_width": 13,
        "error_widths": widths,
        "error_message_caps": caps,
        "repetitions": repetitions,
        "bounded_history_limit": history_limit,
        "max_pending": max_pending,
        "trials": trials,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", default=BEFORE)
    parser.add_argument("--after", default=AFTER)
    parser.add_argument("--repository", type=Path)
    parser.add_argument("--jobs", type=int, default=1000)
    parser.add_argument("--error-widths", type=int, nargs="+", default=[13, 128, 4096])
    parser.add_argument(
        "--message-caps", type=parse_message_cap, nargs="+", default=[None, 0, 128]
    )
    parser.add_argument("--repetitions", type=int, default=4)
    parser.add_argument("--history-limit", type=int, default=100)
    parser.add_argument("--max-pending", type=int, default=16)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        report = compare_versions(
            args.before,
            args.after,
            repository=args.repository,
            jobs=args.jobs,
            error_widths=args.error_widths,
            message_caps=args.message_caps,
            repetitions=args.repetitions,
            history_limit=args.history_limit,
            max_pending=args.max_pending,
        )
    except (ValueError, subprocess.CalledProcessError) as error:
        parser.error(str(error))
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(f"Saved comparison to {args.output}")


if __name__ == "__main__":
    main()
