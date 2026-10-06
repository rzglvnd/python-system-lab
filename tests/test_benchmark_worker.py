import hashlib
import json
import os
import subprocess
import sys
import tracemalloc
from dataclasses import replace
from pathlib import Path

import pytest

from benchmark_worker import (
    POLICIES,
    Measurement,
    expected_checksum,
    measure,
    run_benchmark,
    validate_result,
)
from queued_worker import OutcomeCounts


@pytest.mark.parametrize("jobs", [0, 1, 17])
def test_policies_process_identical_source_sequence(jobs: int) -> None:
    # Independent explicit bytes for the source checksum.
    expected = hashlib.sha256(
        b"".join(f"job-{index:09d}\n".encode("ascii") for index in range(jobs))
    ).hexdigest()
    assert expected_checksum(jobs) == expected
    for policy in POLICIES:
        result = measure(jobs, policy, history_limit=3, max_pending=1)
        assert result.processed == jobs
        assert result.checksum == expected
        assert result.counts == OutcomeCounts(jobs, jobs, 0, 0, 0)
        retained = (
            jobs
            if policy == "unlimited"
            else min(jobs, 3 if policy == "bounded" else 0)
        )
        assert result.retained_details == result.retained_ids == retained
        assert result.omitted_outcomes == jobs - retained
        assert 0 <= result.current_bytes <= result.peak_bytes
        assert not tracemalloc.is_tracing()


def test_existing_tracer_is_preserved() -> None:
    tracemalloc.start()
    try:
        with pytest.raises(RuntimeError, match="initially disabled"):
            measure(1, "bounded")
        assert tracemalloc.is_tracing()
    finally:
        tracemalloc.stop()


def test_measurement_failure_stops_tracing(monkeypatch: pytest.MonkeyPatch) -> None:
    from queued_worker import QueuedWorker

    async def fail_submission(self: QueuedWorker, job: str) -> None:
        raise RuntimeError("injected submission failure")

    monkeypatch.setattr(QueuedWorker, "submit_wait", fail_submission)
    with pytest.raises(RuntimeError, match="injected submission failure"):
        measure(1, "bounded")
    assert not tracemalloc.is_tracing()


def test_controller_rejects_incorrect_results() -> None:
    checksum = expected_checksum(7)
    result = Measurement(
        7,
        "bounded",
        3,
        7,
        checksum,
        OutcomeCounts(7, 7, 0, 0, 0),
        3,
        3,
        4,
        10,
        20,
        os.getpid() + 1,
        "test-loop",
    )
    validate_result(result, 7, "bounded", 3, checksum)
    for invalid in (
        replace(result, checksum="wrong"),
        replace(result, processed=6),
        replace(result, counts=OutcomeCounts(7, 6, 1, 0, 0)),
        replace(result, retained_ids=7),
        replace(result, retained_details=7),
        replace(result, omitted_outcomes=0),
        replace(result, current_bytes=21),
    ):
        with pytest.raises(RuntimeError, match="correctness"):
            validate_result(invalid, 7, "bounded", 3, checksum)


def test_cli_launches_isolated_trials_with_rotating_order(tmp_path: Path) -> None:
    output = tmp_path / "results.json"
    script = Path(__file__).resolve().parents[1] / "benchmark_worker.py"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "run",
            "--sizes",
            "3",
            "7",
            "--repetitions",
            "2",
            "--history-limit",
            "2",
            "--output",
            str(output),
        ],
        check=True,
        capture_output=True,
        text=True,
        timeout=60,
    )
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["schema_version"] == 1
    assert len(report["trials"]) == 12
    for jobs in (3, 7):
        trials = [trial for trial in report["trials"] if trial["jobs"] == jobs]
        assert [trial["policy"] for trial in trials] == [
            "unlimited",
            "bounded",
            "counts-only",
            "bounded",
            "counts-only",
            "unlimited",
        ]
        for trial in trials:
            assert trial["pid"] != os.getpid()
            assert trial["checksum"] == expected_checksum(jobs)
            assert trial["counts"] == {
                "accepted": jobs,
                "completed": jobs,
                "failed": 0,
                "interrupted": 0,
                "unstarted": 0,
            }
    assert (
        report["source_sha256"][script.name]
        == hashlib.sha256(script.read_bytes()).hexdigest()
    )


@pytest.mark.parametrize("sizes,repetitions", [([], 1), ([0], 1), ([1], 0)])
def test_invalid_controller_inputs(sizes: list[int], repetitions: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        run_benchmark(sizes, repetitions)


def test_cli_rejects_zero_size() -> None:
    script = Path(__file__).resolve().parents[1] / "benchmark_worker.py"
    completed = subprocess.run(
        [sys.executable, str(script), "run", "--sizes", "0"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 2
    assert "sizes and repetitions must be positive" in completed.stderr
