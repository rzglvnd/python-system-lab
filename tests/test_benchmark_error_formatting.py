import hashlib
import json
import os
import subprocess
import sys
import tracemalloc
from dataclasses import replace
from pathlib import Path

import pytest

import benchmark_error_formatting as experiment
from benchmark_error_formatting import (
    MODES,
    DiagnosticError,
    FormattingCounters,
    Mode,
    measure_formatting,
    run_formatting_benchmark,
    source_checksum,
    validate_formatting_result,
)
from benchmark_worker import POLICIES
from queued_worker import OutcomeCounts, QueuedWorker, ShutdownReport


@pytest.mark.parametrize("mode", MODES)
def test_exception_builds_text_at_the_selected_stage(mode: Mode) -> None:
    counters = FormattingCounters()
    error = DiagnosticError("job-000000002", 16, mode, counters)
    assert counters.calls == 0
    assert counters.generated_chars == (16 if mode == "prebuilt" else 0)
    assert str(error) == "job-000000002eee"
    assert str(error) == "job-000000002eee"
    assert counters.calls == 2
    assert counters.generated_chars == (16 if mode == "prebuilt" else 32)


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("cap", [None, 0, 16])
def test_policies_keep_counts_and_validate_calls_and_generated_text(
    mode: Mode,
    cap: int | None,
) -> None:
    expected = hashlib.sha256(
        b"job-000000000|16\njob-000000001|16\njob-000000002|16\n"
    ).hexdigest()
    assert source_checksum(3, 16) == expected
    for policy in POLICIES:
        result = measure_formatting(
            3,
            mode,
            policy,
            width=16,
            cap=cap,
            history_limit=2,
            max_pending=1,
        )
        retained = {"unlimited": 3, "bounded": 2, "counts-only": 0}[policy]
        calls = 0 if policy == "counts-only" else 3
        assert result.formatting_calls == calls
        assert result.generated_message_chars == (
            48 if mode == "prebuilt" else calls * 16
        )
        assert result.retained_messages_verified == retained
        assert result.measurement.counts == OutcomeCounts(3, 0, 3, 0, 0)
        assert result.measurement.checksum == expected
        assert result.measurement.retained_message_chars == (
            0 if cap == 0 else retained * 16
        )
        assert result.measurement.retained_truncated_messages == (
            retained if cap == 0 else 0
        )
        assert not tracemalloc.is_tracing()
        # Production controller validates a child PID; unit measurements use ours.
        result = replace(
            result, measurement=replace(result.measurement, pid=os.getpid() + 1)
        )
        validate_formatting_result(result, 3, mode, policy, 2, 16, cap)
        for invalid in (
            replace(result, formatting_calls=result.formatting_calls + 1),
            replace(result, generated_message_chars=result.generated_message_chars + 1),
            replace(
                result, retained_messages_verified=result.retained_messages_verified + 1
            ),
        ):
            with pytest.raises(RuntimeError, match="correctness"):
                validate_formatting_result(invalid, 3, mode, policy, 2, 16, cap)


def test_lazy_counts_only_never_builds_text_even_for_validation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def forbidden(job: str, width: int) -> str:
        pytest.fail("diagnostic text was generated")

    monkeypatch.setattr(experiment, "error_message", forbidden)
    result = measure_formatting(7, "lazy", "counts-only", width=4096)
    assert result.formatting_calls == result.generated_message_chars == 0
    assert result.retained_messages_verified == 0
    assert result.measurement.counts == OutcomeCounts(7, 0, 7, 0, 0)
    assert result.measurement.checksum == source_checksum(7, 4096)


def test_invalid_retained_text_stops_tracing_and_rejects_trial(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original_shutdown = QueuedWorker.shutdown

    async def corrupt_shutdown(
        self: QueuedWorker,
        grace_period: float | None = None,
    ) -> ShutdownReport:
        report = await original_shutdown(self, grace_period)
        return replace(
            report,
            failed=tuple(
                replace(failure, message="wrong") for failure in report.failed
            ),
        )

    monkeypatch.setattr(QueuedWorker, "shutdown", corrupt_shutdown)
    with pytest.raises(RuntimeError, match="retained diagnostic validation"):
        measure_formatting(3, "lazy", "bounded", history_limit=1)
    assert not tracemalloc.is_tracing()


def test_cli_runs_matched_modes_with_rotating_order(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "benchmark_error_formatting.py"
    output = tmp_path / "formatting.json"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "run",
            "--jobs",
            "3",
            "--widths",
            "16",
            "--caps",
            "0",
            "16",
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
    raw = json.loads(output.read_text(encoding="utf-8"))
    assert raw["schema_version"] == 1
    assert len(raw["trials"]) == 24
    for first, second in zip(raw["trials"][::2], raw["trials"][1::2], strict=True):
        assert [first["mode"], second["mode"]] == (
            ["prebuilt", "lazy"] if first["repetition"] == 1 else ["lazy", "prebuilt"]
        )
        for key in (
            "jobs",
            "policy",
            "checksum",
            "error_width",
            "max_error_message_chars",
            "counts",
        ):
            assert first["measurement"][key] == second["measurement"][key]
        for trial in (first, second):
            assert trial["measurement"]["pid"] != os.getpid()
            assert trial["measurement"]["counts"]["failed"] == 3
    assert {
        (
            t["mode"],
            t["measurement"]["policy"],
            t["measurement"]["max_error_message_chars"],
            t["repetition"],
        )
        for t in raw["trials"]
    } == {
        (mode, policy, cap, repetition)
        for mode in MODES
        for policy in POLICIES
        for cap in (0, 16)
        for repetition in (1, 2)
    }


def test_existing_tracer_is_preserved() -> None:
    tracemalloc.start()
    try:
        with pytest.raises(RuntimeError, match="initially disabled"):
            measure_formatting(1, "lazy", "counts-only")
        assert tracemalloc.is_tracing()
    finally:
        tracemalloc.stop()


def test_zero_job_lazy_measurement_has_no_formatting() -> None:
    result = measure_formatting(0, "lazy", "counts-only")
    assert result.measurement.counts == OutcomeCounts(0, 0, 0, 0, 0)
    assert result.formatting_calls == result.generated_message_chars == 0


def test_empty_controller_matrix_is_rejected() -> None:
    with pytest.raises(ValueError, match="nonempty"):
        run_formatting_benchmark(widths=[])
