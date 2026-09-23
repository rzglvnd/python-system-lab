import hashlib
import json
import os
import subprocess
import sys
import tracemalloc
from pathlib import Path

import pytest

from benchmark_csv import Mode, generate_dataset, measure, summarize
from streaming_csv import CsvValidationError, read_records


@pytest.mark.parametrize("rows", [0, 1, 17])
def test_reproducible_input_and_equivalent_results(tmp_path: Path, rows: int) -> None:
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    expected = generate_dataset(first, rows)
    assert generate_dataset(second, rows) == expected
    assert first.read_bytes() == second.read_bytes()
    with first.open(encoding="utf-8", newline="") as stream:
        assert summarize(read_records(stream)) == expected
    modes: tuple[Mode, Mode] = ("streaming", "eager")
    for mode in modes:
        result = measure(first, mode)
        assert result.count == rows
        assert result.checksum == expected.checksum
        assert result.peak_bytes >= 0
        assert not tracemalloc.is_tracing()


def test_checksum_includes_every_field_and_record_order() -> None:
    record = {"id": "001", "amount": "002", "note": "café"}
    original = summarize([record])
    for field in record:
        assert summarize([{**record, field: "changed"}]).checksum != original.checksum
    other = {**record, "id": "002"}
    assert summarize([record, other]).checksum != summarize([other, record]).checksum


@pytest.mark.parametrize("mode", ["streaming", "eager"])
def test_measurement_failure_stops_tracing(tmp_path: Path, mode: Mode) -> None:
    path = tmp_path / "bad.csv"
    path.write_text("id,note\n1,ok\n2\n", encoding="utf-8")
    with pytest.raises(CsvValidationError):
        measure(path, mode)
    assert not tracemalloc.is_tracing()


def test_controller_cli_runs_workers_and_emits_valid_report(tmp_path: Path) -> None:
    output = tmp_path / "result.json"
    script = Path(__file__).resolve().parents[1] / "benchmark_csv.py"
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
    assert len(report["trials"]) == 8
    for dataset in report["datasets"]:
        path = tmp_path / f"{dataset['rows']}.csv"
        generate_dataset(path, dataset["rows"])
        assert dataset["file_bytes"] == path.stat().st_size
        assert dataset["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
        trials = [t for t in report["trials"] if t["count"] == dataset["rows"]]
        assert [(t["repetition"], t["mode"]) for t in trials] == [
            (1, "streaming"),
            (1, "eager"),
            (2, "eager"),
            (2, "streaming"),
        ]
        for trial in trials:
            assert trial["checksum"] == dataset["expected_checksum"]
            assert trial["pid"] != os.getpid()
            assert trial["peak_bytes"] >= 0


def test_cli_rejects_invalid_size(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "benchmark_csv.py"
    completed = subprocess.run(
        [sys.executable, str(script), "run", "--sizes", "0"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 2
    assert "sizes and repetitions must be positive" in completed.stderr
