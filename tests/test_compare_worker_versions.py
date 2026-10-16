import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

from benchmark_worker import expected_checksum
from compare_worker_versions import compare_versions, export_revision


def git(repository: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repository), *arguments],
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    ).stdout.strip()


def commit(repository: Path) -> str:
    git(repository, "add", ".")
    git(
        repository,
        "-c",
        "user.name=Benchmark fixture",
        "-c",
        "user.email=fixture@example.invalid",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-m",
        "fixture source",
    )
    return git(repository, "rev-parse", "HEAD")


@pytest.fixture
def source_repository(tmp_path: Path) -> tuple[Path, str, str]:
    repository = tmp_path / "repository"
    repository.mkdir()
    git(repository, "init", "--initial-branch=main")
    git(repository, "config", "core.autocrlf", "false")
    source = Path(__file__).resolve().parents[1]
    for name in ("benchmark_worker.py", "queued_worker.py"):
        (repository / name).write_bytes((source / name).read_bytes())
    before = commit(repository)
    with (repository / "queued_worker.py").open("ab") as stream:
        stream.write(b"\n# Second committed fixture version.\n")
    after = commit(repository)
    return repository, before, after


def test_exports_use_committed_bytes_instead_of_dirty_checkout(
    source_repository: tuple[Path, str, str], tmp_path: Path
) -> None:
    repository, before, _ = source_repository
    committed = subprocess.run(
        ["git", "-C", str(repository), "show", f"{before}:queued_worker.py"],
        check=True,
        capture_output=True,
        timeout=30,
    ).stdout
    (repository / "queued_worker.py").write_text(
        "uncommitted content", encoding="utf-8"
    )
    exported = export_revision(repository, before, tmp_path / "export")
    assert exported.revision == before
    assert (exported.directory / "queued_worker.py").read_bytes() == committed
    assert (
        exported.source_sha256["queued_worker.py"]
        == hashlib.sha256(committed).hexdigest()
    )
    assert (repository / "queued_worker.py").read_text(
        encoding="utf-8"
    ) == "uncommitted content"


def test_cli_compares_isolated_pairs_and_alternates_version_order(
    source_repository: tuple[Path, str, str], tmp_path: Path
) -> None:
    repository, before, after = source_repository
    output = tmp_path / "comparison.json"
    script = Path(__file__).resolve().parents[1] / "compare_worker_versions.py"
    subprocess.run(
        [
            sys.executable,
            str(script),
            "--repository",
            str(repository),
            "--before",
            before,
            "--after",
            after,
            "--jobs",
            "3",
            "--error-widths",
            "13",
            "--message-caps",
            "none",
            "0",
            "--history-limit",
            "2",
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
    assert report["versions"]["before"]["revision"] == before
    assert report["versions"]["after"]["revision"] == after
    hashes = [
        report["versions"][label]["source_sha256"] for label in ("before", "after")
    ]
    assert hashes[0]["benchmark_worker.py"] == hashes[1]["benchmark_worker.py"]
    assert hashes[0]["queued_worker.py"] != hashes[1]["queued_worker.py"]
    trials = report["trials"]
    assert len(trials) == 24
    assert {
        (trial["policy"], trial["max_error_message_chars"], trial["repetition"])
        for trial in trials
    } == {
        (policy, cap, repetition)
        for policy in ("unlimited", "bounded", "counts-only")
        for cap in (None, 0)
        for repetition in (1, 2)
    }
    for first, second in zip(trials[::2], trials[1::2], strict=True):
        assert [first["version"], second["version"]] == (
            ["before", "after"] if first["repetition"] == 1 else ["after", "before"]
        )
        for key in (
            "policy",
            "error_width",
            "max_error_message_chars",
            "repetition",
            "counts",
            "checksum",
        ):
            assert first[key] == second[key]
        for trial in (first, second):
            assert trial["checksum"] == expected_checksum(3, error_width=13)
            assert trial["counts"] == {
                "accepted": 3,
                "completed": 0,
                "failed": 3,
                "interrupted": 0,
                "unstarted": 0,
            }
            assert 0 <= trial["current_bytes"] <= trial["peak_bytes"]
    assert git(repository, "status", "--porcelain") == ""


def test_comparison_rejects_changed_benchmark_source(
    source_repository: tuple[Path, str, str],
) -> None:
    repository, before, _ = source_repository
    with (repository / "benchmark_worker.py").open("ab") as stream:
        stream.write(b"\n# Different benchmark methodology.\n")
    after = commit(repository)
    with pytest.raises(ValueError, match="identical committed benchmark"):
        compare_versions(before, after, repository=repository, jobs=1, repetitions=1)


def test_comparison_rejects_identical_revisions(
    source_repository: tuple[Path, str, str],
) -> None:
    repository, before, _ = source_repository
    with pytest.raises(ValueError, match="two different commits"):
        compare_versions(before, before, repository=repository, jobs=1, repetitions=1)


@pytest.mark.parametrize("jobs", [0, -1])
def test_invalid_workload_is_rejected_before_git_export(jobs: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        compare_versions("missing", "missing", jobs=jobs)
