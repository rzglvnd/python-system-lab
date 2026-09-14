from contextlib import closing
from pathlib import Path

import pytest

from generator_cleanup import file_numbers


@pytest.mark.parametrize("contents, expected", [("10\n20\n", [10, 20]), ("", [])])
def test_full_consumption_closes_file(
    tmp_path: Path, contents: str, expected: list[int]
) -> None:
    path = tmp_path / "numbers.txt"
    path.write_text(contents, encoding="utf-8")
    events: list[str] = []
    assert list(file_numbers(path, events)) == expected
    assert events == ["opened", "closed: True"]


def test_break_leaves_generator_suspended_until_explicit_close(tmp_path: Path) -> None:
    path = tmp_path / "numbers.txt"
    path.write_text("10\n20\n", encoding="utf-8")
    events: list[str] = []
    numbers = file_numbers(path, events)
    try:
        for number in numbers:
            assert number == 10
            break
        assert events == ["opened"]
        # The retained generator can still read from its open file.
        assert next(numbers) == 20
        assert events == ["opened"]
    finally:
        numbers.close()
    assert events == ["opened", "closed: True"]
    numbers.close()
    assert events == ["opened", "closed: True"]


def test_closing_releases_file_on_context_exit_after_break(tmp_path: Path) -> None:
    path = tmp_path / "numbers.txt"
    path.write_text("10\n20\n", encoding="utf-8")
    events: list[str] = []
    with closing(file_numbers(path, events)) as numbers:
        for _ in numbers:
            break
        assert events == ["opened"]
    assert events == ["opened", "closed: True"]
    with pytest.raises(StopIteration):
        next(numbers)


def test_producer_error_closes_file_and_propagates(tmp_path: Path) -> None:
    path = tmp_path / "numbers.txt"
    path.write_text("10\nbad\n30\n", encoding="utf-8")
    events: list[str] = []
    numbers = file_numbers(path, events)
    try:
        assert next(numbers) == 10
        with pytest.raises(ValueError):
            next(numbers)
        assert events == ["opened", "closed: True"]
        with pytest.raises(StopIteration):
            next(numbers)
    finally:
        numbers.close()


def test_consumer_error_alone_does_not_close_generator(tmp_path: Path) -> None:
    path = tmp_path / "numbers.txt"
    path.write_text("10\n20\n", encoding="utf-8")
    events: list[str] = []
    numbers = file_numbers(path, events)
    try:
        with pytest.raises(RuntimeError, match="consumer failed"):
            for _ in numbers:
                raise RuntimeError("consumer failed")
        assert events == ["opened"]
        assert next(numbers) == 20
    finally:
        numbers.close()
    assert events == ["opened", "closed: True"]


def test_closing_releases_file_and_preserves_consumer_error(tmp_path: Path) -> None:
    path = tmp_path / "numbers.txt"
    path.write_text("10\n20\n", encoding="utf-8")
    events: list[str] = []
    failure = RuntimeError("consumer failed")
    with pytest.raises(RuntimeError) as caught:
        with closing(file_numbers(path, events)) as numbers:
            for _ in numbers:
                raise failure
    assert caught.value is failure
    assert events == ["opened", "closed: True"]


def test_closing_unstarted_generator_does_not_open_file(tmp_path: Path) -> None:
    events: list[str] = []
    # A nonexistent path would fail if the body executed.
    with closing(file_numbers(tmp_path / "missing.txt", events)) as numbers:
        assert events == []
    assert events == []
    with pytest.raises(StopIteration):
        next(numbers)


def test_open_failure_is_deferred_and_not_reported_as_cleanup(tmp_path: Path) -> None:
    events: list[str] = []
    numbers = file_numbers(tmp_path / "missing.txt", events)
    with pytest.raises(FileNotFoundError):
        next(numbers)
    assert events == []
