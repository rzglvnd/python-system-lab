from collections.abc import Iterator

import pytest

from generator_execution import eager_numbers, lazy_numbers


def test_eager_call_processes_all_rows_before_returning() -> None:
    events: list[str] = []
    assert eager_numbers(["10", "20"], events) == [10, 20]
    assert events == ["parse '10'", "parse '20'"]


def test_generator_defers_execution_and_consumes_one_row_at_a_time() -> None:
    consumed: list[str] = []

    def source() -> Iterator[str]:
        for row in ["10", "20"]:
            consumed.append(row)
            yield row

    events: list[str] = []
    numbers = lazy_numbers(source(), events)
    assert consumed == []
    assert events == []

    assert next(numbers) == 10
    assert consumed == ["10"]
    assert events == ["parse '10'"]

    assert next(numbers) == 20
    assert consumed == ["10", "20"]
    assert events == ["parse '10'", "parse '20'"]


def test_exhausted_generator_cannot_be_replayed() -> None:
    rows = ["10", "20"]
    numbers = lazy_numbers(rows, [])
    assert list(numbers) == [10, 20]
    with pytest.raises(StopIteration):
        next(numbers)
    assert list(numbers) == []
    assert list(lazy_numbers(rows, [])) == [10, 20]


def test_eager_failure_occurs_during_call_and_stops_before_later_rows() -> None:
    events: list[str] = []
    with pytest.raises(ValueError):
        eager_numbers(["10", "bad", "30"], events)
    assert events == ["parse '10'", "parse 'bad'"]


def test_lazy_failure_occurs_during_consumption_and_terminates_generator() -> None:
    events: list[str] = []
    numbers = lazy_numbers(["10", "bad", "30"], events)
    assert next(numbers) == 10
    with pytest.raises(ValueError):
        next(numbers)
    with pytest.raises(StopIteration):
        next(numbers)
    assert events == ["parse '10'", "parse 'bad'"]


def test_empty_input() -> None:
    events: list[str] = []
    assert eager_numbers([], events) == []
    assert list(lazy_numbers([], events)) == []
    assert events == []
