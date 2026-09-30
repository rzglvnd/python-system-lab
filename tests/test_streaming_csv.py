import csv
from collections.abc import Iterator
from io import StringIO
from pathlib import Path

import pytest

from streaming_csv import CsvValidationError, read_records


def test_quoted_delimiters_quotes_and_multiline_values(tmp_path: Path) -> None:
    path = tmp_path / "records.csv"
    path.write_bytes(
        'id,note\r\n1,"hello, world"\r\n2,"two\r\nlines"\r\n'
        '3,"say ""hello"""\r\n4,café\r\n'.encode()
    )
    with path.open(encoding="utf-8", newline="") as stream:
        assert list(read_records(stream)) == [
            {"id": "1", "note": "hello, world"},
            {"id": "2", "note": "two\r\nlines"},
            {"id": "3", "note": 'say "hello"'},
            {"id": "4", "note": "café"},
        ]
        assert not stream.closed
    assert stream.closed


@pytest.mark.parametrize(
    "contents, reason, line",
    [
        ("", "missing header", 0),
        ("\n", "header names must be nonblank", 1),
        ("id,\n", "header names must be nonblank", 1),
        ("id, \n", "header names must be nonblank", 1),
        ("id,id\n", "duplicate header name", 1),
    ],
)
def test_invalid_headers(contents: str, reason: str, line: int) -> None:
    with StringIO(contents) as stream:
        with pytest.raises(CsvValidationError, match=reason) as caught:
            list(read_records(stream))
        assert caught.value.record_number == 1
        assert caught.value.line_number == line
        assert not stream.closed


def test_header_only_is_valid_and_fields_are_not_normalized() -> None:
    with StringIO("id,note\n") as stream:
        assert list(read_records(stream)) == []
    with StringIO("ID, id,note\n001, 02,\n") as stream:
        assert list(read_records(stream)) == [{"ID": "001", " id": " 02", "note": ""}]


@pytest.mark.parametrize("bad_row, width", [("3\n", 1), ("3,x,y\n", 3), ("\n", 0)])
def test_width_error_after_partial_success_reports_multiline_location(
    bad_row: str, width: int
) -> None:
    with StringIO('id,note\n1,"two\nlines"\n' + bad_row) as stream:
        records = read_records(stream)
        assert next(records) == {"id": "1", "note": "two\nlines"}
        with pytest.raises(
            CsvValidationError, match=f"expected 2 fields, got {width}"
        ) as caught:
            next(records)
        assert caught.value.record_number == 3
        assert caught.value.line_number == 4
        assert not stream.closed
        with pytest.raises(StopIteration):
            next(records)


@pytest.mark.parametrize(
    "contents, record, line",
    [('"unterminated\n', 1, 1), ('id,note\n1,ok\n2,"broken\n', 3, 3)],
)
def test_csv_syntax_errors_preserve_cause_and_location(
    contents: str, record: int, line: int
) -> None:
    with StringIO(contents) as stream:
        with pytest.raises(CsvValidationError, match="CSV parsing error") as caught:
            list(read_records(stream))
        assert isinstance(caught.value.__cause__, csv.Error)
        assert caught.value.record_number == record
        assert caught.value.line_number == line
        assert not stream.closed


def test_consumption_is_incremental_and_generator_close_leaves_stream_open() -> None:
    first_record = 'id,note\n1,"two\nlines"\n'
    with StringIO(first_record + "2,later\n") as stream:
        records = read_records(stream)
        assert stream.tell() == 0
        assert next(records) == {"id": "1", "note": "two\nlines"}
        assert stream.tell() == len(first_record)
        records.close()
        assert not stream.closed
        assert next(stream) == "2,later\n"


@pytest.mark.parametrize("consumer_fails", [False, True])
def test_caller_context_closes_real_file_on_early_exit(
    tmp_path: Path, consumer_fails: bool
) -> None:
    path = tmp_path / "records.csv"
    path.write_text("id,note\n1,first\n2,later\n", encoding="utf-8")
    failure = RuntimeError("consumer failed")
    stream = path.open(encoding="utf-8", newline="")
    records = read_records(stream)
    try:
        try:
            with stream:
                for row in records:
                    assert row["id"] == "1"
                    if consumer_fails:
                        raise failure
                    break
        except RuntimeError as error:
            assert consumer_fails
            assert error is failure
        else:
            assert not consumer_fails
        assert stream.closed
        # A retained iterator cannot outlive the caller-owned resource.
        with pytest.raises(ValueError, match="closed file"):
            next(records)
    finally:
        records.close()
        stream.close()


@pytest.fixture
def small_field_limit() -> Iterator[int]:
    """Change shared parser configuration only for this serial test's lifetime."""
    previous = csv.field_size_limit()
    try:
        csv.field_size_limit(8)
        yield 8
    finally:
        csv.field_size_limit(previous)


@pytest.mark.parametrize("size", [7, 8, 9])
@pytest.mark.parametrize("kind", ["ascii", "unicode", "multiline"])
def test_field_limit_boundary(small_field_limit: int, size: int, kind: str) -> None:
    if kind == "ascii":
        value = "a" * size
    elif kind == "unicode":
        value = "é" * size
        assert len(value.encode("utf-8")) > small_field_limit
    else:
        value = "abc\n" + "x" * (size - 4)
    with StringIO(newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["note"])
        writer.writerow([value])
        stream.seek(0)
        records = read_records(stream)
        if size <= small_field_limit:
            assert list(records) == [{"note": value}]
        else:
            with pytest.raises(CsvValidationError, match="CSV parsing error") as caught:
                next(records)
            assert isinstance(caught.value.__cause__, csv.Error)
            assert "field larger than field limit" in str(caught.value.__cause__)
            assert caught.value.record_number == 2
            assert caught.value.line_number == (3 if kind == "multiline" else 2)
            with pytest.raises(StopIteration):
                next(records)
        assert not stream.closed


def test_oversized_field_after_partial_success_closes_caller_file(
    tmp_path: Path, small_field_limit: int
) -> None:
    path = tmp_path / "oversized.csv"
    with path.open("w", encoding="utf-8", newline="") as output:
        writer = csv.writer(output, lineterminator="\n")
        writer.writerows(
            [["id", "note"], ["1", "ok"], ["2", "éééé\néééé"], ["3", "later"]]
        )
    delivered: list[dict[str, str]] = []
    with pytest.raises(CsvValidationError) as caught:
        with path.open(encoding="utf-8", newline="") as stream:
            records = read_records(stream)
            for record in records:
                delivered.append(record)
    assert stream.closed
    assert delivered == [{"id": "1", "note": "ok"}]
    assert caught.value.record_number == 3
    assert caught.value.line_number == 4
    assert isinstance(caught.value.__cause__, csv.Error)
    with pytest.raises(StopIteration):
        next(records)


def test_field_limit_also_applies_to_header(small_field_limit: int) -> None:
    with StringIO("h" * (small_field_limit + 1) + "\nvalue\n") as stream:
        with pytest.raises(CsvValidationError) as caught:
            next(read_records(stream))
        assert caught.value.record_number == 1
        assert caught.value.line_number == 1
        assert isinstance(caught.value.__cause__, csv.Error)


def test_field_limit_is_not_a_total_record_limit(small_field_limit: int) -> None:
    value = "x" * small_field_limit
    with StringIO(f"a,b,c\n{value},{value},{value}\n") as stream:
        assert list(read_records(stream)) == [{"a": value, "b": value, "c": value}]


def test_limit_change_affects_already_started_readers(small_field_limit: int) -> None:
    value = "x" * small_field_limit
    with StringIO(f"note\nok\n{value}\n") as first_stream:
        with StringIO(f"note\nok\n{value}\n") as second_stream:
            first = read_records(first_stream)
            second = read_records(second_stream)
            assert next(first) == next(second) == {"note": "ok"}
            # This is module-level configuration, not a per-reader setting.
            csv.field_size_limit(4)
            for records in (first, second):
                with pytest.raises(CsvValidationError) as caught:
                    next(records)
                assert isinstance(caught.value.__cause__, csv.Error)
