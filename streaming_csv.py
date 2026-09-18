"""Validate CSV records incrementally without owning the input stream."""

import csv
from collections.abc import Generator
from io import StringIO
from typing import TextIO


class CsvValidationError(ValueError):
    """A CSV syntax or structure error with logical and physical location."""

    def __init__(self, record_number: int, line_number: int, reason: str) -> None:
        self.record_number = record_number
        self.line_number = line_number
        super().__init__(
            f"record {record_number}, physical line {line_number}: {reason}"
        )


def read_records(stream: TextIO) -> Generator[dict[str, str], None, None]:
    """Yield string-valued records; leave stream closure to the caller.

    The first record must contain unique, nonblank names. Every subsequent
    record must have the same width. Validation happens during consumption.
    """
    reader = csv.reader(stream, strict=True)
    header: list[str] | None = None
    record_number = 1
    while True:
        try:
            row = next(reader)
        except StopIteration:
            if header is None:
                raise CsvValidationError(1, reader.line_num, "missing header") from None
            return
        except csv.Error as error:
            raise CsvValidationError(
                record_number, reader.line_num, f"CSV syntax error: {error}"
            ) from error

        if header is None:
            if not row or any(not name.strip() for name in row):
                raise CsvValidationError(
                    record_number, reader.line_num, "header names must be nonblank"
                )
            if len(set(row)) != len(row):
                raise CsvValidationError(
                    record_number, reader.line_num, "duplicate header name"
                )
            header = row
        else:
            if len(row) != len(header):
                raise CsvValidationError(
                    record_number,
                    reader.line_num,
                    f"expected {len(header)} fields, got {len(row)}",
                )
            yield dict(zip(header, row, strict=True))
        record_number += 1


def main() -> None:
    """Show partial success before a malformed record, with caller cleanup."""
    with StringIO('id,note\n1,"hello, world"\n2,"two\nlines"\n3\n') as stream:
        try:
            for record in read_records(stream):
                print(record)
        except CsvValidationError as error:
            print(error)
        print(f"inside caller context, closed: {stream.closed}")
    print(f"after caller context, closed: {stream.closed}")


if __name__ == "__main__":
    main()
