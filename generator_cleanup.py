"""Observe file ownership when a generator stops or its consumer exits."""

from collections.abc import Generator
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory


def file_numbers(path: Path, events: list[str]) -> Generator[int, None, None]:
    """Own a UTF-8 file; callers must close this generator if they stop early."""
    stream = path.open(encoding="utf-8")
    events.append("opened")
    try:
        with stream:
            for line in stream:
                yield int(line)
    finally:
        # Report the real stream state after its context has exited.
        events.append(f"closed: {stream.closed}")


def main() -> None:
    """Keep references alive so cleanup does not depend on garbage collection."""
    with TemporaryDirectory() as directory:
        path = Path(directory) / "numbers.txt"
        path.write_text("10\n20\n", encoding="utf-8")

        events: list[str] = []
        numbers = file_numbers(path, events)
        print(f"created: {events}")
        try:
            for number in numbers:
                print(f"first number: {number}")
                break
            print(f"after bare break: {events}")
        finally:
            numbers.close()
        print(f"after explicit close: {events}")

        events = []
        with closing(file_numbers(path, events)) as numbers:
            for _ in numbers:
                break
            print(f"after break inside closing: {events}")
        print(f"after closing context: {events}")

        events = []
        try:
            with closing(file_numbers(path, events)) as numbers:
                for _ in numbers:
                    raise RuntimeError("consumer failed")
        except RuntimeError as error:
            print(f"{error}: {events}")


if __name__ == "__main__":
    main()
