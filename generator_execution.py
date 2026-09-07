"""Observe when eager and lazy integer conversion execute."""

from collections.abc import Iterable, Iterator


def eager_numbers(rows: Iterable[str], events: list[str]) -> list[int]:
    """Convert every row before returning a result."""
    numbers = []
    for row in rows:
        events.append(f"parse {row!r}")
        numbers.append(int(row))
    return numbers


def lazy_numbers(rows: Iterable[str], events: list[str]) -> Iterator[int]:
    """Convert a row each time the consumer requests another result."""
    for row in rows:
        events.append(f"parse {row!r}")
        yield int(row)


def main() -> None:
    """Print a deterministic trace; the malformed row is intentional."""
    rows = ["10", "20", "bad"]
    eager_events: list[str] = []
    try:
        eager_numbers(rows, eager_events)
    except ValueError:
        print("eager call: ValueError (no result returned)")
    print(f"eager events: {eager_events}")

    lazy_events: list[str] = []
    numbers = lazy_numbers(rows, lazy_events)
    print(f"lazy created: {lazy_events}")
    print(f"first next: {next(numbers)}")
    print(f"events after first next: {lazy_events}")
    print(f"second next: {next(numbers)}")
    try:
        next(numbers)
    except ValueError:
        print("third next: ValueError")
    print(f"lazy events: {lazy_events}")
    print(f"remaining after failure: {list(numbers)}")


if __name__ == "__main__":
    main()
