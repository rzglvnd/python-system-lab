"""Compare fresh read timeouts with a shared fixed-length receive deadline."""

import json
import math
import socket
from collections.abc import Callable
from dataclasses import dataclass
from time import monotonic, sleep
from typing import Literal, Protocol


class ReceiveSocket(Protocol):
    def gettimeout(self) -> float | None: ...

    def settimeout(self, value: float | None, /) -> None: ...

    def recv(self, size: int, /) -> bytes: ...


@dataclass(frozen=True)
class ReceiveReport:
    outcome: Literal["complete", "read_timeout", "deadline_expired", "eof"]
    received: bytes
    expected_bytes: int
    read_calls: int
    idle_retries: int = 0


def receive_exact(
    reader: ReceiveSocket,
    expected_bytes: int,
    *,
    read_timeout: float,
    total_timeout: float | None = None,
    max_idle_retries: int = 0,
    retry_backoff: float = 0.0,
    clock: Callable[[], float] = monotonic,
    sleeper: Callable[[float], None] = sleep,
) -> ReceiveReport:
    """Receive a fixed byte count, preserving data on each terminal outcome.

    The caller owns the socket and must use it exclusively. Two-byte read chunks
    expose multiple reads in this lab; they are not a throughput recommendation.
    """
    if (
        isinstance(expected_bytes, bool)
        or not isinstance(expected_bytes, int)
        or expected_bytes < 0
    ):
        raise ValueError("expected_bytes must be a non-negative integer")
    if (
        isinstance(read_timeout, bool)
        or not math.isfinite(read_timeout)
        or read_timeout <= 0
    ):
        raise ValueError("read_timeout must be finite and positive")
    if total_timeout is not None and (
        isinstance(total_timeout, bool)
        or not math.isfinite(total_timeout)
        or total_timeout < 0
    ):
        raise ValueError("total_timeout must be finite and non-negative")
    if (
        isinstance(max_idle_retries, bool)
        or not isinstance(max_idle_retries, int)
        or max_idle_retries < 0
    ):
        raise ValueError("max_idle_retries must be a non-negative integer")
    if (
        isinstance(retry_backoff, bool)
        or not math.isfinite(retry_backoff)
        or retry_backoff < 0
    ):
        raise ValueError("retry_backoff must be finite and non-negative")
    if expected_bytes == 0:
        return ReceiveReport("complete", b"", 0, 0)

    deadline = None if total_timeout is None else clock() + total_timeout
    original_timeout = reader.gettimeout()
    received = bytearray()
    read_calls = 0
    idle_retries = 0
    retry_pending = False

    def report(
        outcome: Literal["complete", "read_timeout", "deadline_expired", "eof"],
    ) -> ReceiveReport:
        return ReceiveReport(
            outcome, bytes(received), expected_bytes, read_calls, idle_retries
        )

    try:
        while len(received) < expected_bytes:
            if retry_pending and retry_backoff > 0:
                delay = retry_backoff
                if deadline is not None:
                    remaining = deadline - clock()
                    if remaining <= 0:
                        return report("deadline_expired")
                    delay = min(delay, remaining)
                sleeper(delay)
                # Recompute the read budget below, even if the sleeper returned
                # late. Backoff never starts a new operation deadline.
            timeout = read_timeout
            if deadline is not None:
                remaining = deadline - clock()
                if remaining <= 0:
                    return report("deadline_expired")
                timeout = min(timeout, remaining)
            reader.settimeout(timeout)
            if retry_pending:
                # Count a retry only when its recv is about to start, after the
                # deadline check. Progress never replenishes this allowance.
                idle_retries += 1
                retry_pending = False
            read_calls += 1
            try:
                chunk = reader.recv(min(2, expected_bytes - len(received)))
            except TimeoutError:
                if deadline is not None and clock() >= deadline:
                    return report("deadline_expired")
                if idle_retries >= max_idle_retries:
                    return report("read_timeout")
                retry_pending = True
                continue
            received.extend(chunk)
            # A late observation does not become success just because this read
            # supplied the final bytes. Keep every consumed byte in the report.
            if deadline is not None and clock() >= deadline:
                return report("deadline_expired")
            if not chunk:
                return report("eof")
        return report("complete")
    finally:
        reader.settimeout(original_timeout)


def main() -> None:
    cases: tuple[tuple[str, bytes, bool, float | None, int, float], ...] = (
        ("per_read_complete", b"ABCDEF", False, None, 0, 0.0),
        ("deadline_complete", b"ABCDEF", False, 5.0, 0, 0.0),
        ("partial_eof", b"AB", True, 5.0, 0, 0.0),
        ("read_timeout", b"AB", False, None, 0, 0.0),
        ("deadline_expired", b"AB", False, 0.02, 0, 0.0),
        ("retry_exhausted", b"AB", False, None, 2, 0.0),
        ("deadline_with_retries", b"AB", False, 0.02, 3, 0.0),
        ("backoff_deadline", b"AB", False, 0.15, 3, 0.2),
    )
    reports: dict[str, dict[str, object]] = {}
    for label, payload, eof, total, retries, backoff in cases:
        reader, peer = socket.socketpair()
        with reader, peer:
            peer.sendall(payload)
            if eof:
                peer.shutdown(socket.SHUT_WR)
            result = receive_exact(
                reader,
                6,
                read_timeout=0.05,
                total_timeout=total,
                max_idle_retries=retries,
                retry_backoff=backoff,
            )
            reports[label] = {
                "outcome": result.outcome,
                "received_hex": result.received.hex(),
                "expected_bytes": result.expected_bytes,
                "read_calls": result.read_calls,
                "idle_retries": result.idle_retries,
            }
    print(json.dumps(reports, indent=2))


if __name__ == "__main__":
    main()
