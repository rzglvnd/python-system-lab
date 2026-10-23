import json
import socket
import subprocess
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

import pytest

from socket_receive_deadline import receive_exact


@dataclass
class ControlledClock:
    now: float = 0.0

    def __call__(self) -> float:
        return self.now


class ScriptedSocket:
    """Advance a controlled clock for reads, respecting their timeout budgets."""

    def __init__(
        self, clock: ControlledClock, steps: Iterable[tuple[float, bytes | OSError]]
    ) -> None:
        self.clock = clock
        self.steps = iter(steps)
        self.timeout: float | None = 7.0
        self.timeouts: list[float | None] = []
        self.sizes: list[int] = []

    def gettimeout(self) -> float | None:
        return self.timeout

    def settimeout(self, value: float | None) -> None:
        self.timeout = value
        self.timeouts.append(value)

    def recv(self, size: int) -> bytes:
        self.sizes.append(size)
        delay, value = next(self.steps)
        assert self.timeout is not None
        if delay > self.timeout:
            self.clock.now += self.timeout
            raise TimeoutError("scripted read timeout")
        self.clock.now += delay
        if isinstance(value, OSError):
            raise value
        assert len(value) <= size
        return value


def test_progress_does_not_reset_the_overall_deadline() -> None:
    steps = [(0.75, b"AB"), (0.75, b"CD"), (0.75, b"EF")]
    per_read_clock, total_clock = ControlledClock(), ControlledClock()
    per_read = ScriptedSocket(per_read_clock, steps)
    shared = ScriptedSocket(total_clock, steps)
    complete = receive_exact(per_read, 6, read_timeout=1, clock=per_read_clock)
    expired = receive_exact(
        shared, 6, read_timeout=1, total_timeout=2, clock=total_clock
    )
    assert complete.outcome == "complete"
    assert complete.received == b"ABCDEF"
    assert per_read_clock.now == 2.25
    assert per_read.timeouts == [1, 1, 1, 7]
    assert expired.outcome == "deadline_expired"
    assert expired.received == b"ABCD"
    assert total_clock.now == 2
    assert shared.timeouts == [1, 1, 0.5, 7]
    assert complete.read_calls == expired.read_calls == 3
    assert per_read.sizes == shared.sizes == [2, 2, 2]


@pytest.mark.parametrize("final_chunk", [b"CD", b""])
def test_deadline_wins_at_the_boundary_and_keeps_consumed_bytes(
    final_chunk: bytes,
) -> None:
    clock = ControlledClock()
    reader = ScriptedSocket(clock, [(0.25, b"AB"), (0.75, final_chunk)])
    result = receive_exact(reader, 4, read_timeout=2, total_timeout=1, clock=clock)
    assert result.outcome == "deadline_expired"
    assert result.received == b"AB" + final_chunk
    assert reader.timeouts == [1, 0.75, 7]


def test_expired_budget_starts_no_read_and_restores_socket_mode() -> None:
    clock = ControlledClock()
    reader = ScriptedSocket(clock, [])
    result = receive_exact(reader, 2, read_timeout=1, total_timeout=0, clock=clock)
    assert result.outcome == "deadline_expired"
    assert result.received == b""
    assert result.read_calls == 0
    assert reader.sizes == []
    assert reader.gettimeout() == 7


@pytest.mark.parametrize("total", [None, 5.0])
def test_idle_read_timeout_preserves_partial_data(total: float | None) -> None:
    clock = ControlledClock()
    reader = ScriptedSocket(clock, [(0.25, b"AB"), (2.0, b"CD")])
    result = receive_exact(reader, 4, read_timeout=1, total_timeout=total, clock=clock)
    assert result.outcome == "read_timeout"
    assert result.received == b"AB"
    assert result.read_calls == 2
    assert reader.gettimeout() == 7


def test_partial_eof_is_not_success_and_restores_socket_mode() -> None:
    clock = ControlledClock()
    reader = ScriptedSocket(clock, [(0.25, b"AB"), (0.25, b"")])
    result = receive_exact(reader, 3, read_timeout=1, total_timeout=5, clock=clock)
    assert result.outcome == "eof"
    assert result.received == b"AB"
    assert result.expected_bytes == 3
    assert reader.sizes == [2, 1]
    assert reader.gettimeout() == 7


def test_unexpected_socket_error_propagates_and_restores_timeout() -> None:
    clock = ControlledClock()
    error = ConnectionResetError("connection reset")
    reader = ScriptedSocket(clock, [(0.0, error)])
    with pytest.raises(ConnectionResetError) as caught:
        receive_exact(reader, 2, read_timeout=1, total_timeout=5, clock=clock)
    assert caught.value is error
    assert reader.gettimeout() == 7


@pytest.mark.parametrize("read_timeout", [0.0, -1.0, float("inf"), float("nan"), True])
def test_invalid_read_timeouts_touch_no_socket(read_timeout: float) -> None:
    reader = ScriptedSocket(ControlledClock(), [])
    with pytest.raises(ValueError, match="read_timeout"):
        receive_exact(reader, 2, read_timeout=read_timeout)
    assert reader.timeouts == []


@pytest.mark.parametrize("total", [-1.0, float("inf"), float("nan"), True])
def test_invalid_total_timeouts_touch_no_socket(total: float) -> None:
    reader = ScriptedSocket(ControlledClock(), [])
    with pytest.raises(ValueError, match="total_timeout"):
        receive_exact(reader, 2, read_timeout=1, total_timeout=total)
    assert reader.timeouts == []


@pytest.mark.parametrize("size", [-1, True])
def test_invalid_sizes_touch_no_socket(size: int) -> None:
    reader = ScriptedSocket(ControlledClock(), [])
    with pytest.raises(ValueError, match="expected_bytes"):
        receive_exact(reader, size, read_timeout=1)
    assert reader.timeouts == []


def test_zero_bytes_complete_without_a_read_even_with_zero_budget() -> None:
    reader = ScriptedSocket(ControlledClock(), [])
    result = receive_exact(reader, 0, read_timeout=1, total_timeout=0)
    assert result.outcome == "complete"
    assert result.received == b""
    assert result.read_calls == 0
    assert reader.timeouts == []


@pytest.mark.parametrize("total", [None, 5.0])
def test_real_socket_receives_exact_length_and_leaves_next_bytes(
    total: float | None,
) -> None:
    reader, peer = socket.socketpair()
    with reader, peer:
        reader.settimeout(3)
        peer.sendall(b"ABCDEtail")
        result = receive_exact(reader, 5, read_timeout=1, total_timeout=total)
        assert result.outcome == "complete"
        assert result.received == b"ABCDE"
        assert 3 <= result.read_calls <= 5
        assert reader.gettimeout() == 3
        following = receive_exact(reader, 4, read_timeout=1, total_timeout=total)
        assert following.outcome == "complete"
        assert following.received == b"tail"
        assert reader.gettimeout() == 3


def test_real_socket_eof_preserves_partial_message() -> None:
    reader, peer = socket.socketpair()
    with reader, peer:
        peer.sendall(b"AB")
        peer.shutdown(socket.SHUT_WR)
        result = receive_exact(reader, 4, read_timeout=1, total_timeout=5)
        assert result.outcome == "eof"
        assert result.received == b"AB"
        assert reader.gettimeout() is None


def test_cli_exercises_real_socket_completion_eof_and_timeouts() -> None:
    script = Path(__file__).resolve().parents[1] / "socket_receive_deadline.py"
    completed = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    reports = json.loads(completed.stdout)
    for label in ("per_read_complete", "deadline_complete"):
        assert reports[label]["outcome"] == "complete"
        assert reports[label]["received_hex"] == b"ABCDEF".hex()
    assert reports["partial_eof"]["outcome"] == "eof"
    assert reports["read_timeout"]["outcome"] == "read_timeout"
    assert reports["deadline_expired"]["outcome"] == "deadline_expired"
