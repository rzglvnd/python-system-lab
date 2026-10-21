import asyncio
import json
import socket
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from threading import Event as ThreadEvent
from typing import TypeVar

import pytest

from socket_read_stop import READ_TIMEOUT_SECONDS, ReadMode, socket_read_scenario

T = TypeVar("T")


@pytest.mark.parametrize(
    "mode", ["blocking_stop", "timeout_stop", "data", "eof", "timeout"]
)
def test_read_outcomes_and_stop_checkpoints(mode: ReadMode) -> None:
    async def scenario() -> None:
        report = await asyncio.wait_for(socket_read_scenario(mode=mode), timeout=5)
        requested = mode in ("blocking_stop", "timeout_stop")
        timed = mode in ("timeout", "timeout_stop")
        eof_sent = mode in ("blocking_stop", "eof")
        outcome = "timeout" if timed else "eof" if eof_sent else "data"
        assert report.ran_in_another_thread
        assert report.read_timeout_seconds == (READ_TIMEOUT_SECONDS if timed else None)
        assert report.stop_requested is requested
        assert report.task_done_when_stop_requested is (False if requested else None)
        assert report.peer_write_open_when_stop_requested is (
            True if requested else None
        )
        assert report.peer_eof_sent_before_result is eof_sent
        assert report.read.outcome == outcome
        assert report.read.received_hex == (None if timed else "" if eof_sent else "58")
        assert report.read.stop_observed is requested
        assert report.result == ("stopped" if requested else outcome)
        if timed:
            assert report.read.timeout_count >= 1
            if mode == "timeout":
                assert report.read.timeout_count == 1
        else:
            assert report.read.timeout_count == 0
        expected = ["thread:read-ready"]
        if requested:
            expected.append("caller:stop-requested")
        if eof_sent:
            expected.append("caller:peer-eof")
        elif mode == "data":
            expected.append("caller:sent-data")
        expected.append(f"thread:recv-{outcome}")
        if requested:
            expected.append("thread:stop-observed")
        expected.append("caller:result")
        assert report.events == tuple(expected)

    asyncio.run(scenario())


@pytest.mark.parametrize("before_dispatch", [False, True])
def test_coordinator_cancellation_joins_read_before_closing_sockets(
    monkeypatch: pytest.MonkeyPatch, before_dispatch: bool
) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        pairs: list[tuple[socket.socket, socket.socket]] = []
        returned = ThreadEvent()
        entered, dispatch, worker_started = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        original_pair = socket.socketpair
        original_to_thread = asyncio.to_thread

        def observed_pair() -> tuple[socket.socket, socket.socket]:
            pair = original_pair()
            pairs.append(pair)
            return pair

        async def delayed_to_thread(function: Callable[[], T]) -> T:
            entered.set()
            await dispatch.wait()

            def tracked_read() -> T:
                loop.call_soon_threadsafe(worker_started.set)
                try:
                    result = function()
                    # Returning from recv must precede closing either descriptor.
                    assert all(sock.fileno() != -1 for sock in pairs[0])
                    return result
                finally:
                    returned.set()

            return await original_to_thread(tracked_read)

        # Patch after asyncio.run creates its loop; its own internal socket pair
        # is not part of the experiment's resource ownership assertion.
        monkeypatch.setattr(socket, "socketpair", observed_pair)
        monkeypatch.setattr(asyncio, "to_thread", delayed_to_thread)
        coordinator = asyncio.create_task(socket_read_scenario(mode="blocking_stop"))
        try:
            await asyncio.wait_for(entered.wait(), timeout=5)
            if not before_dispatch:
                dispatch.set()
                await asyncio.wait_for(worker_started.wait(), timeout=5)
            coordinator.cancel()
            dispatch.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(coordinator, timeout=5)
            assert returned.is_set()
            assert len(pairs) == 1
            assert all(sock.fileno() == -1 for sock in pairs[0])
        finally:
            dispatch.set()
            if not coordinator.done():
                coordinator.cancel()
            await asyncio.gather(coordinator, return_exceptions=True)

    asyncio.run(scenario())


def test_cli_observes_all_results_and_finishes_executor_shutdowns() -> None:
    script = Path(__file__).resolve().parents[1] / "socket_read_stop.py"
    completed = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    reports = json.loads(completed.stdout)
    assert set(reports) == {"blocking_stop", "timeout_stop", "data", "eof", "timeout"}
    assert reports["blocking_stop"]["read"]["outcome"] == "eof"
    assert reports["timeout_stop"]["read"]["outcome"] == "timeout"
    assert reports["blocking_stop"]["result"] == "stopped"
    assert reports["timeout_stop"]["result"] == "stopped"
    assert not reports["timeout_stop"]["peer_eof_sent_before_result"]
    assert reports["data"]["result"] == "data"
    assert reports["eof"]["result"] == "eof"
    assert reports["timeout"]["result"] == "timeout"
