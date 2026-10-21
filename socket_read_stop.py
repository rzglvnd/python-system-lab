"""Observe cooperative stop checkpoints after local socket reads return."""

import asyncio
import json
import socket
import threading
from dataclasses import asdict, dataclass
from threading import Event as ThreadEvent
from typing import Literal

ReadMode = Literal["blocking_stop", "timeout_stop", "data", "eof", "timeout"]
ReadOutcome = Literal["data", "eof", "timeout"]
READ_TIMEOUT_SECONDS = 0.05


@dataclass(frozen=True)
class ReadObservation:
    outcome: ReadOutcome
    received_hex: str | None
    timeout_count: int
    stop_observed: bool


@dataclass(frozen=True)
class SocketReadReport:
    events: tuple[str, ...]
    ran_in_another_thread: bool
    read_timeout_seconds: float | None
    stop_requested: bool
    task_done_when_stop_requested: bool | None
    peer_write_open_when_stop_requested: bool | None
    peer_eof_sent_before_result: bool
    read: ReadObservation
    result: Literal["stopped", "data", "eof", "timeout"]


async def socket_read_scenario(*, mode: ReadMode = "timeout_stop") -> SocketReadReport:
    loop = asyncio.get_running_loop()
    loop_thread_id = threading.get_ident()
    ready = asyncio.Event()
    stop = ThreadEvent()
    events: list[str] = []
    worker_thread_id: int | None = None
    read_timeout = READ_TIMEOUT_SECONDS if mode in ("timeout", "timeout_stop") else None
    reader, peer = socket.socketpair()
    with reader, peer:
        reader.settimeout(read_timeout)
        peer_eof_sent = False

        def send_eof() -> None:
            nonlocal peer_eof_sent
            if not peer_eof_sent:
                # Half-close the peer's write side. Keep the reader owned and
                # open until its function returns; do not close it from a caller.
                peer.shutdown(socket.SHUT_WR)
                peer_eof_sent = True

        def record_ready(thread_id: int) -> None:
            nonlocal worker_thread_id
            worker_thread_id = thread_id
            events.append("thread:read-ready")
            ready.set()

        def record_read(outcome: ReadOutcome, stopped: bool) -> None:
            events.append(f"thread:recv-{outcome}")
            if stopped:
                events.append("thread:stop-observed")

        def blocking_read() -> ReadObservation:
            loop.call_soon_threadsafe(record_ready, threading.get_ident())
            timeout_count = 0
            while True:
                try:
                    data: bytes | None = reader.recv(1)
                except TimeoutError:
                    timeout_count += 1
                    stopped = stop.is_set()
                    # Scheduling may delay the caller beyond the first timeout.
                    # Keep reaching checkpoints until it requests a stop; do not
                    # assume one timeout or a particular thread scheduling order.
                    if mode == "timeout_stop" and not stopped:
                        continue
                    outcome: ReadOutcome = "timeout"
                    data = None
                else:
                    outcome = "data" if data else "eof"
                    stopped = stop.is_set()
                loop.call_soon_threadsafe(record_read, outcome, stopped)
                return ReadObservation(
                    outcome,
                    data.hex() if data is not None else None,
                    timeout_count,
                    stopped,
                )

        task = asyncio.create_task(asyncio.to_thread(blocking_read))
        task_done_when_stop_requested: bool | None = None
        peer_write_open_when_stop_requested: bool | None = None
        try:
            await ready.wait()
            if mode in ("blocking_stop", "timeout_stop"):
                events.append("caller:stop-requested")
                stop.set()
                task_done_when_stop_requested = task.done()
                peer_write_open_when_stop_requested = not peer_eof_sent
            if mode == "data":
                events.append("caller:sent-data")
                peer.sendall(b"X")
            elif mode in ("eof", "blocking_stop"):
                events.append("caller:peer-eof")
                send_eof()
            observation = await asyncio.shield(task)
            events.append("caller:result")
            assert worker_thread_id is not None
            return SocketReadReport(
                tuple(events),
                worker_thread_id != loop_thread_id,
                read_timeout,
                stop.is_set(),
                task_done_when_stop_requested,
                peer_write_open_when_stop_requested,
                peer_eof_sent,
                observation,
                "stopped" if observation.stop_observed else observation.outcome,
            )
        finally:
            if not task.done():
                stop.set()
                send_eof()
            # A single coordinator cancellation releases the read through EOF
            # and observes function return before the context closes either socket.
            # Repeated cancellation during cleanup is outside this experiment.
            await asyncio.shield(task)


if __name__ == "__main__":
    modes: tuple[ReadMode, ...] = (
        "blocking_stop",
        "timeout_stop",
        "data",
        "eof",
        "timeout",
    )
    print(
        json.dumps(
            {
                mode: asdict(asyncio.run(socket_read_scenario(mode=mode)))
                for mode in modes
            },
            indent=2,
        )
    )
