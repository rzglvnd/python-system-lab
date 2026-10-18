"""Observe an async task's cancellation while its to_thread function keeps running."""

import asyncio
import json
import threading
from dataclasses import asdict, dataclass
from threading import Event as ThreadEvent


@dataclass(frozen=True)
class ThreadCancellationReport:
    events: tuple[str, ...]
    ran_in_another_thread: bool
    task_done_at_observation: bool
    task_cancelled: bool
    cancelled_error_observed: bool
    side_effect_before_release: bool
    side_effect_after_release: bool
    result: str | None


async def cancellation_scenario(
    *, cancel_task: bool = True
) -> ThreadCancellationReport:
    loop = asyncio.get_running_loop()
    loop_thread_id = threading.get_ident()
    started, effect_visible = asyncio.Event(), asyncio.Event()
    release = ThreadEvent()
    events: list[str] = []
    worker_thread_id: int | None = None

    def record_start(thread_id: int) -> None:
        nonlocal worker_thread_id
        worker_thread_id = thread_id
        events.append("thread:started")
        started.set()

    def record_effect() -> None:
        events.append("thread:side-effect")
        effect_visible.set()

    def blocking_work() -> str:
        loop.call_soon_threadsafe(record_start, threading.get_ident())
        release.wait()
        loop.call_soon_threadsafe(record_effect)
        return "thread-result"

    task = asyncio.create_task(asyncio.to_thread(blocking_work))
    cancelled_error_observed = False
    result: str | None = None
    try:
        await started.wait()
        if cancel_task:
            task.cancel()
            # Capture this task's cancellation as data; coordinator cancellation
            # still propagates from gather and runs the ownership cleanup below.
            (outcome,) = await asyncio.gather(task, return_exceptions=True)
            assert isinstance(outcome, asyncio.CancelledError)
            cancelled_error_observed = True
            events.append("caller:cancelled-error")
        task_done_at_observation = task.done()
        side_effect_before_release = effect_visible.is_set()
        events.append("caller:released")
        release.set()
        await effect_visible.wait()
        if not cancel_task:
            result = await task
            events.append("caller:result")
        assert worker_thread_id is not None
        return ThreadCancellationReport(
            tuple(events),
            worker_thread_id != loop_thread_id,
            task_done_at_observation,
            task.cancelled(),
            cancelled_error_observed,
            side_effect_before_release,
            effect_visible.is_set(),
            result,
        )
    finally:
        release.set()
        if task.cancelled():
            # The function was started before its wrapper was cancelled. Observe
            # its post-gate notification; asyncio.run later joins executor threads.
            await effect_visible.wait()
        else:
            # Also covers a coordinator cancelled before the function starts:
            # the owned task can run and return through an already-open gate.
            await asyncio.shield(task)


if __name__ == "__main__":
    reports = {
        label: asdict(asyncio.run(cancellation_scenario(cancel_task=cancel)))
        for label, cancel in (("normal", False), ("cancelled", True))
    }
    print(json.dumps(reports, indent=2))
