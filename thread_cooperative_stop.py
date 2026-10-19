"""Compare stop requests before a thread checkpoint and after its effect."""

import asyncio
import json
import threading
from dataclasses import asdict, dataclass
from threading import Event as ThreadEvent
from typing import Literal

StopTiming = Literal["none", "before_checkpoint", "after_effect"]
WorkerResult = Literal["stopped", "completed"]


@dataclass(frozen=True)
class CooperativeStopReport:
    events: tuple[str, ...]
    ran_in_another_thread: bool
    stop_requested: bool
    stop_observed: bool
    task_done_when_stop_requested: bool | None
    effect_visible_when_stop_requested: bool | None
    effect_visible_at_completion: bool
    result: WorkerResult


async def cooperative_stop_scenario(
    *, stop_timing: StopTiming = "before_checkpoint"
) -> CooperativeStopReport:
    loop = asyncio.get_running_loop()
    loop_thread_id = threading.get_ident()
    started, decision_visible = asyncio.Event(), asyncio.Event()
    stop, release, finish = ThreadEvent(), ThreadEvent(), ThreadEvent()
    events: list[str] = []
    worker_thread_id: int | None = None
    stop_observed = False
    effect_visible = False

    def record_start(thread_id: int) -> None:
        nonlocal worker_thread_id
        worker_thread_id = thread_id
        events.append("thread:started")
        started.set()

    def record_decision(*, stopped: bool) -> None:
        nonlocal stop_observed, effect_visible
        stop_observed = stopped
        effect_visible = not stopped
        events.append("thread:stop-observed" if stopped else "thread:side-effect")
        decision_visible.set()

    def blocking_work() -> WorkerResult:
        loop.call_soon_threadsafe(record_start, threading.get_ident())
        # Setting stop cannot wake this separate wait. The coordinator must
        # release it before the worker can reach its cooperative checkpoint.
        release.wait()
        if stop.is_set():
            loop.call_soon_threadsafe(lambda: record_decision(stopped=True))
            return "stopped"
        loop.call_soon_threadsafe(lambda: record_decision(stopped=False))
        # Keep the function alive so a late stop is requested before it returns.
        finish.wait()
        return "completed"

    task = asyncio.create_task(asyncio.to_thread(blocking_work))
    task_done_when_stop_requested: bool | None = None
    effect_visible_when_stop_requested: bool | None = None

    def request_stop() -> None:
        nonlocal task_done_when_stop_requested, effect_visible_when_stop_requested
        events.append("caller:stop-requested")
        stop.set()
        task_done_when_stop_requested = task.done()
        effect_visible_when_stop_requested = effect_visible

    try:
        await started.wait()
        if stop_timing == "before_checkpoint":
            request_stop()
        events.append("caller:released")
        release.set()
        await decision_visible.wait()
        if stop_timing == "after_effect":
            request_stop()
        events.append("caller:finish-released")
        finish.set()
        # Preserve the owned task even if this coordinator is cancelled.
        result = await asyncio.shield(task)
        events.append("caller:result")
        assert worker_thread_id is not None
        return CooperativeStopReport(
            tuple(events),
            worker_thread_id != loop_thread_id,
            stop.is_set(),
            stop_observed,
            task_done_when_stop_requested,
            effect_visible_when_stop_requested,
            effect_visible,
            result,
        )
    finally:
        if not task.done():
            stop.set()
        release.set()
        finish.set()
        # One coordinator cancellation still waits for the function's result.
        # Repeated cancellation during this cleanup is outside this experiment.
        await asyncio.shield(task)


if __name__ == "__main__":
    timings: tuple[StopTiming, ...] = ("none", "before_checkpoint", "after_effect")
    reports = {
        timing: asdict(asyncio.run(cooperative_stop_scenario(stop_timing=timing)))
        for timing in timings
    }
    print(json.dumps(reports, indent=2))
