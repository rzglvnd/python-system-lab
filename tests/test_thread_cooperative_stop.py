import asyncio
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from threading import Event as ThreadEvent
from typing import TypeVar

import pytest

import thread_cooperative_stop as experiment
from thread_cooperative_stop import StopTiming, cooperative_stop_scenario

T = TypeVar("T")


@pytest.mark.parametrize(
    "timing", ["none", "before_checkpoint", "after_checkpoint", "after_effect"]
)
def test_stop_checkpoint_controls_whether_effect_is_skipped(timing: StopTiming) -> None:
    report = asyncio.run(cooperative_stop_scenario(stop_timing=timing))
    early = timing == "before_checkpoint"
    requested = timing != "none"
    assert report.ran_in_another_thread
    assert report.stop_requested is requested
    assert report.stop_observed is early
    assert report.task_done_when_stop_requested is (False if requested else None)
    assert report.effect_visible_when_stop_requested is (
        timing == "after_effect" if requested else None
    )
    assert report.checkpoint_passed_when_stop_requested is (
        not early if requested else None
    )
    assert report.effect_visible_at_completion is not early
    assert report.result == ("stopped" if early else "completed")
    expected = ["thread:started"]
    if early:
        expected.append("caller:stop-requested")
    expected.append("caller:released")
    if timing == "after_checkpoint":
        expected.extend(["thread:checkpoint-passed", "caller:stop-requested"])
    expected.append("caller:effect-released")
    if not early and timing != "after_checkpoint":
        expected.append("thread:checkpoint-passed")
    expected.append("thread:stop-observed" if early else "thread:side-effect")
    if timing == "after_effect":
        expected.append("caller:stop-requested")
    expected.extend(["caller:finish-released", "caller:result"])
    assert report.events == tuple(expected)


@pytest.mark.parametrize("before_dispatch", [False, True])
def test_coordinator_cancellation_releases_gates_and_observes_function_return(
    monkeypatch: pytest.MonkeyPatch, before_dispatch: bool
) -> None:
    async def scenario() -> None:
        loop = asyncio.get_running_loop()
        stop, release, effect_release, finish, returned = (
            ThreadEvent(),
            ThreadEvent(),
            ThreadEvent(),
            ThreadEvent(),
            ThreadEvent(),
        )
        gates = iter((stop, release, effect_release, finish))
        entered, dispatch, worker_started = (
            asyncio.Event(),
            asyncio.Event(),
            asyncio.Event(),
        )
        original_to_thread = asyncio.to_thread

        async def delayed_to_thread(function: Callable[[], T]) -> T:
            entered.set()
            await dispatch.wait()

            def tracked_work() -> T:
                loop.call_soon_threadsafe(worker_started.set)
                try:
                    return function()
                finally:
                    returned.set()

            return await original_to_thread(tracked_work)

        monkeypatch.setattr(experiment, "ThreadEvent", lambda: next(gates))
        monkeypatch.setattr(asyncio, "to_thread", delayed_to_thread)
        coordinator = asyncio.create_task(cooperative_stop_scenario(stop_timing="none"))
        try:
            await entered.wait()
            if not before_dispatch:
                dispatch.set()
                await worker_started.wait()
            coordinator.cancel()
            dispatch.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(coordinator, timeout=5)
            assert stop.is_set()
            assert release.is_set()
            assert effect_release.is_set()
            assert finish.is_set()
            assert returned.is_set()
            assert coordinator.cancelled()
        finally:
            stop.set()
            release.set()
            effect_release.set()
            finish.set()
            dispatch.set()
            if not coordinator.done():
                coordinator.cancel()
            await asyncio.gather(coordinator, return_exceptions=True)

    asyncio.run(scenario())


def test_cli_returns_all_results_and_finishes_executor_shutdowns() -> None:
    script = Path(__file__).resolve().parents[1] / "thread_cooperative_stop.py"
    completed = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    reports = json.loads(completed.stdout)
    assert set(reports) == {
        "none",
        "before_checkpoint",
        "after_checkpoint",
        "after_effect",
    }
    assert reports["none"]["result"] == "completed"
    assert reports["before_checkpoint"]["result"] == "stopped"
    assert not reports["before_checkpoint"]["effect_visible_at_completion"]
    between = reports["after_checkpoint"]
    assert between["stop_requested"]
    assert between["checkpoint_passed_when_stop_requested"]
    assert not between["effect_visible_when_stop_requested"]
    assert between["result"] == "completed"
    assert between["effect_visible_at_completion"]
    assert reports["after_effect"]["result"] == "completed"
    assert reports["after_effect"]["effect_visible_at_completion"]
