import asyncio
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from threading import Event as ThreadEvent
from typing import TypeVar

import pytest

import thread_cancellation as experiment
from thread_cancellation import cancellation_scenario

T = TypeVar("T")


@pytest.mark.parametrize("cancel_task", [False, True])
def test_thread_can_produce_effect_after_its_async_task_is_cancelled(
    cancel_task: bool,
) -> None:
    report = asyncio.run(cancellation_scenario(cancel_task=cancel_task))
    assert report.ran_in_another_thread
    assert report.task_cancelled is cancel_task
    assert report.task_done_at_observation is cancel_task
    assert report.cancelled_error_observed is cancel_task
    assert not report.side_effect_before_release
    assert report.side_effect_after_release
    if cancel_task:
        assert report.events == (
            "thread:started",
            "caller:cancelled-error",
            "caller:released",
            "thread:side-effect",
        )
        assert report.result is None
    else:
        assert report.events == (
            "thread:started",
            "caller:released",
            "thread:side-effect",
            "caller:result",
        )
        assert report.result == "thread-result"


def test_cancelled_coordinator_releases_owned_work_before_propagating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def scenario() -> None:
        release = ThreadEvent()
        entered, dispatch = asyncio.Event(), asyncio.Event()
        original_to_thread = asyncio.to_thread

        async def delayed_to_thread(function: Callable[[], T]) -> T:
            entered.set()
            await dispatch.wait()
            return await original_to_thread(function)

        monkeypatch.setattr(experiment, "ThreadEvent", lambda: release)
        monkeypatch.setattr(asyncio, "to_thread", delayed_to_thread)
        coordinator = asyncio.create_task(cancellation_scenario())
        try:
            await entered.wait()
            # Cancel before dispatching the blocking function. Cleanup must
            # release its gate and retain its task long enough to observe return.
            coordinator.cancel()
            dispatch.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(coordinator, timeout=5)
            assert release.is_set()
            assert coordinator.cancelled()
        finally:
            # A failed test assertion must not leave an executor thread blocked.
            release.set()
            dispatch.set()
            if not coordinator.done():
                coordinator.cancel()
            await asyncio.gather(coordinator, return_exceptions=True)

    asyncio.run(scenario())


def test_cli_finishes_both_runner_shutdowns_without_external_release() -> None:
    script = Path(__file__).resolve().parents[1] / "thread_cancellation.py"
    completed = subprocess.run(
        [sys.executable, str(script)],
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )
    reports = json.loads(completed.stdout)
    assert reports["normal"]["result"] == "thread-result"
    assert reports["cancelled"]["task_cancelled"]
    assert reports["cancelled"]["cancelled_error_observed"]
    assert reports["cancelled"]["side_effect_after_release"]
