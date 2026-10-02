import asyncio

import pytest

from taskgroup_failure import run_failure_scenario, worker


def test_taskgroup_cancels_sibling_and_runs_all_cleanup() -> None:
    events, failures = asyncio.run(run_failure_scenario())

    assert failures == ["failing failed"]
    assert events.count("failing:started") == 1
    assert events.count("sibling:started") == 1
    assert events.count("failing:raising") == 1
    assert events.count("sibling:working") == 1
    assert events.count("sibling:cancelled") == 1
    assert events.count("failing:cleanup") == 1
    assert events.count("sibling:cleanup") == 1
    assert events.index("sibling:cancelled") < events.index("sibling:cleanup")


def test_worker_propagates_cancellation_after_cleanup() -> None:
    async def scenario() -> list[str]:
        ready = asyncio.Event()
        proceed = asyncio.Event()
        working = asyncio.Event()
        events: list[str] = []
        task = asyncio.create_task(
            worker("one", ready, proceed, events, working=working)
        )
        await ready.wait()
        proceed.set()
        await working.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return events

    assert asyncio.run(scenario()) == [
        "one:started",
        "one:working",
        "one:cancelled",
        "one:cleanup",
    ]


def test_group_does_not_start_work_before_coordinator_releases_it() -> None:
    async def scenario() -> list[str]:
        ready = asyncio.Event()
        proceed = asyncio.Event()
        events: list[str] = []
        task = asyncio.create_task(worker("one", ready, proceed, events))
        await ready.wait()
        assert events == ["one:started"]
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        return events

    assert asyncio.run(scenario()) == ["one:started", "one:cancelled", "one:cleanup"]
