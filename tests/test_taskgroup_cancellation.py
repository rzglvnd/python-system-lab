import asyncio

from taskgroup_cancellation import cancellation_scenario, run_group


def test_parent_cancellation_waits_for_both_children() -> None:
    events = asyncio.run(cancellation_scenario())
    assert events[-1] == "caller:cancelled"
    for name in ("first", "second"):
        assert [event for event in events if event.startswith(name)] == [
            f"{name}:started",
            f"{name}:cancelled",
            f"{name}:cleanup",
        ]


def test_group_can_finish_normally() -> None:
    async def scenario() -> list[str]:
        ready, finish = asyncio.Event(), asyncio.Event()
        events: list[str] = []
        parent = asyncio.create_task(run_group(ready, finish, events))
        await ready.wait()
        finish.set()
        await parent
        return events

    events = asyncio.run(scenario())
    for name in ("first", "second"):
        assert [event for event in events if event.startswith(name)] == [
            f"{name}:started",
            f"{name}:finished",
            f"{name}:cleanup",
        ]
