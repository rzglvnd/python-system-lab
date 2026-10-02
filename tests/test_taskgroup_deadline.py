import asyncio

import pytest

from taskgroup_deadline import deadline_scenario


def test_expired_deadline_waits_for_cleanup() -> None:
    async def scenario() -> list[str]:
        events: list[str] = []
        with pytest.raises(TimeoutError):
            await deadline_scenario(events)
        events.append("caller:timeout")
        return events

    events = asyncio.run(scenario())
    assert events[-1] == "caller:timeout"
    for name in ("first", "second"):
        assert [event for event in events if event.startswith(name)] == [
            f"{name}:started",
            f"{name}:cancelled",
            f"{name}:cleanup",
        ]


def test_workers_finish_before_deadline() -> None:
    events: list[str] = []
    asyncio.run(deadline_scenario(events, expire=False))
    for name in ("first", "second"):
        assert [event for event in events if event.startswith(name)] == [
            f"{name}:started",
            f"{name}:finished",
            f"{name}:cleanup",
        ]
