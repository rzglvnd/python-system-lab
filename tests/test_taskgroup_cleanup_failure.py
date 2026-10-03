"""Observe worker and cleanup failures without relying on sibling ordering."""

import asyncio

import pytest


@pytest.mark.parametrize("cleanup_fails", [False, True])
def test_group_preserves_failure_and_waits_for_remaining_cleanup(
    cleanup_fails: bool,
) -> None:
    events: list[str] = []

    async def scenario() -> None:
        ready = [asyncio.Event() for _ in range(3)]
        release_failure = asyncio.Event()

        async def failing_worker() -> None:
            ready[0].set()
            await release_failure.wait()
            raise RuntimeError("work failed")

        async def sibling(index: int) -> None:
            ready[index].set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append(f"sibling-{index}:cleanup")
                if index == 1 and cleanup_fails:
                    raise ValueError("cleanup failed")

        async with asyncio.TaskGroup() as group:
            group.create_task(failing_worker())
            group.create_task(sibling(1))
            group.create_task(sibling(2))
            await asyncio.gather(*(signal.wait() for signal in ready))
            release_failure.set()

    with pytest.raises(ExceptionGroup) as caught:
        asyncio.run(scenario())
    failures = {(type(error), str(error)) for error in caught.value.exceptions}
    expected: set[tuple[type[Exception], str]] = {(RuntimeError, "work failed")}
    if cleanup_fails:
        expected.add((ValueError, "cleanup failed"))
    assert failures == expected
    assert len(caught.value.exceptions) == len(expected)
    assert sorted(events) == ["sibling-1:cleanup", "sibling-2:cleanup"]
