"""Cancellation starts cleanup; it does not impose a cleanup completion deadline."""

import asyncio

import pytest


@pytest.mark.parametrize("expire", [False, True], ids=["caller-cancel", "timeout"])
def test_group_waits_for_asynchronous_cleanup(expire: bool) -> None:
    async def scenario() -> list[str]:
        started = asyncio.Event()
        cleaning = asyncio.Event()
        release_cleanup = asyncio.Event()
        events: list[str] = []

        async def worker() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append("cleanup:started")
                cleaning.set()
                await release_cleanup.wait()
                events.append("cleanup:finished")

        async def parent() -> None:
            async with asyncio.timeout(None) as deadline:
                async with asyncio.TaskGroup() as group:
                    group.create_task(worker())
                    await started.wait()
                    if expire:
                        deadline.reschedule(asyncio.get_running_loop().time() - 1)
                    await asyncio.Event().wait()

        task = asyncio.create_task(parent())
        try:
            await started.wait()
            if not expire:
                task.cancel()
            await cleaning.wait()
            assert events == ["cleanup:started"]
            assert not task.done()
        finally:
            release_cleanup.set()
            with pytest.raises(TimeoutError if expire else asyncio.CancelledError):
                await task
        events.append("caller:observed")
        return events

    assert asyncio.run(scenario()) == [
        "cleanup:started",
        "cleanup:finished",
        "caller:observed",
    ]
