"""Show how a second cancellation can interrupt an awaited cleanup operation."""

import asyncio


async def cancellation_scenario(*, cancel_again: bool = True) -> list[str]:
    started, cleaning, release = (asyncio.Event() for _ in range(3))
    events: list[str] = []

    async def worker() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            events.append("cleanup:started")
            cleaning.set()
            try:
                await release.wait()
                events.append("cleanup:finished")
            except asyncio.CancelledError:
                events.append("cleanup:interrupted")
                raise

    task = asyncio.create_task(worker())
    try:
        await started.wait()
        task.cancel()
        await cleaning.wait()
        if cancel_again:
            task.cancel()
        else:
            release.set()
        try:
            await task
        except asyncio.CancelledError:
            events.append("caller:cancelled")
    finally:
        release.set()
        if not task.done():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
    return events


if __name__ == "__main__":
    print(asyncio.run(cancellation_scenario()))
