"""Observe caller cancellation of a group of running workers."""

import asyncio


async def worker(
    name: str, ready: asyncio.Event, finish: asyncio.Event, events: list[str]
) -> None:
    events.append(f"{name}:started")
    ready.set()
    try:
        await finish.wait()
        events.append(f"{name}:finished")
    except asyncio.CancelledError:
        events.append(f"{name}:cancelled")
        raise
    finally:
        events.append(f"{name}:cleanup")


async def run_group(
    ready: asyncio.Event, finish: asyncio.Event, events: list[str]
) -> None:
    """Signal readiness only once both children have started."""
    started = [asyncio.Event(), asyncio.Event()]
    async with asyncio.TaskGroup() as group:
        for name, signal in zip(("first", "second"), started, strict=True):
            group.create_task(worker(name, signal, finish, events))
        await asyncio.gather(*(signal.wait() for signal in started))
        ready.set()


async def cancellation_scenario() -> list[str]:
    events: list[str] = []
    ready, finish = asyncio.Event(), asyncio.Event()
    parent = asyncio.create_task(run_group(ready, finish, events))
    await ready.wait()
    parent.cancel()
    try:
        await parent
    except asyncio.CancelledError:
        events.append("caller:cancelled")
    return events


if __name__ == "__main__":
    print(asyncio.run(cancellation_scenario()))
