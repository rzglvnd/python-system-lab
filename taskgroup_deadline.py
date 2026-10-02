"""Observe a deadline cancelling a TaskGroup and waiting for cleanup."""

import asyncio

from taskgroup_cancellation import run_group


async def deadline_scenario(events: list[str], *, expire: bool = True) -> None:
    """Arm an expired deadline only after workers start, or let them finish."""
    ready, finish = asyncio.Event(), asyncio.Event()
    async with asyncio.timeout(None) as deadline:
        parent = asyncio.create_task(run_group(ready, finish, events))
        try:
            await ready.wait()
            if expire:
                deadline.reschedule(asyncio.get_running_loop().time() - 1)
            else:
                deadline.reschedule(asyncio.get_running_loop().time() + 3600)
                finish.set()
            await parent
        finally:
            # Own the task even if this coordinator is cancelled during readiness.
            if not parent.done():
                parent.cancel()
                try:
                    await parent
                except asyncio.CancelledError:
                    pass


async def main() -> None:
    events: list[str] = []
    try:
        await deadline_scenario(events)
    except TimeoutError:
        events.append("caller:timeout")
    print(events)


if __name__ == "__main__":
    asyncio.run(main())
