"""Compare cancellation through an await with shielding an explicitly owned task."""

import asyncio


async def shield_scenario(
    events: list[str], *, shielded: bool = True, fail_cleanup: bool = False
) -> None:
    started, awaiting, release = (asyncio.Event() for _ in range(3))

    async def cleanup() -> None:
        events.append("cleanup:started")
        started.set()
        try:
            await release.wait()
            if fail_cleanup:
                events.append("cleanup:failed")
                raise ValueError("cleanup failed")
            events.append("cleanup:finished")
        except asyncio.CancelledError:
            events.append("cleanup:cancelled")
            raise

    # The supervisor owns a strong reference independently of the cancelled caller.
    cleanup_task = asyncio.create_task(cleanup())

    async def caller() -> None:
        awaiting.set()
        if shielded:
            await asyncio.shield(cleanup_task)
        else:
            await cleanup_task

    caller_task = asyncio.create_task(caller())
    try:
        await started.wait()
        await awaiting.wait()
        caller_task.cancel()
        try:
            await caller_task
        except asyncio.CancelledError:
            events.append("caller:cancelled")
        events.append(f"cleanup:pending={not cleanup_task.done()}")
    finally:
        release.set()
        # Drain both owned tasks, including when the supervisor is interrupted.
        if not caller_task.done():
            caller_task.cancel()
        try:
            await caller_task
        except asyncio.CancelledError:
            pass
        try:
            await cleanup_task
        except asyncio.CancelledError:
            if shielded:
                raise
    events.append("owner:joined")


async def main() -> None:
    for shielded in (False, True):
        events: list[str] = []
        await shield_scenario(events, shielded=shielded)
        print(f"shielded={shielded}: {events}")


if __name__ == "__main__":
    asyncio.run(main())
