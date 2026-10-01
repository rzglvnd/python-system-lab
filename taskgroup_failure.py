"""Observe TaskGroup sibling cancellation and cleanup after one task fails."""

import asyncio


async def worker(
    name: str,
    ready: asyncio.Event,
    proceed: asyncio.Event,
    events: list[str],
    *,
    fails: bool = False,
    working: asyncio.Event | None = None,
) -> None:
    """Run until released; a failing worker causes its sibling to be cancelled."""
    events.append(f"{name}:started")
    ready.set()
    try:
        await proceed.wait()
        if fails:
            events.append(f"{name}:raising")
            raise RuntimeError(f"{name} failed")
        events.append(f"{name}:working")
        if working is not None:
            working.set()
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        events.append(f"{name}:cancelled")
        raise
    finally:
        events.append(f"{name}:cleanup")


async def run_failure_scenario() -> tuple[list[str], list[str]]:
    """Run two coordinated workers and return events plus grouped failures."""
    ready = [asyncio.Event(), asyncio.Event()]
    proceed = asyncio.Event()
    events: list[str] = []
    failures: list[str] = []
    try:
        async with asyncio.TaskGroup() as group:
            group.create_task(worker("failing", ready[0], proceed, events, fails=True))
            group.create_task(worker("sibling", ready[1], proceed, events))
            await asyncio.gather(*(event.wait() for event in ready))
            proceed.set()
    except* RuntimeError as errors:
        failures = [str(error) for error in errors.exceptions]
    return events, failures


def main() -> None:
    """Print the observable lifecycle and ExceptionGroup result."""
    events, failures = asyncio.run(run_failure_scenario())
    print(f"events: {events}")
    print(f"failures: {failures}")


if __name__ == "__main__":
    main()
