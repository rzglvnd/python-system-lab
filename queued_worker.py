"""A single in-memory worker with explicit admission and shutdown boundaries."""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ShutdownReport:
    completed: tuple[str, ...]


class QueuedWorker:
    """Use from one event loop; job strings are unique identifiers."""

    def __init__(self, handler: Callable[[str], Awaitable[None]]) -> None:
        self._handler = handler
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._closing = False
        self._accepted: set[str] = set()
        self._completed: list[str] = []

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("worker already started")
        self._task = asyncio.create_task(self._run())

    def submit(self, job: str) -> None:
        if self._task is None or self._closing or self._task.done():
            raise RuntimeError("worker is not accepting jobs")
        if job in self._accepted:
            raise ValueError("job identifiers must be unique")
        self._queue.put_nowait(job)
        self._accepted.add(job)

    def close(self) -> None:
        """Synchronously stop admission; place the stop marker after accepted work."""
        if self._task is None:
            raise RuntimeError("worker has not started")
        if not self._closing:
            self._closing = True
            self._queue.put_nowait(None)

    async def shutdown(self) -> ShutdownReport:
        self.close()
        assert self._task is not None
        await self._task
        return ShutdownReport(tuple(self._completed))

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                if job is None:
                    return
                await self._handler(job)
                self._completed.append(job)
            finally:
                self._queue.task_done()


async def main() -> None:
    async def handle(job: str) -> None:
        print(f"processed: {job}")

    worker = QueuedWorker(handle)
    worker.start()
    for job in ("first", "second", "third"):
        worker.submit(job)
    print(await worker.shutdown())


if __name__ == "__main__":
    asyncio.run(main())
