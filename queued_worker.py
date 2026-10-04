"""A single in-memory worker with explicit admission and shutdown boundaries."""

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass


@dataclass(frozen=True)
class ShutdownReport:
    completed: tuple[str, ...]
    interrupted: tuple[str, ...]
    unstarted: tuple[str, ...]
    grace_expired: bool


class QueuedWorker:
    """Use from one event loop; job strings are unique identifiers."""

    def __init__(self, handler: Callable[[str], Awaitable[None]]) -> None:
        self._handler = handler
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._closing = False
        self._accepted: set[str] = set()
        self._completed: list[str] = []
        self._interrupted: list[str] = []
        self._unstarted: list[str] = []
        self._grace_expired = False
        self._shutting_down = False

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

    async def shutdown(self, grace_period: float | None = None) -> ShutdownReport:
        """Drain, then cancel on grace expiry; cancellation remains cooperative."""
        if grace_period is not None and (
            not math.isfinite(grace_period) or grace_period < 0
        ):
            raise ValueError("grace period must be finite and nonnegative")
        if self._shutting_down:
            raise RuntimeError("shutdown already in progress")
        self.close()
        assert self._task is not None
        self._shutting_down = True
        try:
            if self._task.cancelled():
                pass
            elif grace_period is None:
                await asyncio.shield(self._task)
            else:
                done, _ = await asyncio.wait({self._task}, timeout=grace_period)
                if not done:
                    self._grace_expired = True
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                else:
                    await self._task
        except asyncio.CancelledError:
            # A cancelled supervisor still joins the worker before propagating.
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            raise
        finally:
            if self._task.done():
                self._discard_pending()
            self._shutting_down = False
        return ShutdownReport(
            tuple(self._completed),
            tuple(self._interrupted),
            tuple(self._unstarted),
            self._grace_expired,
        )

    def _discard_pending(self) -> None:
        while not self._queue.empty():
            job = self._queue.get_nowait()
            if job is not None:
                self._unstarted.append(job)
            self._queue.task_done()

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                if job is None:
                    return
                try:
                    await self._handler(job)
                except asyncio.CancelledError:
                    self._interrupted.append(job)
                    raise
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
