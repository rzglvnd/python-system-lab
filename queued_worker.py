"""A single in-memory worker with explicit admission and shutdown boundaries."""

import asyncio
import math
from collections import deque
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Literal

OutcomeKind = Literal["completed", "failed", "interrupted", "unstarted"]


@dataclass(frozen=True)
class JobFailure:
    job: str
    error_type: str
    message: str


@dataclass(frozen=True)
class OutcomeCounts:
    accepted: int
    completed: int
    failed: int
    interrupted: int
    unstarted: int


@dataclass(frozen=True)
class _Outcome:
    kind: OutcomeKind
    job: str
    failure: JobFailure | None = None


@dataclass(frozen=True)
class ShutdownReport:
    completed: tuple[str, ...]
    failed: tuple[JobFailure, ...]
    interrupted: tuple[str, ...]
    unstarted: tuple[str, ...]
    grace_expired: bool
    counts: OutcomeCounts
    omitted_outcomes: int


class QueuedWorker:
    """Use from one event loop; identifiers are unique among active/retained jobs."""

    def __init__(
        self,
        handler: Callable[[str], Awaitable[None]],
        *,
        max_pending: int = 0,
        history_limit: int | None = None,
        max_job_id_bytes: int | None = None,
    ) -> None:
        if isinstance(max_pending, bool) or not isinstance(max_pending, int):
            raise TypeError("max_pending must be an integer")
        if max_pending < 0:
            raise ValueError("max_pending must be nonnegative")
        if history_limit is not None:
            if isinstance(history_limit, bool) or not isinstance(history_limit, int):
                raise TypeError("history_limit must be an integer or None")
            if history_limit < 0:
                raise ValueError("history_limit must be nonnegative")
        if max_job_id_bytes is not None:
            if isinstance(max_job_id_bytes, bool) or not isinstance(
                max_job_id_bytes, int
            ):
                raise TypeError("max_job_id_bytes must be an integer or None")
            if max_job_id_bytes < 0:
                raise ValueError("max_job_id_bytes must be nonnegative")
        self._handler = handler
        self._max_pending = max_pending
        self._max_job_id_bytes = max_job_id_bytes
        # Reserve a control slot so close() can enqueue its marker even at capacity.
        self._queue: asyncio.Queue[str | None] = asyncio.Queue(
            maxsize=max_pending + 1 if max_pending else 0
        )
        self._task: asyncio.Task[None] | None = None
        self._closing = False
        self._known_ids: set[str] = set()
        self._history: deque[_Outcome] = deque(maxlen=history_limit)
        self._accepted_count = 0
        self._counts: dict[OutcomeKind, int] = {
            "completed": 0,
            "failed": 0,
            "interrupted": 0,
            "unstarted": 0,
        }
        self._grace_expired = False
        self._shutting_down = False
        self._abort_requested = False
        self._admission_changed = asyncio.Event()

    def start(self) -> None:
        if self._task is not None:
            raise RuntimeError("worker already started")
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(lambda task: self._notify_admission())

    def submit(self, job: str) -> None:
        if self._task is None or self._closing or self._task.done():
            raise RuntimeError("worker is not accepting jobs")
        self._validate_job_id(job)
        if job in self._known_ids:
            raise ValueError(
                "job identifiers must be unique among active and retained jobs"
            )
        if self._max_pending and self._queue.qsize() >= self._max_pending:
            raise asyncio.QueueFull
        self._queue.put_nowait(job)
        self._known_ids.add(job)
        self._accepted_count += 1

    def _validate_job_id(self, job: str) -> None:
        limit = self._max_job_id_bytes
        if limit is None:
            return
        # Every valid UTF-8 code point uses at least one byte. Reject clearly
        # oversized strings before allocating an encoded copy of the whole ID.
        if len(job) > limit:
            raise ValueError("job identifier exceeds max_job_id_bytes")
        try:
            encoded_size = len(job.encode("utf-8"))
        except UnicodeEncodeError as error:
            raise ValueError("job identifier must be encodable as UTF-8") from error
        if encoded_size > limit:
            raise ValueError("job identifier exceeds max_job_id_bytes")

    async def submit_wait(self, job: str) -> None:
        """Wait for capacity; acceptance occurs at submit()'s atomic enqueue."""
        while True:
            try:
                self.submit(job)
                return
            except asyncio.QueueFull:
                # No await occurs between checking capacity and subscribing.
                changed = self._admission_changed
            await changed.wait()

    def _notify_admission(self) -> None:
        """Broadcast a state change; every awakened producer must recheck."""
        self._admission_changed.set()
        self._admission_changed = asyncio.Event()

    def close(self) -> None:
        """Synchronously stop admission; place the stop marker after accepted work."""
        if self._task is None:
            raise RuntimeError("worker has not started")
        if not self._closing:
            self._closing = True
            self._queue.put_nowait(None)
            self._notify_admission()

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
                    self._abort_requested = True
                    self._task.cancel()
                    try:
                        await self._task
                    except asyncio.CancelledError:
                        pass
                else:
                    await self._task
        except asyncio.CancelledError:
            # A cancelled supervisor still joins the worker before propagating.
            self._abort_requested = True
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
            tuple(item.job for item in self._history if item.kind == "completed"),
            tuple(item.failure for item in self._history if item.failure is not None),
            tuple(item.job for item in self._history if item.kind == "interrupted"),
            tuple(item.job for item in self._history if item.kind == "unstarted"),
            self._grace_expired,
            OutcomeCounts(
                self._accepted_count,
                self._counts["completed"],
                self._counts["failed"],
                self._counts["interrupted"],
                self._counts["unstarted"],
            ),
            sum(self._counts.values()) - len(self._history),
        )

    def _record(
        self, kind: OutcomeKind, job: str, failure: JobFailure | None = None
    ) -> None:
        self._counts[kind] += 1
        if self._history.maxlen == 0:
            self._known_ids.remove(job)
        elif (
            self._history.maxlen is not None
            and len(self._history) == self._history.maxlen
        ):
            self._known_ids.remove(self._history[0].job)
        self._history.append(_Outcome(kind, job, failure))

    def _discard_pending(self) -> None:
        while not self._queue.empty():
            job = self._queue.get_nowait()
            if job is not None:
                self._record("unstarted", job)
            self._queue.task_done()

    async def _run(self) -> None:
        while not self._abort_requested:
            job = await self._queue.get()
            self._notify_admission()
            try:
                if job is None:
                    return
                try:
                    await self._handler(job)
                except asyncio.CancelledError:
                    self._record("interrupted", job)
                    raise
                except Exception as error:
                    self._record(
                        "failed", job, JobFailure(job, type(error).__name__, str(error))
                    )
                else:
                    self._record("completed", job)
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
