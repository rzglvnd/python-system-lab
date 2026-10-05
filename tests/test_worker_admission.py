import asyncio

import pytest

from queued_worker import QueuedWorker


def test_full_queue_rejects_without_accepting_and_still_closes() -> None:
    async def scenario() -> None:
        started, release = asyncio.Event(), asyncio.Event()

        async def handle(job: str) -> None:
            if job == "running":
                started.set()
                await release.wait()

        worker = QueuedWorker(handle, max_pending=1)
        worker.start()
        worker.submit("running")
        try:
            await started.wait()
            worker.submit("queued")
            with pytest.raises(asyncio.QueueFull):
                worker.submit("rejected")
            worker.close()
            worker.close()
            with pytest.raises(RuntimeError, match="not accepting"):
                worker.submit("late")
        finally:
            release.set()
            report = await worker.shutdown()
        assert report.completed == ("running", "queued")
        assert report.failed == ()
        assert report.interrupted == report.unstarted == ()
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


def test_rejected_identifier_can_be_retried_after_capacity_frees() -> None:
    async def scenario() -> None:
        started, release = asyncio.Event(), asyncio.Event()

        async def handle(job: str) -> None:
            started.set()
            await release.wait()

        worker = QueuedWorker(handle, max_pending=1)
        worker.start()
        worker.submit("first")
        with pytest.raises(asyncio.QueueFull):
            worker.submit("retry")
        try:
            await started.wait()
            worker.submit("retry")
        finally:
            release.set()
            report = await worker.shutdown()
        assert report.completed == ("first", "retry")

    asyncio.run(scenario())


@pytest.mark.parametrize("limit", [-1, -10])
def test_negative_capacity_is_invalid(limit: int) -> None:
    async def handle(job: str) -> None:
        pass

    with pytest.raises(ValueError, match="nonnegative"):
        QueuedWorker(handle, max_pending=limit)
