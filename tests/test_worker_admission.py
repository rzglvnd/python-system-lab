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


def test_waiting_submission_resumes_when_capacity_frees() -> None:
    async def scenario() -> None:
        started, entering, release = (asyncio.Event() for _ in range(3))

        async def handle(job: str) -> None:
            if job == "running":
                started.set()
                await release.wait()

        worker = QueuedWorker(handle, max_pending=1)
        worker.start()
        worker.submit("running")
        await started.wait()
        worker.submit("queued")

        async def produce() -> None:
            entering.set()
            await worker.submit_wait("waiting")

        producer = asyncio.create_task(produce())
        try:
            await entering.wait()
            assert not producer.done()
            release.set()
            await producer
        finally:
            release.set()
            await worker.shutdown()
            await asyncio.gather(producer, return_exceptions=True)
        report = await worker.shutdown()
        assert report.completed == ("running", "queued", "waiting")

    asyncio.run(scenario())


def test_closure_wakes_waiting_producer_without_accepting_job() -> None:
    async def scenario() -> None:
        started, entering, release = (asyncio.Event() for _ in range(3))

        async def handle(job: str) -> None:
            started.set()
            await release.wait()

        worker = QueuedWorker(handle, max_pending=1)
        worker.start()
        worker.submit("running")
        await started.wait()
        worker.submit("queued")

        async def produce() -> None:
            entering.set()
            await worker.submit_wait("waiting")

        producer = asyncio.create_task(produce())
        try:
            await entering.wait()
            worker.close()
            with pytest.raises(RuntimeError, match="not accepting"):
                await producer
        finally:
            release.set()
            report = await worker.shutdown()
            await asyncio.gather(producer, return_exceptions=True)
        assert report.completed == ("running", "queued")

    asyncio.run(scenario())


def test_cancelling_waiting_submission_leaves_identifier_available() -> None:
    async def scenario() -> None:
        started, entering, release = (asyncio.Event() for _ in range(3))

        async def handle(job: str) -> None:
            if job == "running":
                started.set()
                await release.wait()

        worker = QueuedWorker(handle, max_pending=1)
        worker.start()
        worker.submit("running")
        await started.wait()
        worker.submit("queued")

        async def produce() -> None:
            entering.set()
            await worker.submit_wait("retry")

        producer = asyncio.create_task(produce())
        try:
            await entering.wait()
            producer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await producer
            release.set()
            await worker.submit_wait("retry")
        finally:
            release.set()
            report = await worker.shutdown()
            await asyncio.gather(producer, return_exceptions=True)
        assert report.completed == ("running", "queued", "retry")

    asyncio.run(scenario())
