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


@pytest.mark.parametrize("action", ["close", "cancel"])
def test_capacity_wakeup_rechecks_closure_and_cancellation(action: str) -> None:
    async def scenario() -> None:
        started, entering, release_first, release_second = (
            asyncio.Event() for _ in range(4)
        )
        producer: asyncio.Task[None] | None = None

        async def handle(job: str) -> None:
            if job == "running":
                started.set()
                await release_first.wait()
            elif job == "queued":
                # Dequeue has just notified capacity waiters. They cannot resume
                # before this handler synchronously closes or cancels them.
                if action == "close":
                    worker.close()
                else:
                    assert producer is not None
                    producer.cancel()
                await release_second.wait()

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
            release_first.set()
            if action == "close":
                with pytest.raises(RuntimeError, match="not accepting"):
                    await producer
            else:
                with pytest.raises(asyncio.CancelledError):
                    await producer
        finally:
            release_first.set()
            release_second.set()
            report = await worker.shutdown()
            await asyncio.gather(producer, return_exceptions=True)
        assert report.completed == ("running", "queued")
        assert report.unstarted == ()

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))


@pytest.mark.parametrize("duplicate", [False, True])
def test_competing_waiters_cannot_overfill_or_duplicate_jobs(duplicate: bool) -> None:
    async def scenario() -> None:
        started, release_first, release_second = (asyncio.Event() for _ in range(3))
        entering = [asyncio.Event(), asyncio.Event()]
        admitted: asyncio.Queue[str] = asyncio.Queue()

        async def handle(job: str) -> None:
            if job == "running":
                started.set()
                await release_first.wait()
            elif job == "queued":
                await release_second.wait()

        worker = QueuedWorker(handle, max_pending=1)
        worker.start()
        worker.submit("running")
        await started.wait()
        worker.submit("queued")
        jobs = ["same", "same"] if duplicate else ["one", "two"]

        async def produce(index: int) -> None:
            entering[index].set()
            await worker.submit_wait(jobs[index])
            admitted.put_nowait(jobs[index])

        producers = [asyncio.create_task(produce(index)) for index in range(2)]
        try:
            await asyncio.gather(*(signal.wait() for signal in entering))
            assert all(not task.done() for task in producers)
            release_first.set()
            accepted = await admitted.get()
            if duplicate:
                results = await asyncio.gather(*producers, return_exceptions=True)
                assert sum(isinstance(result, ValueError) for result in results) == 1
            else:
                # One waiter took the single free slot; the other must still wait.
                assert sum(not task.done() for task in producers) == 1
                worker.close()
                results = await asyncio.gather(*producers, return_exceptions=True)
                assert sum(isinstance(result, RuntimeError) for result in results) == 1
            assert results.count(None) == 1
        finally:
            worker.close()
            release_first.set()
            release_second.set()
            report = await worker.shutdown()
            await asyncio.gather(*producers, return_exceptions=True)
        assert report.completed == ("running", "queued", accepted)
        assert admitted.empty()

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))


def test_worker_termination_wakes_waiting_producer() -> None:
    async def scenario() -> None:
        started, entering, release = (asyncio.Event() for _ in range(3))

        async def handle(job: str) -> None:
            started.set()
            await release.wait()
            raise asyncio.CancelledError

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
            release.set()
            with pytest.raises(RuntimeError, match="not accepting"):
                await producer
        finally:
            release.set()
            report = await worker.shutdown()
            await asyncio.gather(producer, return_exceptions=True)
        assert report.completed == ()
        assert report.interrupted == ("running",)
        assert report.unstarted == ("queued",)

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))
