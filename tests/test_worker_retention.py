import asyncio
from dataclasses import FrozenInstanceError

import pytest

from queued_worker import OutcomeCounts, QueuedWorker


@pytest.mark.parametrize("limit", [None, 0, 2])
def test_history_limit_keeps_counts_and_latest_details(limit: int | None) -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            if job == "bad":
                raise ValueError("invalid job")

        worker = QueuedWorker(handle, history_limit=limit)
        worker.start()
        for job in ("first", "bad", "last"):
            worker.submit(job)
        report = await worker.shutdown()
        assert report.counts == OutcomeCounts(3, 2, 1, 0, 0)
        if limit is None:
            assert report.completed == ("first", "last")
            assert [failure.job for failure in report.failed] == ["bad"]
            assert report.omitted_outcomes == 0
        elif limit == 0:
            assert report.completed == ()
            assert report.failed == ()
            assert report.omitted_outcomes == 3
        else:
            assert report.completed == ("last",)
            assert [failure.job for failure in report.failed] == ["bad"]
            assert report.omitted_outcomes == 1
        assert await worker.shutdown() == report

    asyncio.run(scenario())


def test_negative_history_limit_is_invalid() -> None:
    async def handle(job: str) -> None:
        pass

    with pytest.raises(ValueError, match="nonnegative"):
        QueuedWorker(handle, history_limit=-1)


@pytest.mark.parametrize("limit", [None, 0, 1])
@pytest.mark.parametrize("failed", [False, True])
def test_eviction_allows_reuse_but_protects_active_ids(
    limit: int | None, failed: bool
) -> None:
    async def scenario() -> None:
        started, release = asyncio.Event(), asyncio.Event()

        async def handle(job: str) -> None:
            if job == "old" and failed:
                raise ValueError("failed job")
            if job == "barrier":
                started.set()
                await release.wait()

        worker = QueuedWorker(handle, history_limit=limit)
        worker.start()
        for job in ("old", "recent", "barrier"):
            worker.submit(job)
        try:
            # Previous outcomes are recorded before the barrier handler runs.
            await started.wait()
            with pytest.raises(ValueError, match="unique"):
                worker.submit("barrier")
            if limit is None:
                with pytest.raises(ValueError, match="unique"):
                    worker.submit("old")
            else:
                worker.submit("old")
            if limit != 0:
                with pytest.raises(ValueError, match="unique"):
                    worker.submit("recent")
        finally:
            release.set()
            report = await worker.shutdown()
        runs = 1 if limit is None else 2
        assert report.counts.accepted == runs + 2
        assert report.counts.failed == (runs if failed else 0)
        assert report.counts.completed == (2 if failed else runs + 2)
        assert report.counts.interrupted == report.counts.unstarted == 0

    asyncio.run(scenario())


def test_finite_retention_does_not_accumulate_old_identifiers() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle, max_pending=1, history_limit=3)
        worker.start()
        try:
            for index in range(300):
                await worker.submit_wait(f"job-{index}")
                # Check the retained-reference bound throughout, not only at exit.
                assert len(worker._known_ids) <= 3 + 1 + 1
        finally:
            report = await worker.shutdown()
        assert report.counts == OutcomeCounts(300, 300, 0, 0, 0)
        assert report.completed == ("job-297", "job-298", "job-299")
        assert report.omitted_outcomes == 297
        assert worker._known_ids == set(report.completed)

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))


@pytest.mark.parametrize("limit", [None, 0, 1, 3])
def test_abort_counts_remain_complete_when_details_are_evicted(
    limit: int | None,
) -> None:
    async def scenario() -> None:
        running = asyncio.Event()

        async def handle(job: str) -> None:
            if job == "bad":
                raise RuntimeError("failed")
            if job == "blocked":
                running.set()
                await asyncio.Event().wait()

        worker = QueuedWorker(handle, history_limit=limit)
        worker.start()
        for job in ("good", "bad", "blocked", "pending-1", "pending-2"):
            worker.submit(job)
        await running.wait()
        report = await worker.shutdown(grace_period=0)
        assert report.counts == OutcomeCounts(5, 1, 1, 1, 2)
        assert report.grace_expired
        retained = 5 if limit is None else min(limit, 5)
        assert report.omitted_outcomes == 5 - retained
        assert (
            len(report.completed)
            + len(report.failed)
            + len(report.interrupted)
            + len(report.unstarted)
        ) == retained
        if limit == 1:
            assert report.unstarted == ("pending-2",)
        if limit == 3:
            assert report.interrupted == ("blocked",)
            assert report.unstarted == ("pending-1", "pending-2")
            assert report.failed == ()
        await asyncio.wait_for(worker._queue.join(), timeout=1)
        assert await worker.shutdown() == report
        if limit is not None:
            assert len(worker._known_ids) == retained

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))


def test_rejected_and_cancelled_submissions_do_not_increment_counts() -> None:
    async def scenario() -> None:
        started, entering = asyncio.Event(), asyncio.Event()

        async def handle(job: str) -> None:
            started.set()
            await asyncio.Event().wait()

        worker = QueuedWorker(handle, max_pending=1, history_limit=1)
        worker.start()
        worker.submit("running")
        await started.wait()
        worker.submit("queued")
        with pytest.raises(ValueError, match="unique"):
            worker.submit("queued")
        with pytest.raises(asyncio.QueueFull):
            worker.submit("rejected")

        async def produce() -> None:
            entering.set()
            await worker.submit_wait("cancelled-producer")

        producer = asyncio.create_task(produce())
        try:
            await entering.wait()
            producer.cancel()
            with pytest.raises(asyncio.CancelledError):
                await producer
        finally:
            report = await worker.shutdown(grace_period=0)
            await asyncio.gather(producer, return_exceptions=True)
        assert report.counts == OutcomeCounts(2, 0, 0, 1, 1)
        assert report.unstarted == ("queued",)
        assert report.omitted_outcomes == 1

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))


def test_report_and_counts_are_immutable() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle, history_limit=1)
        worker.start()
        worker.submit("one")
        report = await worker.shutdown()
        with pytest.raises(FrozenInstanceError):
            report.omitted_outcomes = 100  # type: ignore[misc]
        with pytest.raises(FrozenInstanceError):
            report.counts.completed = 100  # type: ignore[misc]
        assert await worker.shutdown() == report

    asyncio.run(scenario())
