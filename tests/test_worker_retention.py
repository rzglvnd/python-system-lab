import asyncio

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
