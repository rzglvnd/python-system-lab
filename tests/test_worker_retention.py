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
