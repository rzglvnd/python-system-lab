import asyncio

import pytest

from queued_worker import QueuedWorker


def test_empty_shutdown_is_repeatable() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pytest.fail(f"unexpected job: {job}")

        worker = QueuedWorker(handle)
        worker.start()
        report = await worker.shutdown()
        assert report.completed == ()
        assert await worker.shutdown() == report
        with pytest.raises(RuntimeError, match="not accepting"):
            worker.submit("late")

    asyncio.run(scenario())


def test_close_drains_accepted_jobs_and_rejects_new_work() -> None:
    async def scenario() -> None:
        started, release = asyncio.Event(), asyncio.Event()
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            if job == "first":
                started.set()
                await release.wait()

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("first")
        worker.submit("second")
        try:
            await started.wait()
            worker.close()
            worker.close()
            with pytest.raises(RuntimeError, match="not accepting"):
                worker.submit("late")
            assert seen == ["first"]
        finally:
            release.set()
            report = await worker.shutdown()
        assert seen == ["first", "second"]
        assert report.completed == ("first", "second")

    asyncio.run(scenario())


def test_lifecycle_and_duplicate_admission() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle)
        with pytest.raises(RuntimeError, match="not accepting"):
            worker.submit("early")
        with pytest.raises(RuntimeError, match="not started"):
            worker.close()
        worker.start()
        try:
            with pytest.raises(RuntimeError, match="already started"):
                worker.start()
            worker.submit("one")
            with pytest.raises(ValueError, match="unique"):
                worker.submit("one")
        finally:
            report = await worker.shutdown()
        assert report.completed == ("one",)

    asyncio.run(scenario())
