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


def test_grace_expiry_reports_interrupted_and_unstarted_jobs() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        cleaned: list[str] = []

        async def handle(job: str) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(job)

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("running")
        worker.submit("queued")
        await started.wait()
        report = await worker.shutdown(grace_period=0)
        assert report.completed == ()
        assert report.interrupted == ("running",)
        assert report.unstarted == ("queued",)
        assert report.grace_expired
        assert cleaned == ["running"]
        assert await worker.shutdown() == report

    asyncio.run(scenario())


def test_expired_grace_still_waits_for_async_cleanup() -> None:
    async def scenario() -> None:
        started, cleaning, release = (asyncio.Event() for _ in range(3))

        async def handle(job: str) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaning.set()
                await release.wait()

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("running")
        await started.wait()
        shutdown = asyncio.create_task(worker.shutdown(grace_period=0))
        try:
            await cleaning.wait()
            assert not shutdown.done()
            with pytest.raises(RuntimeError, match="already in progress"):
                await worker.shutdown()
        finally:
            release.set()
            report = await shutdown
        assert report.interrupted == ("running",)

    asyncio.run(scenario())


def test_completion_within_grace_period() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("one")
        report = await worker.shutdown(grace_period=3600)
        assert report.completed == ("one",)
        assert report.interrupted == report.unstarted == ()
        assert not report.grace_expired

    asyncio.run(scenario())


@pytest.mark.parametrize("grace", [-1, float("nan"), float("inf")])
def test_invalid_grace_does_not_close_admission(grace: float) -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle)
        worker.start()
        try:
            with pytest.raises(ValueError, match="finite and nonnegative"):
                await worker.shutdown(grace)
            worker.submit("still-open")
        finally:
            report = await worker.shutdown()
        assert report.completed == ("still-open",)

    asyncio.run(scenario())


def test_caller_cancellation_joins_worker_and_propagates() -> None:
    async def scenario() -> None:
        started, waiting = asyncio.Event(), asyncio.Event()
        cleaned: list[str] = []

        async def handle(job: str) -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append(job)

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("one")
        await started.wait()

        async def shutdown() -> None:
            waiting.set()
            await worker.shutdown()

        caller = asyncio.create_task(shutdown())
        await waiting.wait()
        caller.cancel()
        with pytest.raises(asyncio.CancelledError):
            await caller
        assert cleaned == ["one"]
        report = await worker.shutdown()
        assert report.interrupted == ("one",)
        assert not report.grace_expired

    asyncio.run(scenario())


def test_failed_job_does_not_stop_drain_or_count_as_success() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            if job == "bad":
                raise ValueError("invalid job")

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("bad")
        worker.submit("good")
        report = await worker.shutdown()
        assert report.completed == ("good",)
        assert [(f.job, f.error_type, f.message) for f in report.failed] == [
            ("bad", "ValueError", "invalid job")
        ]
        assert report.interrupted == report.unstarted == ()
        # White-box check of queue accounting, not a success criterion for jobs.
        await asyncio.wait_for(worker._queue.join(), timeout=1)
        assert await worker.shutdown() == report

    asyncio.run(scenario())


def test_each_accepted_job_has_exactly_one_outcome() -> None:
    async def scenario() -> None:
        running = asyncio.Event()

        async def handle(job: str) -> None:
            if job == "bad":
                raise RuntimeError("job failed")
            if job == "blocked":
                running.set()
                await asyncio.Event().wait()

        worker = QueuedWorker(handle)
        worker.start()
        accepted = ["good", "bad", "blocked", "queued"]
        for job in accepted:
            worker.submit(job)
        await running.wait()
        report = await worker.shutdown(grace_period=0)
        assert report.completed == ("good",)
        assert [failure.job for failure in report.failed] == ["bad"]
        assert report.interrupted == ("blocked",)
        assert report.unstarted == ("queued",)
        outcomes = [
            *report.completed,
            *(failure.job for failure in report.failed),
            *report.interrupted,
            *report.unstarted,
        ]
        assert sorted(outcomes) == sorted(accepted)
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


def test_cleanup_failure_after_deadline_does_not_start_next_job() -> None:
    async def scenario() -> None:
        running = asyncio.Event()
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            running.set()
            try:
                await asyncio.Event().wait()
            finally:
                raise ValueError("cleanup failed")

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("running")
        worker.submit("queued")
        await running.wait()
        report = await worker.shutdown(grace_period=0)
        assert seen == ["running"]
        assert report.completed == report.interrupted == ()
        assert [(f.job, f.message) for f in report.failed] == [
            ("running", "cleanup failed")
        ]
        assert report.unstarted == ("queued",)
        assert report.grace_expired
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())
