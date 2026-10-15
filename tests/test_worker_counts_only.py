import asyncio

import pytest

from queued_worker import OutcomeCounts, QueuedWorker


@pytest.mark.parametrize("cap", [None, 0, 4])
def test_counts_only_skips_formatting_and_allows_failed_id_reuse(
    cap: int | None,
) -> None:
    async def scenario() -> None:
        rendered: list[str] = []
        seen: list[str] = []

        class ObservedError(Exception):
            def __str__(self) -> str:
                rendered.append("str")
                raise AssertionError("message should not be needed")

            def __repr__(self) -> str:
                rendered.append("repr")
                raise AssertionError("repr should not be needed")

        async def handle(job: str) -> None:
            seen.append(job)
            if job == "bad":
                raise ObservedError()

        worker = QueuedWorker(handle, history_limit=0, max_error_message_chars=cap)
        worker.start()
        worker.submit("bad")
        worker.submit("good")
        await asyncio.wait_for(worker._queue.join(), timeout=1)
        # A completed outcome releases its identifier even when the job failed.
        worker.submit("bad")
        report = await worker.shutdown()
        assert rendered == []
        assert seen == ["bad", "good", "bad"]
        assert report.counts == OutcomeCounts(3, 1, 2, 0, 0)
        assert report.completed == ()
        assert report.failed == ()
        assert report.interrupted == report.unstarted == ()
        assert report.omitted_outcomes == 3
        assert await worker.shutdown() == report
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


@pytest.mark.parametrize("history_limit", [None, 1])
def test_retained_failures_still_format_messages_with_a_zero_cap(
    history_limit: int | None,
) -> None:
    async def scenario() -> None:
        rendered: list[str] = []

        class ObservedError(Exception):
            def __str__(self) -> str:
                rendered.append("str")
                return "message"

        async def handle(job: str) -> None:
            raise ObservedError()

        worker = QueuedWorker(
            handle, history_limit=history_limit, max_error_message_chars=0
        )
        worker.start()
        worker.submit("bad")
        report = await worker.shutdown()
        assert rendered == ["str"]
        assert report.counts == OutcomeCounts(1, 0, 1, 0, 0)
        (failure,) = report.failed
        assert failure.message == ""
        assert failure.message_truncated
        assert not failure.message_format_failed

    asyncio.run(scenario())


class FormattingSignal(BaseException):
    pass


@pytest.mark.parametrize("signal_type", [asyncio.CancelledError, FormattingSignal])
def test_counts_only_does_not_execute_signalling_formatters(
    signal_type: type[BaseException],
) -> None:
    async def scenario() -> None:
        rendered: list[str] = []
        seen: list[str] = []

        class SignallingError(Exception):
            def __str__(self) -> str:
                rendered.append("str")
                raise signal_type()

        async def handle(job: str) -> None:
            seen.append(job)
            if job == "bad":
                raise SignallingError()

        worker = QueuedWorker(handle, history_limit=0)
        worker.start()
        worker.submit("bad")
        worker.submit("good")
        report = await worker.shutdown()
        assert rendered == []
        assert seen == ["bad", "good"]
        assert report.counts == OutcomeCounts(2, 1, 1, 0, 0)
        assert report.failed == ()
        assert report.omitted_outcomes == 2

    asyncio.run(scenario())


def test_counts_only_preserves_handler_cancellation_and_accounting() -> None:
    async def scenario() -> None:
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            raise asyncio.CancelledError()

        worker = QueuedWorker(handle, history_limit=0)
        worker.start()
        worker.submit("cancelled")
        worker.submit("pending")
        with pytest.raises(asyncio.CancelledError):
            await worker.shutdown()
        assert worker._task is not None and worker._task.cancelled()
        report = await worker.shutdown()
        assert report.counts == OutcomeCounts(2, 0, 0, 1, 1)
        assert report.completed == ()
        assert report.failed == ()
        assert report.interrupted == report.unstarted == ()
        assert report.omitted_outcomes == 2
        assert seen == ["cancelled"]
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


def test_counts_only_cleanup_failure_stops_at_abort_without_formatting() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        rendered: list[str] = []
        seen: list[str] = []

        class CleanupError(Exception):
            def __str__(self) -> str:
                rendered.append("str")
                return "cleanup failed"

        async def handle(job: str) -> None:
            seen.append(job)
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                raise CleanupError()

        worker = QueuedWorker(handle, history_limit=0)
        worker.start()
        worker.submit("running")
        worker.submit("pending")
        await started.wait()
        report = await worker.shutdown(grace_period=0)
        assert report.grace_expired
        assert rendered == []
        assert seen == ["running"]
        assert report.counts == OutcomeCounts(2, 0, 1, 0, 1)
        assert report.completed == ()
        assert report.failed == ()
        assert report.interrupted == report.unstarted == ()
        assert report.omitted_outcomes == 2
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))
