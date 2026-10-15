import asyncio
from typing import cast

import pytest

from queued_worker import JobFailure, OutcomeCounts, QueuedWorker


class BrokenMessageError(Exception):
    def __str__(self) -> str:
        raise RuntimeError("formatting failed")

    def __repr__(self) -> str:
        raise AssertionError("fallback must not call repr")


class NonStringMessageError(BrokenMessageError):
    def __str__(self) -> str:
        return cast(str, 42)


@pytest.mark.parametrize("error_type", [BrokenMessageError, NonStringMessageError])
@pytest.mark.parametrize(
    ("cap", "expected", "truncated"),
    [
        (None, "error message unavailable", False),
        (0, "", True),
        (4, "erro", True),
        (25, "error message unavailable", False),
        (26, "error message unavailable", False),
    ],
    ids=["unrestricted", "zero", "short-prefix", "exact", "below-limit"],
)
def test_formatting_fallback_preserves_failures_and_continues_draining(
    error_type: type[BrokenMessageError],
    cap: int | None,
    expected: str,
    truncated: bool,
) -> None:
    async def scenario() -> None:
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            if job == "broken":
                raise error_type()
            if job == "ordinary":
                # Identical text must remain distinguishable from a fallback.
                raise ValueError("error message unavailable")

        worker = QueuedWorker(handle, max_error_message_chars=cap)
        worker.start()
        for job in ("broken", "ordinary", "good"):
            worker.submit(job)
        report = await worker.shutdown()
        assert report.failed == (
            JobFailure("broken", error_type.__name__, expected, truncated, True),
            JobFailure("ordinary", "ValueError", expected, truncated),
        )
        assert seen == ["broken", "ordinary", "good"]
        assert report.completed == ("good",)
        assert report.interrupted == report.unstarted == ()
        assert report.counts == OutcomeCounts(3, 1, 2, 0, 0)
        assert report.omitted_outcomes == 0
        assert await worker.shutdown() == report
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


@pytest.mark.parametrize("history_limit", [None, 0, 1, 2])
def test_formatting_fallback_obeys_history_retention(history_limit: int | None) -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            if job == "broken":
                raise BrokenMessageError()

        worker = QueuedWorker(handle, history_limit=history_limit)
        worker.start()
        worker.submit("broken")
        worker.submit("good")
        report = await worker.shutdown()
        assert report.counts == OutcomeCounts(2, 1, 1, 0, 0)
        if history_limit is None or history_limit == 2:
            assert report.failed == (
                JobFailure(
                    "broken",
                    "BrokenMessageError",
                    "error message unavailable",
                    message_format_failed=True,
                ),
            )
            assert report.completed == ("good",)
            assert report.omitted_outcomes == 0
        else:
            assert report.failed == ()
            assert report.completed == (() if history_limit == 0 else ("good",))
            assert report.omitted_outcomes == 2 - history_limit
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


def test_cleanup_formatting_failure_does_not_start_work_after_abort() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise BrokenMessageError() from None

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("running")
        worker.submit("pending")
        await started.wait()
        report = await worker.shutdown(grace_period=0)
        assert report.grace_expired
        assert report.counts == OutcomeCounts(2, 0, 1, 0, 1)
        assert report.failed == (
            JobFailure(
                "running",
                "BrokenMessageError",
                "error message unavailable",
                message_format_failed=True,
            ),
        )
        assert report.unstarted == ("pending",)
        assert seen == ["running"]
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(asyncio.wait_for(scenario(), timeout=5))


class FormattingSignal(BaseException):
    pass


@pytest.mark.parametrize("signal_type", [asyncio.CancelledError, FormattingSignal])
def test_formatting_control_flow_is_not_converted_to_a_fallback(
    signal_type: type[BaseException],
) -> None:
    async def scenario() -> None:
        class SignallingMessageError(Exception):
            def __str__(self) -> str:
                raise signal_type()

        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            raise SignallingMessageError()

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("signalling")
        worker.submit("pending")
        with pytest.raises(signal_type):
            await worker.shutdown()
        assert seen == ["signalling"]
        # These fatal formatting paths remain outside supported job accounting.
        assert worker._counts["failed"] == 0
        assert worker._task is not None and worker._task.done()
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())
