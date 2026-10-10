import asyncio
from typing import cast

import pytest

from queued_worker import JobFailure, OutcomeCounts, QueuedWorker


@pytest.mark.parametrize(
    ("limit", "message", "expected", "truncated"),
    [
        (0, "", "", False),
        (0, "failure", "", True),
        (4, "abc", "abc", False),
        (4, "abcd", "abcd", False),
        (4, "abcde", "abcd", True),
        (4, "caf\u00e9", "caf\u00e9", False),
        (1, "\U0001f642", "\U0001f642", False),
        (1, "\U0001f642x", "\U0001f642", True),
        (1, "e\u0301", "e", True),
        (1, "\ud800x", "\ud800", True),
        (16, "x" * 1_000_000, "x" * 16, True),
    ],
    ids=[
        "empty-at-zero",
        "nonempty-at-zero",
        "below-limit",
        "exact-limit",
        "above-limit",
        "accent-code-points",
        "emoji-at-limit",
        "emoji-prefix",
        "split-combining-sequence",
        "unpaired-surrogate",
        "million-character-message",
    ],
)
def test_message_cap_preserves_prefix_and_marks_only_lost_text(
    limit: int, message: str, expected: str, truncated: bool
) -> None:
    async def scenario() -> None:
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            if job == "bad":
                raise ValueError(message)

        worker = QueuedWorker(handle, max_error_message_chars=limit)
        worker.start()
        worker.submit("bad")
        worker.submit("good")
        report = await worker.shutdown()
        assert report.failed == (JobFailure("bad", "ValueError", expected, truncated),)
        assert report.completed == ("good",)
        assert report.interrupted == report.unstarted == ()
        assert report.counts == OutcomeCounts(2, 1, 1, 0, 0)
        assert report.omitted_outcomes == 0
        assert seen == ["bad", "good"]
        await asyncio.wait_for(worker._queue.join(), timeout=1)
        assert await worker.shutdown() == report

    asyncio.run(scenario())


@pytest.mark.parametrize("explicit_none", [False, True])
def test_unlimited_messages_preserve_existing_behavior(explicit_none: bool) -> None:
    async def scenario() -> None:
        message = "\ud800" + "x" * 10000

        async def handle(job: str) -> None:
            raise RuntimeError(message)

        worker = (
            QueuedWorker(handle, max_error_message_chars=None)
            if explicit_none
            else QueuedWorker(handle)
        )
        worker.start()
        worker.submit("bad")
        report = await worker.shutdown()
        # Three-argument construction also keeps the original public interface.
        assert report.failed == (JobFailure("bad", "RuntimeError", message),)
        assert not report.failed[0].message_truncated
        assert report.counts == OutcomeCounts(1, 0, 1, 0, 0)

    asyncio.run(scenario())


@pytest.mark.parametrize("history_limit", [None, 0, 1, 2])
def test_truncated_failures_follow_shared_history_eviction(
    history_limit: int | None,
) -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            if job != "good":
                raise RuntimeError(job + " details")

        worker = QueuedWorker(
            handle, history_limit=history_limit, max_error_message_chars=3
        )
        worker.start()
        for job in ("one", "two", "six", "good"):
            worker.submit(job)
        report = await worker.shutdown()
        assert report.counts == OutcomeCounts(4, 1, 3, 0, 0)
        assert report.completed == (() if history_limit == 0 else ("good",))
        if history_limit is None:
            assert report.failed == tuple(
                JobFailure(job, "RuntimeError", job, True)
                for job in ("one", "two", "six")
            )
            assert report.omitted_outcomes == 0
        elif history_limit == 2:
            assert report.failed == (JobFailure("six", "RuntimeError", "six", True),)
            assert report.omitted_outcomes == 2
        else:
            assert report.failed == ()
            assert report.omitted_outcomes == 4 - history_limit
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


@pytest.mark.parametrize("limit", [True, False, 1.5, "4"])
def test_message_limit_requires_integer_configuration(limit: object) -> None:
    async def handle(job: str) -> None:
        pass

    with pytest.raises(TypeError, match="max_error_message_chars.*integer or None"):
        QueuedWorker(handle, max_error_message_chars=cast(int, limit))


def test_negative_message_limit_is_invalid() -> None:
    async def handle(job: str) -> None:
        pass

    with pytest.raises(ValueError, match="max_error_message_chars.*nonnegative"):
        QueuedWorker(handle, max_error_message_chars=-1)
