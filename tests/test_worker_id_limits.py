import asyncio
from typing import cast

import pytest

from queued_worker import OutcomeCounts, QueuedWorker


@pytest.mark.parametrize(
    "job",
    ["abcd", "caf\u00e9", "\u00e9\u00e9", "\U0001f642", "a\U0001f642", "e\u0301", ""],
)
@pytest.mark.parametrize("waiting", [False, True])
@pytest.mark.parametrize("spare_byte", [0, 1])
def test_utf8_admission_boundaries(job: str, waiting: bool, spare_byte: int) -> None:
    async def scenario() -> None:
        seen: list[str] = []

        async def handle(identifier: str) -> None:
            seen.append(identifier)

        limit = len(job.encode("utf-8")) + spare_byte
        worker = QueuedWorker(handle, max_job_id_bytes=limit)
        worker.start()
        try:
            if waiting:
                await worker.submit_wait(job)
            else:
                worker.submit(job)
            oversized = job + "x" * (spare_byte + 1)
            with pytest.raises(ValueError, match="exceeds max_job_id_bytes"):
                if waiting:
                    await worker.submit_wait(oversized)
                else:
                    worker.submit(oversized)
        finally:
            report = await worker.shutdown()
        assert seen == [job]
        assert report.completed == (job,)
        assert report.counts == OutcomeCounts(1, 1, 0, 0, 0)
        assert report.omitted_outcomes == 0

    asyncio.run(scenario())


@pytest.mark.parametrize("waiting", [False, True])
def test_oversized_id_is_rejected_before_waiting_on_full_queue(waiting: bool) -> None:
    async def scenario() -> None:
        started, release = asyncio.Event(), asyncio.Event()
        seen: list[str] = []

        async def handle(job: str) -> None:
            seen.append(job)
            if job == "one":
                started.set()
                await release.wait()

        worker = QueuedWorker(handle, max_pending=1, max_job_id_bytes=4)
        worker.start()
        worker.submit("one")
        try:
            await started.wait()
            worker.submit("two")
            # Three characters, six UTF-8 bytes: the queue is still full.
            with pytest.raises(ValueError, match="exceeds max_job_id_bytes"):
                if waiting:
                    await asyncio.wait_for(
                        worker.submit_wait("\u00e9\u00e9\u00e9"), timeout=1
                    )
                else:
                    worker.submit("\u00e9\u00e9\u00e9")
            assert seen == ["one"]
        finally:
            release.set()
            report = await worker.shutdown()
        assert seen == ["one", "two"]
        assert report.counts == OutcomeCounts(2, 2, 0, 0, 0)
        assert report.completed == ("one", "two")
        await asyncio.wait_for(worker._queue.join(), timeout=1)

    asyncio.run(scenario())


@pytest.mark.parametrize("waiting", [False, True])
def test_invalid_utf8_has_no_accepted_outcome(waiting: bool) -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pytest.fail("invalid UTF-8 identifier reached handler")

        worker = QueuedWorker(handle, max_job_id_bytes=4)
        worker.start()
        try:
            with pytest.raises(ValueError, match="encodable as UTF-8") as caught:
                if waiting:
                    await worker.submit_wait("\ud800")
                else:
                    worker.submit("\ud800")
            assert isinstance(caught.value.__cause__, UnicodeEncodeError)
        finally:
            report = await worker.shutdown()
        assert report.counts == OutcomeCounts(0, 0, 0, 0, 0)
        assert report.completed == ()

    asyncio.run(scenario())


def test_unlimited_default_preserves_existing_identifier_behavior() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle)
        worker.start()
        worker.submit("x" * 10000)
        await worker.submit_wait("\ud800")
        report = await worker.shutdown()
        assert report.completed == ("x" * 10000, "\ud800")
        assert report.counts == OutcomeCounts(2, 2, 0, 0, 0)

    asyncio.run(scenario())


def test_unicode_identifiers_are_preserved_without_normalization() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle, max_job_id_bytes=3)
        worker.start()
        worker.submit("\u00e9")
        worker.submit("e\u0301")
        report = await worker.shutdown()
        assert report.completed == ("\u00e9", "e\u0301")

    asyncio.run(scenario())


@pytest.mark.parametrize("limit", [True, 1.5, "4"])
def test_byte_limit_requires_integer_configuration(limit: object) -> None:
    async def handle(job: str) -> None:
        pass

    with pytest.raises(TypeError, match="integer or None"):
        QueuedWorker(handle, max_job_id_bytes=cast(int, limit))


def test_negative_byte_limit_is_invalid() -> None:
    async def handle(job: str) -> None:
        pass

    with pytest.raises(ValueError, match="nonnegative"):
        QueuedWorker(handle, max_job_id_bytes=-1)


def test_closed_admission_takes_precedence_over_size_validation() -> None:
    async def scenario() -> None:
        async def handle(job: str) -> None:
            pass

        worker = QueuedWorker(handle, max_job_id_bytes=1)
        with pytest.raises(RuntimeError, match="not accepting"):
            worker.submit("oversized")
        worker.start()
        worker.close()
        try:
            with pytest.raises(RuntimeError, match="not accepting"):
                await worker.submit_wait("oversized")
        finally:
            report = await worker.shutdown()
        assert report.counts.accepted == 0

    asyncio.run(scenario())
