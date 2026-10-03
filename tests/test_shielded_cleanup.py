import asyncio

import pytest

from shielded_cleanup import shield_scenario


def test_shield_preserves_cleanup_but_not_caller() -> None:
    events: list[str] = []
    asyncio.run(shield_scenario(events))
    assert events == [
        "cleanup:started",
        "caller:cancelled",
        "cleanup:pending=True",
        "cleanup:finished",
        "owner:joined",
    ]


def test_plain_await_propagates_cancellation_to_cleanup() -> None:
    events: list[str] = []
    asyncio.run(shield_scenario(events, shielded=False))
    assert events == [
        "cleanup:started",
        "cleanup:cancelled",
        "caller:cancelled",
        "cleanup:pending=False",
        "owner:joined",
    ]


def test_owner_observes_shielded_cleanup_failure() -> None:
    events: list[str] = []
    with pytest.raises(ValueError, match="^cleanup failed$"):
        asyncio.run(shield_scenario(events, fail_cleanup=True))
    assert events == [
        "cleanup:started",
        "caller:cancelled",
        "cleanup:pending=True",
        "cleanup:failed",
    ]
