import asyncio

import pytest

from repeated_cancellation import cancellation_scenario


@pytest.mark.parametrize("cancel_again", [False, True])
def test_second_cancellation_interrupts_cleanup(cancel_again: bool) -> None:
    events = asyncio.run(cancellation_scenario(cancel_again=cancel_again))
    assert events == [
        "cleanup:started",
        "cleanup:interrupted" if cancel_again else "cleanup:finished",
        "caller:cancelled",
    ]
