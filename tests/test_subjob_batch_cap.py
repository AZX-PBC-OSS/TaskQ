"""`ctx.jobs.enqueue_batch` must honour the same 1000-item cap as the client.

`JobsClient.enqueue_batch` has capped batches at 1000 since it was written.
`SubJobEnqueuer.enqueue_batch` is the same operation reached from inside an actor
body and had no cap at all, so a job could fan out without bound by going one
layer down. The backend binds every item as 21 parallel array parameters to a
single `unnest` INSERT in one transaction, so the cap is what keeps that
statement a bounded size.

The rejection must happen BEFORE any connection is resolved: these tests pass
`worker_pool=None` and no loop-scope connection, so anything that reached the
insert path would raise `RuntimeError("ctx.jobs is only available inside an actor
body")` instead of the `ValueError` asserted here. That distinction is the point,
not an accident of the fixture.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel

from taskq.actor import ActorRef, actor
from taskq.batch import EnqueueItem
from taskq.client._enqueuer import SubJobEnqueuer

_CAP = 1000


class _Payload(BaseModel):
    value: int


def _ref() -> ActorRef[_Payload, None]:
    """Built through the decorator, as every other test does: `ActorRef` is
    constructed by `@actor`, not directly."""

    @actor(name="fan_out")
    async def _a(payload: _Payload) -> None: ...

    return _a


def _enqueuer() -> SubJobEnqueuer:
    return SubJobEnqueuer(loop_scope_resolved=None, worker_pool=None, backend=None)  # type: ignore[arg-type]  # Why: the cap is checked before the backend is touched, so a real one is not needed.


def _items(count: int) -> list[EnqueueItem[Any, Any]]:
    ref = _ref()
    return [EnqueueItem(actor_ref=ref, payload=_Payload(value=i)) for i in range(count)]


async def test_a_batch_over_the_cap_is_rejected() -> None:
    with pytest.raises(ValueError, match=r"at most 1000 entries, got 1001"):
        await _enqueuer().enqueue_batch(_items(_CAP + 1))


async def test_a_batch_at_the_cap_is_not_rejected_by_the_cap() -> None:
    """Exactly at the limit must pass the check.

    It then fails on the missing connection, which is what proves the cap let it
    through rather than the fixture masking an off-by-one.
    """
    with pytest.raises(RuntimeError, match="only available inside an actor body"):
        await _enqueuer().enqueue_batch(_items(_CAP))
