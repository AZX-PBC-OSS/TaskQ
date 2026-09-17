"""Out-of-domain enqueue values are refused at the backend boundary, identically
on Postgres and in memory.

``EnqueueArgs`` is the single struct every enqueue path funnels through --
single, batch, the COPY-based fast batch, the atomic batch, and the in-memory
mirror.  It is therefore the boundary where a value is *created* as a row, and
the rule is that a value is validated once at that boundary so it
cannot be wrong anywhere downstream.

Three things an operator sees when this does not hold:

- A ``max_attempts`` of 0 or a negative number is accepted and the job is
  written.  It is dispatchable but can never complete: the first transient
  failure finds no remaining budget, so the work silently dies on attempt one
  with a retry budget the caller never meant to express.  Nothing at enqueue
  named the mistake.
- A ``priority`` or ``max_attempts`` beyond the smallint column's domain is
  accepted by the in-memory backend and rejected by Postgres with a raw
  ``asyncpg`` numeric-out-of-range error -- a bare driver exception naming a
  column, not a typed library error naming the parameter and its range.  Tests
  and staging (in memory) pass; production (Postgres) raises something no caller
  has a handler for.
- A negative ``heartbeat_timeout`` or ``start_to_close`` is accepted and stored,
  so every dispatch of that job is instantly past its own deadline.  The client
  layer already rejects the negative forms it sees; a producer that builds
  ``EnqueueArgs`` directly (the documented direct-backend path the batch helpers
  use) bypasses that check entirely.

The parity half matters as much as the refusal: a divergence here is the false
confidence the project's backend-equivalence rule exists to prevent -- the
in-memory twin certifying an enqueue that Postgres would refuse.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq._ids import new_job_id
from taskq.backend import Backend, EnqueueArgs, JobFilter
from taskq.retry import MAX_ENQUEUABLE_MAX_ATTEMPTS

# The parity half exercises PG via backend_pair; the PG branch must be opt-in.
pytestmark = pytest.mark.integration

_START = datetime(2025, 1, 1, tzinfo=UTC)

_SMALLINT_MAX = 32767
_SMALLINT_MIN = -32768


def _args(**overrides: object) -> EnqueueArgs:
    """Build an otherwise-valid EnqueueArgs with the named field overridden."""
    base: dict[str, object] = {
        "id": new_job_id(),
        "actor": "actor_a",
        "queue": "default",
        "payload": {"key": "value"},
        "max_attempts": 3,
        "retry_kind": "transient",
        "scheduled_at": _START,
    }
    base.update(overrides)
    return EnqueueArgs(**base)  # pyright: ignore[reportArgumentType]  # Why: overrides are per-test field values; the struct validates them at runtime, which is the contract under test.


# ── Refusal at construction ────────────────────────────────────────────


@pytest.mark.parametrize(
    ("field", "value", "why"),
    [
        ("max_attempts", 0, "a job with no attempt budget can never run to completion"),
        ("max_attempts", -1, "a negative attempt budget is not expressible as a retry policy"),
        (
            "max_attempts",
            MAX_ENQUEUABLE_MAX_ATTEMPTS + 1,
            "beyond the enqueuable ceiling the smallint column has no headroom to raise",
        ),
        ("max_attempts", _SMALLINT_MAX + 1, "beyond the smallint column's domain entirely"),
        ("priority", _SMALLINT_MAX + 1, "beyond the smallint priority column's domain"),
        ("priority", _SMALLINT_MIN - 1, "below the smallint priority column's domain"),
        (
            "heartbeat_timeout",
            timedelta(seconds=-5),
            "a negative liveness deadline is expired the instant the job is dispatched",
        ),
        (
            "start_to_close",
            timedelta(seconds=-5),
            "a negative execution deadline is expired the instant the job is dispatched",
        ),
        (
            "result_ttl",
            timedelta(seconds=-1),
            "a negative result lifetime expires the result before it is written",
        ),
    ],
)
def test_out_of_domain_enqueue_value_is_refused_where_it_is_created(
    field: str, value: object, why: str
) -> None:
    """A value outside its column's domain is refused by EnqueueArgs itself.

    Refusing here -- at the struct every enqueue path funnels through -- is
    what makes the defect unrepresentable: a producer building the struct
    directly, a batch helper, and the client all inherit the same refusal,
    so no later path can reintroduce the gap.  The error must name the
    parameter, so the caller knows which argument to fix rather than reading
    a column name out of a driver traceback.
    """
    with pytest.raises(ValueError) as excinfo:
        _args(**{field: value})

    message = str(excinfo.value)
    assert field in message, (
        f"the refusal for {field}={value!r} must name the parameter the caller "
        f"passed ({why}); got: {message!r}"
    )


def test_boundary_values_inside_the_domain_are_accepted() -> None:
    """The refusal is a domain check, not a narrowing: the extremes a caller
    may legitimately express still construct.  A guard that also rejects
    valid input is a worse ergonomic failure than no guard, because the
    caller has no correct value left to pass."""
    assert _args(max_attempts=1).max_attempts == 1
    assert _args(max_attempts=MAX_ENQUEUABLE_MAX_ATTEMPTS).max_attempts == (
        MAX_ENQUEUABLE_MAX_ATTEMPTS
    )
    assert _args(priority=_SMALLINT_MAX).priority == _SMALLINT_MAX
    assert _args(priority=_SMALLINT_MIN).priority == _SMALLINT_MIN
    assert _args(heartbeat_timeout=timedelta(0)).heartbeat_timeout == timedelta(0)
    assert _args(start_to_close=timedelta(seconds=1)).start_to_close == timedelta(seconds=1)


# ── Parity: both backends refuse, neither writes ───────────────────────


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("max_attempts", 0),
        ("max_attempts", MAX_ENQUEUABLE_MAX_ATTEMPTS + 1),
        ("priority", _SMALLINT_MAX + 1),
        ("heartbeat_timeout", timedelta(seconds=-5)),
    ],
)
async def test_both_backends_refuse_the_same_out_of_domain_enqueue(
    backend_pair: Backend, field: str, value: object
) -> None:
    """Postgres and the in-memory twin reject the same out-of-domain enqueue
    with the same typed error, and neither writes a row.

    Divergence here is the false-confidence failure the backend-equivalence
    rule exists to prevent: an in-memory suite going green over an enqueue
    Postgres would refuse at runtime with a bare driver error.
    """
    with pytest.raises(ValueError):
        await backend_pair.enqueue(_args(**{field: value}))

    remaining = await backend_pair.list_jobs(JobFilter(actor="actor_a", limit=50))
    assert remaining == [], (
        f"a refused enqueue ({field}={value!r}) must write nothing; found {len(remaining)} row(s)"
    )


async def test_a_valid_enqueue_still_lands_on_both_backends(backend_pair: Backend) -> None:
    """The parity refusals above are not a blanket refusal: an in-domain
    enqueue still writes exactly one pending row on both backends, so the
    guard cannot be satisfied by refusing everything."""
    args = _args(max_attempts=MAX_ENQUEUABLE_MAX_ATTEMPTS, priority=_SMALLINT_MAX)
    row = await backend_pair.enqueue(args)

    assert row.max_attempts == MAX_ENQUEUABLE_MAX_ATTEMPTS
    assert row.priority == _SMALLINT_MAX
    assert row.status == "pending"


async def test_batch_enqueue_inherits_the_same_domain_refusal(backend_pair: Backend) -> None:
    """The batch paths take the same struct, so they inherit the same refusal
    and no batch lands partially.

    This is the "unrepresentable" half of the property: a caller cannot reach
    a wrong stored value by choosing a different enqueue entry point.  Without
    it the refusal is per-path and the next path added reopens the class --
    which is how every previous occurrence of a boundary gap in this struct
    escaped.

    The refusal may land at struct construction or inside the batch call; the
    contract is only that the batch as a whole is refused with a typed error
    and that no sibling item is written.
    """

    async def submit_batch_with_one_bad_item() -> None:
        await backend_pair.enqueue_batch(
            [_args(), _args(), _args(max_attempts=0)],
        )

    with pytest.raises(ValueError):
        await submit_batch_with_one_bad_item()

    remaining = await backend_pair.list_jobs(JobFilter(actor="actor_a", limit=50))
    assert remaining == [], (
        "a batch whose items include an out-of-domain value must write "
        f"nothing; found {len(remaining)} row(s)"
    )
