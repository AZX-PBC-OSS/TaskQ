"""Tests for the :func:`actor` decorator and :class:`ActorRef`.

Actor decorator parameter validation and field propagation.
"""

import inspect
from datetime import timedelta

import pytest
from pydantic import BaseModel

from taskq.actor import ActorRef, actor
from taskq.retry import RetryPolicy


class SimplePayload(BaseModel):
    x: int


# ── Positive: max_concurrent parameter ─────────────────────────────────


def test_max_concurrent_explicit_value() -> None:
    """@actor(max_concurrent=4) produces an ActorRef with max_concurrent == 4."""

    @actor(max_concurrent=4)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_concurrent == 4
    assert my_actor.metadata == {}


def test_max_concurrent_default_is_none() -> None:
    """@actor(max_concurrent=None) (default) produces max_concurrent is None."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_concurrent is None


def test_max_concurrent_zero_accepted() -> None:
    """@actor(max_concurrent=0) is accepted (drain mode)."""

    @actor(max_concurrent=0)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_concurrent == 0


# ── Positive: metadata parameter ───────────────────────────────────────


def test_metadata_round_trip() -> None:
    """@actor(metadata={...}) round-trips the dict value."""

    @actor(metadata={"retention_days": 30})
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.metadata["retention_days"] == 30


def test_metadata_default_is_empty_dict() -> None:
    """@actor() produces metadata == {}."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.metadata == {}


# ── Metadata isolation ─────────────────────────────────────────────────


def test_metadata_not_shared_between_refs() -> None:
    """Each ActorRef carries its own metadata dict — no shared mutable default."""

    @actor
    async def actor_a(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    @actor
    async def actor_b(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert isinstance(actor_a, ActorRef)
    assert isinstance(actor_b, ActorRef)
    assert actor_a.metadata == {}
    assert actor_b.metadata == {}

    actor_a.metadata["tag"] = "x"
    assert actor_b.metadata == {}
    assert "tag" not in actor_b.metadata
    assert actor_a.metadata["tag"] == "x"


# ── Validation: negative max_concurrent ────────────────────────────────


def test_max_concurrent_negative_raises_value_error() -> None:
    """@actor(max_concurrent=-1) raises ValueError."""

    with pytest.raises(ValueError, match="max_concurrent"):

        @actor(max_concurrent=-1)
        async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
            pass


# ── Validation: bad metadata type ──────────────────────────────────────


def test_metadata_not_a_dict_raises_type_error() -> None:
    """@actor(metadata='not a dict') raises TypeError."""

    with pytest.raises(TypeError, match="metadata"):

        @actor(metadata="not a dict")  # type: ignore[arg-type]  # Why: deliberately passing wrong type to test runtime validation.
        async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
            pass


# ── Docstring content assertion ────────────────────────────────────────


def test_actor_docstring_contains_bounded_overcount_text() -> None:
    """The actor() docstring contains the bounded over-count formula text."""
    doc = inspect.getdoc(actor)
    assert doc is not None
    normalized = " ".join(doc.split())
    assert (
        "max_concurrent may transiently exceed configured value by up to "
        "(num_active_producers - 1) * max_concurrent per actor under heavy "
        "contention"
    ) in normalized


def test_actorref_max_concurrent_docstring_contains_bounded_overcount_text() -> None:
    """ActorRef.max_concurrent attribute docstring contains the bounded over-count text."""
    doc = inspect.getdoc(ActorRef)
    assert doc is not None
    normalized = " ".join(doc.split())
    assert (
        "max_concurrent may transiently exceed configured value by up to "
        "(num_active_producers - 1) * max_concurrent per actor under heavy "
        "contention"
    ) in normalized


# ── Positive: singleton parameter ──────────────────────────────────────


def test_singleton_explicit_true() -> None:
    """@actor(singleton=True) produces an ActorRef with singleton == True."""

    @actor(singleton=True)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.singleton is True


def test_singleton_explicit_false() -> None:
    """@actor(singleton=False) produces an ActorRef with singleton == False."""

    @actor(singleton=False)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.singleton is False


def test_singleton_default_is_false() -> None:
    """Omitting singleton produces an ActorRef with singleton == False."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.singleton is False


# ── Validation: non-bool singleton ─────────────────────────────────────


def test_singleton_non_bool_raises_type_error() -> None:
    """@actor(singleton='yes') raises TypeError at decoration time."""

    with pytest.raises(TypeError, match="singleton"):

        @actor(singleton="yes")  # type: ignore[arg-type]  # Why: deliberately passing wrong type to test runtime validation.
        async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
            pass


# ── Positive: unique_for / unique_states parameters ──────────────────────


def test_unique_for_default_is_none() -> None:
    """@actor() produces an ActorRef with unique_for is None."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.unique_for is None


def test_unique_states_default() -> None:
    """@actor() produces an ActorRef with unique_states == ('pending', 'scheduled', 'running')."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.unique_states == ("pending", "scheduled", "running")


def test_unique_for_round_trip() -> None:
    """@actor(unique_for=timedelta(minutes=15)) round-trips to ActorRef.unique_for."""

    @actor(unique_for=timedelta(minutes=15))
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.unique_for == timedelta(minutes=15)


def test_unique_states_custom_round_trip() -> None:
    """@actor(unique_states=('pending',)) round-trips to ActorRef.unique_states."""

    @actor(unique_states=("pending",))
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.unique_states == ("pending",)


# ── Registration warnings ───────────────────────────────────────────


def test_indefinite_no_budget_warning() -> None:
    """kind=indefinite with time_budget=None still produces a valid ActorRef."""

    @actor(retry=RetryPolicy(kind="indefinite", time_budget=None))
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
        pass

    assert my_actor.retry.kind == "indefinite"
    assert my_actor.retry.time_budget is None


def test_indefinite_long_budget_warning() -> None:
    """kind=indefinite with time_budget=25h still produces a valid ActorRef."""

    @actor(retry=RetryPolicy(kind="indefinite", time_budget=timedelta(hours=25)))
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
        pass

    assert my_actor.retry.kind == "indefinite"
    assert my_actor.retry.time_budget == timedelta(hours=25)


def test_time_budget_ignored_warning() -> None:
    """kind=transient with time_budget still produces a valid ActorRef."""

    @actor(retry=RetryPolicy(kind="transient", time_budget=timedelta(hours=2)))
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
        pass

    assert my_actor.retry.kind == "transient"
    assert my_actor.retry.time_budget == timedelta(hours=2)


def test_default_retry_policy_no_warnings() -> None:
    """Negative control: default RetryPolicy() produces a valid ActorRef."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
        pass

    assert my_actor.retry.kind == "transient"
    assert my_actor.retry.max_attempts > 0


# ── Positive: max_pending parameter ─────────────────────────────────────


def test_max_pending_explicit_value() -> None:
    """@actor(max_pending=10) produces an ActorRef with max_pending == 10."""

    @actor(max_pending=10)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_pending == 10


def test_max_pending_default_is_none() -> None:
    """@actor without max_pending produces max_pending is None."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_pending is None


def test_max_pending_none_accepted() -> None:
    """@actor(max_pending=None) produces an ActorRef with max_pending is None."""

    @actor(max_pending=None)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_pending is None


def test_max_pending_zero_accepted() -> None:
    """@actor(max_pending=0) is accepted (never accept any jobs)."""

    @actor(max_pending=0)
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.max_pending == 0


# ── Validation: invalid max_pending ─────────────────────────────────────


def test_max_pending_negative_raises_value_error() -> None:
    """@actor(max_pending=-1) raises ValueError at decoration time."""

    with pytest.raises(ValueError, match="max_pending"):

        @actor(max_pending=-1)
        async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
            pass


def test_max_pending_non_int_raises_value_error() -> None:
    """@actor(max_pending='ten') raises ValueError at decoration time (runtime guard)."""

    with pytest.raises(ValueError, match="max_pending"):

        @actor(max_pending="ten")  # type: ignore[arg-type]  # Why: deliberately passing wrong type to test runtime validation.
        async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
            pass


def test_max_pending_error_references_handler_qualname() -> None:
    """ValueError raised for invalid max_pending references the handler qualname."""

    with pytest.raises(
        ValueError,
        match=r"actor handler.*max_pending_error_references_handler_qualname.*max_pending",
    ):

        @actor(max_pending=-5)
        async def max_pending_error_qualname(  # pyright: ignore[reportUnusedFunction]  # Why: only exists to test decoration-time validation; never called.
            payload: SimplePayload, *args: object, **kwargs: object
        ) -> None:
            pass


# ── Positive: rate_limits and reservations parameters ─────────────────────


def test_rate_limits_and_reservations_attributes_accessible() -> None:
    """Decorated actor has rate_limits and reservations attributes."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert hasattr(my_actor, "rate_limits")
    assert hasattr(my_actor, "reservations")


def test_rate_limits_and_reservations_store_plain_string_lists() -> None:
    """@actor(rate_limits=[...], reservations=[...]) stores the plain string lists."""

    @actor(rate_limits=["openai", "vendor_x"], reservations=["gpu_pool"])
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.rate_limits == ["openai", "vendor_x"]
    assert my_actor.reservations == ["gpu_pool"]


def test_rate_limits_and_reservations_default_to_empty_list() -> None:
    """@actor() without rate_limits/reservations defaults to [] for both."""

    @actor
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.rate_limits == []
    assert my_actor.reservations == []


def test_reservations_accepts_mixed_str_and_keyed_reservation_ref() -> None:
    """@actor(reservations=[...]) accepts a mix of plain str names and
    KeyedReservationRef instances, stored unchanged (resolution happens at
    dispatch time, not decoration time)."""
    from datetime import timedelta as _timedelta

    from taskq.ratelimit.refs import KeyedReservationRef

    ref = KeyedReservationRef(
        base_name="geocode-session",
        key_fn=lambda p: str(p["session_id"]),
        slots=3,
        lease=_timedelta(minutes=5),
    )

    @actor(reservations=["geocode-global", ref])
    async def my_actor(payload: SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert my_actor.reservations == ["geocode-global", ref]
    assert isinstance(my_actor.reservations[1], KeyedReservationRef)
