"""Unit tests for ``dispatch._effective_reservations`` (queue-cap prepend).

The helper prepends the fleet-wide queue-cap reservation name to the
actor-declared reservations when — and only when — that queue's cap is
registered in the worker's ``RateLimitRegistry``. The end-to-end acquire
behaviour through ``dispatch_one_job`` is pinned in
``tests/test_dispatch_one_job.py`` (queue-cap wiring section); these tests
pin the helper's prepend/membership/no-copy semantics directly.
"""

from datetime import timedelta

from taskq.ratelimit.refs import KeyedReservationRef
from taskq.ratelimit.registry import RateLimitRegistry, queue_concurrency_reservation_name
from taskq.ratelimit.reservation import ConcurrencyReservation
from taskq.worker.dispatch import (
    _effective_reservations,  # pyright: ignore[reportPrivateUsage]  # Why: unit-testing the documented prepend helper directly
)


def _keyed_ref() -> KeyedReservationRef:
    return KeyedReservationRef(
        base_name="tenants",
        key_fn=lambda payload: str(payload["tenant"]),
        slots=1,
        lease=timedelta(seconds=30),
    )


def _register_cap(rl: RateLimitRegistry, queue: str) -> str:
    cap_name = queue_concurrency_reservation_name(queue)
    rl.register_queue_cap_reservation(
        ConcurrencyReservation(name=cap_name, slots=2, lease=timedelta(minutes=5))
    )
    return cap_name


def test_prepends_queue_cap_and_preserves_entries_in_order() -> None:
    """With the queue cap registered, its name is prepended; the mixed
    str / KeyedReservationRef / ConcurrencyReservation entries are preserved
    in declaration order — instances stay instances (normalization to names
    happens in acquire_for_actor, not here)."""
    rl = RateLimitRegistry()
    cap_name = _register_cap(rl, "default")

    keyed = _keyed_ref()
    instance = ConcurrencyReservation(name="own", slots=1, lease=timedelta(seconds=30))
    declared: list[str | KeyedReservationRef | ConcurrencyReservation] = [
        "static_name",
        keyed,
        instance,
    ]

    effective = _effective_reservations(declared, "default", rl)

    assert len(effective) == 4
    assert effective[0] == cap_name
    assert effective[1] == "static_name"
    assert effective[2] is keyed
    assert effective[3] is instance


def test_no_cap_registered_returns_declared_unchanged() -> None:
    """No queue cap registered for the job's queue (the common case): the
    actor's own list is returned unchanged — same object, no per-job copy."""
    rl = RateLimitRegistry()
    declared: list[str | KeyedReservationRef | ConcurrencyReservation] = ["static_name"]

    assert _effective_reservations(declared, "default", rl) is declared


def test_cap_registered_for_other_queue_only_returns_declared_unchanged() -> None:
    """A cap registered for a DIFFERENT queue does not leak into this one."""
    rl = RateLimitRegistry()
    _register_cap(rl, "other")
    declared: list[str | KeyedReservationRef | ConcurrencyReservation] = ["static_name"]

    assert _effective_reservations(declared, "default", rl) is declared


def test_none_registry_returns_declared_unchanged() -> None:
    """rl_registry=None (rate limiting not wired): unchanged, no copy."""
    declared: list[str | KeyedReservationRef | ConcurrencyReservation] = ["static_name"]

    assert _effective_reservations(declared, "default", None) is declared
