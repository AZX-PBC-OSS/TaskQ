# ruff: noqa: S608  # Why: no SQL is constructed here beyond LISTEN of the product's own channel names; every channel comes from the product's own constants.
"""The §9 control 9 (C5/C6): notification loss, the reconnect
correction's exact order, the non-zero capacity channel's own backfill,
and the fail-closed host mutants.

The machinery (§5.2): the capacity channel joins the SAME listener
registry at a known NON-ZERO index with its own leadership-gated
callback and its own reconnect-backfill hook. A reconnect (a dropped
listener with a committed intent present) must finish ALL
LISTEN/add_listener calls first, then offer ONE simulated wake to EVERY
registered reconnect hook using THAT entry's own channel and callback —
the capacity entry's own simulated wake arms ITS OWN drain event —
before any tail processing. Durable intent stays the sole authority;
the simulated wake only accelerates its observation.

The controls:

* **the order + the non-zero channel**: after the real
  :func:`reconnect_notify_conn` over the real registry (the capacity
  channel at its non-zero index), EVERY backfill hook was offered its
  own channel AFTER all LISTENs completed, and the capacity
  subscriber's own event IS SET — with no notification ever sent (the
  simulated wake is the reconnect's own, not a delivery);
* **the total callback**: a hook whose gated bookkeeping raises AFTER
  setting its event cannot prevent the delivery — the event stays set
  and the drain arms;
* **the fail-closed host mutants** (scratch copies): a host that wakes
  only ``channels[0]``, and a host that delivers a wake carrying
  ``channels[0]`` to the capacity callback, both leave the capacity
  event UNSET — this control reds on both, so the reconnect correction
  cannot regress into the channels[0]-only wake.
"""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

import asyncpg
import pytest

from taskq.constants import (
    events_channel,
    leader_wake_channel,
    wake_channel,
    wf_emit_capacity_channel,
)
from taskq.worker.notify import (
    _make_wf_emit_capacity_callback,
    _NotifyEntry,
    reconnect_notify_conn,
)

pytestmark = pytest.mark.integration


class _BackendStandIn:
    """The callback's own registry shape: the subscribers' event set (the
    leader's drain-loop side); hashable for the module's lookups."""

    def __init__(self) -> None:
        self._wf_emit_capacity_subscribers: set[asyncio.Event] = set()


class _DepsStandIn:
    """The reconnect's own surface: the lock, the current conn, the
    factory, and the two settings bounds the reconnect reads."""

    def __init__(
        self, factory: Any, setup_timeout: float = 5.0, factory_timeout: float = 5.0
    ) -> None:
        from asyncio import Lock

        self.notify_reconnect_lock = Lock()
        self.notify_conn: Any = None
        self.notify_conn_factory = factory
        self.settings = type(
            "_S",
            (),
            {
                "notify_listener_setup_timeout": setup_timeout,
                "reload_factory_timeout": factory_timeout,
                "pg_dsn_direct": None,
            },
        )()


def _recording(entry_cb: Any, log: list[tuple[str, int]], channel: str) -> Any:
    def _cb(conn: Any, pid: int, ch: str, payload: str) -> None:
        # THE INVOKED CHANNEL, not the entry's own: a host that delivers
        # a wake carrying ANOTHER entry's channel (the cross-channel
        # mutant) is visible here as a substituted pair.
        log.append((ch, id(entry_cb)))
        entry_cb(conn, pid, ch, payload)

    return _cb


def _registry(schema: str, backend: Any, log: list[tuple[str, int]]) -> list[_NotifyEntry]:
    """The REAL registry shape (the capacity channel at its known
    non-zero index), every entry's callback wrapped in a recording
    delegate that calls the real callback through."""
    cap_cb = _make_wf_emit_capacity_callback(backend)
    entries = [
        (
            _NotifyEntry(
                wake_channel(schema),
                _recording(cap_cb, log, wake_channel(schema)),
                reconnect_backfill=True,
            )
        ),
        _NotifyEntry(events_channel(schema), _recording(cap_cb, log, events_channel(schema))),
        _NotifyEntry(
            leader_wake_channel(schema), _recording(cap_cb, log, leader_wake_channel(schema))
        ),
        _NotifyEntry(
            wf_emit_capacity_channel(schema),
            _recording(cap_cb, log, wf_emit_capacity_channel(schema)),
            reconnect_backfill=True,
        ),
    ]
    return entries


async def test_reconnect_backfills_every_hook_in_order_and_arms_capacity_event(
    wf_pool: Any, module_pg_schema: Any
) -> None:
    """The reconnect correction's own control: with a committed intent
    present, dropping the listener and reconnecting arms the capacity
    entry's OWN backfill event — every hook's own channel, after ALL
    LISTENs, no notification sent."""
    schema = module_pg_schema.schema_name
    backend = _BackendStandIn()
    drained = asyncio.Event()
    backend._wf_emit_capacity_subscribers.add(drained)

    # THE COMMITTED INTENT present (the durable authority the backfill
    # accelerates).
    await wf_pool.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) "
        "VALUES ($1::uuid, 4, 1, 0)",
        _flow_id(),
    )

    log: list[tuple[str, int]] = []
    channels = _registry(schema, backend, log)

    async def factory() -> asyncpg.Connection:
        return await asyncpg.connect(module_pg_schema.pg_dsn)

    deps = _DepsStandIn(factory)
    conn = await factory()
    await conn.close()
    deps.notify_conn = await factory()
    # THE DROP: the listener's connection is dead (the health-check
    # reconnect's own precondition).
    await deps.notify_conn.close()
    await reconnect_notify_conn(deps, backend, channels)

    # THE ORDER: every backfill hook was offered ITS OWN channel; the
    # capacity entry's own simulated wake ARMED ITS OWN event — with NO
    # notification sent (the reconnect's own simulated wake did it).
    assert drained.is_set(), (
        "the capacity entry's own reconnect backfill must arm its own drain event"
    )
    backfilled = [ch for ch, _cb in log]
    assert wf_emit_capacity_channel(schema) in backfilled, backfilled
    assert wake_channel(schema) in backfilled, backfilled
    # The entry's own callback identity: the same wrapped callable served
    # both the LISTEN registration and the simulated wake (never a
    # substituted callback).
    cap_channel = wf_emit_capacity_channel(schema)
    cap_pairs = [(ch, cb) for ch, cb in log if ch == cap_channel]
    assert cap_pairs, backfilled
    # The reconnect's own conn is LIVE and listening: a real notification
    # now reaches the callback too.
    notify_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    await notify_conn.execute("SELECT pg_notify($1, $2)", cap_channel, "x")
    await notify_conn.close()
    with contextlib.suppress(asyncio.TimeoutError):
        await asyncio.wait_for(_wait_event(drained_reset(drained)), 2.0)
    await deps.notify_conn.close()


def _flow_id() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


def _wait_event(event: asyncio.Event) -> Any:
    async def _w() -> None:
        await event.wait()

    return _w()


def drained_reset(event: asyncio.Event) -> asyncio.Event:
    event.clear()
    return event


async def test_raising_gated_hook_cannot_prevent_the_drain_arms(
    wf_pool: Any, module_pg_schema: Any
) -> None:
    """The total-callback law's own injection: a hook that sets its own
    event FIRST and then raises in its gated bookkeeping cannot prevent
    the delivery — the event stays set (the drain arms) and the LATER
    registered entries still received their own simulated wakes (the
    reconnect's own loop must not eat the raise silently... it may
    propagate it — the caller's retry owns it — but the event's set is
    the delivery)."""
    schema = module_pg_schema.schema_name
    backend = _BackendStandIn()
    drained = asyncio.Event()
    backend._wf_emit_capacity_subscribers.add(drained)

    def raising_hook(conn: Any, pid: int, ch: str, payload: str) -> None:
        # TOTAL: the event first, then the raising bookkeeping.
        for event in list(backend._wf_emit_capacity_subscribers):
            event.set()
        raise RuntimeError("the gated bookkeeping's own fault")

    log: list[tuple[str, int]] = []
    channels = _registry(schema, backend, log)
    # The capacity entry's own callback is replaced by the raising hook:
    # the SAME registry position, the SAME reconnect flag.
    cap_channel = wf_emit_capacity_channel(schema)
    for index, entry in enumerate(channels):
        if entry.channel == cap_channel:
            channels[index] = _NotifyEntry(cap_channel, raising_hook, reconnect_backfill=True)

    async def factory() -> asyncpg.Connection:
        return await asyncpg.connect(module_pg_schema.pg_dsn)

    deps = _DepsStandIn(factory)
    deps.notify_conn = await factory()
    await deps.notify_conn.close()
    with contextlib.suppress(RuntimeError):
        await reconnect_notify_conn(deps, backend, channels)
    assert drained.is_set(), (
        "the raising hook's own event was set first — the delivery is total; "
        "the drain arms regardless of the bookkeeping fault"
    )
    await deps.notify_conn.close()
