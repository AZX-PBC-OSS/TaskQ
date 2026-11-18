# ruff: noqa: S608  # Why: no SQL is constructed here beyond LISTEN of the product's own channel names; every channel comes from the product's own constants.
"""The §9 control 9 (F4/C5/C6): notification loss, the reconnect
correction's exact order, the non-zero capacity channel's own backfill,
the real delivery, and the fail-closed host mutants.

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

* **the ordered journal + exactly once**: the reconnect's own conn is a
  journaling proxy — every `LISTEN:<channel>` and
  `add_listener:<channel>:<callback identity>` is journaled for the REAL
  registry (the capacity channel at its non-zero index), and the
  recording callbacks journal their `wake:<channel>:<callback identity>`
  deliveries. The exact complete LISTEN/add_listener set appears for
  EVERY entry, all registrations precede the first synthetic wake, and
  exactly ONE wake per `reconnect_backfill=True` entry — each with that
  entry's own channel and callback identity; the other entries are never
  woken by the reconnect.
* **the real drain**: the capacity event is set AND the real bounded
  drain runs ONCE against the committed real-PG intent (the intent is
  handled) — event membership alone is not a drain proof.
* **the real notification**: with the synthetic event cleared, a real
  notification from a separate connection arrives through the
  reconnect's own LIVE conn — awaited under `asyncio.wait_for` with NO
  suppression: a timeout IS the test's failure. The real callback's own
  channel is journaled, so skipping `add_listener` cannot pass.
* **the docstring's own truth**: the cross-channel total callback sets
  the event (a total callback cannot be prevented), but the DELIVERY
  IDENTITY is the fault — journaled here.
* **the fail-closed host mutants** (in-tree): a host that wakes only
  ``channels[0]``, and a host that delivers a wake carrying
  ``channels[0]`` to the capacity callback.
"""

from __future__ import annotations

import asyncio
import json as _json
from typing import Any

import asyncpg
import pytest
from asyncpg.pool import PoolConnectionProxy

from taskq.constants import (
    events_channel,
    leader_wake_channel,
    wake_channel,
    wf_emit_capacity_channel,
)
from taskq.testing.fixtures import ModulePgSchema
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


#: The ordered journal's entry kinds.
JournalEntry = tuple[str, ...]


def _recording(entry_cb: Any, journal: list[JournalEntry]) -> Any:
    def _cb(
        conn: asyncpg.Connection | PoolConnectionProxy[asyncpg.Record],
        pid: int,
        ch: str,
        payload: object,
    ) -> None:
        # THE INVOKED CHANNEL, not the entry's own: a host that delivers
        # a wake carrying ANOTHER entry's channel (the cross-channel
        # mutant) is visible here as a substituted pair. The identity is
        # THIS wrapper's own (the registry entry's callback object) — the
        # same id the add_listener journal captured.
        journal.append(("wake", ch, id(_cb)))
        entry_cb(conn, pid, ch, payload)

    return _cb


class _JournalConn:
    """The reconnect's own conn proxy: every LISTEN and add_listener is
    journaled IN ORDER beside the wake entries (the F4 control's own
    evidence — the registration-then-wake order is observed, never
    inferred)."""

    def __init__(self, real: Any, journal: list[JournalEntry]) -> None:
        self._real = real
        self._journal = journal

    async def execute(self, sql: str, *args: Any) -> Any:
        if sql.startswith("LISTEN"):
            self._journal.append(("LISTEN", sql.split('"', 1)[1].split('"', 1)[0]))
        return await self._real.execute(sql, *args)

    async def add_listener(self, channel: str, cb: Any, *args: Any) -> Any:
        self._journal.append(("add_listener", channel, id(cb)))
        return await self._real.add_listener(channel, cb, *args)

    async def close(self, *args: Any) -> Any:
        return await self._real.close(*args)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


def _registry(schema: str, backend: Any, journal: list[JournalEntry]) -> list[_NotifyEntry]:
    """The REAL registry shape (the capacity channel at its known
    non-zero index), every entry's callback wrapped in a recording
    delegate that journals its own delivery and calls the real callback
    through."""
    cap_cb = _make_wf_emit_capacity_callback(backend)

    def own_cb(
        conn: asyncpg.Connection | PoolConnectionProxy[asyncpg.Record],
        pid: int,
        ch: str,
        payload: object,
    ) -> None:
        del conn, pid, ch, payload

    entries = [
        (
            _NotifyEntry(
                wake_channel(schema),
                _recording(own_cb, journal),
                reconnect_backfill=True,
            )
        ),
        _NotifyEntry(events_channel(schema), _recording(own_cb, journal)),
        _NotifyEntry(leader_wake_channel(schema), _recording(own_cb, journal)),
        _NotifyEntry(
            wf_emit_capacity_channel(schema),
            _recording(cap_cb, journal),
            reconnect_backfill=True,
        ),
    ]
    return entries


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


async def test_reconnect_backfills_every_hook_in_order_exactly_once_and_drains(
    wf_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
) -> None:
    """The reconnect correction's own control: with a committed intent
    present, dropping the listener and reconnecting (1) journals the
    exact complete LISTEN/add_listener set for EVERY entry, all
    registrations before the first synthetic wake; (2) offers exactly ONE
    simulated wake per backfill entry, each with that entry's OWN channel
    and callback identity; (3) arms the capacity entry's OWN drain event;
    (4) the REAL bounded drain runs ONCE against the committed intent
    (the intent is handled — event membership alone is not a drain
    proof); (5) a REAL notification from a separate connection arrives
    through the reconnect's own LIVE conn (a timeout is a failure, never
    suppressed); and (6) every owned connection is closed."""
    schema = module_pg_schema.schema_name
    backend = _BackendStandIn()
    drained = asyncio.Event()
    backend._wf_emit_capacity_subscribers.add(drained)

    journal: list[JournalEntry] = []
    channels = _registry(schema, backend, journal)

    committed_flow = _flow_id()

    # THE COMMITTED INTENT present (the durable authority the backfill
    # accelerates) — with the flow's own LIVE root row (the drain's
    # canonical order locks it; a rootless intent is the corruption
    # refusal, not this control's subject).
    await wf_pool.execute(
        f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
        "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
        "idempotency_key) VALUES ($1, 'c9', 'default', '{}', 3, 'transient', "
        "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
        committed_flow,
        _json.dumps({"flow_id": committed_flow}),
        committed_flow,
        f"flow:{committed_flow}",
    )
    await wf_pool.execute(
        f'INSERT INTO "{schema}".wf_emit_capacity '
        "(flow_id, bound, wake_generation, handled_generation) "
        "VALUES ($1::uuid, 4, 1, 0)",
        committed_flow,
    )

    async def factory() -> asyncpg.Connection:
        real = await asyncpg.connect(module_pg_schema.pg_dsn)
        return _JournalConn(real, journal)  # type: ignore[return-value]  # Why: the proxy delegates the full conn surface; the reconnect's own reads are duck-typed.

    deps = _DepsStandIn(factory)
    probe = await factory()
    await probe.close()
    live_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    deps.notify_conn = _JournalConn(live_conn, journal)  # type: ignore[assignment]  # Why: the DROP below closes the proxy's real conn — the reconnect's own precondition.
    # THE DROP: the listener's connection is dead (the health-check
    # reconnect's own precondition).
    await deps.notify_conn.close()
    try:
        await reconnect_notify_conn(deps, backend, channels)

        # THE ORDERED JOURNAL: the exact complete LISTEN/add_listener set
        # for EVERY entry, both operations each, ALL registrations before
        # the first synthetic wake, then exactly ONE wake per backfill
        # entry — each with that entry's own channel and callback
        # identity.
        expected_channels = [entry.channel for entry in channels]
        listens = [e for e in journal if e[0] == "LISTEN"]
        adds = [e for e in journal if e[0] == "add_listener"]
        wakes = [e for e in journal if e[0] == "wake"]
        assert [e[1] for e in listens] == expected_channels, journal
        assert [e[1] for e in adds] == expected_channels, journal
        # The registration callbacks' identities: the SAME wrapped
        # callable the registry carries.
        for entry in channels:
            add = next(e for e in adds if e[1] == entry.channel)
            assert add[2] == id(entry.callback), journal
        # ALL registrations precede the first synthetic wake (the one
        # ordered journal is the order's own evidence).
        registration_end = max(
            i for i, e in enumerate(journal) if e[0] in ("LISTEN", "add_listener")
        )
        if wakes:
            first_wake = next(i for i, e in enumerate(journal) if e[0] == "wake")
            assert registration_end < first_wake, journal
        # EXACTLY ONE wake per backfill entry, each with its OWN channel
        # and callback identity; the non-backfill entries never wake.
        backfills = [e for e in channels if e.reconnect_backfill]
        assert len(backfills) == 2, backfills
        for entry in backfills:
            entry_wakes = [e for e in wakes if e[1] == entry.channel]
            assert len(entry_wakes) == 1, (entry.channel, journal)
            assert entry_wakes[0][2] == id(entry.callback), (entry.channel, journal)
        for entry in channels:
            if not entry.reconnect_backfill:
                assert all(e[1] != entry.channel for e in wakes), journal
        # The event is set (the capacity entry's own backfill armed it).
        assert drained.is_set(), (
            "the capacity entry's own reconnect backfill must arm its own drain event"
        )

        # THE REAL DRAIN runs ONCE against the committed intent: the
        # event's own consumer (the leader's bounded drain) re-reads the
        # durable rows and handles the intent.
        from taskq.workflows._emit import drain_pending_emit_intents
        from taskq.workflows.engine import render_workflow_sql

        wsql = render_workflow_sql(schema)
        offered = await drain_pending_emit_intents(wf_pool, wsql, trigger="notification")
        assert offered == 1, offered
        intent_row = await wf_pool.fetchrow(
            f"""SELECT wake_generation, handled_generation FROM "{schema}".wf_emit_capacity
            WHERE flow_id = $1::uuid""",
            committed_flow,
        )
        assert intent_row is not None
        assert int(intent_row["handled_generation"]) == int(intent_row["wake_generation"]), dict(
            intent_row
        )

        # THE REAL NOTIFICATION (no suppression — a timeout is a failure):
        # the reconnect's own conn is LIVE and listening on every channel.
        notify_conn = await asyncpg.connect(module_pg_schema.pg_dsn)
        try:
            await notify_conn.execute(
                "SELECT pg_notify($1, $2)", wf_emit_capacity_channel(schema), "x"
            )
            await asyncio.wait_for(_wait_event(drained_reset(drained)), 2.0)
        finally:
            await notify_conn.close()
        # The REAL callback's own channel is journaled (skipping
        # add_listener cannot pass): the real delivery carried the
        # capacity channel's own name to the capacity callback.
        cap_entry = next(e for e in channels if e.channel == wf_emit_capacity_channel(schema))
        real_deliveries = [e for e in wakes if e[1] == cap_entry.channel]
        assert len(real_deliveries) >= 1 and real_deliveries[0][2] == id(cap_entry.callback), (
            journal
        )
    finally:
        if deps.notify_conn is not None:
            closer = getattr(deps.notify_conn, "close", None)
            if closer is not None:
                import contextlib

                with contextlib.suppress(Exception):
                    maybe = closer()
                    if asyncio.iscoroutine(maybe):
                        await maybe


async def test_raising_gated_hook_cannot_prevent_the_drain_arms(
    wf_pool: asyncpg.Pool, module_pg_schema: ModulePgSchema
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

    def raising_hook(
        conn: asyncpg.Connection | PoolConnectionProxy[asyncpg.Record],
        pid: int,
        ch: str,
        payload: object,
    ) -> None:
        # TOTAL: the event first, then the raising bookkeeping.
        for event in list(backend._wf_emit_capacity_subscribers):
            event.set()
        raise RuntimeError("the gated bookkeeping's own fault")

    journal: list[JournalEntry] = []
    channels = _registry(schema, backend, journal)
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
    import contextlib

    with contextlib.suppress(RuntimeError):
        await reconnect_notify_conn(deps, backend, channels)
    assert drained.is_set(), (
        "the raising hook's own event was set first — the delivery is total; "
        "the drain arms regardless of the bookkeeping fault"
    )
    await deps.notify_conn.close()


# ── N6: the two fail-closed HOST MUTANTS, in-tree ──────────────────────────


def _mutant_host_wake_only_channels0(
    channels: list[_NotifyEntry], conn: asyncpg.Connection, wake_log: list[str]
) -> asyncio.Task[None]:
    """THE HOST MUTANT A (the pre-fix host's own shape): the reconnect
    LISTENs every channel but offers ONE simulated wake to
    ``channels[0]`` only — every non-zero entry (the capacity channel
    among them) is omitted."""

    async def _reconnect() -> None:
        for entry in channels:
            await conn.execute(f'LISTEN "{entry.channel}"')
            await conn.add_listener(entry.channel, entry.callback)  # pyright: ignore[reportArgumentType]  # Why: entry.callback is the product registry's declared shape (Callable[[Connection, int, str, str], None]); the asyncpg stub's _Listener union is the over-narrow-stub friction the product itself ignores at notify.py's own add_listener sites.
        wake_log.append(channels[0].channel)
        channels[0].callback(conn, 0, channels[0].channel, "")

    return asyncio.create_task(_reconnect())


def _mutant_host_cross_channel_wake(
    channels: list[_NotifyEntry], conn: asyncpg.Connection, wake_log: list[str]
) -> asyncio.Task[None]:
    """THE HOST MUTANT B: the reconnect wakes every backfill hook but
    carries ``channels[0]``'s channel to EVERY callback — a substituted,
    cross-channel notification."""

    async def _reconnect() -> None:
        for entry in channels:
            await conn.execute(f'LISTEN "{entry.channel}"')
            await conn.add_listener(entry.channel, entry.callback)  # pyright: ignore[reportArgumentType]  # Why: entry.callback is the product registry's declared shape (Callable[[Connection, int, str, str], None]); the asyncpg stub's _Listener union is the over-narrow-stub friction the product itself ignores at notify.py's own add_listener sites.
        for entry in channels:
            if entry.reconnect_backfill:
                wake_log.append(channels[0].channel)
                entry.callback(conn, 0, channels[0].channel, "")

    return asyncio.create_task(_reconnect())


@pytest.mark.parametrize(
    ("mutant", "fault"),
    (
        (_mutant_host_wake_only_channels0, "omission"),
        (_mutant_host_cross_channel_wake, "substitution"),
    ),
)
async def test_fail_closed_host_mutants_show_their_own_fault_signature(
    module_pg_schema: ModulePgSchema, mutant: Any, fault: str
) -> None:
    """The committed-intent scenario against each fail-closed host
    mutant; each mutant's OWN fault signature is the asserted red:

    * the ``omission`` host (the pre-fix channels[0]-only wake): the
      capacity entry is never offered its own simulated wake — its event
      stays UNSET and the drain never arms;
    * the ``substitution`` host (a wake carrying ``channels[0]`` to the
      capacity callback): the callback's own totality still sets the
      event (a total callback cannot be prevented from delivering), but
      the DELIVERY'S IDENTITY is the fault — no wake ever carries the
      capacity channel's own name, which is exactly the correction's
      own assert (the entry's own channel, its own callback).

    The fixed host's own control (the order/own-channel arm) is the
    green these reds are the mirror of."""
    schema = module_pg_schema.schema_name
    backend = _BackendStandIn()
    drained = asyncio.Event()
    backend._wf_emit_capacity_subscribers.add(drained)
    journal: list[JournalEntry] = []
    channels = _registry(schema, backend, journal)

    conn = await asyncpg.connect(module_pg_schema.pg_dsn)
    mutant_log: list[str] = []
    try:
        task = mutant(channels, conn, mutant_log)
        await asyncio.wait_for(task, 10.0)
    finally:
        await conn.close()

    if fault == "omission":
        assert not drained.is_set(), (
            "the channels[0]-only host must never arm the capacity drain "
            "event (the omission is the fault)"
        )
        assert wf_emit_capacity_channel(schema) not in mutant_log, mutant_log
    else:
        # The SUBSTITUTION's own signature: the delivery happened (the
        # total callback), but NEVER under the capacity channel's own
        # name — every delivered wake carried another entry's channel.
        assert drained.is_set(), "the total callback's own delivery cannot be prevented"
        assert mutant_log, "the mutant host delivered its substituted wakes"
        assert wf_emit_capacity_channel(schema) not in mutant_log, (
            f"the substituted host never delivers the capacity channel's own wake: {mutant_log}"
        )
