"""Pins for the decode/deserialization boundaries: no decode exception
escapes its boundary unclassified, and no poison row crashes a worker.

Three boundaries are attacked with hand-constructed malformed input:

1. The dispatch claim path (``taskq.backend._dispatch``): a claimed row
   whose jsonb columns hold text that is not valid JSON, or a ROW-CONTRACT
   dict column (metadata, progress_state) whose valid-JSON body is not the
   object the row contract declares. The decode
   sits AFTER the claim committed, so an escaping decode exception made
   the whole round the producer loop's problem (the unexpected-failure
   backstop kills the worker at its consecutive cap) while the poisoned
   row looped forever through claim and lease-sweep reclaim. Pinned: the
   corrupt row is terminally failed with ``error_class='CorruptJobDataError'``
   through the standard fenced ``mark_failed`` statement and the round's
   healthy rows still dispatch.

   The USER-CONTENT columns (result, payload) hold the actor's data and
   declare no shape: they decode through ``jsonb_to_dict``'s sibling
   ``jsonb_to_value``, which keeps ONLY the malformed-text guard. Pinned:
   a list result round-trips verbatim, a scalar payload dispatches, and a
   corrupt-TEXT result still fails with the named class.

2. The NOTIFY event callbacks (``taskq.worker.notify``): a hostile
   payload on a channel TaskQ does not exclusively own. Pinned: the drop
   carries its counter, and the sync callback never raises into asyncpg.

3. The AAD JWT claim decode (``taskq.aad``): a pathologically nested
   claims object. stdlib ``json.loads`` recurses (``RecursionError``, a
   BaseException-adjacent escape the caught ``ValueError`` tuple never
   saw); the orjson-backed ``taskq._json.loads`` depth-limits to a
   ``ValueError``-shaped ``JSONDecodeError``. Pinned: a 10k-deep claims
   payload returns ``None`` (the honest no-oid answer), no escape.
"""

import asyncio
import base64
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from taskq._ids import new_uuid
from taskq.backend import _dispatch as _dispatch_mod
from taskq.backend._records import _job_row_from_record, jsonb_to_dict, jsonb_to_value
from taskq.backend._sql_templates import render
from taskq.exceptions import CorruptJobDataError
from taskq.obs import (
    _otel as otel_mod,  # pyright: ignore[reportPrivateUsage]  # Why: the pin rebinds the module-level instrument, the same seam taskq.testing.otel.setup_meter uses.
)
from taskq.testing.otel import counter_value, setup_tracer
from taskq.worker import notify as notify_mod

_WORKER_ID = UUID("00000000-0000-0000-0000-000000000001")
_NOW = datetime(2026, 9, 23, tzinfo=UTC)

_COLUMNS = [
    "id",
    "actor",
    "queue",
    "identity_key",
    "idempotency_key",
    "idempotency_scope",
    "fairness_key",
    "payload",
    "payload_schema_ver",
    "status",
    "priority",
    "attempt",
    "max_attempts",
    "retry_kind",
    "schedule_to_close",
    "start_to_close",
    "heartbeat_timeout",
    "created_at",
    "scheduled_at",
    "started_at",
    "finished_at",
    "last_heartbeat_at",
    "locked_by_worker",
    "lock_expires_at",
    "cancel_requested_at",
    "cancel_phase",
    "error_class",
    "error_message",
    "error_traceback",
    "progress_state",
    "progress_seq",
    "result",
    "result_size_bytes",
    "result_expires_at",
    "trace_id",
    "span_id",
    "metadata",
    "tags",
    "snooze_count",
    "rate_limit_blocked_count",
    "interrupt_count",
    "retry_base_seconds",
    "retry_cap_seconds",
    "retry_backoff",
    "retry_jitter",
    "assignment_routed",
    "claim_epoch",
]


def _record(**overrides: object) -> dict[str, object]:
    """A claimed-row record in the exact shape the claim CTE returns."""
    base: dict[str, object] = {
        "id": new_uuid(),
        "actor": "A",
        "queue": "default",
        "identity_key": None,
        "idempotency_key": None,
        "idempotency_scope": None,
        "fairness_key": None,
        "payload": "{}",
        "payload_schema_ver": 1,
        "status": "running",
        "priority": 5,
        "attempt": 3,
        "max_attempts": 3,
        "retry_kind": "transient",
        "schedule_to_close": timedelta(seconds=60),
        "start_to_close": timedelta(seconds=30),
        "heartbeat_timeout": timedelta(seconds=15),
        "created_at": _NOW,
        "scheduled_at": _NOW,
        "started_at": _NOW,
        "finished_at": None,
        "last_heartbeat_at": None,
        "locked_by_worker": _WORKER_ID,
        "lock_expires_at": _NOW + timedelta(seconds=30),
        "cancel_requested_at": None,
        "cancel_phase": 0,
        "error_class": None,
        "error_message": None,
        "error_traceback": None,
        "progress_state": None,
        "progress_seq": 0,
        "result": None,
        "result_size_bytes": None,
        "result_expires_at": None,
        "trace_id": None,
        "span_id": None,
        "metadata": "{}",
        "tags": [],
        "snooze_count": 0,
        "rate_limit_blocked_count": 0,
        "interrupt_count": 0,
        "retry_base_seconds": 1.0,
        "retry_cap_seconds": 60.0,
        "retry_backoff": "exponential",
        "retry_jitter": "full",
        "assignment_routed": False,
        "claim_epoch": 7,
    }
    base.update(overrides)
    return base


class _FakeConn:
    """Records every fetchrow (the corrupt-row fail-write is the only one
    on this path) and returns the pre-set claimed records from fetch."""

    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self.fail_writes: list[tuple[str, tuple[object, ...]]] = []

    async def fetch(self, sql: str, *args: object) -> list[dict[str, object]]:
        return self._records

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        self.fail_writes.append((sql, args))
        return {"id": 1, "attempt": 1}

    def terminate(self) -> None:  # pragma: no cover - not hit on this path
        raise AssertionError("terminate called")


class _FailingFailWriteConn(_FakeConn):
    """The fail-write itself raises: the realistic dead-connection case."""

    async def fetchrow(self, sql: str, *args: object) -> dict[str, object] | None:
        self.fail_writes.append((sql, args))
        raise ConnectionResetError("connection reset during the fail-write")


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self.conn

    async def __aexit__(self, *exc: object) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def acquire(self, timeout: float) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


def _strict_cache() -> _dispatch_mod.QueueModeCache:
    cache = _dispatch_mod.QueueModeCache()
    cache.store({"default": "strict_fifo"})
    return cache


async def _run_round(
    conn: _FakeConn,
    monkeypatch: pytest.MonkeyPatch | None = None,
) -> list[object]:
    if monkeypatch is not None:
        setup_tracer(monkeypatch)
    rows: list[object] = list(
        await _dispatch_mod._dispatch_batch(
            _FakePool(conn),  # pyright: ignore[reportArgumentType]  # Why: the pin drives the seam with a duck-typed fake, not a real pool.
            render("taskq"),
            dispatch_oversample=2,
            acquire_timeout=2.0,
            schema="taskq",
            worker_id=_WORKER_ID,
            queues=["default"],
            limit=5,
            lock_lease=timedelta(seconds=30),
            queue_mode_cache=_strict_cache(),
        )
    )
    return rows


# ── the decode boundary itself ───────────────────────────────────────


@pytest.mark.parametrize(
    "blob",
    ['{"broken"', "not json at all", "", "[1, 2, 3]", "5", '"a bare string"', "null"],
)
def test_jsonb_to_dict_classifies_every_malformed_shape(blob: str) -> None:
    """Malformed text AND valid-but-non-object JSON both raise the named
    CorruptJobDataError, never a raw orjson/driver vocabulary word."""
    with pytest.raises(CorruptJobDataError):
        jsonb_to_dict(blob)


def test_jsonb_to_dict_names_the_column() -> None:
    """The raise names the refusing column, so the fail-write's error
    message and the counter's label attribute carry attribution."""
    with pytest.raises(CorruptJobDataError, match="payload") as exc_info:
        jsonb_to_dict('{"broken"', column="payload")
    assert exc_info.value.column == "payload"


def test_jsonb_to_dict_recursion_bomb_is_classified_not_recursion() -> None:
    """A 10k-deep nest is the recursion bomb: stdlib json would raise
    RecursionError (BaseException-adjacent, outside every ValueError
    tuple). The orjson-backed decode depth-limits to a ValueError, which
    the boundary classifies; the exception must be the named class."""
    bomb = "[" * 10_000 + "]" * 10_000
    with pytest.raises(CorruptJobDataError) as exc_info:
        jsonb_to_dict(bomb, column="payload")
    assert not isinstance(exc_info.value, RecursionError)
    assert exc_info.value.column == "payload"


def test_jsonb_to_dict_happy_shapes() -> None:
    """None passes through, a dict passes through, a JSON object parses."""
    assert jsonb_to_dict(None) is None
    obj: dict[str, object] = {"a": 1}
    assert jsonb_to_dict(obj) is obj
    assert jsonb_to_dict('{"a": 1}') == {"a": 1}


# ── jsonb_to_value: the user-content sibling (result, payload) ────────


def test_jsonb_to_value_round_trips_every_valid_json_shape() -> None:
    """A user-content column declares no shape: dict, list, scalar,
    string, and null all pass through verbatim, byte-faithfully."""
    assert jsonb_to_value(None) is None
    obj: dict[str, object] = {"a": 1}
    assert jsonb_to_value(obj) is obj  # pre-decoded by a codec: untouched
    pre_decoded = [1, "x"]
    assert jsonb_to_value(pre_decoded) is pre_decoded
    assert jsonb_to_value('{"a": 1}') == {"a": 1}
    assert jsonb_to_value("[1, 2]") == [1, 2]
    assert jsonb_to_value("5") == 5
    assert jsonb_to_value('"a bare string"') == "a bare string"
    assert jsonb_to_value("true") is True


@pytest.mark.parametrize("blob", ['{"broken"', "not json at all", ""])
def test_jsonb_to_value_still_guards_malformed_text(blob: str) -> None:
    """The ONLY guard the user-content decode drops is the shape
    assertion. Text that is not valid JSON raises the same named class
    jsonb_to_dict raises: a corrupt row fails loudly, user content or
    not."""
    with pytest.raises(CorruptJobDataError):
        jsonb_to_value(blob, column="result")


def test_jsonb_to_value_names_the_column_and_classifies_the_recursion_bomb() -> None:
    """The raise names the refusing column, and the 10k-deep nest is the
    classified ValueError shape, never a RecursionError escape."""
    bomb = "[" * 10_000 + "]" * 10_000
    with pytest.raises(CorruptJobDataError) as exc_info:
        jsonb_to_value(bomb, column="payload")
    assert not isinstance(exc_info.value, RecursionError)
    assert exc_info.value.column == "payload"


def test_list_result_round_trips_through_the_row_decode() -> None:
    """The regression pin (the round-trip the PG test anchors): an actor
    may return a list; the claimed-row decode must hand it back verbatim,
    no CorruptJobDataError, no dict coercion."""
    rec = _record(result="[1, 2]")
    row = _job_row_from_record(rec)  # pyright: ignore[reportArgumentType]  # Why: the pin drives the seam with a duck-typed dict, not a real Record.
    assert row.result == [1, 2]


def test_scalar_payload_decodes_verbatim_through_the_row_decode() -> None:
    """A valid-but-scalar payload is USER CONTENT: the decode passes it
    through untouched (its shape is the actor-schema layer's problem),
    and a falsy scalar body (0, empty list) is not clobbered by the NULL
    normalization."""
    rec = _record(payload="0", result=None)
    row = _job_row_from_record(rec)  # pyright: ignore[reportArgumentType]  # Why: duck-typed dict, see above.
    assert row.payload == 0
    list_rec = _record(payload="[]")
    assert _job_row_from_record(list_rec).payload == []  # pyright: ignore[reportArgumentType]  # Why: duck-typed dict, see above.
    null_rec = _record(payload=None)
    assert _job_row_from_record(null_rec).payload == {}  # pyright: ignore[reportArgumentType]  # Why: SQL NULL keeps its empty-dict normalization.


# ── the claim path: fail-visible per row, round ticks on ─────────────


async def test_corrupt_row_fails_terminally_and_healthy_rows_still_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The central pin: one poisoned row must cost exactly that row, not
    the round. The corrupt row is terminally failed through the fenced
    mark_failed statement with the named error_class; the healthy row is
    still returned to the producer loop."""
    healthy = _record()
    corrupt = _record(payload='{"broken"')
    conn = _FakeConn([corrupt, healthy])

    rows = await _run_round(conn, monkeypatch)

    assert [getattr(r, "id", None) for r in rows] == [healthy["id"]], (
        "the round must dispatch its healthy rows; a poisoned row may not cost the round its work"
    )
    assert len(conn.fail_writes) == 1, (
        "the corrupt row must be terminally failed in the same round, not "
        "left claimed to loop through reclaim"
    )
    sql, args = conn.fail_writes[0]
    assert "status = 'failed'" in sql
    # args: job_id, worker_id, error_class, error_message, error_traceback,
    #       progress_seq, progress_state, attempt, claim_epoch
    assert args[0] == corrupt["id"]
    assert args[1] == _WORKER_ID
    assert args[2] == "CorruptJobDataError"
    assert args[7] == corrupt["attempt"], "the fence must bind the claimed attempt"
    assert args[8] == corrupt["claim_epoch"], "the fence must bind the claimed epoch"
    assert "payload" in str(args[3]), "the persisted error message names the refusing column"


async def test_corrupt_metadata_row_fails_terminally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A corrupt metadata column fails the row with the column named in
    the persisted error message."""
    corrupt = _record(metadata="not json at all")
    conn = _FakeConn([corrupt])

    rows = await _run_round(conn, monkeypatch)

    assert rows == []
    assert len(conn.fail_writes) == 1
    assert conn.fail_writes[0][1][2] == "CorruptJobDataError"
    assert "metadata" in str(conn.fail_writes[0][1][3])


async def test_recursion_bomb_on_the_real_claim_path_is_fail_visible(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 10k-deep payload through the REAL claim path: the round must
    return cleanly (the row terminally failed), never leak a
    RecursionError into the producer loop's backstop."""
    bomb = "[" * 10_000 + "]" * 10_000
    healthy = _record()
    conn = _FakeConn([_record(payload=bomb), healthy])

    rows = await _run_round(conn, monkeypatch)

    assert [getattr(r, "id", None) for r in rows] == [healthy["id"]]
    assert len(conn.fail_writes) == 1
    assert conn.fail_writes[0][1][2] == "CorruptJobDataError"


async def test_fail_write_failure_does_not_poison_the_round(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fail-write that cannot land (dead connection) degrades to a
    logged warning: the round still dispatches its healthy rows, the row
    stays claimed for the lease sweep, and the next round retries."""
    healthy = _record()
    corrupt = _record(payload='{"broken"')
    conn = _FailingFailWriteConn([corrupt, healthy])

    rows = await _run_round(conn, monkeypatch)

    assert [getattr(r, "id", None) for r in rows] == [healthy["id"]]
    assert len(conn.fail_writes) == 1, "the fail-write was attempted exactly once"


async def test_corrupt_row_increments_its_counter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The drop carries its counter: a fleet eating poisoned rows is
    visible in the metric stream, not only in warning logs."""
    setup_tracer(monkeypatch)
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    counter = provider.get_meter("taskq").create_counter("taskq.dispatch.corrupt_rows", unit="1")
    monkeypatch.setattr(otel_mod, "_corrupt_dispatch_rows", counter)
    conn = _FakeConn([_record(payload='{"broken"')])

    await _run_round(conn)

    assert counter_value(reader, "taskq.dispatch.corrupt_rows") == 1, (
        "a terminally-failed corrupt row must be counted, not merely logged"
    )


# ── the claim path: the shape partition ───────────────────────────────


async def test_corrupt_text_result_fails_terminally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The user-content columns keep the malformed-text guard: a claimed
    row whose RESULT column holds text that is not valid JSON is failed
    terminally with the named class, the column named in the persisted
    error message."""
    corrupt = _record(result='{"broken"')
    conn = _FakeConn([corrupt])

    rows = await _run_round(conn, monkeypatch)

    assert rows == []
    assert len(conn.fail_writes) == 1
    assert conn.fail_writes[0][1][2] == "CorruptJobDataError"
    assert "result" in str(conn.fail_writes[0][1][3])


async def test_non_object_row_contract_columns_still_fail_terminally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The partition's other edge: a valid-but-non-object body in a
    ROW-CONTRACT dict column (metadata here, progress_state equally)
    still fails the row terminally. The shape gate belongs to the fields
    TaskQ owns and indexes into, and the user-content relaxation must
    not widen to them."""
    corrupt = _record(metadata="[1, 2, 3]")
    conn = _FakeConn([corrupt])

    rows = await _run_round(conn, monkeypatch)

    assert rows == []
    assert len(conn.fail_writes) == 1
    assert conn.fail_writes[0][1][2] == "CorruptJobDataError"
    assert "metadata" in str(conn.fail_writes[0][1][3])


async def test_scalar_payload_dispatches_through_the_claim_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A valid-but-scalar payload (an interop writer's row) DISPATCHES:
    no fail-write, the row returned to the producer loop with its
    payload verbatim. Its shape is the actor-schema layer's verdict to
    make, never the decode boundary's."""
    scalar = _record(payload="5")
    conn = _FakeConn([scalar])

    rows = await _run_round(conn, monkeypatch)

    assert [r.id for r in rows] == [scalar["id"]]  # pyright: ignore[reportAttributeAccessIssue]  # Why: the fake-driven round returns JobRow-shaped objects.
    assert rows[0].payload == 5  # pyright: ignore[reportAttributeAccessIssue, reportArgumentType]  # Why: see above.
    assert conn.fail_writes == [], "user content must never wear a CorruptJobDataError verdict"


# ── the NOTIFY event callbacks: drop WITH the counter ─────────────────


class _BackendStub:
    """The attributes _make_events_callback / _make_worker_events_callback
    read off the backend."""

    def __init__(self) -> None:
        self._cancel_subscribers: list[asyncio.Event] = []
        self._wake_subscribers: list[asyncio.Event] = []


@pytest.fixture()
def _notify_counter(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    calls = [0]

    class _Counting:
        def add(self, amount: int, **_kw: object) -> None:
            calls[0] += amount

    monkeypatch.setattr(notify_mod, "_notify_payload_parse_failed_counter", _Counting())
    return calls


@pytest.mark.parametrize("callback_name", ["_make_events_callback", "_make_worker_events_callback"])
def test_notify_hostile_payload_drops_with_its_counter(
    callback_name: str,
    _notify_counter: list[int],
) -> None:
    """A payload that is not the cancel envelope (binary garbage, hostile
    deep nest) is dropped, the drop is counted, and the sync callback
    never raises into asyncpg's protocol loop."""
    factory = getattr(notify_mod, callback_name)
    backend = _BackendStub()
    cb = (
        factory(backend, _WORKER_ID)
        if callback_name == "_make_events_callback"
        else factory(backend)
    )
    for hostile in ("not-json{{{", "\xff\xfe binary", '{"type":', "[" * 10_000):
        cb(None, 0, "channel", hostile)  # pyright: ignore[reportArgumentType]  # Why: asyncpg passes a real Connection at runtime; the callback never touches it on this path.
    assert _notify_counter[0] == 4, (
        "every dropped payload must increment taskq.notify.payload_parse_failed"
    )
    assert backend._cancel_subscribers == []


def test_notify_valid_cancel_payload_still_wakes(_notify_counter: list[int]) -> None:
    """The counter counts only DROPS: a well-formed cancel envelope still
    wakes the subscribers and does not touch the parse-failed counter."""
    import orjson

    backend = _BackendStub()
    event = asyncio.Event()
    backend._cancel_subscribers.append(event)
    cb = notify_mod._make_events_callback(backend, _WORKER_ID)
    payload = orjson.dumps({"type": "cancel", "job_id": "x", "worker_id": str(_WORKER_ID)}).decode()
    cb(None, 0, "channel", payload)  # pyright: ignore[reportArgumentType]  # Why: duck-typed backend/connection, see above.
    assert event.is_set()
    assert _notify_counter[0] == 0


# ── the AAD JWT claim decode: depth-bounded, no recursion escape ──────


def test_aad_deep_claims_returns_none_not_recursion() -> None:
    """A JWT whose claims decode to a 10k-deep JSON document: the decode
    must answer None (no oid), not escape a RecursionError past the
    caught ValueError tuple."""
    from taskq.aad import (
        _decode_jwt_oid,  # pyright: ignore[reportPrivateUsage]  # Why: the pin exercises the private decode boundary directly.
    )

    deep = ("[" * 10_000 + "]" * 10_000).encode()
    payload = base64.urlsafe_b64encode(deep).rstrip(b"=").decode()
    token = f"header.{payload}."
    assert _decode_jwt_oid(token) is None


def test_aad_malformed_base64_returns_none() -> None:
    """Undecodable base64 and non-JSON bytes answer None, the honest
    no-oid answer the caller turns into its helpful ValueError."""
    from taskq.aad import _decode_jwt_oid  # pyright: ignore[reportPrivateUsage]  # Why: see above.

    bad_b64 = base64.urlsafe_b64encode(b"\xff\xfe not json").rstrip(b"=").decode()
    assert _decode_jwt_oid(f"header.{bad_b64}.") is None
    assert _decode_jwt_oid("not-a-jwt") is None
