"""The input monsters, run through the real progress and enqueue paths.

Each pin holds a gate that exists because a payload-from-hell shape broke
a mechanism, not a style preference. The inventory:

1. THE PERCENT MONSTER (NaN): the coalesce buffer's retire protocol keys
   on ``==`` equality between the pending state and the flushed snapshot,
   and ``nan != nan`` is True in Python. One ``ctx.progress(percent=nan)``
   call used to enter the buffer, never retire, and keep the buffer dirty
   FOREVER: the flush loop re-wrote the row every tick for the job's
   remaining lifetime. Refused at the caller-supplied door now (finiteness
   only, no range: a percent of 150.0 stays the actor's semantics).
2. THE WIRE-DIVERGENCE MONSTERS (percent inf / str, step str, detail
   non-str): pydantic and orjson quietly rewrote these between the two
   progress surfaces - a non-finite float became ``null`` on the wire, a
   string percent coerced onto the Redis event but stored as a string in
   the row. Type-confused inputs are refused at the door instead.
3. THE NUL MONSTER (the #414 gate's missing rooms): the enqueue door
   refuses a NUL in payload/metadata/tags; the progress door did not. A
   NUL in ``detail`` or ``data`` used to poison the buffer exactly like
   the NaN percent: the flush's jsonb guard only SKIPPED the row, and the
   loop re-logged the same permanent defect every tick forever, the actor
   none the wiser. Refused at the door now (the flush guard stays as
   defense-in-depth for direct buffer writers).
4. THE SIZE MONSTER (an unbounded detail): ``data`` had
   ``progress_data_max_bytes``; ``detail`` had nothing. A multi-megabyte
   detail rode every flush and TOASTed the jobs row's progress_state.
   Same bound, same measurement (serialized bytes), same exception.
5. THE DEPTH MONSTER (a 10k-deep data dict): orjson's encoder refuses
   nesting past its recursion limit - the door raise is pinned here, and
   the admin portal's fingerprint machine (the JS canonicalize) is
   depth-capped in realtime.js so no server state can RangeError it.
6. THE NAME MONSTERS (a 100k-char queue or actor name): both ride btree
   index items bounded at 2704 bytes, and an incompressible name past the
   limit failed the jobs INSERT with an opaque storage-engine error far
   from the declaration that caused it. Bounded at both chokepoints.
7. THE SEQ MONSTER (2^31 and 2^53): progress_seq is a strict monotone
   cursor the flush ADVANCES by arithmetic; on an int4 column the
   arithmetic overflowed at INT_MAX (SQLSTATE 22003), the flush errored
   every tick, the terminal write's 22003 was misclassified as transient
   infrastructure, and the job was stuck ``running`` forever - re-executed
   by every reclaim. Migrated to bigint (01.00.20_03); the JS-side double
   precision bound (2^53) is documented at the comparison in realtime.js,
   and the poll's cursor reads the exact decimal from the ETag header.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID

import asyncpg
import pytest
from hypothesis import given
from hypothesis import settings as hyp_settings
from hypothesis import strategies as st
from pydantic import BaseModel

from taskq._json import dumps
from taskq.actor import actor
from taskq.exceptions import ProgressTooLarge
from taskq.progress._buffer import _ProgressBuffer
from taskq.progress._flush import _retire_flushed_snapshot
from taskq.settings import TaskQSettings, WorkerSettings
from tests._progress_context import make_progress_context

pytestmark = pytest.mark.integration

_JOB_ID = UUID("aaaaaaaa-bbbb-cccc-dddd-00000000d001")
_SETTINGS = WorkerSettings.load_from_dict({})


class _SimplePayload(BaseModel):
    x: int = 0


async def _retire_roundtrip(buf: _ProgressBuffer) -> None:
    """One flush-retire cycle over *buf*, the shape a tick applies."""
    snapshot_delta = buf.pending_seq_delta
    snapshot_state = dict(buf.pending_state)
    _retire_flushed_snapshot(
        buf,
        returned_seq=buf.base_seq + snapshot_delta,
        snapshot_delta=snapshot_delta,
        snapshot_state=snapshot_state,
    )


# ── 1. the percent monster: NaN must never reach the buffer ────────────


@pytest.mark.parametrize("poison", [float("nan"), -float("nan")])
async def test_percent_nan_is_refused_at_the_door(poison: float) -> None:
    """``percent=nan`` raises at the call: a NaN in pending_state never
    retires (nan != nan), so the buffer stayed dirty forever and the flush
    loop re-wrote the row EVERY tick - unbounded work from one call."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(ValueError, match="percent must be a finite number"):
        await ctx.progress(percent=poison)

    assert buf.pending_seq_delta == 0
    assert buf.dirty is False
    assert "percent" not in buf.pending_state


@pytest.mark.parametrize("poison", [float("inf"), float("-inf")])
async def test_percent_infinity_is_refused_at_the_door(poison: float) -> None:
    """A non-finite percent is also refused: both encoders silently rewrote
    it to ``null`` on the wire while the row kept the float - the two
    progress surfaces diverging on garbage. The door refuses instead."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(ValueError, match="percent must be a finite number"):
        await ctx.progress(percent=poison)

    assert buf.dirty is False


async def test_percent_at_the_door_stays_range_free() -> None:
    """Finiteness is the ONLY numeric rule: 150.0 and -0.0 pass and land in
    the buffer - range is the actor's semantics, and -0.0 retires cleanly
    (its == holds, unlike nan's)."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    await ctx.progress(percent=150.0)
    assert buf.pending_state["percent"] == 150.0
    await _retire_roundtrip(buf)
    assert buf.dirty is False, "150.0 must retire like any finite percent"

    await ctx.progress(percent=-0.0)
    assert buf.pending_state["percent"] == -0.0
    await _retire_roundtrip(buf)
    assert buf.dirty is False, "-0.0 must retire like any finite percent"


@given(
    percent=st.one_of(
        st.floats(allow_nan=False, allow_infinity=False),
        st.integers(min_value=0, max_value=100),
    )
)
@hyp_settings(max_examples=100, deadline=None)
async def test_finite_percent_door_roundtrip_never_poisons(percent: float) -> None:
    """Any finite percent the door accepts must retire cleanly on the first
    flush - the poison shape (a buffer that never empties) is unreachable
    for every value the gate lets through."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buf.pending_seq_delta = 1
    buf.pending_state["percent"] = percent
    buf.dirty = True
    await _retire_roundtrip(buf)
    assert buf.dirty is False


# ── 2. the wire-divergence monsters: types are checked at the door ─────


@pytest.mark.parametrize(
    ("kwargs", "match"),
    [
        ({"percent": "50"}, "percent must be a number"),
        ({"percent": True}, "percent must be a number"),
        ({"percent": "half"}, "percent must be a number"),
        ({"step": "1"}, "step must be an int"),
        ({"step": True}, "step must be an int"),
        ({"step": 1.0}, "step must be an int"),
        ({"detail": 42}, "detail must be a str"),
        ({"detail": b"bytes"}, "detail must be a str"),
    ],
)
async def test_type_confused_progress_inputs_are_refused(
    kwargs: dict[str, object], match: str
) -> None:
    """A str percent coerced onto the Redis event but stored as a string in
    the row (the surfaces diverging); a str step failed the publish's
    pydantic validation downstream (the event silently dropped, a
    progress-publish-failure logged per call) while the flush still stored
    the junk. Every argument is type-checked at the door instead, and a
    refusal mutates nothing."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(TypeError, match=match):
        await ctx.progress(**kwargs)  # pyright: ignore[reportArgumentType]  # Why: the parametrize table IS the type-confusion matrix under test.

    assert buf.pending_seq_delta == 0
    assert buf.dirty is False
    assert buf.pending_state == {}


async def test_step_and_detail_types_still_accept_their_documented_shapes() -> None:
    """The gates refuse confusion, not use: int step, str detail, and an
    int percent (an int is a number) all land."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    await ctx.progress(step=3, detail="halfway", percent=50)
    assert buf.pending_state == {"step": 3, "detail": "halfway", "percent": 50}


# ── 3. the NUL monster: the #414 gate reaches the progress door ────────


async def test_nul_in_detail_is_refused_at_the_door() -> None:
    """A NUL in caller-supplied ``detail`` is refused at the call: the
    flush's jsonb guard only SKIPped such a row (re-logging the same
    permanent defect every tick forever, the actor none the wiser) - the
    same poison shape the NaN percent produced."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(ValueError, match="NUL"):
        await ctx.progress(step=1, detail="bad\x00value")

    assert buf.dirty is False
    assert buf.pending_state == {}


async def test_nul_in_data_is_refused_at_the_door() -> None:
    """A NUL anywhere in ``data`` - a nested value included - is refused on
    the bytes the size check already encoded (no second walk), the same
    rejection the enqueue door issues for a payload."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(ValueError, match="NUL"):
        await ctx.progress(data={"path": "ok", "deep": {"s": "bad\x00value"}})

    assert buf.dirty is False


async def test_nul_scan_sees_the_literal_escape_only_as_escape() -> None:
    """The text ``\\u0000`` (six literal characters) is a legal string, not
    a NUL: the byte-level scan keys on the escape parity of orjson's
    output, so a detail SHOWING the escape passes the door."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    await ctx.progress(detail="literal \\u0000 escape text")
    assert buf.pending_state["detail"] == "literal \\u0000 escape text"


# ── 4. the size monster: detail obeys the data cap's bound ─────────────


async def test_oversized_detail_raises_progress_too_large() -> None:
    """``detail`` had no bound while ``data`` had
    ``progress_data_max_bytes``: a multi-megabyte detail rode every flush
    and TOASTed the row's progress_state. Same ceiling, same measurement
    (serialized bytes), same exception, raised before the buffer mutates."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(ProgressTooLarge) as exc_info:
        await ctx.progress(detail="a" * (_SETTINGS.progress_data_max_bytes + 1))

    assert exc_info.value.limit == _SETTINGS.progress_data_max_bytes
    assert buf.dirty is False


async def test_detail_at_the_cap_is_accepted() -> None:
    """A detail whose SERIALIZED form is exactly at the ceiling passes (the
    data cap's boundary convention: the LIMIT is legal, limit + 1 is not).
    The measurement is the serialized bytes, so the JSON quotes count."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    at_cap = _SETTINGS.progress_data_max_bytes - 2  # the enclosing quotes
    assert len(dumps("a" * at_cap)) == _SETTINGS.progress_data_max_bytes
    await ctx.progress(detail="a" * at_cap)
    assert buf.dirty is True


# ── 5. the depth monster: the encoder's recursion limit is the door ────


def _nested(depth: int) -> dict[str, object]:
    v: object = 1
    for _ in range(depth):
        v = {"a": v}
    return v  # type: ignore[no-any-return]  # Why: the walk builds nested dicts of exactly the shape under test.


@pytest.mark.parametrize("depth", [600, 10_000])
async def test_deeply_nested_data_raises_at_the_door(depth: int) -> None:
    """``data`` nested past orjson's encoder recursion limit raises
    (``UnencodableValue``, a TypeError) at the call - the actor sees the
    refusal, the buffer never holds it, and the flush loop is never asked
    to store what its own encoder cannot walk."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    with pytest.raises(TypeError):
        await ctx.progress(data=_nested(depth))

    assert buf.dirty is False
    assert buf.pending_seq_delta == 0


async def test_deeply_nested_data_at_the_encoder_limit_is_stored_and_retires() -> None:
    """Depth the encoder accepts (just under its recursion limit) is the
    contract's other half: the state document builds, the flush retires,
    and the client side is covered by realtime.js's depth-capped
    canonicalize (pinned under Node in the web_admin suite)."""
    buf = _ProgressBuffer(job_id=_JOB_ID, base_seq=0, attempt=1)
    buffers: dict[UUID, _ProgressBuffer] = {_JOB_ID: buf}
    ctx = make_progress_context(buffers, _JOB_ID, settings=_SETTINGS)

    await ctx.progress(data=_nested(200))
    assert buf.dirty is True
    await _retire_roundtrip(buf)
    assert buf.dirty is False


# ── 6. the name monsters: btree-bounded identifiers at both doors ──────


def test_queue_name_at_the_bound_is_accepted() -> None:
    from taskq.backend._protocol import _validate_queue_name  # pyright: ignore[reportPrivateUsage]

    assert _validate_queue_name("a" * 255) == "a" * 255


def test_queue_name_past_the_bound_is_refused_with_a_bounded_message() -> None:
    """A 100k-char queue name used to pass validation and fail the jobs
    INSERT deep in btree index-item arithmetic (an incompressible name
    over ~2700 bytes: "index row size exceeds btree version 4 maximum").
    Refused at the chokepoint now - and the rejection must not echo the
    whole monster back."""
    from taskq.backend._protocol import _validate_queue_name  # pyright: ignore[reportPrivateUsage]

    monster = "q9Z_" * 25_000
    with pytest.raises(ValueError, match="exceeds 255 characters") as exc_info:
        _validate_queue_name(monster)
    assert len(str(exc_info.value)) < 500, (
        "a rejection for a megabyte name must not itself carry a megabyte of it"
    )


def test_queue_name_rejection_echo_truncates_at_exactly_64_chars() -> None:
    """The bounded echo's own boundary: an invalid name of exactly 64
    characters is echoed whole (no truncation marks), one of 65 is cut at
    64 with the ellipsis and never echoed whole. Both edges pinned."""
    import re

    from taskq.backend._protocol import _validate_queue_name  # pyright: ignore[reportPrivateUsage]

    whole = "b" * 63 + "!"  # 64 chars, invalid last: echoed in full
    with pytest.raises(ValueError) as exc_info:
        _validate_queue_name(whole)
    msg = str(exc_info.value)
    assert re.search(re.escape(whole) + r"(?!\.\.\.)", msg), (
        "a 64-char invalid name must be echoed whole without truncation marks"
    )

    cut = "b" * 64 + "!"  # 65 chars: echoed as the first 64 + "..."
    with pytest.raises(ValueError) as exc_info:
        _validate_queue_name(cut)
    msg = str(exc_info.value)
    assert re.search(re.escape("b" * 64) + r"\.\.\.", msg), (
        "a 65-char invalid name must be truncated at 64 with an ellipsis"
    )
    assert cut not in msg, "a 65-char invalid name must never be echoed whole"


def test_queue_name_offender_classifies_the_bound_as_legal() -> None:
    """The message helper's boundary: a name AT the 255-char bound is legal,
    so the offender must not classify it as over the bound - only a name
    past it gets the 'exceeds' verdict."""
    from taskq.backend._protocol import (  # pyright: ignore[reportPrivateUsage]
        _queue_name_offender,
    )

    assert "exceeds" not in _queue_name_offender("a" * 255)
    assert "exceeds" in _queue_name_offender("a" * 256)
    # The charset diagnoses stay exact: the first character and a mid-string
    # character get DISTINCT verdicts naming the offending character and its
    # actual position, so a rejection names the real defect.
    assert "first character" in _queue_name_offender("!abc")
    mid = _queue_name_offender("a!b")
    assert "'!'" in mid and "position 1" in mid


def test_actor_name_past_the_bound_is_refused_at_registration() -> None:
    """The actor name rides the same composite dispatch indexes beside the
    queue name; the same bound applies at registration, where the fix is
    one edit - not at enqueue, an opaque storage error far from the cause."""
    monster = "a" * 256

    with pytest.raises(ValueError, match="actor name exceeds 255 characters"):

        @actor(name=monster)
        async def long_named_actor(
            payload: _SimplePayload, *args: object, **kwargs: object
        ) -> None:
            pass


def test_actor_name_at_the_bound_is_accepted() -> None:
    @actor(name="a" * 255)
    async def bounded_actor(payload: _SimplePayload, *args: object, **kwargs: object) -> None:
        pass

    assert len(bounded_actor.name) == 255


def test_list_page_columns_do_not_carry_error_message() -> None:
    """The jobs list renders no error text, but both list queries selected
    ``error_message`` - actor-derived text with no storage bound, so one
    poisoned row (an actor raising ValueError('a' * 10_000_000)) made
    every list page view drag the whole message out of TOAST per row,
    per fetch. The detail page bounds it at render; the list refuses the
    transfer outright."""
    from taskq.web.admin.jobs import (  # pyright: ignore[reportPrivateUsage]
        _ARCHIVE_COLS,
        _LIVE_COLS,
    )

    assert "error_message" not in _LIVE_COLS
    assert "error_message" not in _ARCHIVE_COLS


# ── 7. the seq monster: the cursor's storage domain is bigint ──────────


async def test_flush_sql_advances_progress_seq_past_int4_max(
    pg_conn: asyncpg.Connection, settings: TaskQSettings
) -> None:
    """The real mechanism, against real PG: the flush UPDATE advances
    ``progress_seq`` by arithmetic, and on the old int4 column the row at
    2147483646 + delta raised 22003 (integer out of range) every tick,
    then the terminal write's 22003 was misclassified as transient infra -
    the job stuck ``running``, re-executed by every reclaim. On the bigint
    column the same write lands."""
    from taskq.migrate import apply_pending

    await apply_pending(pg_conn, schema=settings.schema_name)

    column_type = await pg_conn.fetchval(
        """
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs' AND column_name = 'progress_seq'
        """,
        settings.schema_name,
    )
    assert column_type == "bigint", (
        f"progress_seq must be bigint (the flush ADVANCES it by arithmetic; "
        f"int4 overflow at INT_MAX stuck the job running forever), got {column_type}"
    )
    archive_type = await pg_conn.fetchval(
        """
        SELECT data_type FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = 'jobs_archive'
          AND column_name = 'progress_seq'
        """,
        settings.schema_name,
    )
    assert archive_type == "bigint"

    # The overflow write itself, on a live row: the exact arithmetic the
    # flush statement applies, at the boundary that used to raise 22003.
    await pg_conn.execute(
        f'INSERT INTO "{settings.schema_name}".jobs '  # noqa: S608  # Why: the schema name comes from the test's own settings fixture, the same f-string SQL shape every migration-backed test in this repo binds.
        "(id, actor, queue, payload, max_attempts, retry_kind) "
        "VALUES ($1, 'monster', 'default', '{}', 1, 'transient')",
        _JOB_ID,
    )
    await pg_conn.execute(
        f'UPDATE "{settings.schema_name}".jobs SET progress_seq = $1 WHERE id = $2',  # noqa: S608
        2**31 - 1,
        _JOB_ID,
    )
    advanced = await pg_conn.fetchval(
        f'UPDATE "{settings.schema_name}".jobs '  # noqa: S608
        "SET progress_seq = progress_seq + $1 "
        "WHERE id = $2 AND progress_seq = $3 RETURNING progress_seq",
        3,
        _JOB_ID,
        2**31 - 1,
    )
    assert advanced == 2**31 + 2, "the cursor must advance past INT_MAX, not overflow"


def test_progress_seq_bind_params_are_bigint_in_the_terminal_templates() -> None:
    """The four deferral/release templates bind the caller's progress_seq
    with an explicit ``::int`` cast: past INT_MAX the CAST is what raised
    (a Python int binds as int8, the ::int cast overflows server-side).
    The casts follow the column's domain."""
    from taskq.backend._sql_templates import render as render_templates

    templates = render_templates("taskq")
    for name in (
        "mark_snoozed",
        "mark_retry_after_consume_true",
        "mark_retry_after_consume_false",
        "mark_interrupted",
    ):
        sql = getattr(templates, name)
        assert "::int AS progress_seq" not in sql, (
            f"{name} binds progress_seq with a stale int4 cast"
        )
        assert "::bigint AS progress_seq" in sql, (
            f"{name} must bind progress_seq at the column's bigint domain"
        )


def test_bigint_migration_documents_the_full_rewrite_ops_window() -> None:
    """The migration's header must keep stating the apply cost honestly:
    ALTER TYPE ... TYPE bigint rewrites each table (a fresh relfilenode)
    and rebuilds EVERY index on it, an ops window an operator budgets
    against the live row count. What it must never claim again is the
    "widens in place" / "no index rebuild" story: that understates the
    lock horizon and hides the rebuild from the ops-window arithmetic."""
    migration = (
        Path(__file__).parents[1] / "src/taskq/migrations/01.00.20_03_pre_progress_seq_bigint.sql"
    )
    header = migration.read_text(encoding="utf-8").lower()
    assert "rewrites" in header, "the header must state the full-table rewrite"
    assert "rebuilt" in header, "the header must state every index is rebuilt"
    assert "no index rebuild" not in header, "the no-rebuild claim is false"
    assert "in place" not in header, "the in-place claim is false"
    assert "ops window" in header or "ops-window" in header, (
        "the header must frame the rewrite as an ops-window statement"
    )
