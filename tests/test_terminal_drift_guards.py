"""Integration pins for the terminal writes' drift guards and the PG
progress-escape boundary, against real PG.

Three template-drift guards (``mark_retry``, ``mark_retry_after``,
``mark_interrupted``) had no pin — only ``mark_snoozed``'s did
(tests/test_postgres_terminal_writes.py). The drift is seeded the same
way: the fused statement's ``outcome_branch`` literal is renamed out of
step with its Python reader, and the reader must fail loudly AFTER the
write has landed (a drifted arm is template drift; the row's state is
real, only the caller's report is impossible to name).

The progress-escape pins cover the cold path
``backend/_terminal.py:_progress_jsonb_escaped`` shares with its
in-memory twin (tests/test_in_memory_terminal_write_edges.py): a lone
surrogate in the actor's progress state lands ESCAPED (the write must
not strand the job running in the crash-reclaim loop), while a value
the escape cannot repair (over-deep nesting) is refused with the
original ``UnencodableValue`` and the row stays running.
"""

from datetime import UTC, datetime, timedelta

import pytest

from taskq.backend._protocol import ErrorInfo
from taskq.exceptions import UnencodableValue
from taskq.testing.fixtures import JobsApp
from taskq.testing.pg import setup_running_job

pytestmark = pytest.mark.integration

_START = datetime(2026, 1, 1, tzinfo=UTC)

_RETRY_ERROR_INFO = ErrorInfo(
    error_class="BoomError",
    error_message="boom",
    error_traceback=None,
)


def _deep_with_shallow_surrogate(depth: int) -> dict[str, object]:
    """A lone surrogate at the TOP level, over-deep nesting BELOW it.

    orjson serializes dict entries in insertion order, so the first
    serialization attempt hits the surrogate (``UnencodableValue``) before
    the depth becomes the interesting failure; the repair walk then dies
    of stack exhaustion on the deep tail (``RecursionError``).
    """
    deep_tail: dict[str, object] = {"bottom": True}
    for _ in range(depth):
        deep_tail = {"next": deep_tail}
    return {"s": "\udcff", "deep": deep_tail}


# ── mark_retry's drift guard ────────────────────────────────────────────


async def test_mark_retry_drifted_branch_raises_after_repending(
    clean_jobs_app: JobsApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The retried arm renamed out of step with its reader must fail
    loudly on an already-re-pended row — never fall through to a
    masqueraded contract."""
    from dataclasses import replace as dc_replace

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema)

    sql = backend._sql
    drifted = sql.mark_retry.replace(
        "'retried'::text AS outcome_branch", "'snoozed'::text AS outcome_branch"
    )
    assert drifted != sql.mark_retry
    monkeypatch.setattr(backend, "_sql", dc_replace(sql, mark_retry=drifted))

    with pytest.raises(AssertionError, match="mark_retry cannot emit outcome branch"):
        await backend.mark_failed_or_retry(
            job_id,
            worker_id,
            _RETRY_ERROR_INFO,
            timedelta(seconds=30),
            attempt=1,
            claim_epoch=1,
        )

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    # The raise is post-write: the fused statement re-pended the row
    # before the parsed branch reached the caller's match.
    assert row is not None
    assert row["status"] == "scheduled"


# ── mark_retry_after's drift guard ──────────────────────────────────────


async def test_mark_retry_after_drifted_branch_raises_after_deferring(
    clean_jobs_app: JobsApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace as dc_replace

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema)

    sql = backend._sql
    drifted = sql.mark_retry_after_consume_true.replace(
        "'snoozed'::text AS outcome_branch", "'retried'::text AS outcome_branch"
    )
    assert drifted != sql.mark_retry_after_consume_true
    monkeypatch.setattr(backend, "_sql", dc_replace(sql, mark_retry_after_consume_true=drifted))

    with pytest.raises(AssertionError, match="mark_retry_after cannot emit outcome branch"):
        await backend.mark_retry_after(
            job_id, worker_id, timedelta(seconds=10), attempt=1, claim_epoch=1
        )

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    assert row["status"] == "scheduled"


# ── mark_interrupted's drift guard ──────────────────────────────────────


async def test_mark_interrupted_drifted_branch_raises_after_releasing(
    clean_jobs_app: JobsApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dataclasses import replace as dc_replace

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema)

    sql = backend._sql
    drifted = sql.mark_interrupted.replace(
        "'released'::text AS outcome_branch", "'retried'::text AS outcome_branch"
    )
    assert drifted != sql.mark_interrupted
    monkeypatch.setattr(backend, "_sql", dc_replace(sql, mark_interrupted=drifted))

    with pytest.raises(AssertionError, match="mark_interrupted cannot emit outcome branch"):
        await backend.mark_interrupted(
            job_id, worker_id, attempt=1, claim_epoch=1, hold=timedelta(0)
        )

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    assert row["status"] == "pending"


# ── the reader's terminal backstop (assert_never) ───────────────────────


async def test_mark_snoozed_unknown_branch_literal_is_refused_by_the_parser(
    clean_jobs_app: JobsApp,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A branch literal NO reader case names (a wholesale arm rename) must
    still fail loudly: the fused statement's ``outcome_branch`` column is
    untrusted input, and ``parse_outcome_branch`` refuses a value outside
    the closed union BEFORE any caller match runs — so a future arm can
    never silently no-op an operator's deferral."""
    from dataclasses import replace as dc_replace

    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema)

    sql = backend._sql
    drifted = sql.mark_snoozed.replace(
        "'snoozed'::text AS outcome_branch", "'bogus'::text AS outcome_branch"
    )
    assert drifted != sql.mark_snoozed
    monkeypatch.setattr(backend, "_sql", dc_replace(sql, mark_snoozed=drifted))

    with pytest.raises(ValueError, match="unknown outcome_branch from backend row: 'bogus'"):
        await backend.mark_snoozed(
            job_id, worker_id, timedelta(seconds=30), attempt=1, claim_epoch=1
        )


# ── the progress-escape boundary ────────────────────────────────────────


async def test_surrogate_progress_state_lands_escaped(
    clean_jobs_app: JobsApp,
) -> None:
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema)

    landed = await backend.mark_succeeded(
        job_id,
        worker_id,
        {"ok": True},
        progress_state={"detail": {"name": "\udcff"}},
        attempt=1,
        claim_epoch=1,
    )

    assert landed is True
    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(
            f'SELECT status, progress_state FROM "{schema}".jobs WHERE id = $1', job_id
        )
    assert row is not None
    assert row["status"] == "succeeded"
    import orjson

    progress = orjson.loads(row["progress_state"])
    assert progress["detail"] == {"name": "\\udcff"}, (
        f"the surrogate must land escaped (visible in the stored jsonb), got {progress!r}"
    )


async def test_progress_state_the_escape_cannot_repair_is_refused_and_leaves_the_row_running(
    clean_jobs_app: JobsApp,
) -> None:
    deps = clean_jobs_app.deps
    backend = clean_jobs_app.backend
    schema = deps.settings.schema_name

    async with deps.worker_pool.acquire() as conn:
        worker_id, job_id = await setup_running_job(conn, schema)

    with pytest.raises(UnencodableValue):
        await backend.mark_succeeded(
            job_id,
            worker_id,
            {"ok": True},
            progress_state=_deep_with_shallow_surrogate(3000),
            attempt=1,
            claim_epoch=1,
        )

    async with deps.worker_pool.acquire() as conn:
        row = await conn.fetchrow(f'SELECT status FROM "{schema}".jobs WHERE id = $1', job_id)
    assert row is not None
    assert row["status"] == "running", (
        "a progress value the escape cannot repair must not terminalise "
        "the row: the structural refusal stands and the caller decides"
    )
