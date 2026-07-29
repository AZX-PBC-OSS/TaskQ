"""Bundled migrations applied STEPWISE onto a POPULATED database.

Owner requirement: every bundled migration — present AND future — must run
cleanly as an end user against a database that already holds data. The
stepwise test below applies migrations one at a time (``max_steps=1``) and
seeds a full slice of realistic rows after each step, so migration N+1
always applies onto the data volumes migration N left behind. A migration
that damages a populated database fails CI naming the offending migration
key in the assertion message.

Two tiers live here:

* ``test_seed_data_is_deterministic`` — pure (no PG) contract for the row
  generator: identical output across calls, globally-unique idempotency
  keys, exact status distribution, and no singleton-metadata on
  active-status rows (which ``jobs_singleton_uniq`` would reject).
* ``test_bundled_migrations_apply_stepwise_onto_populated_database`` — the
  integration harness: stepwise apply + per-step invariants + per-step
  seeding, then a final integrity pass and a functional smoke through the
  REAL backend API, ending with the end-user CLI contract (``migrate up``
  on a fully-migrated populated DB is a safe no-op).

Runtime budget: the module must stay under ~45s (CI runs ``pytest -n 2``).
Tune ONLY via the ``SEED_*`` volume knobs below — halve ``SEED_JOBS``
first, trim ``SEED_EVENTS_PER_JOB`` second.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest
from typer.testing import CliRunner

from taskq import migrate as migrate_mod
from taskq._ids import new_base62, new_uuid
from taskq._json import dumps, dumps_str, loads
from taskq.backend._sql_templates import COPY_FROM_COLUMNS
from taskq.backend.clock import SystemClock
from taskq.backend.postgres import PostgresBackend
from taskq.cli import app
from taskq.settings import WorkerSettings
from taskq.testing.assertions import plain_cli_output
from taskq.testing.jobs import make_enqueue_args
from taskq.testing.pg import DEFAULT_ACTORS, seed_actors
from taskq.testing.settings import make_integration_settings_dict
from taskq.worker.deps import open_worker_deps

pytestmark = pytest.mark.integration

# ── Volume knobs ──────────────────────────────────────────────────────────
# Tunable in ONE place. SEED_JOBS is per slice; across the full stepwise run
# ~10k ACTIVE-status rows land in the hot partial indexes (dispatch /
# scheduled-wake / running-lock), which is the volume that matters for
# index-building migrations. Keep SEED_JOBS a multiple of 100: the status
# wheel below then yields the declared distribution exactly.

SEED_JOBS = 7_000
SEED_EVENTS_PER_JOB = 2
SEED_ATTEMPTS = 2_000
SEED_ARCHIVE_ROWS = 1_000

# Declared status distribution (percent). The stepwise harness's job: prove
# migrations behave against terminal rows (result/error payloads), active
# rows (locks, future schedules), and everything between.
_STATUS_MIX: tuple[tuple[str, int], ...] = (
    ("succeeded", 55),
    ("failed", 10),
    ("cancelled", 5),
    ("crashed", 3),
    ("abandoned", 2),
    ("pending", 15),
    ("scheduled", 5),
    ("running", 5),
)
_TERMINAL_ORDER: tuple[str, ...] = ("succeeded", "failed", "cancelled", "crashed", "abandoned")
_TERMINAL_STATUSES = frozenset(_TERMINAL_ORDER)
_ACTIVE_STATUSES = frozenset({"pending", "scheduled", "running"})

# Flat 100-slot wheel: ``i % 100`` indexes into it, so per-slice counts are
# EXACT whenever SEED_JOBS is a multiple of 100.
_STATUS_WHEEL: tuple[str, ...] = tuple(
    status for status, weight in _STATUS_MIX for _ in range(weight)
)
assert len(_STATUS_WHEEL) == 100

# Fixed uuid5 namespace: seed ids are pure functions of (slice, counter), so
# two generator calls with the same ``now`` produce byte-identical rows and
# the final integrity pass can re-derive any row's id without the generator.
_SEED_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "taskq/tests/test_migrations_populated")
_SPREAD_DAYS = 90


def _uuid5(name: str) -> uuid.UUID:
    return uuid.uuid5(_SEED_NAMESPACE, name)


def _payload(i: int) -> dict[str, object]:
    if i % 10 == 0:
        # ~2KB payload every 10th row: COPY, GIN jsonb indexing, and the
        # idempotency index build must handle more than toy documents.
        return {"job": i, "kind": "large", "blob": "x" * 2048}
    return {"job": i, "kind": "small"}


def _tags(slice_id: int, i: int) -> list[str]:
    return ["seed", f"slice-{slice_id}", f"bucket-{i % 7}"]


def _result(i: int) -> dict[str, object]:
    return {"ok": True, "value": i}


# Attempt outcome for a terminal job status; active jobs are given an
# earlier FAILED attempt (they were retried and are still in flight).
_ATTEMPT_OUTCOME = {
    "succeeded": "succeeded",
    "failed": "failed",
    "cancelled": "cancelled",
    "crashed": "crashed",
}


def _job_row(
    slice_id: int,
    i: int,
    *,
    status: str,
    row_id: uuid.UUID,
    idempotency_key: str | None,
    now: datetime,
    created_at: datetime,
    worker_id: uuid.UUID,
) -> dict[str, object]:
    """One canonical ``jobs`` row (every column any schema version knows).

    ``idempotency_scope`` is included unconditionally — the seeder's runtime
    column intersection drops it against pre-``01.00.03_01:pre`` schemas.
    """
    terminal = status in _TERMINAL_STATUSES
    started_at = (
        created_at + timedelta(minutes=1) if status not in ("pending", "scheduled") else None
    )
    finished_at = started_at + timedelta(minutes=5) if terminal and started_at else None
    result = _result(i) if status == "succeeded" else None

    error_class = error_message = error_traceback = None
    if status == "failed":
        error_class, error_message, error_traceback = (
            "SeedError",
            f"boom {i}",
            "simulated traceback",
        )
    elif status == "crashed":
        error_class, error_message = "WorkerCrashed", "worker died mid-flight"
    elif status == "abandoned":
        error_class, error_message = "MaxAttemptsExceeded", "attempts exhausted"

    metadata: dict[str, object] = {"seed": True}
    if terminal and i % 97 == 0:
        # Singleton metadata is legal ONLY on terminal rows (the partial
        # unique index covers active statuses); the generator never puts it
        # on active rows — pinned by test_seed_data_is_deterministic.
        metadata["singleton"] = True

    # scheduled rows are genuinely future-due; pending rows are long since due
    scheduled_at = now + timedelta(hours=i % 72) if status == "scheduled" else created_at

    return {
        "id": row_id,
        "actor": ("actor_a", "actor_b", "actor_c", "test_actor")[i % 4],
        "queue": "default",
        "identity_key": f"ident-{i % 50}" if i % 4 == 0 else None,
        "fairness_key": f"fair-{i % 10}" if i % 7 == 0 else None,
        "payload": _payload(i),
        "payload_schema_ver": 1,
        "status": status,
        "priority": i % 3,
        "attempt": 0 if status in ("pending", "scheduled") else 1,
        "max_attempts": 3,
        "retry_kind": "transient",
        "schedule_to_close": now + timedelta(days=1) if status in _ACTIVE_STATUSES else None,
        "start_to_close": timedelta(minutes=5),
        "heartbeat_timeout": timedelta(seconds=30) if status == "running" else None,
        "created_at": created_at,
        "scheduled_at": scheduled_at,
        "started_at": started_at,
        "finished_at": finished_at,
        "last_heartbeat_at": now - timedelta(seconds=5) if status == "running" else None,
        "locked_by_worker": worker_id if status == "running" else None,
        "lock_expires_at": now + timedelta(hours=1) if status == "running" else None,
        "cancel_requested_at": created_at + timedelta(minutes=2) if status == "cancelled" else None,
        "cancel_phase": 2 if status == "cancelled" else 0,
        "error_class": error_class,
        "error_message": error_message,
        "error_traceback": error_traceback,
        "progress_state": {},
        "progress_seq": 0,
        "result": result,
        "result_size_bytes": len(dumps(result)) if result is not None else None,
        "result_expires_at": finished_at + timedelta(days=7) if finished_at and result else None,
        "idempotency_scope": "",
        "idempotency_key": idempotency_key,
        "trace_id": f"{i:032x}" if i % 6 == 0 else None,
        "span_id": f"{i:016x}" if i % 6 == 0 else None,
        "metadata": metadata,
        "tags": _tags(slice_id, i),
    }


@dataclass(frozen=True, slots=True)
class _SliceSeed:
    """All rows one slice contributes, keyed by table. Pure data — no PG."""

    workers: list[dict[str, object]]
    jobs: list[dict[str, object]]
    job_events: list[dict[str, object]]
    job_attempts: list[dict[str, object]]
    jobs_archive: list[dict[str, object]]
    job_attempts_archive: list[dict[str, object]]
    queues: list[dict[str, object]]
    cron_schedules: list[dict[str, object]]
    rate_limit_buckets: list[dict[str, object]]
    reservation_slots: list[dict[str, object]]


def _generate_slice(slice_id: int, *, now: datetime) -> _SliceSeed:
    """Deterministically generate one slice of seed rows.

    Pure: same ``(slice_id, now)`` in → identical rows out. No unseeded RNG
    anywhere — ids are uuid5 over a fixed namespace, keys are counter-based,
    timestamps spread over the trailing ``_SPREAD_DAYS`` days relative to
    ``now``.
    """
    worker_id = _uuid5(f"worker:{slice_id}")
    base = now - timedelta(days=_SPREAD_DAYS)

    jobs: list[dict[str, object]] = []
    events: list[dict[str, object]] = []
    attempts: list[dict[str, object]] = []
    job_step = timedelta(seconds=_SPREAD_DAYS * 86_400 / SEED_JOBS)
    for i in range(SEED_JOBS):
        status = _STATUS_WHEEL[i % 100]
        created_at = base + job_step * i
        # Counter-based keys are globally unique ACROSS slices: the legacy
        # single-column jobs_idempotency_key_uniq index enforces global
        # uniqueness until 01.00.03_01:post drops it.
        key = f"idem-{slice_id * SEED_JOBS + i:05d}" if i % 5 == 0 else None
        job = _job_row(
            slice_id,
            i,
            status=status,
            row_id=_uuid5(f"job:{slice_id}:{i}"),
            idempotency_key=key,
            now=now,
            created_at=created_at,
            worker_id=worker_id,
        )
        jobs.append(job)

        # Kinds per the job_events contract comment: state_change (a subset
        # with detail->>'reason'='lock_expired' so 01.00.02_01's partial
        # reclaim index has real rows to index when it builds), progress,
        # cancel_request, heartbeat_miss.
        kind = ("state_change", "progress", "cancel_request", "heartbeat_miss")[i % 4]
        if kind == "state_change":
            detail: dict[str, object] = {
                "reason": "lock_expired",
                "from_state": "running",
                "to_state": "pending",
            }
        elif kind == "progress":
            detail = {"pct": i % 100}
        elif kind == "cancel_request":
            detail = {"requested_by": "seed"}
        else:
            detail = {"missed": 2}
        events.append(
            {
                "job_id": job["id"],
                "occurred_at": created_at + timedelta(minutes=1),
                "kind": kind,
                "detail": detail,
            }
        )
        events.append(
            {
                "job_id": job["id"],
                "occurred_at": created_at + timedelta(minutes=2),
                "kind": "state_change",
                "detail": {"from_state": "pending", "to_state": status},
            }
        )

        if i < SEED_ATTEMPTS:
            outcome = _ATTEMPT_OUTCOME.get(status, "failed")
            attempts.append(
                {
                    "job_id": job["id"],
                    "attempt": 1,
                    "started_at": created_at + timedelta(minutes=1),
                    "finished_at": created_at + timedelta(minutes=2),
                    "outcome": outcome,
                    "error_class": "SeedError" if outcome in ("failed", "crashed") else None,
                    "error_message": f"boom {i}" if outcome in ("failed", "crashed") else None,
                    "error_traceback": None,
                    "duration_ms": 60_000,
                    "worker_id": worker_id,
                    "metadata": {},
                }
            )

    archive: list[dict[str, object]] = []
    archive_attempts: list[dict[str, object]] = []
    archive_step = timedelta(seconds=_SPREAD_DAYS * 86_400 / SEED_ARCHIVE_ROWS)
    for i in range(SEED_ARCHIVE_ROWS):
        # jobs_archive mirrors jobs (and is ALTERed by 01.00.03_01:pre too),
        # so it is seeded through the same canonical row builder.
        status = _TERMINAL_ORDER[i % len(_TERMINAL_ORDER)]
        key = f"idem-arc-{slice_id * SEED_ARCHIVE_ROWS + i:05d}" if i % 5 == 0 else None
        created_at = base + archive_step * i
        row = _job_row(
            slice_id,
            i,
            status=status,
            row_id=_uuid5(f"arc:{slice_id}:{i}"),
            idempotency_key=key,
            now=now,
            created_at=created_at,
            worker_id=worker_id,
        )
        row["archived_at"] = now - timedelta(days=1)
        row["expire_at"] = now + timedelta(days=300)
        archive.append(row)
        archive_attempts.append(
            {
                "job_id": row["id"],
                "attempt": 1,
                "started_at": created_at + timedelta(minutes=1),
                "finished_at": created_at + timedelta(minutes=2),
                "outcome": _ATTEMPT_OUTCOME.get(status, "failed"),
                "error_class": None,
                "error_message": None,
                "error_traceback": None,
                "duration_ms": 60_000,
                "worker_id": worker_id,
                "metadata": {},
            }
        )

    workers = [
        {
            "id": worker_id,
            "hostname": f"seed-host-{slice_id}",
            "pid": 10_000 + slice_id,
            "queues": ["default"],
            "started_at": base,
            "last_seen_at": now - timedelta(minutes=1),
            "worker_label": f"seed-{slice_id}",
            "workgroup_instance": None,
            "metadata": {"seed": True},
        }
    ]
    queues = [
        {
            "name": "default" if slice_id == 0 and q == 0 else f"q_{slice_id}_{q}",
            "mode": ("strict_fifo", "round_robin")[q % 2],
            "created_at": base,
            "updated_at": now - timedelta(days=1),
            "max_concurrent": None if q == 0 else 10,
        }
        for q in range(2)
    ]
    cron = [
        {
            "id": _uuid5(f"cron:{slice_id}:{c}"),
            # DISTINCT actors: cron_schedules has UNIQUE(actor) until
            # 01.00.01_01 replaces it with UNIQUE(actor, name).
            "actor": f"cron_actor_{slice_id}_{c}",
            "cron_expr": "*/5 * * * *",
            "timezone": "UTC",
            "dst_strategy": "skip",
            "payload_factory": None,
            "enabled": True,
            "last_fired_at": None,
            "last_fire_error": None,
            "consecutive_failures": 0,
            "next_fire_at": now + timedelta(hours=1),
            "metadata": {},
            "name": f"sched_{slice_id}_{c}",
            "identity_key": None,
        }
        for c in range(2)
    ]
    buckets = [
        {
            "bucket_name": f"bucket_{slice_id}_{b}",
            "kind": "token_bucket",
            "state": {"tokens": 5.0, "capacity": 10.0},
            "updated_at": now - timedelta(minutes=5),
        }
        for b in range(2)
    ]
    # Two free + two held slots per bucket; held slots reference this
    # slice's running jobs (wheel positions 95/96).
    slots = [
        {
            "bucket_name": f"rsv_{slice_id}",
            "slot_index": s,
            "job_id": jobs[95 + s - 2]["id"] if s >= 2 else None,
            "held_by_worker_id": worker_id if s >= 2 else None,
            "acquired_at": now - timedelta(minutes=5) if s >= 2 else None,
            "lease_expires_at": now + timedelta(hours=1) if s >= 2 else None,
        }
        for s in range(4)
    ]

    return _SliceSeed(
        workers=workers,
        jobs=jobs,
        job_events=events,
        job_attempts=attempts,
        jobs_archive=archive,
        job_attempts_archive=archive_attempts,
        queues=queues,
        cron_schedules=cron,
        rate_limit_buckets=buckets,
        reservation_slots=slots,
    )


# ── Column supersets ─────────────────────────────────────────────────────
# Canonical ORDERED supersets of every column any schema version knows, in
# COPY order. For `jobs` that superset IS the library's own COPY_FROM_COLUMNS
# (single source of truth for the COPY path; pinned to the live table by
# tests/test_migrations.py::test_copy_from_columns_match_jobs_table_exactly).

_WORKERS_COLUMNS: tuple[str, ...] = (
    "id",
    "hostname",
    "pid",
    "queues",
    "started_at",
    "last_seen_at",
    "worker_label",
    "workgroup_instance",
    "metadata",
)
_QUEUES_COLUMNS: tuple[str, ...] = ("name", "mode", "created_at", "updated_at", "max_concurrent")
_CRON_COLUMNS: tuple[str, ...] = (
    "id",
    "actor",
    "cron_expr",
    "timezone",
    "dst_strategy",
    "payload_factory",
    "enabled",
    "last_fired_at",
    "last_fire_error",
    "consecutive_failures",
    "next_fire_at",
    "metadata",
    "name",
    "identity_key",
)
# `id` omitted deliberately: bigserial assigns it.
_JOB_EVENTS_COLUMNS: tuple[str, ...] = ("job_id", "occurred_at", "kind", "detail")
_JOB_ATTEMPTS_COLUMNS: tuple[str, ...] = (
    "job_id",
    "attempt",
    "started_at",
    "finished_at",
    "outcome",
    "error_class",
    "error_message",
    "error_traceback",
    "duration_ms",
    "worker_id",
    "metadata",
)
_JOBS_ARCHIVE_COLUMNS: tuple[str, ...] = (*COPY_FROM_COLUMNS, "archived_at", "expire_at")
_RATE_LIMIT_COLUMNS: tuple[str, ...] = ("bucket_name", "kind", "state", "updated_at")
_RESERVATION_COLUMNS: tuple[str, ...] = (
    "bucket_name",
    "slot_index",
    "job_id",
    "held_by_worker_id",
    "acquired_at",
    "lease_expires_at",
)

# asyncpg's binary COPY encodes jsonb from str — dict values must be
# serialized (dumps_str) before they go into a record.
_JSONB_COLUMNS: dict[str, frozenset[str]] = {
    "workers": frozenset({"metadata"}),
    "jobs": frozenset({"payload", "progress_state", "result", "metadata"}),
    "job_events": frozenset({"detail"}),
    "job_attempts": frozenset({"metadata"}),
    "jobs_archive": frozenset({"payload", "progress_state", "result", "metadata"}),
    "job_attempts_archive": frozenset({"metadata"}),
    "queues": frozenset(),
    "cron_schedules": frozenset({"metadata"}),
    "rate_limit_buckets": frozenset({"state"}),
    "reservation_slots": frozenset(),
}


async def _live_columns(conn: asyncpg.Connection, schema: str, table: str) -> set[str]:
    rows = await conn.fetch(
        """
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        """,
        schema,
        table,
    )
    return {str(r["column_name"]) for r in rows}


async def _seed_slice(
    conn: asyncpg.Connection, schema: str, *, slice_id: int, counts: dict[str, int]
) -> None:
    """Bulk-load one generated slice into the CURRENT schema shape.

    The runtime column intersection (live ``information_schema.columns`` ∩
    canonical superset) is deliberate: when a future migration adds a
    column, the seeder adapts and keeps loading; when a future migration
    adds a NOT NULL-without-default column the seeder FAILS — and that
    failure is the CI alarm, because the harness's job is precisely to
    surface migrations that cannot run against a populated database.

    ``counts`` accumulates expected per-table row totals across slices so
    the caller can assert nothing was silently lost or duplicated.
    """
    seed = _generate_slice(slice_id, now=datetime.now(UTC))
    # actor_config goes through the library's own seeder (ON CONFLICT DO
    # NOTHING makes it idempotent across slices).
    await seed_actors(conn, schema)
    counts["actor_config"] = len(DEFAULT_ACTORS)

    plan: list[tuple[str, tuple[str, ...], list[dict[str, object]]]] = [
        ("workers", _WORKERS_COLUMNS, seed.workers),
        ("jobs", COPY_FROM_COLUMNS, seed.jobs),
        ("job_events", _JOB_EVENTS_COLUMNS, seed.job_events),
        ("job_attempts", _JOB_ATTEMPTS_COLUMNS, seed.job_attempts),
        ("jobs_archive", _JOBS_ARCHIVE_COLUMNS, seed.jobs_archive),
        ("job_attempts_archive", _JOB_ATTEMPTS_COLUMNS, seed.job_attempts_archive),
        ("queues", _QUEUES_COLUMNS, seed.queues),
        ("cron_schedules", _CRON_COLUMNS, seed.cron_schedules),
        ("rate_limit_buckets", _RATE_LIMIT_COLUMNS, seed.rate_limit_buckets),
        ("reservation_slots", _RESERVATION_COLUMNS, seed.reservation_slots),
    ]
    for table, superset, rows in plan:
        live = await _live_columns(conn, schema, table)
        columns = [c for c in superset if c in live]
        jsonb = _JSONB_COLUMNS[table]
        records = [
            tuple(
                dumps_str(row[c]) if c in jsonb and row[c] is not None else row[c] for c in columns
            )
            for row in rows
        ]
        # The tr_notify_job_insert trigger fires pg_notify per COPY'd
        # pending row; with no listener Postgres discards them — harmless.
        await conn.copy_records_to_table(
            table, records=records, columns=columns, schema_name=schema
        )
        counts[table] = counts.get(table, 0) + len(records)


# ── Catalog helpers (same shapes as tests/test_migrate_no_transaction.py) ──


async def _drop_schema(conn: asyncpg.Connection, schema: str) -> None:
    await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')


async def _index_validity(conn: asyncpg.Connection, schema: str, index: str) -> bool | None:
    """``pg_index.indisvalid`` for ``index`` in ``schema``; None if absent."""
    return await conn.fetchval(
        """
        SELECT i.indisvalid
        FROM pg_class c
        JOIN pg_index i ON i.indexrelid = c.oid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1 AND c.relname = $2
        """,
        schema,
        index,
    )


async def _ledger_transactions(conn: asyncpg.Connection, schema: str) -> dict[str, bool]:
    rows = await conn.fetch(f'SELECT version, use_transaction FROM "{schema}".schema_migrations')
    return {r["version"]: r["use_transaction"] for r in rows}


async def _assert_table_counts(
    conn: asyncpg.Connection, schema: str, counts: dict[str, int]
) -> None:
    for table, expected in sorted(counts.items()):
        actual = await conn.fetchval(f'SELECT count(*) FROM "{schema}"."{table}"')
        assert actual == expected, (
            f"{table}: expected {expected} cumulative rows after seeding, found {actual}"
        )


# ── Migration-specific checks ─────────────────────────────────────────────
# Generic per-step invariants (runner order, no INVALID indexes, ledger
# use_transaction) apply to EVERY discovered key — unknown keys get ONLY
# those, so future migrations (incl. PRs #25/#27's CIC index rebuilds)
# automatically join this harness the day they land. Entries below are for
# migrations whose populated-DB effect deserves a sharper assertion.

_MigrationCheck = Callable[[asyncpg.Connection, str], Awaitable[None]]


async def _noop_check(conn: asyncpg.Connection, schema: str) -> None:
    pass


async def _check_idempotency_scope_post(conn: asyncpg.Connection, schema: str) -> None:
    """After ``01.00.03_01:post``: the legacy global-unique index is GONE
    and the composite scope-key index is VALID — with seeded idempotency
    keys present in the table while the swap happened."""
    assert await _index_validity(conn, schema, "jobs_idempotency_key_uniq") is None, (
        "01.00.03_01:post must drop the legacy single-column idempotency index"
    )
    assert await _index_validity(conn, schema, "jobs_idempotency_scope_key_uniq") is True, (
        "the composite (idempotency_scope, idempotency_key) index must be VALID"
    )


_MIGRATION_SPECIFIC_CHECKS: dict[str, _MigrationCheck] = {
    "01.00.03_01:post": _check_idempotency_scope_post,
}


# ── Tier 1: generator contract (no PG) ────────────────────────────────────


def test_seed_data_is_deterministic() -> None:
    """Pure generator contract (no PG).

    The seeder must be fully deterministic (fixed ``now`` in, identical
    rows out) so failures reproduce byte-for-byte, and its output must
    respect the invariants of the EARLIEST schema version it seeds: the
    legacy single-column ``jobs_idempotency_key_uniq`` index enforces
    GLOBAL key uniqueness until ``01.00.03_01:post`` drops it, and
    ``jobs_singleton_uniq`` forbids singleton metadata on active rows.
    """
    fixed_now = datetime(2026, 3, 1, tzinfo=UTC)

    first = _generate_slice(0, now=fixed_now)
    second = _generate_slice(0, now=fixed_now)
    assert [r["id"] for r in first.jobs] == [r["id"] for r in second.jobs]
    assert [r["idempotency_key"] for r in first.jobs] == [r["idempotency_key"] for r in second.jobs]
    assert first == second

    # Idempotency keys must be globally unique ACROSS slices — the old
    # single-column unique index spans every seeded generation until the
    # :post migration drops it.
    keys = [
        row["idempotency_key"]
        for slice_id in range(8)
        for row in _generate_slice(slice_id, now=fixed_now).jobs
        if row["idempotency_key"] is not None
    ]
    assert len(keys) > 0
    assert len(keys) == len(set(keys)), "idempotency keys must be globally unique"

    # Status counts match the declared distribution exactly.
    counts = Counter(str(row["status"]) for row in first.jobs)
    assert counts == {status: SEED_JOBS * weight // 100 for status, weight in _STATUS_MIX}

    # jobs_singleton_uniq forbids active-status rows carrying singleton
    # metadata — the seeder must never produce one.
    for row in first.jobs:
        if str(row["status"]) in _ACTIVE_STATUSES:
            metadata = row["metadata"]
            assert isinstance(metadata, dict)
            assert not metadata.get("singleton"), (
                f"active-status job {row['id']} must not carry singleton metadata"
            )


# ── Tier 2: stepwise apply onto populated data ────────────────────────────


def test_bundled_migrations_apply_stepwise_onto_populated_database(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Apply every bundled migration one step at a time onto populated data.

    Each iteration: apply exactly ONE migration → generic invariants →
    migration-specific checks → seed the next slice → assert cumulative
    counts. After the loop: data round-trips, index spot-checks, a
    functional smoke through the real backend API, and the end-user CLI
    contract. CliRunner runs in-process, so the monkeypatched env vars
    apply to the real CLI; it is synchronous, so this test drives async
    phases through asyncio.run and must itself stay sync (asyncpg
    connections are bound to the loop that created them — one asyncio.run
    per phase, like conftest's _pg_admin and tests/test_migrate_no_transaction.py).
    """
    schema = f"mig_pop_{new_base62()}".lower()

    async def _stepwise() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
            counts: dict[str, int] = {}
            for slice_id, migration in enumerate(migrate_mod.discover()):
                applied = await migrate_mod.apply_pending(conn, schema=schema, max_steps=1)
                assert [m.key for m in applied] == [migration.key], (
                    f"step {slice_id}: expected exactly {migration.key!r} to apply, got "
                    f"{[m.key for m in applied]} — runner order drifted or the "
                    "migration failed against a populated database"
                )
                assert await migrate_mod.list_invalid_indexes(conn, schema) == [], (
                    f"{migration.key} left INVALID indexes behind on populated data"
                )
                ledger = await _ledger_transactions(conn, schema)
                assert ledger[migration.key] is migration.use_transaction, (
                    f"ledger use_transaction mismatch for {migration.key}"
                )
                await _MIGRATION_SPECIFIC_CHECKS.get(migration.key, _noop_check)(conn, schema)
                await _seed_slice(conn, schema, slice_id=slice_id, counts=counts)
                await _assert_table_counts(conn, schema, counts)
        finally:
            await conn.close()

    async def _round_trips_and_index_spot_checks() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            # Payload/tags/result round-trip on deterministic sample ids:
            # i=1 (small payload, no key), i=10 (~2KB payload, keyed).
            small = await conn.fetchrow(
                f'SELECT payload, tags, result, status, idempotency_key FROM "{schema}".jobs WHERE id = $1',
                _uuid5("job:0:1"),
            )
            assert small is not None
            assert loads(small["payload"]) == _payload(1)
            assert list(small["tags"]) == _tags(0, 1)
            assert loads(small["result"]) == _result(1)
            assert small["status"] == "succeeded"
            assert small["idempotency_key"] is None

            large = await conn.fetchrow(
                f'SELECT payload, idempotency_key FROM "{schema}".jobs WHERE id = $1',
                _uuid5("job:0:10"),
            )
            assert large is not None
            assert loads(large["payload"]) == _payload(10), "the ~2KB payload must survive intact"
            assert large["idempotency_key"] == "idem-00010"

            running = await conn.fetchrow(
                f'SELECT status, locked_by_worker, lock_expires_at FROM "{schema}".jobs WHERE id = $1',
                _uuid5("job:0:95"),
            )
            assert running is not None
            assert running["status"] == "running"
            assert running["locked_by_worker"] == _uuid5("worker:0")
            assert running["lock_expires_at"] > datetime.now(UTC)

            archived = await conn.fetchrow(
                f'SELECT status, result FROM "{schema}".jobs_archive WHERE id = $1',
                _uuid5("arc:0:0"),
            )
            assert archived is not None
            assert archived["status"] == "succeeded"
            assert loads(archived["result"]) == _result(0)

            # Index spot-checks: the hot dispatch index is VALID, and the
            # lock_expired reclaim partial index (built by 01.00.02_01 over
            # seeded events) exists.
            assert await _index_validity(conn, schema, "jobs_dispatch_idx") is True
            assert await _index_validity(conn, schema, "job_events_reclaim_idx") is True
        finally:
            await conn.close()

    async def _real_api_smoke() -> None:
        """End-state functional smoke through the REAL public API.

        The real API is used ONLY here, against the final schema: the
        current enqueue SQL inserts ``idempotency_scope``, which raises
        UndefinedColumnError against pre-``01.00.03_01:pre`` schemas, and
        ``job_events`` has no public append API at all — intermediate
        schemas are therefore seeded raw (schema-shaped), never via enqueue.
        """
        settings = WorkerSettings.load_from_dict(make_integration_settings_dict(pg_dsn))
        settings.schema_name = schema
        stack = AsyncExitStack()
        deps = await stack.enter_async_context(open_worker_deps(settings))
        try:
            backend = PostgresBackend(
                deps,
                clock=SystemClock(),
                cancellation_grace_period=timedelta(
                    seconds=deps.settings.cancellation_grace_period
                ),
                cleanup_grace_period=timedelta(seconds=deps.settings.cleanup_grace_period),
            )
            key = f"smoke-{schema}"
            row = await backend.enqueue(
                make_enqueue_args(actor="test_actor", idempotency_key=key, priority=10)
            )
            assert row.status == "pending"  # type: ignore[comparison-overlap] # Why: JobStatus is Literal[...]; pyright narrows too conservatively across frozen dataclass fields (same as tests/test_dispatch_pg.py).

            duplicate = await backend.enqueue(
                make_enqueue_args(
                    actor="test_actor", idempotency_key=key, payload={"v": 2}, priority=10
                )
            )
            assert duplicate.id == row.id, "same key + same scope must dedupe"

            scoped = await backend.enqueue(
                make_enqueue_args(
                    actor="test_actor", idempotency_key=key, idempotency_scope="smoke-b"
                )
            )
            assert scoped.id != row.id, (
                "the SAME key under a SECOND scope must insert a second row — only legal "
                "after 01.00.03_01:post dropped the legacy global-unique index; the "
                "sharpest end-to-end proof the deploy sequence landed on populated data"
            )

            # priority=10 outranks every seeded row (priorities 0..2), so
            # limit=1 leases exactly our row.
            worker_id = new_uuid()
            dispatched = await backend.dispatch_batch(
                worker_id=worker_id,
                queues=["default"],
                limit=1,
                lock_lease=timedelta(seconds=30),
            )
            assert len(dispatched) == 1
            assert dispatched[0].id == row.id
            assert dispatched[0].status == "running"  # type: ignore[comparison-overlap] # Why: see enqueue assertion above.
            assert dispatched[0].locked_by_worker == worker_id
        finally:
            await stack.aclose()

    async def _cleanup() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await _drop_schema(conn, schema)
        finally:
            await conn.close()

    try:
        asyncio.run(_stepwise())
        asyncio.run(_round_trips_and_index_spot_checks())
        asyncio.run(_real_api_smoke())

        # End-user contract: re-running `migrate up` on a fully-migrated,
        # populated database is a safe no-op (same CliRunner/env pattern as
        # tests/test_migrate_no_transaction.py).
        monkeypatch.setenv("TASKQ_PG_DSN", pg_dsn)
        monkeypatch.setenv("TASKQ_SCHEMA_NAME", schema)
        result = CliRunner().invoke(app, ["migrate", "up"])
        assert result.exit_code == 0
        assert "no pending migrations" in plain_cli_output(result.output)
    finally:
        asyncio.run(_cleanup())
