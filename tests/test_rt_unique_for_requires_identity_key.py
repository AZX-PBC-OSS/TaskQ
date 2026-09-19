"""``unique_for`` without ``identity_key``: warn-not-raise, never silent.

The adjudicated contract (docs/guides/actors.md's "silent no-op"
disclosure and docs/guides/ops.md's footgun table): ``unique_for``
without ``identity_key`` is a documented no-op that logs a warn-once
event (``actor_config_unique_for_ignored``) and enqueues a fresh job -
raising instead would break the green pins and docs pages that codify
the warning contract. What was defective was SILENCE, not leniency: the
JobsClient actor-declared path warned, but the per-call seam
(SubJobEnqueuer.enqueue - the only caller-facing enqueue surface that
takes a per-call ``unique_for``) accepted the knob without a peep.

Two halves, now coherent:

* The warn pins - every caller-facing single-enqueue seam that accepts
  ``unique_for`` emits the warn-once event when ``identity_key`` is
  omitted (per-call and actor-declared, SubJobEnqueuer and JobsClient).
* The inertness proofs - the same window WITH an identity dedups while
  the identical enqueue WITHOUT one lands duplicate jobs, on the
  in-memory mirror and end-to-end against real PG: the no-op is
  observable, which is exactly what the warning points at.
"""

# ruff: noqa: S608 Why: the schema name comes from the module_pg_schema fixture
# (hashed and validated by the migration runner's _IDENT_RE) and asyncpg has no
# parameter binding for identifiers; the actor value stays $1-bound. Same
# suppression as tests/test_pinned_invariants.py and test_postgres_unique_for.py's
# f-string statements.

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import asyncpg
import pytest
from pydantic import BaseModel

from taskq.actor import actor
from taskq.backend._protocol import IdentityKey
from taskq.client import JobsClient
from taskq.client._args import build_enqueue_args
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend

if TYPE_CHECKING:
    from taskq.backend.postgres import PostgresBackend
    from taskq.worker.deps import WorkerDeps
else:
    WorkerDeps = PostgresBackend = object

_START = datetime(2025, 1, 1, tzinfo=UTC)
_WINDOW = timedelta(minutes=15)


class _Payload(BaseModel):
    value: int = 1


@actor(name="rt_uf_plain")
async def _plain_actor(payload: _Payload) -> None:
    pass


@actor(name="rt_uf_declared", unique_for=_WINDOW)
async def _declared_actor(payload: _Payload) -> None:
    pass


def _make_backend() -> InMemoryBackend:
    return InMemoryBackend(clock=FakeClock(start=_START))


def _make_enqueuer(backend: InMemoryBackend) -> SubJobEnqueuer:
    # LOOP-scope provenance is the shape ctx.jobs gives actor code: the
    # enqueue joins the consumer's transaction, which the in-memory
    # backend's simulation buffers - a green handle without a driver.
    return SubJobEnqueuer(
        loop_scope_resolved={asyncpg.Connection: object()},
        worker_pool=None,
        backend=backend,
        clock=FakeClock(start=_START),
    )


def _warning_count(out: str) -> int:
    """How many warn-once events the captured stdout carries.

    The rendered structlog line names the event twice (as the event and
    as ``kind=``), so this counts WARNING LINES, not substring
    occurrences - one per emitted warning, regardless of render format.
    """
    return sum(1 for line in out.splitlines() if "actor_config_unique_for_ignored" in line)


# ── Half 1: the warn pins (no enqueue path is silent) ──────────────────


async def test_per_call_enqueue_unique_for_without_identity_key_warns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A per-call ``unique_for`` override with ``identity_key`` omitted
    still enqueues (warn, never raise) and is no longer silent: the
    SubJobEnqueuer seam emits the same warn-once event the JobsClient
    actor-declared path emits, and the second enqueue of the same actor
    stays quiet.

    Why capsys, not caplog: taskq's structlog logger renders to stdout
    via PrintLogger unless taskq.obs.setup_logging() has bridged it into
    stdlib logging, which this unit test does not (and should not) set
    up.
    """
    enqueuer = _make_enqueuer(_make_backend())

    handle = await enqueuer.enqueue(_plain_actor, _Payload(value=1), unique_for=_WINDOW)

    assert handle.job_id is not None, "warn-not-raise: the enqueue itself is green"
    assert _warning_count(capsys.readouterr().out) == 1

    await enqueuer.enqueue(_plain_actor, _Payload(value=2), unique_for=_WINDOW)
    assert _warning_count(capsys.readouterr().out) == 0, (
        "the warning is once per actor per enqueuer, not once per enqueue"
    )


async def test_sub_enqueuer_actor_declared_unique_for_without_identity_key_warns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An actor-declared ``unique_for`` with ``identity_key`` omitted at
    the per-call seam (SubJobEnqueuer) warns exactly as the JobsClient
    path does - the same no-op, the same warning, on every seam that
    accepts the knob."""
    enqueuer = _make_enqueuer(_make_backend())

    handle = await enqueuer.enqueue(_declared_actor, _Payload(value=1))

    assert handle.job_id is not None
    assert _warning_count(capsys.readouterr().out) == 1


async def test_client_enqueue_actor_unique_for_without_identity_key_warns(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The public JobsClient.enqueue surface keeps the adjudicated
    warn-not-raise contract: both enqueues are green and land fresh jobs
    (the documented no-op), and the warn-once event fires exactly once
    for the actor across them."""
    client = JobsClient(_make_backend())

    handle1 = await client.enqueue(_declared_actor, _Payload(value=1))
    handle2 = await client.enqueue(_declared_actor, _Payload(value=2))

    assert handle1.job_id is not None and handle2.job_id is not None
    assert handle1.job_id != handle2.job_id, "the documented no-op: fresh jobs"
    assert _warning_count(capsys.readouterr().out) == 1


# ── Control: the warning is the missing pairing, not the knob ───────────


async def test_build_args_unique_for_with_identity_key_is_accepted() -> None:
    """``unique_for`` WITH ``identity_key`` crosses the boundary unchanged -
    the warning pins the missing pairing, never the knob itself."""
    args = build_enqueue_args(
        _plain_actor,
        _Payload(value=1),
        unique_for=_WINDOW,
        identity_key=IdentityKey("rt-uf-control"),
    )

    assert args.unique_for == _WINDOW
    assert args.identity_key == "rt-uf-control"


# ── Half 2: the inertness proofs (the hole observed) ─────────────────────


async def test_per_call_unique_for_without_identity_key_lands_duplicate_jobs() -> None:
    """The identical enqueue minus ``identity_key`` enforces nothing: the
    window is accepted and stored on the args, and two enqueues land two
    fresh jobs - while the same window WITH an identity dedups on the same
    backend, isolating the missing identity as the cause."""
    backend = InMemoryBackend(clock=FakeClock(_START))

    dedup_args_1 = build_enqueue_args(
        _plain_actor,
        _Payload(value=1),
        unique_for=_WINDOW,
        identity_key=IdentityKey("rt-uf-inert"),
    )
    dedup_args_2 = build_enqueue_args(
        _plain_actor,
        _Payload(value=2),
        unique_for=_WINDOW,
        identity_key=IdentityKey("rt-uf-inert"),
    )
    dedup_row_1 = await backend.enqueue(dedup_args_1)
    dedup_row_2 = await backend.enqueue(dedup_args_2)
    assert dedup_row_2.id == dedup_row_1.id, (
        "precondition: with identity_key the window dedups - the arm works"
    )

    bare_args_1 = build_enqueue_args(_plain_actor, _Payload(value=3), unique_for=_WINDOW)
    bare_args_2 = build_enqueue_args(_plain_actor, _Payload(value=4), unique_for=_WINDOW)
    assert bare_args_1.unique_for == _WINDOW and bare_args_1.identity_key is None, (
        "the hole's precondition: the knob was accepted and stored, keyed by nothing"
    )

    bare_row_1 = await backend.enqueue(bare_args_1)
    bare_row_2 = await backend.enqueue(bare_args_2)

    assert bare_row_1.id != bare_row_2.id, "no dedup happened - the window enforced nothing"
    assert await backend.get(bare_row_1.id) is not None
    assert await backend.get(bare_row_2.id) is not None


@pytest.mark.integration
async def test_pg_actor_unique_for_without_identity_key_lands_duplicate_jobs(
    clean_jobs_app: tuple[WorkerDeps, PostgresBackend],
) -> None:
    """End-to-end against real PG: the same actor and window that
    single-flights WITH an identity lands two fresh jobs WITHOUT one - the
    production gate no-ops, and the caller is handed two green handles."""
    deps, pg_backend = clean_jobs_app
    schema = deps.settings.schema_name
    client = JobsClient(pg_backend)

    identity = IdentityKey("rt-uf-pg")
    dedup_1 = await client.enqueue(_declared_actor, _Payload(value=1), identity_key=identity)
    dedup_2 = await client.enqueue(_declared_actor, _Payload(value=2), identity_key=identity)
    assert dedup_2.was_existing is True, (
        "precondition: with identity_key the single-flight arm fires on PG"
    )
    assert dedup_2.job_id == dedup_1.job_id

    bare_1 = await client.enqueue(_declared_actor, _Payload(value=3))
    bare_2 = await client.enqueue(_declared_actor, _Payload(value=4))
    assert bare_1.was_existing is False
    assert bare_2.was_existing is False
    assert bare_1.job_id != bare_2.job_id

    async with deps.worker_pool.acquire() as conn:
        count = await conn.fetchval(
            f'SELECT count(*) FROM "{schema}".jobs WHERE actor = $1 AND identity_key IS NULL',
            "rt_uf_declared",
        )
    assert count == 2, (
        f"two no-identity enqueues of one unique_for actor landed {count} rows - "
        "the window is stored on the row but enforces nothing"
    )
