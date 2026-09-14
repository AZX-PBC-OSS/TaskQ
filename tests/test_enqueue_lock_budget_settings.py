"""Enqueue advisory-lock wait budgets are operator-tunable settings.

Contract under attack (#161, composed with #151 + #139): the two bounded
advisory-lock waits on the single-enqueue path -- the max_pending
capacity lock and the unique_for single-flight lock -- are operator
knobs. Today they are the hard-coded 5 s module constants
``DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS`` / ``DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS``
in ``taskq.backend._enqueue``, reachable only as keyword defaults on
module-level functions that no production caller passes: the
``PostgresBackend`` enqueue wrappers forward nothing, so an operator
cannot widen them during an outage that slows lock holders -- while
lock exhaustion is a typed refusal (``MaxPendingLockTimeoutError`` /
``UniqueForLockTimeoutError``) whose denials consume retry budget,
converting the fixed ceiling directly into refused enqueues and
permanent job failure.

The knobs are spelled the way every existing backend knob is spelled
(the ``dispatch_oversample`` path): a ``WorkerSettings`` field under the
``TASKQ_`` env prefix, declared on the ``BackendSettings`` protocol, and
read by ``PostgresBackend`` at the enqueue use sites. This module pins:

1. Both budgets exist as ``WorkerSettings`` fields.
2. Both load from their ``TASKQ_*`` env keys.
3. Their defaults preserve today's 5 s behavior (the module constants
   become the field defaults, so wiring the knob cannot silently change
   the shipped ceiling).
4. An operator-set budget is the budget the enqueue seam actually uses:
   the typed refusal reports it and the server-side ``lock_timeout``
   GUC the contended acquire sets carries it -- driven through the REAL
   ``PostgresBackend.enqueue`` / ``.enqueue_with_conn`` with only the DB
   boundary faked (a connection modelling a holder that outlives the
   default budget, the same fake shape
   tests/test_postgres_enqueue_max_pending_lock.py drives the module
   functions with).

If the fix lands the capability under different names or a different
injection seam, update this driver -- the assertions below are the
contract, not the spelling.
"""

import dataclasses
from dataclasses import asdict
from datetime import UTC, datetime, timedelta

import asyncpg
import pytest

from taskq.backend._enqueue import (
    DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS,
    DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS,
    DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS,
)
from taskq.backend._protocol import EnqueueArgs
from taskq.backend.postgres import PostgresBackend
from taskq.exceptions import (
    IdempotencyKeyLockTimeoutError,
    MaxPendingLockTimeoutError,
    UniqueForLockTimeoutError,
)
from taskq.settings import WorkerSettings
from taskq.testing.clock import FakeClock
from taskq.testing.jobs import make_enqueue_args, make_job_row

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
_START = datetime(2025, 1, 1, tzinfo=UTC)

_MAX_PENDING_ACTOR = "budget_actor"
_CAP = 5
#: An operator budget strictly above the 5 s default, so a wiring that
#: falls back to (or cross-wires) the frozen constants cannot pass.
_OPERATOR_MAX_PENDING_BUDGET_MS = 30000.0

_UNIQUE_FOR_ACTOR = "identity_actor"
_IDENTITY = "acct-161"
_UNIQUE_FOR = timedelta(minutes=15)
#: Distinct from the max_pending budget so cross-wiring the two knobs
#: onto one parameter is caught, not masked.
_OPERATOR_UNIQUE_FOR_BUDGET_MS = 17000.0


def _load(**overrides: str) -> WorkerSettings:
    """Load WorkerSettings from a dict with a valid DSN base.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix and is
    hermetic (no dotfiles, no process env) -- the house pattern from
    tests/test_settings.py.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


# ── Settings-surface pins: the knobs exist and load ─────────────────────


def test_enqueue_lock_budgets_are_worker_settings_fields() -> None:
    """Both wait budgets are WorkerSettings fields -- the operator
    surface for the enqueue advisory-lock budgets."""
    missing = [
        name
        for name in ("max_pending_lock_timeout_ms", "unique_for_lock_timeout_ms")
        # WorkerSettings is a dotenvmodel DotEnvConfig, not a pydantic
        # BaseModel: get_fields() is its introspection seam, mapping
        # name -> (type, FieldInfo).
        if name not in WorkerSettings.get_fields()
    ]
    assert not missing, (
        f"WorkerSettings has no {missing} -- the enqueue advisory-lock "
        "wait budgets are hard-coded module constants "
        "(taskq/backend/_enqueue.py: DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS / "
        "DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS, 5 s each) that the "
        "PostgresBackend enqueue wrappers never forward, so an operator "
        "cannot widen them. Lock exhaustion is a typed refusal "
        "(MaxPendingLockTimeoutError / UniqueForLockTimeoutError) and "
        "denials consume retry budget, so the fixed 5 s ceiling converts "
        "an outage that slows lock holders into refused enqueues and "
        "permanent job failure (#161, composed with #151/#139). Plumb them "
        "the way dispatch_oversample reaches _dispatch: WorkerSettings "
        "field under the TASKQ_ prefix -> BackendSettings protocol -> "
        "the PostgresBackend enqueue use sites. "
        "grep -rn 'lock_timeout' src/taskq/settings.py finds nothing."
    )


def test_enqueue_lock_budgets_load_from_the_taskq_env_prefix() -> None:
    """Both budgets round-trip through their TASKQ_* env keys -- the env
    var an operator actually sets during an outage is the value the
    settings object carries (load_from_dict silently ignores unknown
    keys, so a missing field loads clean and the value stays absent)."""
    s = _load(
        TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS=f"{_OPERATOR_MAX_PENDING_BUDGET_MS:g}",
        TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS=f"{_OPERATOR_UNIQUE_FOR_BUDGET_MS:g}",
    )
    expectations = (
        (
            "max_pending_lock_timeout_ms",
            "TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS",
            _OPERATOR_MAX_PENDING_BUDGET_MS,
        ),
        (
            "unique_for_lock_timeout_ms",
            "TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS",
            _OPERATOR_UNIQUE_FOR_BUDGET_MS,
        ),
    )
    for name, env_var, expected in expectations:
        loaded = getattr(s, name, None)
        assert loaded == expected, (
            f"{env_var}={expected:g} did not reach WorkerSettings.{name} "
            f"(got {loaded!r}): the field does not exist, so the env var "
            "an operator sets to widen a lock budget during an outage is "
            "silently ignored -- no knob at all (#161). "
            "grep -rn 'lock_timeout' src/taskq/settings.py finds nothing."
        )


def test_enqueue_lock_budget_settings_default_to_the_current_constants() -> None:
    """The knobs' defaults are today's constants (5 s each), so wiring
    the settings surface cannot silently change the shipped ceiling --
    and the constants stop being the live source once the fields exist."""
    defaults = (
        ("max_pending_lock_timeout_ms", DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS),
        ("unique_for_lock_timeout_ms", DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS),
    )
    for name, constant in defaults:
        entry = WorkerSettings.get_fields().get(name)
        assert entry is not None, (
            f"WorkerSettings has no {name!r} field -- the "
            f"{constant:g} ms bounded-wait budget is a hard-coded constant "
            "(taskq/backend/_enqueue.py) with no operator surface (#161)."
        )
        _type, info = entry
        assert info.default == constant, (
            f"WorkerSettings.{name} defaults to {info.default!r}, not the "
            f"shipped {constant:g} ms constant -- wiring the knob must "
            "preserve today's ceiling or every deployment that does not "
            "set the env var changes behavior on upgrade."
        )


# ── Seam pins: an operator budget is the budget the lock wait uses ──────


class _ContendedFakeConn:
    """ConnLike stand-in modelling a holder that outlives any default
    budget: the fast-path try-lock returns False (someone holds the lock
    now) and the contended tier's blocking acquire raises the raw 55P03
    (the server-side ``lock_timeout`` fired at the budget the acquire
    set). The set_config traffic is recorded so the tests can observe
    which budget actually reached the server-side wait bound -- the same
    fake shape tests/test_postgres_enqueue_max_pending_lock.py drives
    the module functions with; here it sits behind the REAL backend
    wrappers, which is the plumbing under attack."""

    def __init__(self, *, actor: str, identity_key: str | None = None) -> None:
        self._in_tx = False
        self.try_lock_calls = 0
        self.blocking_lock_calls = 0
        self.savepoint_opens = 0
        self.set_config_values: list[str | None] = []
        self._record = asdict(
            make_job_row(status="pending", actor=actor, identity_key=identity_key)
        )

    def is_in_transaction(self) -> bool:
        return self._in_tx

    def transaction(self) -> "_ContendedFakeConn._NullSavepoint":
        self.savepoint_opens += 1
        return self._NullSavepoint(self)

    class _NullSavepoint:
        """async with conn.transaction() stand-in: tracks the open
        transaction flag, suppresses nothing."""

        def __init__(self, conn: "_ContendedFakeConn") -> None:
            self._conn = conn

        async def __aenter__(self) -> "_ContendedFakeConn._NullSavepoint":
            self._conn._in_tx = True
            return self

        async def __aexit__(self, *exc: object) -> bool:
            self._conn._in_tx = False
            return False

    async def fetchval(self, sql: str, *params: object) -> object:
        if "pg_try_advisory_xact_lock" in sql:
            self.try_lock_calls += 1
            return False
        if "current_setting" in sql:
            # The GUC save: a session that never set one (PG's default
            # lock_timeout is 0 = off).
            return "0"
        return 0

    async def execute(self, sql: str, *params: object) -> str:
        if "set_config" in sql:
            self.set_config_values.append(str(params[0]) if params else None)
            return "OK"
        if "pg_advisory_xact_lock" in sql and "pg_try" not in sql:
            self.blocking_lock_calls += 1
            raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
        return "OK"

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object]:
        return self._record


class _FakePoolAcquire:
    """``pool.acquire()`` context-manager stand-in yielding one conn."""

    def __init__(self, conn: _ContendedFakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _ContendedFakeConn:
        return self._conn

    async def __aexit__(self, *exc: object) -> bool:
        return False


class _FakePool:
    """asyncpg.Pool stand-in: acquire() hands out the one fake conn."""

    def __init__(self, conn: _ContendedFakeConn) -> None:
        self._conn = conn

    def acquire(self) -> _FakePoolAcquire:
        return _FakePoolAcquire(self._conn)


class _BackendDepsStub:
    """BackendDeps stand-in carrying a REAL loaded WorkerSettings (the
    settings object a correct plumbing reads, whatever the injection
    spelling) and the fake pool -- the DB boundary is the only thing
    faked."""

    def __init__(self, settings: WorkerSettings, pool: _FakePool) -> None:
        self.settings = settings
        self.worker_pool = pool


def _capped_args() -> EnqueueArgs:
    return dataclasses.replace(make_enqueue_args(actor=_MAX_PENDING_ACTOR), max_pending=_CAP)


def _unique_for_args() -> EnqueueArgs:
    return dataclasses.replace(
        make_enqueue_args(actor=_UNIQUE_FOR_ACTOR, identity_key=_IDENTITY),
        unique_for=_UNIQUE_FOR,
    )


async def test_operator_max_pending_lock_budget_reaches_the_enqueue_seam() -> None:
    """A capped-actor enqueue whose lock is contended refuses with the
    OPERATOR's budget, not the frozen 5 s constant -- driven through the
    real PostgresBackend.enqueue (pool path) so the assertion covers the
    whole settings-to-lock-wait chain, however the fix spells the
    injection."""
    settings = _load(TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS=f"{_OPERATOR_MAX_PENDING_BUDGET_MS:g}")
    conn = _ContendedFakeConn(actor=_MAX_PENDING_ACTOR)
    backend = PostgresBackend(
        _BackendDepsStub(settings, _FakePool(conn)),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps stand-in carrying a real WorkerSettings and the fake pool
        FakeClock(_START),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=10),
    )
    with pytest.raises(MaxPendingLockTimeoutError) as exc_info:
        await backend.enqueue(_capped_args())
    assert exc_info.value.timeout_ms == _OPERATOR_MAX_PENDING_BUDGET_MS, (
        f"the operator set TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS="
        f"{_OPERATOR_MAX_PENDING_BUDGET_MS:g} but the refusal reports "
        f"{exc_info.value.timeout_ms:g} ms (the frozen default "
        f"{DEFAULT_MAX_PENDING_LOCK_TIMEOUT_MS:g}) -- the settings value "
        "stops before the enqueue seam: PostgresBackend.enqueue calls "
        "_enqueue without any budget, so the module constant is the only "
        "source (#161). Plumb it the way dispatch_oversample reaches "
        "_dispatch: the backend reads self._deps.settings at the use site."
    )
    # The server-side wait bound the contended acquire actually set --
    # the budget real Postgres would have enforced, not just the number
    # in the error.
    assert conn.set_config_values == [f"{round(_OPERATOR_MAX_PENDING_BUDGET_MS)}ms"], (
        f"the server-side lock_timeout GUC was set to "
        f"{conn.set_config_values!r}, not the operator's "
        f"{_OPERATOR_MAX_PENDING_BUDGET_MS:g} ms budget -- the wait that "
        "real Postgres enforces is still the frozen default."
    )


async def test_operator_unique_for_lock_budget_reaches_the_enqueue_seam() -> None:
    """A unique_for single-flight enqueue whose lock is contended
    refuses with the OPERATOR's budget, not the frozen 5 s constant --
    driven through the real PostgresBackend.enqueue_with_conn (the
    caller-connection path) so both backend entry points are pinned."""
    settings = _load(TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS=f"{_OPERATOR_UNIQUE_FOR_BUDGET_MS:g}")
    conn = _ContendedFakeConn(actor=_UNIQUE_FOR_ACTOR, identity_key=_IDENTITY)
    backend = PostgresBackend(
        _BackendDepsStub(settings, _FakePool(conn)),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps stand-in carrying a real WorkerSettings and the fake pool
        FakeClock(_START),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=10),
    )
    with pytest.raises(UniqueForLockTimeoutError) as exc_info:
        await backend.enqueue_with_conn(
            conn,  # type: ignore[arg-type]  # Why: duck-typed ConnLike stand-in modelling a contended lock
            _unique_for_args(),
        )
    assert exc_info.value.timeout_ms == _OPERATOR_UNIQUE_FOR_BUDGET_MS, (
        f"the operator set TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS="
        f"{_OPERATOR_UNIQUE_FOR_BUDGET_MS:g} but the refusal reports "
        f"{exc_info.value.timeout_ms:g} ms (the frozen default "
        f"{DEFAULT_UNIQUE_FOR_LOCK_TIMEOUT_MS:g}) -- the settings value "
        "stops before the enqueue seam: PostgresBackend.enqueue_with_conn "
        "calls _enqueue_with_conn without any budget, so the module "
        "constant is the only source (#161). Plumb it the way "
        "dispatch_oversample reaches _dispatch: the backend reads "
        "self._deps.settings at the use site."
    )
    assert conn.set_config_values == [f"{round(_OPERATOR_UNIQUE_FOR_BUDGET_MS)}ms"], (
        f"the server-side lock_timeout GUC was set to "
        f"{conn.set_config_values!r}, not the operator's "
        f"{_OPERATOR_UNIQUE_FOR_BUDGET_MS:g} ms budget -- the wait that "
        "real Postgres enforces is still the frozen default."
    )


# ── The idempotency sibling's seam (added with the class fix: the third
# bounded enqueue wait, plumbed with the two above but unpinned by the
# original attack) ──────────────────────────────────────────────────────


#: An operator budget distinct from both sibling budgets, so cross-wiring
#: any pair of the three knobs onto one parameter is caught, not masked.
_OPERATOR_IDEMPOTENCY_BUDGET_MS = 23000.0

_IDEMPOTENCY_ACTOR = "idem_budget_actor"
_IDEMPOTENCY_KEY = "idem-161"


class _IdempotencyContendedFakeConn(_ContendedFakeConn):
    """The idempotency arm's contended stand-in: the same
    holder-outlives-any-budget shape, but the contention surfaces at the
    token INSERT itself -- the server-side ``lock_timeout`` the bounded
    speculative wait set fires on the uncommitted same-pair row (55P03),
    the arm's documented exhaustion path."""

    async def fetchrow(self, sql: str, *params: object) -> dict[str, object]:
        if "INSERT" in sql:
            raise asyncpg.LockNotAvailableError("simulated server lock_timeout")
        return await super().fetchrow(sql, *params)


def _idempotency_args() -> EnqueueArgs:
    return make_enqueue_args(actor=_IDEMPOTENCY_ACTOR, idempotency_key=_IDEMPOTENCY_KEY)


async def test_operator_idempotency_lock_budget_reaches_the_enqueue_seam() -> None:
    """An idempotency-keyed enqueue whose speculative-token wait is
    contended refuses with the OPERATOR's budget, not the frozen 5 s
    constant -- the third bounded enqueue wait (the token INSERT's
    speculative-lock conflict), driven through the real
    PostgresBackend.enqueue_with_conn so its plumbing seam is pinned the
    same way the other two budgets' are. The attack above pinned the
    max_pending and unique_for knobs; this seam was added with the class
    fix so a regression in the third knob's plumbing cannot land
    silently."""
    settings = _load(TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS=f"{_OPERATOR_IDEMPOTENCY_BUDGET_MS:g}")
    conn = _IdempotencyContendedFakeConn(actor=_IDEMPOTENCY_ACTOR)
    backend = PostgresBackend(
        _BackendDepsStub(settings, _FakePool(conn)),  # type: ignore[arg-type]  # Why: duck-typed BackendDeps stand-in carrying a real WorkerSettings and the fake pool
        FakeClock(_START),
        cancellation_grace_period=timedelta(seconds=30),
        cleanup_grace_period=timedelta(seconds=10),
    )
    with pytest.raises(IdempotencyKeyLockTimeoutError) as exc_info:
        await backend.enqueue_with_conn(
            conn,  # type: ignore[arg-type]  # Why: ConnLike stand-in modelling the contended token INSERT
            _idempotency_args(),
        )
    assert exc_info.value.timeout_ms == _OPERATOR_IDEMPOTENCY_BUDGET_MS, (
        f"the operator set TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS="
        f"{_OPERATOR_IDEMPOTENCY_BUDGET_MS:g} but the refusal reports "
        f"{exc_info.value.timeout_ms:g} ms (the frozen default "
        f"{DEFAULT_IDEMPOTENCY_LOCK_TIMEOUT_MS:g}) -- the settings value "
        "stops before the enqueue seam for the third bounded wait: the "
        "token INSERT's speculative-lock budget, the idempotency sibling "
        "of the two knobs above, must be plumbed with them or an operator "
        "widening it during an outage is silently ignored (#161)."
    )
    assert conn.set_config_values == [f"{round(_OPERATOR_IDEMPOTENCY_BUDGET_MS)}ms"], (
        f"the server-side lock_timeout GUC was set to "
        f"{conn.set_config_values!r}, not the operator's "
        f"{_OPERATOR_IDEMPOTENCY_BUDGET_MS:g} ms budget -- the wait that "
        "real Postgres enforces on the speculative token INSERT is still "
        "the frozen default."
    )
