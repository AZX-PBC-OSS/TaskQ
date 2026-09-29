"""`taskq doctor`: a read-only capacity and configuration health report.

TaskQ deliberately refuses to fail boot on anything short of structural
stored-config drift - a worker that can do work must start. The cost of
that choice is that a whole family of misconfigurations produces no
error anywhere: an actor with no stored ``actor_config`` row never
dispatches, a ``queues`` row left behind by a queue move caps an actor
nobody thinks is capped, a stored ``max_concurrent=0`` drains an actor
that looks configured. `doctor` is the surface that pays that cost back:
one command an operator runs against a live deployment to see every one
of those conditions at once, named and explained.

Two properties make it usable, and both are pinned here. It is read-only
- an operator must be able to run it against production during an
incident without wondering whether it will write anything. And it never
exits non-zero on a warning: a diagnostic that fails the shell trains
people to stop running it, and `doctor` reports exactly the conditions
TaskQ has decided are workable. The non-zero-on-drift gate is a
different command (`actor-config diff`), which exists to be a CI gate.

Unit tier: the database reads are faked at the ``taskq.cli`` boundary,
following ``tests/test_cli_actor_config_diff_exit_code.py``.
"""

import os
import re
from collections.abc import Mapping, Sequence
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.actor import ActorRef, actor
from taskq.actor_config_ops import ActorConfigRow
from taskq.cli import app
from taskq.testing.assertions import plain_cli_output
from taskq.timescale import StorageMode
from taskq.worker.queue_ops import QueueRow

runner = CliRunner()


class _Payload(BaseModel):
    value: int


@actor(name="doctor_alpha", queue="default")
async def _doctor_alpha(payload: _Payload) -> None: ...


@actor(name="doctor_beta", queue="batch")
async def _doctor_beta(payload: _Payload) -> None: ...


_REGISTRY: Mapping[str, ActorRef[Any, Any]] = {
    "doctor_alpha": _doctor_alpha,
    "doctor_beta": _doctor_beta,
}
_REGISTRY_PATH = "tests.test_cli_doctor:_REGISTRY"

# Writing SQL verbs: any of these appearing as a WORD in a statement the
# command issued means `doctor` is no longer read-only.  Word-boundary
# matched: a read selecting `updated_at` is not an UPDATE.
_WRITE_VERBS = re.compile(r"\b(insert|update|delete|truncate|drop|alter|create)\b", re.IGNORECASE)


def _row(
    actor_name: str,
    *,
    queue: str,
    max_concurrent: int | None = None,
    max_pending: int | None = None,
) -> ActorConfigRow:
    return ActorConfigRow(
        actor=actor_name,
        max_concurrent=max_concurrent,
        max_pending=max_pending,
        queue=queue,
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )


def _patch_db(
    monkeypatch: pytest.MonkeyPatch,
    *,
    actor_rows: list[ActorConfigRow],
    queue_rows: list[QueueRow],
    stranded_rows: list[dict[str, Any]] | None = None,
    worker_rows: list[dict[str, Any]] | None = None,
    storage_mode: StorageMode | None = StorageMode.VANILLA,
    downgraded_policies: Sequence[str] = (),
    imbalance_rows: list[dict[str, Any]] | None = None,
    wait_rows: list[dict[str, Any]] | None = None,
    overprovisioning_rows: list[dict[str, Any]] | None = None,
    drain_rows: list[dict[str, Any]] | None = None,
    cron_rows: list[dict[str, Any]] | None = None,
) -> list[str]:
    """Fake the doctor's reads at the ``taskq.cli`` boundary.

    Returns the list every statement the command executes is recorded
    into, so the read-only property can be asserted rather than assumed.

    ``stranded_rows`` feeds the pending/scheduled jobs scan (the one read
    that reaches the raw connection rather than a patched helper): rows in
    the per-actor shape ``_list_stranded_pending_jobs`` returns.  The scan
    is identified by its ``.jobs`` table reference - the insights reads
    ride the ``taskq.insights`` fetchers, faked as functions below like
    ``list_actor_configs`` is.

    The ``*_rows`` insight parameters feed the four operational-insight
    families (the shapes ``fetch_queue_imbalance``, ``fetch_wait_distribution``,
    ``fetch_overprovisioning``, ``fetch_drain_estimates`` and
    ``fetch_cron_ledger`` return); empty by default, which is every
    pre-existing test's healthy fleet.

    ``storage_mode`` fakes the detected storage mode
    (``taskq.cli.detect_storage_mode``, patched here) - the rendering
    pins below inject each mode; the mode's DETECTION against real
    servers is pinned in ``test_storage_mode_detection.py`` and the
    three-mode lifecycle module.

    ``downgraded_policies`` fakes the license-downgrade probe
    (``taskq.cli.probe_registered_policy_jobs``, patched here): the
    ``"proc:table"`` strings of the TimescaleDB policy jobs still
    registered on a server whose license was downgraded to apache after
    an earlier timescale-license deployment - the downgrade-drift arm's
    input. Empty (the default) on every healthy deployment.
    """
    executed: list[str] = []
    stranded = [] if stranded_rows is None else stranded_rows
    worker_rows = [] if worker_rows is None else worker_rows
    imbalance = [] if imbalance_rows is None else imbalance_rows
    wait = [] if wait_rows is None else wait_rows
    overprovisioning = [] if overprovisioning_rows is None else overprovisioning_rows
    drain = [] if drain_rows is None else drain_rows
    cron = [] if cron_rows is None else cron_rows

    class _FakeConn:
        async def execute(self, query: str, *args: Any) -> str:
            executed.append(query)
            return "OK"

        async def fetch(self, query: str, *args: Any) -> list[Any]:
            executed.append(query)
            if ".jobs " in query:
                return list(stranded)
            if ".workers" in query:
                return list(worker_rows)
            return []

        async def fetchval(self, query: str, *args: Any) -> Any:
            executed.append(query)
            return 0

        async def fetchrow(self, query: str, *args: Any) -> Any:
            executed.append(query)
            return None

        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_detect_storage_mode(conn: Any) -> StorageMode | None:
        executed.append("fake: detect_storage_mode")
        return storage_mode

    async def fake_probe_registered_policy_jobs(conn: Any, schema: str) -> tuple[str, ...]:
        executed.append("fake: probe_registered_policy_jobs")
        return tuple(downgraded_policies)

    async def fake_list_actor_configs(conn: Any, **kwargs: Any) -> list[ActorConfigRow]:
        return actor_rows

    async def fake_list_queues(conn: Any, **kwargs: Any) -> list[QueueRow]:
        return queue_rows

    async def fake_fetch_queue_imbalance(conn: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(imbalance)

    async def fake_fetch_wait_distribution(conn: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(wait)

    async def fake_fetch_overprovisioning(conn: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(overprovisioning)

    async def fake_fetch_drain_estimates(conn: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(drain)

    async def fake_fetch_cron_ledger(conn: Any, **kwargs: Any) -> list[dict[str, Any]]:
        return list(cron)

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.list_actor_configs", fake_list_actor_configs)
    monkeypatch.setattr("taskq.cli.list_queues", fake_list_queues)
    monkeypatch.setattr("taskq.cli.fetch_queue_imbalance", fake_fetch_queue_imbalance)
    monkeypatch.setattr("taskq.cli.fetch_wait_distribution", fake_fetch_wait_distribution)
    monkeypatch.setattr("taskq.cli.fetch_overprovisioning", fake_fetch_overprovisioning)
    monkeypatch.setattr("taskq.cli.fetch_drain_estimates", fake_fetch_drain_estimates)
    monkeypatch.setattr("taskq.cli.fetch_cron_ledger", fake_fetch_cron_ledger)

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.list_actor_configs", fake_list_actor_configs)
    monkeypatch.setattr("taskq.cli.list_queues", fake_list_queues)
    monkeypatch.setattr("taskq.cli.detect_storage_mode", fake_detect_storage_mode)
    monkeypatch.setattr("taskq.cli.probe_registered_policy_jobs", fake_probe_registered_policy_jobs)
    return executed


def _invoke(*extra: str) -> Any:
    return runner.invoke(app, ["doctor", "--actors", _REGISTRY_PATH, *extra])


def test_doctor_stranded_scan_applies_the_worker_liveness_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Doctor's stranded-jobs scan must filter worker rows by the admin
    liveness window, the same arm the leader sweep applies (a worker row
    whose heartbeat went stale is not serving the queue until the
    stale-worker sweep removes it). Without the filter, a ghost worker row
    hides an unserved queue from the operator mid-incident - the false-green
    the sweep's own comment forbids. Pinned on the issued SQL and its bound
    parameter so the arm cannot silently drop out again; the end-to-end
    walkthrough (tests/test_ops_flow_walkthroughs.py) proves the behavior
    against a real stale row."""
    executed = _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
    )

    result = _invoke()

    assert result.exit_code == 0
    stranded_sql = [q for q in executed if ".jobs " in q]
    assert len(stranded_sql) == 1, f"expected exactly one stranded scan, saw {len(stranded_sql)}"
    assert "last_seen_at > statement_timestamp()" in stranded_sql[0], (
        "the stranded scan lost the worker-liveness arm - a stale worker row "
        "will again hide an unserved queue from doctor"
    )
    assert "make_interval" in stranded_sql[0]


def test_doctor_reports_actor_with_no_stored_config_row_as_never_dispatching(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The dispatch capacity gate joins ``actor_config``, so a registered
    actor with no row is not merely uncapped - it is never selected at
    all. Nothing fails anywhere; the jobs accumulate pending. This
    is the condition `doctor` most exists to surface, so the report must
    say what actually happens, not just note the row's absence."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
    )

    result = _invoke()

    assert "doctor_beta" in plain_cli_output(result.output)
    assert "never dispatches" in plain_cli_output(result.output).lower()


def test_doctor_reports_queue_cap_staleness(monkeypatch: pytest.MonkeyPatch) -> None:
    """A ``queues`` row for a queue no actor is assigned to is a leftover - a
    queue move retires the assignment but the row's cap survives, and the
    next actor moved onto that queue silently inherits a cap nobody chose.
    The stale row is inert until it is not, which is exactly why it
    belongs in a health report rather than in an error."""
    _patch_db(
        monkeypatch,
        actor_rows=[
            _row("doctor_alpha", queue="default"),
            _row("doctor_beta", queue="batch"),
        ],
        queue_rows=[
            QueueRow(name="default", mode="strict_fifo", max_concurrent=None),
            QueueRow(name="batch", mode="strict_fifo", max_concurrent=4),
            QueueRow(name="retired_tier", mode="round_robin", max_concurrent=2),
        ],
    )

    result = _invoke()

    assert "retired_tier" in plain_cli_output(result.output)
    assert "stale" in plain_cli_output(result.output).lower()


def test_doctor_labels_drain_mode_explicitly(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stored ``max_concurrent=0`` is a deliberate drain, and an actor in
    drain mode is indistinguishable from a broken one by its symptoms:
    jobs enqueue and never run. The label is what separates "someone did
    this on purpose" from an incident."""
    _patch_db(
        monkeypatch,
        actor_rows=[
            _row("doctor_alpha", queue="default", max_concurrent=0),
            _row("doctor_beta", queue="batch", max_concurrent=4),
        ],
        queue_rows=[],
    )

    result = _invoke()

    assert "drain" in plain_cli_output(result.output).lower(), (
        "a zero stored cap must be labelled drain mode, not printed as a bare 0"
    )


def test_doctor_labels_stored_null_capacity_as_uncapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``NULL`` means "no actor-level cap" - a real configuration, not
    missing data. Printed as a blank it reads as a partially written row
    and sends the operator looking for a problem that is not there."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default", max_concurrent=None)],
        queue_rows=[],
    )

    result = _invoke()

    assert "uncapped" in plain_cli_output(result.output).lower()


def test_doctor_reports_incoherent_max_pending_below_max_concurrent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``max_pending`` below ``max_concurrent`` cannot ever be satisfied: the
    actor is allowed fewer queued jobs than it is allowed to run at once,
    so the cap it was given is unreachable. Neither value is invalid on
    its own, which is why only a combination check can catch it."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default", max_concurrent=10, max_pending=2)],
        queue_rows=[],
    )

    result = _invoke()

    assert "doctor_alpha" in plain_cli_output(result.output)
    assert "max_pending" in plain_cli_output(result.output)


def test_doctor_reports_queue_cap_below_actor_cap_as_incoherent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An actor cap above its queue's cap can never be reached - the queue
    binds first. It is the "I raised the cap and nothing happened" report
    in its stored form, and only visible by reading two tables together."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default", max_concurrent=20)],
        queue_rows=[QueueRow(name="default", mode="strict_fifo", max_concurrent=2)],
    )

    result = _invoke()

    assert "doctor_alpha" in plain_cli_output(result.output)
    assert "default" in plain_cli_output(result.output)


def test_doctor_never_exits_non_zero_on_warnings(monkeypatch: pytest.MonkeyPatch) -> None:
    """A diagnostic that fails the shell on findings gets wrapped in
    ``|| true`` and then ignored. Every condition `doctor` reports is one
    TaskQ has decided a worker should keep running through, so reporting
    them must not be an error. Gating on drift is `actor-config diff`'s
    job, and keeping the two separate is what lets each be trusted."""
    _patch_db(
        monkeypatch,
        actor_rows=[
            # Every warnable condition at once: drain mode, an unreachable
            # actor cap under a smaller queue cap, and (via doctor_beta
            # having no row) the never-dispatches case.
            _row("doctor_alpha", queue="default", max_concurrent=0, max_pending=1),
        ],
        queue_rows=[QueueRow(name="retired_tier", mode="strict_fifo", max_concurrent=1)],
    )

    result = _invoke()

    assert result.exit_code == 0, (
        f"doctor must exit 0 while reporting warnings; got {result.exit_code}: {result.output}"
    )


def test_doctor_is_read_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Operators run `doctor` mid-incident against production. It must not be
    a command anyone has to reason about before running: no statement it
    issues may write. A reporting tool that repairs what it finds also
    destroys the evidence of what went wrong."""
    executed = _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default", max_concurrent=0)],
        queue_rows=[QueueRow(name="retired_tier", mode="strict_fifo", max_concurrent=1)],
    )

    result = _invoke()

    assert result.exit_code == 0, (
        "the read-only property is only meaningful once the command runs; "
        f"exit_code={result.exit_code} output={result.output!r}"
    )
    offenders = [statement for statement in executed if _WRITE_VERBS.search(statement)]
    assert offenders == [], f"doctor issued writing statements: {offenders}"


def test_doctor_on_a_healthy_deployment_exits_zero_and_reports_no_findings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: with every registered actor seeded and every queue row
    matching a live assignment there is nothing to report. Without this,
    a command that unconditionally printed every warning string would
    pass every test above."""
    _patch_db(
        monkeypatch,
        actor_rows=[
            _row("doctor_alpha", queue="default", max_concurrent=4, max_pending=100),
            _row("doctor_beta", queue="batch", max_concurrent=2, max_pending=100),
        ],
        queue_rows=[
            QueueRow(name="default", mode="strict_fifo", max_concurrent=None),
            QueueRow(name="batch", mode="strict_fifo", max_concurrent=None),
        ],
    )

    result = _invoke()

    assert result.exit_code == 0
    lowered = plain_cli_output(result.output).lower()
    assert "never dispatches" not in lowered
    assert "stale" not in lowered
    assert "drain" not in lowered


def test_doctor_does_not_gate_boot(monkeypatch: pytest.MonkeyPatch) -> None:
    """`doctor` is a separate command, not a step the worker runs. Pinning
    that the worker entry point does not call it keeps the diagnostic
    from quietly becoming a boot dependency - which would hand it the
    power to refuse a worker that can do work."""
    import ast
    from pathlib import Path

    import taskq.worker._bootstrap as bootstrap_mod

    source = Path(bootstrap_mod.__file__).read_text()
    tree = ast.parse(source)

    called = {
        node.func.id if isinstance(node.func, ast.Name) else node.func.attr
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name | ast.Attribute)
    }
    assert not any("doctor" in name for name in called), (
        "worker bootstrap must not invoke doctor - a diagnostic must never gate boot"
    )


def test_doctor_reports_pending_jobs_whose_actor_has_no_registry_or_config_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`doctor`'s own stated purpose is to surface "an actor with no stored
    ``actor_config`` row [that] never dispatches" (module docstring, and
    the previous test's assertion string). But `_doctor_findings` only
    computes ``set(registry) - set(stored_by_actor)`` - actors the CURRENT
    process still declares. It never reads the ``jobs`` table, so a job
    already sitting ``pending``/``scheduled`` for an actor name that is in
    NEITHER the registry NOR ``actor_config`` - the shape left behind when
    an actor is renamed or removed from the codebase but old producers or
    old rows still reference the retired name - is invisible to `doctor`
    even though it can never dispatch for exactly the reason `doctor`
    exists to name.

    Confirmed live against a real worker+Postgres: a job was inserted with
    ``actor='ghost_actor_not_registered'`` (no registry entry, no
    actor_config row). `taskq doctor --actors myapp.actors:registry`
    printed "no findings - every registered actor has a stored row and
    every queue row backs a live assignment." The only place this
    surfaced was a leader-only sweep log line
    (``stranded-jobs-no-actor-config``, ``taskq/worker/_leader_sweeps.py``)
    that does not fire for 60 seconds and only when a worker happens to be
    elected leader - not something `doctor`, a read-only, on-demand,
    run-anytime command, should depend on.

    Resolving an unknown worker module is NOT silent in the enqueue-time
    check a Postgres queue can make: the job is claimed and fails loudly
    with a named error
    rather than sitting unclaimed forever with nothing to say why. TaskQ's
    dispatch SQL instead joins ``actor_config``, so a job like this is
    never even a dispatch candidate - no attempt, no error, nothing. If
    TaskQ keeps the "never a candidate" dispatch design (its documented,
    deliberate tradeoff - troubleshooting.md, "Stranded jobs: ... The
    detector only warns - it does not delete or reassign."), the burden
    shifts entirely onto `doctor` and the stranded-jobs sweep to be the
    loud surface instead of a dispatch-time error - and
    `doctor` is the one of those two an operator can run on demand,
    read-only, mid-incident, without waiting up to 60s for a leader tick.

    This test pins the behaviour `doctor` should have: scan pending/
    scheduled ``jobs`` rows for actor names with no stored ``actor_config``
    row (the same condition the stranded-jobs sweep already computes,
    see ``_stranded_jobs_loop``'s ``no_actor_config`` shape in
    ``taskq/worker/_leader_sweeps.py``) and report them the same way it
    reports a registered actor with no row today.
    """
    _patch_db(
        monkeypatch,
        actor_rows=[
            _row("doctor_alpha", queue="default"),
            _row("doctor_beta", queue="batch"),
        ],
        queue_rows=[],
        stranded_rows=[
            {
                "actor": "ghost_actor_not_registered",
                "no_actor_config_cnt": 1,
                "unserved_queue_cnt": 0,
                "unserved_queues": [],
            }
        ],
    )

    # The jobs scan is fed through `_patch_db`'s raw-fetch seam (the one
    # read no patched helper covers); the assertion below is on the CLI's
    # *printed report*, not on the query shape, so it stays valid across
    # implementations that read jobs via fetch, a dedicated helper, or
    # otherwise.

    result = _invoke()

    output = plain_cli_output(result.output).lower()
    assert "ghost_actor_not_registered" not in output or "no findings" not in output, (
        "if doctor is ever fed the stranded actor name it must not still "
        "print 'no findings' - that combination means the report and the "
        "reality it should describe have diverged"
    )
    # The behaviour this test pins: doctor must name an orphaned pending
    # job's actor even though that actor is in neither the registry nor
    # actor_config - the jobs-side scan is what makes the name knowable,
    # where the registry walk (set(registry) - set(stored_by_actor)) cannot
    # surface a name the current process no longer declares.
    assert "ghost_actor_not_registered" in output, (
        "doctor did not report a pending job for an actor absent from both "
        "the registry and actor_config - this is the exact 'never "
        "dispatches, no error anywhere' condition doctor's own docstring "
        "says it exists to surface. The scan must cover pending/scheduled "
        "jobs rows with no stored actor_config row, the same condition the "
        "leader's stranded-jobs sweep computes (see _stranded_jobs_loop's "
        "'stranded-jobs-no-actor-config' event in "
        "taskq/worker/_leader_sweeps.py)."
    )


# ── Attributed event-loop stalls (the workers metadata tally) ─────────


def test_doctor_reports_attributed_stalls_from_worker_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A worker's stall tally rides the workers row metadata the heartbeat
    already rewrites, so doctor reads it without any new write surface:
    a non-empty tally becomes a finding naming the actor, the kind and
    the remedy, pointing back at the warning that carries the file:line."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default"), _row("doctor_beta", queue="batch")],
        queue_rows=[],
        worker_rows=[
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "metadata": {"loop_stalls": {"send_email": {"gil_held": 12, "blocking_call": 3}}},
            }
        ],
    )

    result = _invoke()

    assert "send_email" in plain_cli_output(result.output)
    assert "gil_held" in plain_cli_output(result.output)
    assert "worker 11111111-1111-1111-1111-111111111111" in plain_cli_output(result.output)
    # gil_held dominates (12 against 3), so the finding's remedy is the
    # GIL one.
    assert "chunk" in plain_cli_output(result.output)
    assert "event-loop-stall-attributed" in plain_cli_output(result.output)


def test_doctor_is_silent_when_no_worker_attributed_stalls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean fleet: no tally in any workers row, no attribution finding
    and no actor named for one."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default"), _row("doctor_beta", queue="batch")],
        queue_rows=[],
        worker_rows=[
            {
                "id": "11111111-1111-1111-1111-111111111111",
                "metadata": {"notify_enabled": True, "max_concurrency": 4},
            }
        ],
    )

    result = _invoke()

    assert "send_email" not in plain_cli_output(result.output)
    assert "stalled the event loop" not in plain_cli_output(result.output)


def test_doctor_platform_grace_below_worst_case_reports_the_sigkill_risk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The platform's stop grace is invisible to the running worker, so a
    deployment that pinned it against an older, smaller worst case gets
    SIGKILLed mid-teardown on every shutdown after the tail grew. Doctor
    names the shortfall and the fix when the operator supplies the number."""
    _patch_db(monkeypatch, actor_rows=[_row("doctor_alpha", queue="default")], queue_rows=[])

    result = _invoke("--platform-grace-seconds", "30")

    assert "below the worker's modelled worst-case shutdown" in plain_cli_output(result.output)
    assert "crash reclaim" in plain_cli_output(result.output)
    assert "SIGKILL" in plain_cli_output(result.output)


def test_doctor_platform_grace_above_worst_case_confirms_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_db(monkeypatch, actor_rows=[_row("doctor_alpha", queue="default")], queue_rows=[])

    result = _invoke("--platform-grace-seconds", "600")

    assert "covers the worker's modelled worst-case shutdown" in plain_cli_output(result.output)
    assert "SIGKILL" not in plain_cli_output(result.output)


# ── Unknown TASKQ_ environment variables (the config-drift family) ────


def _clean_taskq_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Strip every ambient ``TASKQ_`` variable so the env scan under test
    starts from a known-clean process environment. The sanctioned
    monkeypatch seam - the suite-hygiene pin bans direct ``os.environ``
    writes in tests - and the same discipline conftest applies to the
    OTel trigger variables: the scan reads the REAL process environment
    at doctor runtime, so the developer's ambient ``TASKQ_*`` must not
    decide what a finding test sees."""
    for name in list(os.environ):
        if name.startswith("TASKQ_"):
            monkeypatch.delenv(name, raising=False)


def test_doctor_reports_unknown_taskq_env_var_naming_the_closest_real_name(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The audit's real config-drift trap: a typo'd ``TASKQ_`` variable
    (``TASKQ_MAX_PENDNG_LOCK_TIMEOUT_MS``, missing the second C) loads
    noiselessly - the settings loader reads only the names it defines -
    and the intended field applies its documented default with no error
    anywhere. Doctor must name the offender AND the closest real setting
    name, the one-character-drift remedy a typo almost always wants."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default"), _row("doctor_beta", queue="batch")],
        queue_rows=[],
    )
    _clean_taskq_env(monkeypatch)
    monkeypatch.setenv("TASKQ_MAX_PENDNG_LOCK_TIMEOUT_MS", "5000")

    result = _invoke()

    assert "unknown TASKQ_ setting: TASKQ_MAX_PENDNG_LOCK_TIMEOUT_MS" in plain_cli_output(
        result.output
    )
    assert "did you mean TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS?" in plain_cli_output(result.output)
    # The consequence is stated, not just the name: the report must tell
    # the operator WHY an unknown variable is worth acting on.
    assert "a typo applies defaults silently" in plain_cli_output(result.output)


def test_doctor_reports_no_unknown_env_finding_when_every_taskq_var_is_known(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: a fully legitimate ``TASKQ_`` environment - core settings,
    an SSO sub-config's variable, a deprecated no-op - produces no
    unknown-setting finding. Without this, a scan that flagged every
    prefixed name would bury real findings in false positives."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default"), _row("doctor_beta", queue="batch")],
        queue_rows=[],
    )
    _clean_taskq_env(monkeypatch)
    monkeypatch.setenv("TASKQ_PG_DSN", "postgresql://taskq:taskq@localhost:5432/taskq")
    monkeypatch.setenv("TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS", "5000")
    monkeypatch.setenv("TASKQ_OIDC_ISSUER", "https://login.microsoftonline.com/tenant/v2.0")
    monkeypatch.setenv("TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE", "false")

    result = _invoke()

    assert "unknown TASKQ_ setting" not in plain_cli_output(result.output)


def test_doctor_unknown_env_finding_is_actionable_for_a_name_no_setting_resembles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ``TASKQ_`` variable that matches no real setting's name gets the
    honest remedy - "no similar setting name exists" - not a bogus
    difflib suggestion: a hint pointing at an unrelated setting sends the
    operator editing the wrong line."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default"), _row("doctor_beta", queue="batch")],
        queue_rows=[],
    )
    _clean_taskq_env(monkeypatch)
    monkeypatch.setenv("TASKQ_FLEET_ENCHILADA_MODE", "1")

    result = _invoke()

    assert "unknown TASKQ_ setting: TASKQ_FLEET_ENCHILADA_MODE" in plain_cli_output(result.output)
    assert "no similar setting name exists" in plain_cli_output(result.output)
    assert "docs/guides/configuration.md" in plain_cli_output(result.output)


def test_unknown_env_scan_ignores_non_taskq_names() -> None:
    """The scan is scoped to the ``TASKQ_`` namespace: the process
    environment's other variables are not TaskQ's to report."""
    from taskq.cli import _unknown_taskq_env_vars

    assert (
        _unknown_taskq_env_vars(
            {
                "PATH": "/usr/bin",
                "TASKQ_PG_DSN": "postgresql://t:t@h/db",
                "OTEL_SDK_DISABLED": "true",
            }
        )
        == []
    )


# ── The storage-mode family: the detected mode, first in every report ──


def test_doctor_leads_with_the_detected_storage_mode_vanilla(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The vanilla mode renders its one-glance consequences: plain tables,
    no hypertables, no columnstore, retention owned by the row-level
    sweeps. It is the FIRST finding in the report - the ground the rest
    of the report's advice stands on."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.VANILLA,
    )

    result = _invoke()

    assert (
        "storage mode: vanilla - plain tables - no hypertables, no columnstore; "
        "retention is the row-level sweeps (bounded batch deletes)" in result.output
    )
    # First finding-family, literally: the first bullet under the findings
    # header is the storage-mode line, ahead of every other family.
    findings_at = result.output.index("findings (")
    tail = result.output[findings_at:]
    assert re.match(r"findings \(\d+\):\n  - storage mode: ", tail), tail.splitlines()[:3]
    # The vanilla mode with the flag off is the world's default deployment:
    # no drift arm fired.
    assert "storage mode drift" not in result.output


def test_doctor_leads_with_the_detected_storage_mode_tsl(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The full TSL mode renders the whole consequence chain: hypertables,
    the archive columnstore, policy-driven chunk-drop retention - and the
    boundary the sweeps still own (expire_at exactness inside young
    chunks)."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.TIMESCALE_TSL,
    )

    result = _invoke()

    assert (
        "storage mode: timescale-tsl - hypertables + columnstore on the archive "
        "tables + policy-driven chunk-drop retention; the row-level sweeps still "
        "expire inside young chunks and remain the only expire_at-exact mechanism" in result.output
    )
    assert "storage mode drift" not in result.output


def test_doctor_leads_with_the_detected_storage_mode_apache(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The apache mode renders the mode's truth in one glance: hypertables
    yes, columnstore no, chunk-drop policies NO (they are Timescale-license
    features the server refuses under its license), retention back with
    the row-level sweeps."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.TIMESCALE_APACHE,
    )

    result = _invoke()

    assert (
        "storage mode: timescale-apache - hypertables, rowstore - the chunk-drop "
        "policies AND the columnstore are Timescale-license features this "
        "server's license disables, so retention is the row-level sweeps "
        "(bounded batch deletes)" in result.output
    )
    assert "storage mode drift" not in result.output


def test_doctor_names_the_license_downgrade_drift_apache_with_live_policies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The family's second red arm, measured on real 2.30.1: a server
    converted under the full TSL license and then downgraded to apache
    keeps its five policy jobs registered - and they FAIL on every
    background run under the downgraded license (measured
    ``sqlerrcode 0A000``, retried forever), while the row-level sweeps
    defer the aged end to them (the retention-policy floor). Rows older
    than the dead policies' horizon strand: nothing deletes them. A
    doctor that renders only the mode's healthy summary ("retention is
    the row-level sweeps") LIES on this server - the drift arm must name
    the stranded state, its mechanism, and the honest remedy (restore the
    license; the policies cannot be removed under apache - every removal
    API refuses too)."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.TIMESCALE_APACHE,
        downgraded_policies=(
            "policy_retention:job_events",
            "policy_retention:jobs_archive",
            "policy_retention:job_attempts_archive",
            "policy_compression:jobs_archive",
            "policy_compression:job_attempts_archive",
        ),
    )

    result = _invoke()

    assert (
        "storage mode drift: this server's timescaledb.license is 'apache' but 5 "
        "TimescaleDB policy job(s) from an earlier timescale-license deployment are "
        "still registered" in result.output
    ), result.output
    # The mechanism, named so the operator believes it: the policies fail on
    # every run AND the sweeps defer to them - the strand is real, not cosmetic.
    assert "fail on every" in result.output
    assert "strand" in result.output
    # The honest remedy: the license cannot be worked around - the removal
    # APIs refuse under apache too - so the way out is the license restore.
    assert "ALTER SYSTEM SET timescaledb.license = 'timescale'" in result.output


def test_doctor_flags_the_flag_on_vanilla_drift_as_unworkable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The family's one red arm: the environment's flag says hypertables,
    the server detects vanilla. That contradiction is not workable - the
    next ``taskq migrate up`` refuses - so the report names the refusal,
    the setting, and both remedies."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.VANILLA,
    )
    monkeypatch.setenv("TASKQ_TIMESCALEDB_HYPERTABLES", "true")

    result = _invoke()

    assert (
        "storage mode drift: TASKQ_TIMESCALEDB_HYPERTABLES=true but this server "
        "detects vanilla" in result.output
    )
    assert "TimescaleDBUnavailableError" in result.output


def test_doctor_flag_on_tsl_is_no_drift(monkeypatch: pytest.MonkeyPatch) -> None:
    """The aligned case - flag on, server detects the full TSL mode - is
    the family's green: the mode line, no drift arm."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.TIMESCALE_TSL,
    )
    monkeypatch.setenv("TASKQ_TIMESCALEDB_HYPERTABLES", "true")

    result = _invoke()

    assert "storage mode: timescale-tsl" in result.output
    assert "storage mode drift" not in result.output


def test_doctor_stays_read_only_with_the_storage_mode_family(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The mode detection rides the report's existing read-only connection
    and must not add a write to it: the recorded statement log carries no
    write verb (the detection's probes are catalog reads)."""
    executed = _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_alpha", queue="default")],
        queue_rows=[],
        storage_mode=StorageMode.TIMESCALE_TSL,
    )

    result = _invoke()

    assert result.exit_code == 0
    for statement in executed:
        assert not _WRITE_VERBS.search(statement), statement


# ── Operational-insight families (the insights.py reads) ───────────────
#
# The rows below are the shapes taskq.insights' fetchers return (the PG
# tier for these families lives in tests/test_cli_doctor_insights_pg.py,
# which seeds the pathological shapes in a real container).


def _imbalance_row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "queue": "batch",
        "depth": 12,
        "oldest_due_at": None,
        "oldest_due_age_s": 30.0,
        "scheduled_depth": 0,
        "wave_min_scheduled_at": None,
        "wave_max_scheduled_at": None,
        "live_workers": 3,
        "actor_capacity": 4,
        "effective_capacity": 12,
        "utilization": 1.0,
    }
    row.update(over)
    return row


def _wait_row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "queue": "batch",
        "segment": "clean",
        "count": 40,
        "p50_wait_s": 3.0,
        "p95_wait_s": 5.0,
        "max_wait_s": 9.0,
    }
    row.update(over)
    return row


def _overprovisioning_row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "queue": "over_q",
        "live_workers": 3,
        "depth": 0,
        "terminalisations": 1,
        "overprovisioned": False,
    }
    row.update(over)
    return row


def _drain_row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "queue": "bulk_q",
        "depth": 200,
        "terminalisations": 50,
        "completions_per_second": 50 / 86400,
        "has_traffic": True,
        "eta_seconds": 200 * 86400 / 50,
        "scheduled_depth": 0,
        "wave_min_scheduled_at": None,
        "wave_max_scheduled_at": None,
    }
    row.update(over)
    return row


def _cron_row(**over: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        "schedule_id": "9e1e1e1e-1111-1111-1111-111111111111",
        "actor": "send_email",
        "cron_expr": "*/5 * * * *",
        "timezone": "UTC",
        "dst_strategy": "skip",
        "enabled": True,
        "fires_window": 10,
        "cleared_window": 2,
        "fires_prior": 10,
        "cleared_prior": 2,
        "outstanding": 20,
        "runaway_trending": True,
    }
    row.update(over)
    return row


def test_doctor_reports_starved_queue_and_names_the_capacity_levers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A queue whose utilization (depth / effective capacity) blows past the
    2x threshold AND whose oldest due job has outlived the persistence
    floor holds a full second claim-wave of due work after the first
    drains - a fleet too small for its arrival rate, not a transient
    burst. The remedy must name the levers the product actually has:
    another worker on the queue, or a raised max_concurrent."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="batch", max_concurrent=4)],
        queue_rows=[],
        imbalance_rows=[
            _imbalance_row(
                depth=12,
                live_workers=3,
                actor_capacity=4,
                utilization=6.0,
                oldest_due_age_s=600.0,
            )
        ],
    )

    result = _invoke()

    assert "batch" in result.output
    assert "STARVED" in result.output
    assert "6.0x" in result.output
    # The remedy names the real levers, not a fantasy knob.
    assert "worker" in result.output
    assert "max_concurrent" in result.output
    assert "doctor_beta" in result.output


def test_doctor_is_silent_on_a_burst_younger_than_the_persistence_floor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control (the measured false positive): a 12-deep queue at 6x
    utilization whose oldest due job is 10s old is a burst the dispatcher
    may absorb before the report is read - an arrival-rate claim cannot
    rest on a depth snapshot younger than the claim-in-flight window."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="batch", max_concurrent=4)],
        queue_rows=[],
        imbalance_rows=[
            _imbalance_row(
                depth=12,
                live_workers=3,
                actor_capacity=4,
                utilization=6.0,
                oldest_due_age_s=10.0,
            )
        ],
    )

    result = _invoke()

    assert "STARVED" not in result.output


def test_doctor_is_silent_on_a_balanced_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Control: utilization at the balanced edge (1.0 - one full wave, the
    shape any enqueue burst produces transiently) renders NO imbalance
    finding. A threshold at 1.0 would cry wolf on every burst."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="batch")],
        queue_rows=[],
        imbalance_rows=[_imbalance_row(utilization=1.0)],
    )

    result = _invoke()

    assert "STARVED" not in result.output


def test_doctor_reports_stranded_work_when_oldest_due_blows_past_p95(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The strand shape: a due row whose age exceeds its queue's own p95
    wait by the strand factor has outlived the entire observed wait
    distribution including its tail - a burst cannot explain it, because
    the p95 is computed over the same window the burst would inflate."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="batch")],
        queue_rows=[],
        imbalance_rows=[_imbalance_row(oldest_due_age_s=600.0)],
        wait_rows=[_wait_row(p95_wait_s=5.0)],
    )

    result = _invoke()

    assert "batch" in result.output
    assert "STRANDED WORK" in result.output
    assert "600s" in result.output
    assert "worker" in result.output
    assert "max_concurrent" in result.output


def test_doctor_is_silent_when_oldest_due_is_within_the_distribution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: an oldest-due age inside the observed distribution's tail
    (under both the factor arm and the absolute floor) renders NOTHING -
    a just-enqueued job is not a strand."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="batch")],
        queue_rows=[],
        imbalance_rows=[_imbalance_row(oldest_due_age_s=10.0)],
        wait_rows=[_wait_row(p95_wait_s=5.0)],
    )

    result = _invoke()

    assert "STRANDED WORK" not in result.output


def test_doctor_reports_overprovisioned_queue_with_consolidation_remedy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Live workers on a queue with zero due depth and near-zero
    terminalisations over the whole window: the fleet's payroll outruns
    its work. The remedy is consolidation in the workgroup config -
    NEVER a destructive suggestion."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="over_q")],
        queue_rows=[],
        overprovisioning_rows=[
            _overprovisioning_row(live_workers=3, depth=0, terminalisations=1, overprovisioned=True)
        ],
    )

    result = _invoke()

    assert "over_q" in result.output
    assert "OVERPROVISIONED" in result.output
    assert "consolidate" in result.output.lower()
    assert "workgroup" in result.output.lower()
    # The remedy must be non-destructive, and SAY so: no purge/drop
    # suggestion, and the explicit nothing-is-deleted disclaimer.
    assert "nothing is deleted" in result.output.lower()
    assert "purge" not in result.output.lower()
    assert "drop" not in result.output.lower()


def test_doctor_is_silent_when_the_queue_earns_its_workers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: the same worker count with real throughput (the verdict
    keys on work done, not queue emptiness) renders NOTHING."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="over_q")],
        queue_rows=[],
        overprovisioning_rows=[
            _overprovisioning_row(
                live_workers=3, depth=0, terminalisations=50, overprovisioned=False
            )
        ],
    )

    result = _invoke()

    assert "OVERPROVISIONED" not in result.output


def test_doctor_reports_slow_drain_with_eta_and_confidence_caveat(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A drain eta beyond the observation window itself, on a depth that
    has persisted past the persistence floor, means the due depth exceeds
    everything the entire window completed: the operator would not
    recognize the queue as draining by the time the extrapolation says it
    lands. The finding states the eta AND the has_traffic confidence
    caveat."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="bulk_q")],
        queue_rows=[],
        imbalance_rows=[_imbalance_row(queue="bulk_q", oldest_due_age_s=600.0)],
        drain_rows=[_drain_row(eta_seconds=4 * 86400.0)],
    )

    result = _invoke()

    assert "bulk_q" in result.output
    assert "SLOW DRAIN" in result.output
    assert "4.0 days" in result.output
    assert "extrapolation" in result.output.lower()
    assert "50 completion(s)" in result.output


def test_doctor_is_silent_when_a_young_burst_reads_a_fictional_eta(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control (the measured false positive): an idle-capacity fleet's
    51-job burst reads a ~1.0-day eta off a demand-limited rate, but the
    depth is 2s old - the idle workers absorb it before the report is
    read. No persistence, no rate claim."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="bulk_q")],
        queue_rows=[],
        imbalance_rows=[_imbalance_row(queue="bulk_q", oldest_due_age_s=2.0)],
        drain_rows=[_drain_row(eta_seconds=4 * 86400.0)],
    )

    result = _invoke()

    assert "SLOW DRAIN" not in result.output


def test_doctor_is_silent_when_drain_fits_inside_the_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control: an eta inside the observation window (and the no-traffic
    shape, whose estimate is honestly undefined) renders NOTHING."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("doctor_beta", queue="bulk_q")],
        queue_rows=[],
        drain_rows=[
            _drain_row(eta_seconds=3600.0),
            _drain_row(queue="empty_q", has_traffic=False, eta_seconds=None),
        ],
    )

    result = _invoke()

    assert "SLOW DRAIN" not in result.output


def test_doctor_reports_cron_lag_naming_schedule_backlog_and_both_remedies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schedule whose fan-out outruns clearance (two consecutive windows,
    or an outstanding backlog above the catch-up window's demonstrated slot
    capacity) must be named by id, with the backlog size and BOTH honest
    remedies: slow the cron, or add workers for the actor."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("send_email", queue="batch")],
        queue_rows=[],
        cron_rows=[_cron_row()],
    )

    result = _invoke()

    assert "9e1e1e1e-1111-1111-1111-111111111111" in result.output
    assert "send_email" in result.output
    assert "CRON LAG" in result.output
    assert "20 fire(s) outstanding" in result.output
    assert "slow the cron" in result.output.lower()
    assert "max_concurrent" in result.output


def test_doctor_is_silent_on_a_healthy_cron_ledger(monkeypatch: pytest.MonkeyPatch) -> None:
    """Control: fires fully cleared in both windows, zero outstanding -
    the ledger renders NOTHING."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("send_email", queue="batch")],
        queue_rows=[],
        cron_rows=[
            _cron_row(
                fires_window=5,
                cleared_window=5,
                fires_prior=5,
                cleared_prior=5,
                outstanding=0,
                runaway_trending=False,
            )
        ],
    )

    result = _invoke()

    assert "CRON LAG" not in result.output


def test_doctor_cron_lag_requires_demonstrated_clearance_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schedule that fired nothing in the trend horizon has no measured
    clearance capacity to compare against: a weekly cron with one long
    running fire must not read as a lag. The outstanding arm requires the
    schedule to have actually fired within the two windows."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("send_email", queue="batch")],
        queue_rows=[],
        cron_rows=[
            _cron_row(
                fires_window=0,
                cleared_window=0,
                fires_prior=0,
                cleared_prior=0,
                outstanding=1,
                runaway_trending=False,
            )
        ],
    )

    result = _invoke()

    assert "CRON LAG" not in result.output


def test_doctor_is_silent_on_a_first_in_flight_fire_with_no_clearance_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Control (the measured false positive): a brand-new schedule's FIRST
    fire is running right now - fires_window=1, no clearance in either
    window, outstanding=1. With a zero demonstrated capacity the backlog
    arm would read every in-flight fire as an uncatchable backlog; the
    arm requires a POSITIVE demonstrated clearance to compare against."""
    _patch_db(
        monkeypatch,
        actor_rows=[_row("send_email", queue="batch")],
        queue_rows=[],
        cron_rows=[
            _cron_row(
                fires_window=1,
                cleared_window=0,
                fires_prior=0,
                cleared_prior=0,
                outstanding=1,
                runaway_trending=False,
            )
        ],
    )

    result = _invoke()

    assert "CRON LAG" not in result.output
