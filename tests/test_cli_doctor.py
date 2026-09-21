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

from collections.abc import Mapping
from typing import Any

import pytest
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.actor import ActorRef, actor
from taskq.actor_config_ops import ActorConfigRow
from taskq.cli import app
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

# Writing SQL verbs: any of these appearing in a statement the command
# issued means `doctor` is no longer read-only.
_WRITE_VERBS = ("insert", "update", "delete", "truncate", "drop", "alter", "create")


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
) -> list[str]:
    """Fake the doctor's reads at the ``taskq.cli`` boundary.

    Returns the list every statement the command executes is recorded
    into, so the read-only property can be asserted rather than assumed.

    ``stranded_rows`` feeds the pending/scheduled jobs scan (the one read
    that reaches the raw connection rather than a patched helper): rows in
    the per-actor shape ``_list_stranded_pending_jobs`` returns.  The scan
    is identified by its ``.jobs`` table reference - it is the only
    jobs-table statement the command issues.
    """
    executed: list[str] = []
    stranded = [] if stranded_rows is None else stranded_rows
    worker_rows = [] if worker_rows is None else worker_rows

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

    async def fake_list_actor_configs(conn: Any, **kwargs: Any) -> list[ActorConfigRow]:
        return actor_rows

    async def fake_list_queues(conn: Any, **kwargs: Any) -> list[QueueRow]:
        return queue_rows

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.list_actor_configs", fake_list_actor_configs)
    monkeypatch.setattr("taskq.cli.list_queues", fake_list_queues)
    return executed


def _invoke(*extra: str) -> Any:
    return runner.invoke(app, ["doctor", "--actors", _REGISTRY_PATH, *extra])


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

    assert "doctor_beta" in result.output
    assert "never dispatches" in result.output.lower()


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

    assert "retired_tier" in result.output
    assert "stale" in result.output.lower()


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

    assert "drain" in result.output.lower(), (
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

    assert "uncapped" in result.output.lower()


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

    assert "doctor_alpha" in result.output
    assert "max_pending" in result.output


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

    assert "doctor_alpha" in result.output
    assert "default" in result.output


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
    offenders = [
        statement
        for statement in executed
        if any(verb in statement.lower() for verb in _WRITE_VERBS)
    ]
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
    lowered = result.output.lower()
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

    output = result.output.lower()
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

    assert "send_email" in result.output
    assert "gil_held" in result.output
    assert "worker 11111111-1111-1111-1111-111111111111" in result.output
    # gil_held dominates (12 against 3), so the finding's remedy is the
    # GIL one.
    assert "chunk" in result.output
    assert "event-loop-stall-attributed" in result.output


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

    assert "send_email" not in result.output
    assert "stalled the event loop" not in result.output


def test_doctor_platform_grace_below_worst_case_reports_the_sigkill_risk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The platform's stop grace is invisible to the running worker, so a
    deployment that pinned it against an older, smaller worst case gets
    SIGKILLed mid-teardown on every shutdown after the tail grew. Doctor
    names the shortfall and the fix when the operator supplies the number."""
    _patch_db(monkeypatch, actor_rows=[_row("doctor_alpha", queue="default")], queue_rows=[])

    result = _invoke("--platform-grace-seconds", "30")

    assert "below the worker's modelled worst-case shutdown" in result.output
    assert "crash reclaim" in result.output
    assert "SIGKILL" in result.output


def test_doctor_platform_grace_above_worst_case_confirms_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_db(monkeypatch, actor_rows=[_row("doctor_alpha", queue="default")], queue_rows=[])

    result = _invoke("--platform-grace-seconds", "600")

    assert "covers the worker's modelled worst-case shutdown" in result.output
    assert "SIGKILL" not in result.output
