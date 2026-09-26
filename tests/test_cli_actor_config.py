"""Tests for the `taskq actor-config` CLI surface.

The asyncpg connection and the ops functions are monkeypatched at the
``taskq.cli`` import boundary - these tests pin the CLI's argument
parsing, validation, error messages, output shape, and exit-code
contract, not Postgres behavior (covered by the integration tier in
test_actor_config_ops.py).
"""

from collections.abc import Mapping
from typing import Any, cast

import pytest
import typer.main
from pydantic import BaseModel
from typer.testing import CliRunner

from taskq.actor import ActorRef, actor
from taskq.actor_config_ops import ActorConfigRow, ActorQueueMoveResult
from taskq.cli import app
from taskq.exceptions import ActorNotFoundError

runner = CliRunner()

_ROW = ActorConfigRow(
    actor="diff_actor",
    max_concurrent=5,
    max_pending=10,
    queue="default",
    result_ttl=60.0,
    metadata={},
    updated_at="2026-01-01 00:00:00+00",
)


class _DiffPayload(BaseModel):
    value: int


@actor(name="diff_actor", max_concurrent=4, max_pending=100, queue="critical")
async def _diff_actor(payload: _DiffPayload) -> None: ...


_REGISTRY: Mapping[str, ActorRef[Any, Any]] = {"diff_actor": _diff_actor}
_REGISTRY_PATH = "tests.test_cli_actor_config:_REGISTRY"


def _patch_db(
    monkeypatch: pytest.MonkeyPatch,
    *,
    set_result: ActorConfigRow | None = _ROW,
    get_result: ActorConfigRow | None = _ROW,
    list_result: list[ActorConfigRow] | None = None,
) -> dict[str, Any]:
    """Fake asyncpg.connect + the ops functions; return captured call kwargs."""
    captured: dict[str, Any] = {}

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    async def fake_set(conn: Any, actor: str, **kwargs: Any) -> ActorConfigRow | None:
        captured["set"] = {"actor": actor, **kwargs}
        return set_result

    async def fake_get(conn: Any, actor: str, **kwargs: Any) -> ActorConfigRow | None:
        captured["get"] = {"actor": actor, **kwargs}
        return get_result

    async def fake_list(conn: Any, **kwargs: Any) -> list[ActorConfigRow]:
        return list_result if list_result is not None else [_ROW]

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.set_actor_config_capacity", fake_set)
    monkeypatch.setattr("taskq.cli.get_actor_config", fake_get)
    monkeypatch.setattr("taskq.cli.list_actor_configs", fake_list)
    return captured


# ── set ──────────────────────────────────────────────────────────────────


def test_set_max_pending_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    """--max-pending reaches set_actor_config_capacity - the flag exists and
    drives the stored value the enqueue path now enforces."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--max-pending", "7"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["set"]["actor"] == "diff_actor"
    assert captured["set"]["max_pending"] == 7


def test_set_max_pending_clear_round_trip(monkeypatch: pytest.MonkeyPatch) -> None:
    """--clear-max-pending writes NULL (revert to the code literal)."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--clear-max-pending"])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["set"]["max_pending"] is None


def test_set_max_pending_and_clear_are_mutually_exclusive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_db(monkeypatch)
    result = runner.invoke(
        app, ["actor-config", "set", "diff_actor", "--max-pending", "7", "--clear-max-pending"]
    )
    assert result.exit_code == 1
    assert "mutually exclusive" in result.stderr


def test_set_negative_max_pending_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    """typer's min=0 guard rejects negative values before any DB call."""
    captured = _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--max-pending", "-1"])
    assert result.exit_code != 0
    assert "set" not in captured


def test_set_without_flags_is_an_error(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor"])
    assert result.exit_code == 1
    assert "nothing to change" in result.stderr


def test_set_unknown_actor_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch, set_result=None)
    result = runner.invoke(app, ["actor-config", "set", "ghost", "--max-concurrent", "5"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr


def _patch_connect_only(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fake only asyncpg.connect; let the REAL ops function run.

    Validation in set_actor_config_capacity happens before any I/O, so a
    bare fake connection is enough to exercise it end-to-end through the
    CLI - this is how NaN/±inf (which typer's min=0 cannot see) must be
    rejected with a clean operator-facing message instead of a traceback.
    """

    class _FakeConn:
        async def close(self) -> None: ...

    async def fake_connect(dsn: str) -> Any:
        return _FakeConn()

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)


def test_set_result_ttl_nan_rejected_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    """--result-ttl nan parses (float('nan')) and typer's min=0 cannot see
    it (nan < 0 is False), but writing it would break every completion
    for the actor: clock_timestamp() + NaN * interval '1 second' raises
    'interval out of range' in the terminal-write UPDATE. The ops-layer
    finite guard rejects it, and the CLI prints the reason, not a
    traceback."""
    _patch_connect_only(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--result-ttl", "nan"])
    assert result.exit_code == 1
    assert "finite" in result.stderr
    assert "Traceback" not in result.output


def test_set_result_ttl_inf_rejected_cleanly(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_connect_only(monkeypatch)
    result = runner.invoke(app, ["actor-config", "set", "diff_actor", "--result-ttl", "inf"])
    assert result.exit_code == 1
    assert "finite" in result.stderr
    assert "Traceback" not in result.output


def test_set_help_lists_all_capacity_fields() -> None:
    """`actor-config set` exposes a flag for every capacity field.

    Asserted against the declared parameters rather than rendered ``--help``
    text: Rich wraps help output to the terminal width, so a substring check
    on the rendering fails on narrow terminals (e.g. CI) even though the flag
    is present.
    """
    root = cast(Any, typer.main.get_command(app))
    set_cmd = root.commands["actor-config"].commands["set"]
    declared = {opt for param in set_cmd.params for opt in param.opts}
    for flag in ("--max-concurrent", "--max-pending", "--result-ttl", "--clear-max-pending"):
        assert flag in declared, f"{flag} missing from `actor-config set`"


# ── get / list ───────────────────────────────────────────────────────────


def test_get_unknown_actor_exit_one(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch, get_result=None)
    result = runner.invoke(app, ["actor-config", "get", "ghost"])
    assert result.exit_code == 1
    assert "no stored actor_config row" in result.stderr


def test_list_prints_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "list"])
    assert result.exit_code == 0
    assert "diff_actor" in result.output
    assert "max_pending=10" in result.output


# ── diff ─────────────────────────────────────────────────────────────────

# Same capacity values as _ROW but with queue/metadata matching the
# registry literals: only the capacity fields drift, which is operator-
# owned and never blocks dispatch or boot.
_CAPACITY_DRIFT_ROW = ActorConfigRow(
    actor="diff_actor",
    max_concurrent=5,
    max_pending=10,
    queue="critical",
    result_ttl=60.0,
    metadata={},
    updated_at="2026-01-01 00:00:00+00",
)


def test_diff_shows_literal_stored_and_effective(monkeypatch: pytest.MonkeyPatch) -> None:
    """The operator's debugging view: why is my change (not) taking effect.

    Registry literal max_pending=100 vs stored 10 → effective is the
    stored 10, and the output says so. Only capacity drifts here - stored
    capacity is operator-owned - so the exit code stays 0.
    """
    _patch_db(monkeypatch, list_result=[_CAPACITY_DRIFT_ROW])
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "diff_actor" in result.output
    assert "literal=100" in result.output
    assert "stored=10" in result.output
    assert "effective=10" in result.output


def test_diff_flags_queue_mismatch_as_assignment_drift(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Registry queue=critical vs stored queue=default → the output names
    the assignment drift and its remedy (`actor-config move-queue`), and
    the exit code fails the run: boot adopts the stored queue, but the
    cron leader's fires follow it while producers enqueue by their own
    literal, so the two routing halves disagree until the move or the
    deploy completes - drift the command itself calls gate-failing must
    not report success."""
    _patch_db(monkeypatch)
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code != 0
    assert "queue" in result.output
    assert "MISMATCH" in result.output
    assert "move-queue" in result.output


def test_diff_marks_actor_without_stored_row(monkeypatch: pytest.MonkeyPatch) -> None:
    """An actor the registry declares but no worker has synced yet.

    max_concurrent must NOT show the literal as effective: the dispatch
    capacity gate builds FROM actor_config (inner join), so with no row
    the actor is never dispatched - effective is 0. max_pending /
    result_ttl enforcement can see the code literal, so those do fall
    back to it. Blocking dispatch blocks the run, so the exit is
    non-zero even though the next worker startup would seed the row.
    """
    _patch_db(monkeypatch, list_result=[])
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code != 0
    assert "no stored row" in result.output
    assert "DOES NOT DISPATCH" in result.output
    assert "max_concurrent  literal=4  effective=0 (no stored row" in result.output
    assert "max_pending     literal=100  effective=100 (literal)" in result.output


def test_diff_marks_leftover_row_not_in_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stored row whose actor is no longer registered is shown as leftover.

    The registered actor keeps a structurally matching row so the leftover
    is the only state in play: a leftover row is inert (it only serves
    already-queued jobs), so the exit code stays 0.
    """
    ghost = ActorConfigRow(
        actor="ghost",
        max_concurrent=1,
        max_pending=None,
        queue="default",
        result_ttl=None,
        metadata={},
        updated_at="2026-01-01 00:00:00+00",
    )
    _patch_db(monkeypatch, list_result=[ghost, _CAPACITY_DRIFT_ROW])
    result = runner.invoke(app, ["actor-config", "diff", "--actors", _REGISTRY_PATH])
    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert "ghost" in result.output
    assert "not in the registry" in result.output


# ── move-queue ───────────────────────────────────────────────────────────


def _patch_move_db(
    monkeypatch: pytest.MonkeyPatch,
    *,
    result: Any = None,
    error: Exception | None = None,
) -> dict[str, Any]:
    """Fake asyncpg.connect + move_actor_queue; return captured call kwargs."""
    captured: dict[str, Any] = {}

    class _MoveConn:
        closed = False

        async def close(self) -> None:
            self.closed = True

    conn_holder: dict[str, Any] = {}

    async def fake_connect(dsn: str) -> Any:
        conn_holder["dsn"] = dsn
        return _MoveConn()

    async def fake_move(conn: Any, actor: str, new_queue: str, **kwargs: Any) -> Any:
        captured["move"] = {"actor": actor, "new_queue": new_queue, **kwargs}
        captured["conn"] = conn
        if error is not None:
            raise error
        return result

    monkeypatch.setattr("taskq.cli.asyncpg.connect", fake_connect)
    monkeypatch.setattr("taskq.cli.move_actor_queue", fake_move)
    return captured


_MOVE_RESULT = ActorQueueMoveResult(
    actor="diff_actor",
    from_queue="critical",
    to_queue="q2",
    jobs_moved=3,
    running_jobs_left=1,
    queues_row_carried=True,
    pending_jobs_on_old_queue=2,
)


def test_move_queue_reports_the_move_and_closes_the_conn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`actor-config move-queue ACTOR NEW_QUEUE` reaches move_actor_queue
    with the actor, target and schema, prints the move report, and closes
    the connection.

    Regression caught: this is the one CLI command whose only prior
    execution was by hand — a typo in the dispatcher (a swapped argument
    pair, a dropped schema kwarg) or a conn leak on the success path had
    no red test anywhere; the residual-on-stderr split is what drives
    the operator's "when can the old queue's consumers stop" decision.
    """
    captured = _patch_move_db(monkeypatch, result=_MOVE_RESULT)

    result = runner.invoke(app, ["actor-config", "move-queue", "diff_actor", "q2"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    # The schema literal the test drives: TaskQSettings.load()'s default
    # (no env override in this test), asserted exactly so a dropped or
    # renamed schema kwarg fails the compare instead of self-satisfying it.
    assert captured["move"] == {
        "actor": "diff_actor",
        "new_queue": "q2",
        "schema": "taskq",
    }
    assert captured["move"]["schema"] and "taskq" in captured["move"]["schema"]
    assert "Moved actor 'diff_actor': 'critical' -> 'q2'" in result.stdout
    assert "jobs_moved=3" in result.stdout
    assert "running_jobs_left=1" in result.stdout
    assert "queues_row_carried=True" in result.stdout
    # The residual is an operator action driver: stderr, not stdout.
    assert "2 pending/scheduled job(s) still carry queue 'critical'" in result.stderr
    assert captured["conn"].closed, "the move connection must be closed"


def test_move_queue_ghost_actor_exits_three_with_a_clean_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A ghost actor (no stored row) exits 3 with the error text on
    stderr — never a traceback, and never the refusal exit 2 (the two
    codes drive different operator responses: fix the name vs fix the
    state)."""
    _patch_move_db(
        monkeypatch,
        error=ActorNotFoundError("no actor_config row for actor 'ghost'"),
    )

    result = runner.invoke(app, ["actor-config", "move-queue", "ghost", "q2"])

    assert result.exit_code == 3
    assert "no actor_config row for actor 'ghost'" in result.stderr
    assert "Traceback" not in result.stderr


def test_move_queue_transposed_arguments_are_a_usage_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The transpose defense: `actor-config move-queue` takes two bare
    positionals, so an operator carrying `queue migrate`'s `--to` shape
    gets a usage error BEFORE any dispatcher call — the two-arguments-
    both-strings shape is what an operator transposes under pressure.

    Regression caught: giving move-queue an `--to` option would let the
    transposed invocation parse (silently dropping one queue name) and
    run the move against a half-named target.
    """
    captured = _patch_move_db(monkeypatch, result=_MOVE_RESULT)

    result = runner.invoke(app, ["actor-config", "move-queue", "diff_actor", "--to", "q2"])

    assert result.exit_code != 0
    assert "Usage: taskq actor-config move-queue" in result.stderr
    assert captured == {}, "the transposed shape must be refused before any dispatcher call"


def test_queue_migrate_is_the_same_move_named_by_to(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`queue migrate ACTOR --to q2` is the same one-step move with the
    target REQUIRED as an option — the command pair shares one
    dispatcher, so a contract change (exit codes, the report line) must
    land on both.

    Regression caught: the `--to` option is never defaulted; a refactor
    that gave it a default (or unlinked the shared dispatcher) would
    split the two commands' exit-code contracts apart.
    """
    captured = _patch_move_db(monkeypatch, result=_MOVE_RESULT)

    result = runner.invoke(app, ["queue", "migrate", "diff_actor", "--to", "q2"])

    assert result.exit_code == 0, f"stderr: {result.stderr}"
    assert captured["move"]["actor"] == "diff_actor"
    assert captured["move"]["new_queue"] == "q2"
    assert "Moved actor 'diff_actor': 'critical' -> 'q2'" in result.stdout
