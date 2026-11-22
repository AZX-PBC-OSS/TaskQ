"""Mutation and contract pins for the crash lane and its AST claim checks.

The identity-boundary crash lane
(``tests/system_e2e/test_wf_identity_boundary_kill.py``) and its structural
claim pins (``tests/test_wf_identity_boundary_kill_claims.py``) guard two
surfaces that silent regressions can hollow out:

1. **The crash lane itself**:
   * the pre-kill claim gate must deterministically hold or freeze the victim
     with a ledgered record claim across the ``SIGKILL`` window — never poll
     ``EXISTS`` against a no-op record body and await extra DB round trips
     before signalling;
   * the fence equality must compare ``(job_id, attempt)`` pairs on both
     sides so the crashed attempt itself is enforced, not only the ``job_id``.
2. **The claim pins' teeth**:
   * deleting the ledger join or its ``running`` status predicate from the
     pre-kill gate must fail the non-zero claim pin;
   * deleting either or both ``len(corpse_ledger_claims) > 0`` guards must
     fail the non-zero claim pin;
   * substituting a non-equality or non-positive ``census`` assertion (such
     as ``assert census is not None``) must fail the census pin;
   * filtering the corpse ledger subset by ``status`` (a ``status = 'fenced'``
     tautology) or dropping the corpse intersection from the fence equality
     must fail the fence pin.
3. **The hold-door identity refusal test**:
   * ``tests/test_wf_identity_717_repair.py``'s hold-door refusal test must
     exercise string and float ``map_index`` inputs in addition to negative
     integers and booleans.

Every test here is hermetic, deterministic, and runs in under a second.
"""

from __future__ import annotations

import ast
import re
import sys
from pathlib import Path
from typing import Any

import pytest
from _pytest.outcomes import Failed

import tests.test_wf_identity_boundary_kill_claims as claims_mod

_TESTS_DIR = Path(__file__).resolve().parent
_LANE_PATH = _TESTS_DIR / "system_e2e" / "test_wf_identity_boundary_kill.py"
_WF_APP_PATH = _TESTS_DIR / "system_e2e" / "_wf_app.py"
_HARNESS_PATH = _TESTS_DIR / "system_e2e" / "_harness.py"
_REPAIR_PATH = _TESTS_DIR / "test_wf_identity_717_repair.py"

#: Minimal canonical crash-lane source satisfying every claim contract.
#: Used as the mutation baseline so teeth checks remain immune to harmless
#: variable renames or comment edits in the real system-e2e lane.
_CANONICAL_LANE = '''\
import signal

BOUNDARY_STEP = "record"
BOUNDARY_GATE = "identity-boundary"
BOUNDARY_RECORD_GATE = "identity-boundary-record"
_TAG = "wf-identity-boundary"


async def test_canonical(conn, pool, schema, flow_id, source_id, victim, victim_worker, fleet, sys_ledger):
    await seed_e2e_release_gate(conn, schema, BOUNDARY_GATE)
    await seed_e2e_release_gate(conn, schema, BOUNDARY_RECORD_GATE)

    async def _victim_holds_a_ledgered_record() -> bool:
        return bool(
            await conn.fetchval(
                f"""SELECT EXISTS (
                        SELECT 1 FROM "{schema}".jobs j
                                 JOIN "{schema}".wf_step_ledger l
                                   ON l.flow_id = $1::uuid AND l.step_key = $2
                                  AND l.job_id = j.id AND l.attempt = j.attempt
                        WHERE j.locked_by_worker = $3::uuid AND j.step_key = $2
                          AND j.status = 'running' AND l.status = 'running')""",
                flow_id,
                BOUNDARY_STEP,
                victim_worker,
            )
        )

    await _wait_for(_victim_holds_a_ledgered_record, interval_s=0.02, deadline_s=3.5, what="claim")
    fleet[victim].proc.send_signal(signal.SIGKILL)
    reap(fleet[victim])

    corpse_records = {
        str(r["id"]): int(r["attempt"])
        for r in await conn.fetch(
            f"""SELECT id, attempt FROM "{schema}".jobs
                WHERE locked_by_worker = $1::uuid AND step_key = $2
                  AND status = 'running'""",
            victim_worker,
            BOUNDARY_STEP,
        )
    }
    assert len(corpse_records) > 0

    corpse_ledger_claims = {
        (str(r["job_id"]), int(r["attempt"]))
        for r in await conn.fetch(
            f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1 AND step_key = $2""",
            flow_id,
            BOUNDARY_STEP,
        )
    } & {(job_id, corpse_records[job_id]) for job_id in corpse_records}
    assert len(corpse_ledger_claims) > 0

    await open_e2e_release_gate(conn, schema, BOUNDARY_GATE)
    await open_e2e_release_gate(conn, schema, BOUNDARY_RECORD_GATE)

    status, _ = await _wait_terminal(conn, schema, str(flow_id))
    assert status == "complete"

    retried = {
        str(r["id"])
        for r in await conn.fetch(
            f"""SELECT id FROM "{schema}".jobs
                WHERE parent_id = $1 AND step_key = $2 AND attempt > 1""",
            source_id,
            BOUNDARY_STEP,
        )
    }
    assert not (set(corpse_records) - retried)

    fenced_claims = {
        (str(r["job_id"]), int(r["attempt"]))
        for r in await conn.fetch(
            f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger
                WHERE flow_id = $1 AND step_key = $2 AND status = 'fenced'""",
            flow_id,
            BOUNDARY_STEP,
        )
    }
    assert len(corpse_ledger_claims) > 0
    assert {
        (job_id, attempt) for job_id, attempt in fenced_claims if job_id in corpse_records
    } == corpse_ledger_claims

    await tag_run_rows(pool, schema, flow_id, _TAG)
    census = await conn.fetchrow(
        f"""SELECT count(*)::bigint AS run_rows,
                   count(*) FILTER (WHERE tags @> ARRAY[$2::text])::bigint AS tagged
            FROM "{schema}".jobs WHERE (metadata->>'flow_id')::uuid = $1::uuid""",
        flow_id,
        _TAG,
    )
    assert census is not None and census["run_rows"] == census["tagged"] > 0
    await assert_balanced(sys_ledger, schema, _TAG)
'''


def _write_lane(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str, source: str) -> Path:
    path = tmp_path / f"{name}.py"
    path.write_text(source, encoding="utf-8")
    monkeypatch.setattr(claims_mod, "_LANE", path)
    return path


def _segment(source: str, node: ast.AST) -> str:
    return ast.get_source_segment(source, node) or ""


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _parse(path: Path) -> tuple[str, ast.Module]:
    source = path.read_text(encoding="utf-8")
    return source, ast.parse(source, filename=str(path))


def _function_def(tree: ast.Module, name: str) -> ast.FunctionDef | ast.AsyncFunctionDef | None:
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == name:
            return node
    return None


def test_crash_lane_claim_gate_deterministically_holds_or_freezes_victim_before_sigkill() -> None:
    """The kill window must deterministically hold or freeze the victim's
    ledgered record claim across ``SIGKILL`` with no awaited DB round trip
    between gate observation and the freeze/kill signal."""
    lane_src, lane_tree = _parse(_LANE_PATH)
    app_src, app_tree = _parse(_WF_APP_PATH)

    calls = [node for node in ast.walk(lane_tree) if isinstance(node, ast.Call)]
    sigkill_calls = [
        call
        for call in calls
        if _call_name(call) == "send_signal" and "SIGKILL" in _segment(lane_src, call)
    ]
    assert sigkill_calls, "the crash lane has no send_signal(SIGKILL) call"
    sigkill_line = min(call.lineno for call in sigkill_calls)

    # Locate the pre-kill claim gate (_wait_for or loop on locked_by_worker + BOUNDARY_STEP).
    gate_lines: list[int] = []
    for node in ast.walk(lane_tree):
        lineno = getattr(node, "lineno", sigkill_line)
        if lineno >= sigkill_line:
            continue
        if isinstance(node, ast.Call) and _call_name(node) == "_wait_for":
            seg = claims_mod._predicate_segment(lane_src, lane_tree, node)
        elif isinstance(node, (ast.While, ast.For, ast.AsyncFor)):
            seg = _segment(lane_src, node)
        else:
            continue
        if "locked_by_worker" in seg and "BOUNDARY_STEP" in seg:
            gate_lines.append(lineno)
    assert gate_lines, "the crash lane has no pre-kill claim gate before SIGKILL"
    gate_line = min(gate_lines)

    # Mechanism A: deterministic record release gate in _boundary_record_body,
    # seeded closed before SIGKILL and opened only after SIGKILL.
    record_body = _function_def(app_tree, "_boundary_record_body")
    assert record_body is not None, "_wf_app.py no longer defines _boundary_record_body"
    record_gate_tokens: set[str] = set()
    for node in ast.walk(record_body):
        if isinstance(node, ast.Call) and _call_name(node) == "_await_test_release" and node.args:
            record_gate_tokens.add(_segment(app_src, node.args[0]).strip())

    def _gate_arg(call: ast.Call) -> str:
        if len(call.args) >= 3:
            return _segment(lane_src, call.args[2]).strip()
        for kw in call.keywords:
            if kw.arg == "gate":
                return _segment(lane_src, kw.value).strip()
        return ""

    record_gate_seeded_before_kill = bool(record_gate_tokens) and any(
        _call_name(call) == "seed_e2e_release_gate"
        and call.lineno < sigkill_line
        and _gate_arg(call) in record_gate_tokens
        for call in calls
    )
    record_gate_opened_before_kill = bool(record_gate_tokens) and any(
        _call_name(call) == "open_e2e_release_gate"
        and call.lineno < sigkill_line
        and _gate_arg(call) in record_gate_tokens
        for call in calls
    )
    record_gate_opened_after_kill = bool(record_gate_tokens) and any(
        _call_name(call) == "open_e2e_release_gate"
        and call.lineno > sigkill_line
        and _gate_arg(call) in record_gate_tokens
        for call in calls
    )
    has_record_release_gate = (
        record_gate_seeded_before_kill
        and record_gate_opened_after_kill
        and not record_gate_opened_before_kill
    )

    # Mechanism B: deterministic OS freeze (SIGSTOP + stop confirmation +
    # snapshot/check while stopped before SIGKILL, with SIGCONT on miss).
    sigstop_calls = [
        call
        for call in calls
        if _call_name(call) == "send_signal"
        and "SIGSTOP" in _segment(lane_src, call)
        and call.lineno < sigkill_line
    ]
    confirms_stop = "WUNTRACED" in lane_src or "/proc/" in lane_src
    has_sigstop_freeze = bool(sigstop_calls) and confirms_stop

    assert has_record_release_gate or has_sigstop_freeze, (
        "the crash lane neither parks a record cohort on a release gate across "
        "SIGKILL nor freezes the victim with SIGSTOP + stop confirmation before "
        "SIGKILL; polling EXISTS against a no-op record body is a TOCTOU race"
    )

    # No awaited DB round trip may sit between the claim gate observation and
    # the first freeze/kill signal (SIGSTOP if present, else SIGKILL).
    first_signal_line = min(c.lineno for c in sigstop_calls) if sigstop_calls else sigkill_line
    intervening_db_awaits = [
        node
        for node in ast.walk(lane_tree)
        if isinstance(node, ast.Await)
        and gate_line < node.lineno < first_signal_line
        and "conn." in _segment(lane_src, node)
    ]
    assert not intervening_db_awaits, (
        "an awaited DB round trip sits between the claim gate and the freeze/kill "
        f"signal: {[_segment(lane_src, n) for n in intervening_db_awaits]}"
    )


def test_vacuity_pin_rejects_gate_without_ledger_join_or_running_status(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The non-zero claim pin must require the pre-kill gate to join
    ``wf_step_ledger`` and filter the ledger row's ``running`` status."""
    _write_lane(tmp_path, monkeypatch, "baseline", _CANONICAL_LANE)
    claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 1: drop the wf_step_ledger join from the gate (jobs-only EXISTS).
    jobs_only_gate = re.sub(
        r'SELECT 1 FROM "\{schema\}"\.jobs j.*?AND l\.status = \'running\'',
        (
            'SELECT 1 FROM "{schema}".jobs j '
            "WHERE j.locked_by_worker = $3::uuid AND j.step_key = $2 "
            "AND j.status = 'running'"
        ),
        _CANONICAL_LANE,
        count=1,
        flags=re.DOTALL,
    )
    assert "wf_step_ledger" not in jobs_only_gate.split("corpse_records")[0]
    _write_lane(tmp_path, monkeypatch, "jobs_only_gate", jobs_only_gate)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 2: keep the ledger join, drop the l.status = 'running' predicate.
    no_ledger_status = _CANONICAL_LANE.replace(" AND l.status = 'running'", "", 1)
    assert no_ledger_status != _CANONICAL_LANE
    _write_lane(tmp_path, monkeypatch, "no_ledger_status", no_ledger_status)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()


def test_vacuity_pin_rejects_deleted_corpse_ledger_claims_nonempty_asserts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The non-zero claim pin must fail when either or both non-empty asserts
    on ``corpse_ledger_claims`` are deleted, and must accept the same guard
    written as a bare truthiness check: the pin holds the non-vacuity
    contract, not one AST spelling of it."""
    _write_lane(tmp_path, monkeypatch, "baseline", _CANONICAL_LANE)
    claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    target = "    assert len(corpse_ledger_claims) > 0\n"
    assert _CANONICAL_LANE.count(target) == 2

    # Mutant 1: delete BOTH non-empty asserts on corpse_ledger_claims.
    both_deleted = _CANONICAL_LANE.replace(target, "")
    _write_lane(tmp_path, monkeypatch, "both_deleted", both_deleted)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 2: delete ONLY the post-kill non-empty assert.
    first_deleted = _CANONICAL_LANE.replace(target, "", 1)
    _write_lane(tmp_path, monkeypatch, "first_deleted", first_deleted)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 3: delete ONLY the pre-fence non-empty assert.
    head, tail = _CANONICAL_LANE.rsplit(target, 1)
    second_deleted = head + tail
    _write_lane(tmp_path, monkeypatch, "second_deleted", second_deleted)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Acceptance: a bare truthiness guard on the set is the same non-vacuity
    # contract, so both occurrences must keep the pin green.
    truthiness = _CANONICAL_LANE.replace(
        "    assert len(corpse_ledger_claims) > 0", "    assert corpse_ledger_claims"
    )
    assert truthiness != _CANONICAL_LANE
    _write_lane(tmp_path, monkeypatch, "truthiness_guards", truthiness)
    claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()


def test_census_pin_rejects_non_equality_or_non_positive_census_assert(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The census coverage pin must reject ``assert census is not None`` and
    an equality that drops the ``> 0`` non-vacuity bound, and must not be
    satisfied by a disjunction whose truth does not need the equality. The
    same contract written as a plain conjunction must be accepted: the pin
    holds the contract, not one AST spelling of it."""
    _write_lane(tmp_path, monkeypatch, "baseline", _CANONICAL_LANE)
    claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    real_assert = 'assert census is not None and census["run_rows"] == census["tagged"] > 0'
    assert real_assert in _CANONICAL_LANE

    # Mutant 1: bare presence check.
    not_none_only = _CANONICAL_LANE.replace(real_assert, "assert census is not None", 1)
    _write_lane(tmp_path, monkeypatch, "census_not_none", not_none_only)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    # Mutant 2: equality without strict positivity (allows 0 == 0).
    no_positive_bound = _CANONICAL_LANE.replace(
        real_assert,
        'assert census is not None and census["run_rows"] == census["tagged"]',
        1,
    )
    _write_lane(tmp_path, monkeypatch, "census_no_gt_zero", no_positive_bound)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    # Mutant 3: the comparison survives, but inside a disjunction that can
    # satisfy the assertion without it — a vacuous coverage claim.
    disjoined = _CANONICAL_LANE.replace(
        real_assert,
        'assert (census["run_rows"] == census["tagged"] > 0) or True',
        1,
    )
    _write_lane(tmp_path, monkeypatch, "census_disjoined", disjoined)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    # Acceptance: the same contract as a plain conjunction (a safe rewrite)
    # must keep the pin green.
    conjunctive = _CANONICAL_LANE.replace(
        real_assert,
        'assert (\n            census is not None\n            and census["run_rows"] == census["tagged"]\n            and census["tagged"] > 0\n        )',
        1,
    )
    _write_lane(tmp_path, monkeypatch, "census_conjunctive", conjunctive)
    claims_mod.test_census_is_restamped_immediately_before_assert_balanced()


def test_fence_pin_rejects_status_filtered_tautology_and_missing_corpse_intersection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The fence pin must forbid a ``status`` predicate in the corpse ledger
    subset query and require the corpse intersection on the fenced side."""
    _write_lane(tmp_path, monkeypatch, "baseline", _CANONICAL_LANE)
    claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    unfiltered_query = 'WHERE flow_id = $1 AND step_key = $2""",'
    assert _CANONICAL_LANE.count(unfiltered_query) == 1

    pair_eq = """    assert {
        (job_id, attempt) for job_id, attempt in fenced_claims if job_id in corpse_records
    } == corpse_ledger_claims"""
    assert pair_eq in _CANONICAL_LANE

    # Mutant 1: hand-made status='fenced' tautology + id-only equality without
    # corpse intersection.
    tautology_no_intersect = _CANONICAL_LANE.replace(
        unfiltered_query,
        'WHERE flow_id = $1 AND step_key = $2 AND status = \'fenced\'""",',
        1,
    ).replace(
        pair_eq,
        "    assert fenced_claims == corpse_ledger_claims",
        1,
    )
    _write_lane(tmp_path, monkeypatch, "tautology_no_intersect", tautology_no_intersect)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 2: aliased status='fenced' tautology WITH corpse intersection kept
    # (does not accidentally match _fenced_set_names' literal 'SELECT job_id').
    tautology_aliased = _CANONICAL_LANE.replace(
        'SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
        "                WHERE flow_id = $1 AND step_key = $2",
        'SELECT l.job_id, l.attempt FROM "{schema}".wf_step_ledger l\n'
        "                WHERE l.flow_id = $1 AND l.step_key = $2 AND l.status = 'fenced'",
        1,
    )
    assert tautology_aliased != _CANONICAL_LANE
    _write_lane(tmp_path, monkeypatch, "tautology_aliased", tautology_aliased)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 3: status-unfiltered subset, but corpse intersection dropped from
    # the final equality.
    missing_intersection = _CANONICAL_LANE.replace(
        pair_eq,
        "    assert fenced_claims == corpse_ledger_claims",
        1,
    )
    _write_lane(tmp_path, monkeypatch, "missing_intersection", missing_intersection)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 4: the corpse scope hidden behind a named intermediate that does
    # NOT scope to the corpse's rows — the named form is accepted only when its
    # own definition carries the scope.
    unscoped_intermediate = _CANONICAL_LANE.replace(
        pair_eq,
        "    fenced_unscoped = {(job_id, attempt) for job_id, attempt in fenced_claims}\n"
        "    assert fenced_unscoped == corpse_ledger_claims",
        1,
    )
    _write_lane(tmp_path, monkeypatch, "unscoped_intermediate", unscoped_intermediate)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Acceptance: the same contract with the corpse-scoped set named (a safe
    # extraction, and the shipped lane's own shape) must keep the pin green.
    named_intermediate = _CANONICAL_LANE.replace(
        pair_eq,
        "    corpse_scoped_fences = {\n"
        "        (job_id, attempt) for job_id, attempt in fenced_claims if job_id in corpse_records\n"
        "    }\n"
        "    assert corpse_scoped_fences == corpse_ledger_claims",
        1,
    )
    _write_lane(tmp_path, monkeypatch, "named_intermediate", named_intermediate)
    claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Acceptance: the same contract extracted twice — the scope may sit on the
    # intermediate's own definition, not only on the compared name.
    two_level_intermediate = _CANONICAL_LANE.replace(
        pair_eq,
        "    fenced_on_corpse = {\n"
        "        (job_id, attempt) for job_id, attempt in fenced_claims if job_id in corpse_records\n"
        "    }\n"
        "    corpse_scoped_fences = set(fenced_on_corpse)\n"
        "    assert corpse_scoped_fences == corpse_ledger_claims",
        1,
    )
    _write_lane(tmp_path, monkeypatch, "two_level_intermediate", two_level_intermediate)
    claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 5: a two-level chain whose every definition omits the corpse scope
    # — the transitive read must not blanket-accept a named chain.
    unscoped_chain = _CANONICAL_LANE.replace(
        pair_eq,
        "    fenced_unscoped = {(job_id, attempt) for job_id, attempt in fenced_claims}\n"
        "    fenced_renamed = set(fenced_unscoped)\n"
        "    assert fenced_renamed == corpse_ledger_claims",
        1,
    )
    _write_lane(tmp_path, monkeypatch, "unscoped_chain", unscoped_chain)
    with pytest.raises((AssertionError, Failed)):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()


def test_fence_leg_enforces_pair_level_attempt_equality() -> None:
    """The crash lane's fence query must select ``(job_id, attempt)`` and
    compare pairs directly rather than projecting to ``job_id`` alone."""
    source, tree = _parse(_LANE_PATH)

    fenced_assigns = [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign))
        and all(
            tok in _segment(source, node)
            for tok in ("SELECT", "job_id", "wf_step_ledger", "status = 'fenced'")
        )
    ]
    assert fenced_assigns, "no fenced wf_step_ledger assignment found in the crash lane"

    selects_attempt = any(
        re.search(r"SELECT\s+[^;]*\bjob_id\b[^;]*\battempt\b", _segment(source, node), re.I | re.S)
        for node in fenced_assigns
    )
    assert selects_attempt, (
        "the crash lane's fenced query selects job_id without attempt — the "
        "fence LHS is attempt-blind"
    )

    # Confirm the fence equality does not project corpse_ledger_claims down to
    # job_id-only sets.
    corpse_names = claims_mod._corpse_set_names(source, tree)
    ledger_names = claims_mod._assigned_names_matching(
        source, tree, "wf_step_ledger", *corpse_names
    )
    fenced_names = claims_mod._fenced_set_names(source, tree)
    fenced_derived_names = set(fenced_names)
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.Assign, ast.AnnAssign))
            and node.value is not None
            and (claims_mod._names(node.value) & fenced_names)
        ):
            fenced_derived_names |= claims_mod._assigned_names(node)

    pair_equalities = 0
    for assertion in claims_mod._asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare) or not claims_mod._is_eq(test):
            continue
        seg = _segment(source, assertion)
        names = claims_mod._names(test)
        if not (names & fenced_derived_names and names & ledger_names):
            continue
        projects_away_attempt = bool(re.search(r"\{\s*job_id\s+for\s+job_id\s*,\s*_\s+in\b", seg))
        assert not projects_away_attempt, (
            "the fence equality projects corpse_ledger_claims to job_id only, "
            f"dropping the crashed attempt: {seg.splitlines()[0].strip()}"
        )
        pair_equalities += 1

    assert pair_equalities >= 1, (
        "the crash lane has no pair-level equality comparing fenced claims to corpse_ledger_claims"
    )


def _fleet_max_concurrency(path: Path) -> int:
    """The concurrency every worker pod boots with, read from the fleet env."""
    _source, tree = _parse(path)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        for key, value in zip(node.keys, node.values, strict=True):
            if (
                isinstance(key, ast.Constant)
                and key.value == "TASKQ_MAX_CONCURRENCY"
                and isinstance(value, ast.Constant)
                and isinstance(value.value, str)
            ):
                return int(value.value)
    raise AssertionError(
        f"{path.name} no longer declares _BASE_ENV['TASKQ_MAX_CONCURRENCY'] as a "
        "literal; this pin must be re-read"
    )


def _cohort_value(source: str, tree: ast.Module, *, max_concurrency: int) -> int:
    """The parked cohort the record body actually compares against, evaluated
    from its own expression (so a cadence move is read, not assumed)."""
    assignment = next(
        (
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Assign)
            and any(
                isinstance(target, ast.Name) and target.id == "BOUNDARY_RECORD_COHORT"
                for target in node.targets
            )
        ),
        None,
    )
    assert assignment is not None, (
        "_wf_app.py no longer defines BOUNDARY_RECORD_COHORT; the parked cohort "
        "is what forces the victim to hold a claim the SIGKILL cannot miss"
    )

    def evaluate(node: ast.AST) -> int:
        if isinstance(node, ast.Constant) and isinstance(node.value, int):
            assert not isinstance(node.value, bool), "a bool is not an ordinal bound"
            return node.value
        if isinstance(node, ast.BinOp) and isinstance(node.op, (ast.Add, ast.Sub, ast.Mult)):
            left, right = evaluate(node.left), evaluate(node.right)
            if isinstance(node.op, ast.Add):
                return left + right
            if isinstance(node.op, ast.Sub):
                return left - right
            return left * right
        if isinstance(node, ast.Call) and _call_name(node) == "int" and len(node.args) == 1:
            return evaluate(node.args[0])
        if (
            isinstance(node, ast.Subscript)
            and isinstance(node.value, ast.Name)
            and node.value.id == "_BASE_ENV"
        ):
            key = ast.literal_eval(node.slice)
            assert key == "TASKQ_MAX_CONCURRENCY", (
                f"BOUNDARY_RECORD_COHORT reads _BASE_ENV[{key!r}]; the cohort must "
                "derive from the cadence the record slots actually boot with"
            )
            return max_concurrency
        raise AssertionError(
            "BOUNDARY_RECORD_COHORT is no longer an arithmetic expression over "
            f"_BASE_ENV: {_segment(source, node)!r} — this pin must be re-read"
        )

    return evaluate(assignment.value)


def _pods_before_kill(source: str, tree: ast.Module, sigkill_line: int) -> int:
    """The fleet size the lane has booted when it sends the SIGKILL."""
    spawns = [
        call
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and _call_name(call) == "spawn_wf_fleet"
        and call.lineno < sigkill_line
    ]
    assert spawns, (
        "the crash lane no longer spawns a fleet before the SIGKILL; this pin must be re-read"
    )
    pods = 0
    for call in spawns:
        names = call.args[3] if len(call.args) > 3 else None
        assert isinstance(names, (ast.List, ast.Tuple)), (
            "spawn_wf_fleet's name list is no longer a literal; the parked "
            f"cohort cannot be checked against the fleet: {_segment(source, call)[:80]}"
        )
        pods += len(names.elts)
    return pods


def test_parked_record_cohort_forces_a_parked_claim_on_the_victim() -> None:
    """The kill window is deterministic only because the parked cohort is
    larger than every OTHER pod's record capacity: by pigeonhole the victim
    must then hold at least one parked (unfinalizable) claim. Nothing else
    pins that population, so a disabled or undersized park leaves every other
    check green and silently restores the TOCTOU race this change cures.

    Both halves are pinned: the cohort covers the non-victim capacity, and the
    park's guard is a ``record_id < cohort`` comparison over the LOW ordinals
    the claim gate samples."""
    lane_source, lane_tree = _parse(_LANE_PATH)
    app_source, app_tree = _parse(_WF_APP_PATH)
    max_concurrency = _fleet_max_concurrency(_HARNESS_PATH)

    calls = [node for node in ast.walk(lane_tree) if isinstance(node, ast.Call)]
    sigkill_calls = [
        call
        for call in calls
        if _call_name(call) == "send_signal" and "SIGKILL" in _segment(lane_source, call)
    ]
    assert sigkill_calls, "the crash lane has no send_signal(SIGKILL) call"
    sigkill_line = min(call.lineno for call in sigkill_calls)

    pods = _pods_before_kill(lane_source, lane_tree, sigkill_line)
    assert pods >= 2, f"the lane boots {pods} pod(s); the pigeonhole needs a second pod"
    non_victim_capacity = (pods - 1) * max_concurrency
    cohort = _cohort_value(app_source, app_tree, max_concurrency=max_concurrency)
    assert cohort > non_victim_capacity, (
        f"the parked record cohort ({cohort}) does not exceed the other "
        f"{pods - 1} pod(s)' record capacity ({non_victim_capacity} = "
        f"{pods - 1} x TASKQ_MAX_CONCURRENCY {max_concurrency}): the victim is "
        "not forced to hold a parked claim, so the claim gate can miss and the "
        "SIGKILL lands on a window whose claims have already finalized"
    )

    record_body = _function_def(app_tree, "_boundary_record_body")
    assert record_body is not None, "_wf_app.py no longer defines _boundary_record_body"
    parks = [
        node
        for node in ast.walk(record_body)
        if isinstance(node, ast.If)
        and any(
            isinstance(call, ast.Call) and _call_name(call) == "_await_test_release"
            for statement in node.body
            for call in ast.walk(statement)
        )
    ]
    assert parks, (
        "_boundary_record_body parks no record on a release gate — every claim "
        "the kill window stages can finalize, the TOCTOU race returns"
    )
    guard = parks[0].test
    assert isinstance(guard, ast.Compare) and len(guard.ops) == 1, (
        "the record park's guard is no longer a single comparison: "
        f"{_segment(app_source, parks[0].test)}"
    )
    assert isinstance(guard.ops[0], (ast.Lt, ast.LtE)), (
        "the record park no longer gates the LOW ordinals (< cohort): "
        f"{_segment(app_source, parks[0].test)} — the claim gate samples the low "
        "end, so a park of the high ordinals never overlaps its window"
    )
    assert "record_id" in _segment(app_source, guard.left) and "BOUNDARY_RECORD_COHORT" in (
        _segment(app_source, guard.comparators[0])
    ), (
        "the record park's guard no longer compares the record's ordinal against "
        f"BOUNDARY_RECORD_COHORT: {_segment(app_source, parks[0].test)}"
    )

    # If the claim gate still bounds the ordinal it samples, it must sample the
    # SAME parked population; a drifted bound skips the victim's parked claims.
    gate_segments = [
        claims_mod._predicate_segment(lane_source, lane_tree, call)
        for call in calls
        if _call_name(call) == "_wait_for" and call.lineno < sigkill_line
    ]
    gate_segments = [
        segment
        for segment in gate_segments
        if "locked_by_worker" in segment and "BOUNDARY_STEP" in segment
    ]
    assert gate_segments, "the crash lane has no pre-kill claim gate; this pin must be re-read"
    for segment in gate_segments:
        if "map_index" not in segment:
            continue
        assert re.search(r"map_index\s*<\s*\$", segment) and "BOUNDARY_RECORD_COHORT" in segment, (
            "the claim gate bounds the ordinal it samples differently from the "
            "parked cohort (BOUNDARY_RECORD_COHORT): it can observe only "
            "claims outside the parked population and expire"
        )


def test_cohort_pin_kills_a_disabled_or_missized_park(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The cohort pin must red when the park is disabled or undersized.

    Each mutant is the regression the determinism claim cannot survive: no
    record parks, too few park, or the wrong end parks. All four left every
    other pin in this module green before this pin existed."""
    test_parked_record_cohort_forces_a_parked_claim_on_the_victim()

    real_path = _WF_APP_PATH
    original = real_path.read_text(encoding="utf-8")
    cohort_expr = 'BOUNDARY_RECORD_COHORT = 2 * int(_BASE_ENV["TASKQ_MAX_CONCURRENCY"]) - 1'
    park_guard = 'if int(item["record_id"]) < BOUNDARY_RECORD_COHORT:'
    assert cohort_expr in original, "the cohort expression moved; this pin must be re-read"
    assert park_guard in original, "the park guard moved; this pin must be re-read"

    mutants = {
        "cohort_zero": original.replace(cohort_expr, "BOUNDARY_RECORD_COHORT = 0"),
        "cohort_one": original.replace(cohort_expr, "BOUNDARY_RECORD_COHORT = 1"),
        "park_never": original.replace(park_guard, "if False:"),
        "park_high_ordinals": original.replace(
            park_guard, 'if int(item["record_id"]) >= BOUNDARY_RECORD_COHORT:'
        ),
    }
    for name, body in mutants.items():
        assert body != original, f"mutant {name} did not apply"
        path = tmp_path / f"{name}.py"
        path.write_text(body, encoding="utf-8")
        monkeypatch.setattr(sys.modules[__name__], "_WF_APP_PATH", path)
        with pytest.raises((AssertionError, Failed)):
            test_parked_record_cohort_forces_a_parked_claim_on_the_victim()
    monkeypatch.setattr(sys.modules[__name__], "_WF_APP_PATH", real_path)


def test_hold_door_refusal_test_covers_string_and_float_identities() -> None:
    """The hold-door identity refusal test must pin string and float inputs
    alongside negative integers and booleans."""
    source, tree = _parse(_REPAIR_PATH)

    candidates: list[ast.AsyncFunctionDef | ast.FunctionDef] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
            seg = _segment(source, node)
            if "register_hold" in seg and "exact int" in seg:
                candidates.append(node)
    assert candidates, (
        "no hold-door refusal test covering register_hold and 'exact int' found "
        f"in {_REPAIR_PATH.name}"
    )

    def _const_value(expr: ast.AST) -> tuple[bool, Any]:
        if isinstance(expr, ast.Constant):
            return True, expr.value
        if (
            isinstance(expr, ast.UnaryOp)
            and isinstance(expr.op, ast.USub)
            and isinstance(expr.operand, ast.Constant)
            and isinstance(expr.operand.value, (int, float))
            and type(expr.operand.value) is not bool
        ):
            return True, -expr.operand.value
        if isinstance(expr, ast.Call) and _call_name(expr) == "cast" and len(expr.args) == 2:
            return _const_value(expr.args[1])
        return False, None

    def _collect_bad_values(fn: ast.AST) -> list[Any]:
        values: list[Any] = []
        for node in ast.walk(fn):
            # Pattern 1: (bad, "exact int") or (bad, "non-negative") pairs
            if isinstance(node, ast.Tuple) and len(node.elts) == 2:
                val_node, match_node = node.elts
                if (
                    isinstance(match_node, ast.Constant)
                    and isinstance(match_node.value, str)
                    and match_node.value in {"exact int", "non-negative"}
                ):
                    ok, val = _const_value(val_node)
                    if ok:
                        values.append(val)
            # Pattern 2: direct register_hold(..., map_index=<const>)
            if isinstance(node, ast.Call) and _call_name(node) in {
                "register_hold",
                "register_timer_hold",
            }:
                for kw in node.keywords:
                    if kw.arg == "map_index":
                        ok, val = _const_value(kw.value)
                        if ok:
                            values.append(val)
        return values

    bad_values: list[Any] = []
    for fn in candidates:
        bad_values.extend(_collect_bad_values(fn))

    has_str = any(type(v) is str for v in bad_values)
    has_float = any(type(v) is float for v in bad_values)
    assert has_str, (
        "the hold-door refusal test does not exercise a string map_index "
        f"(observed constant inputs: {bad_values!r})"
    )
    assert has_float, (
        "the hold-door refusal test does not exercise a float map_index "
        f"(observed constant inputs: {bad_values!r})"
    )
