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

#: The real shipped files. Mutant helpers read these, never the monkeypatched
#: module pointers, so several mutants in one test each start from the shipped
#: bytes rather than from the previous mutant.
_SHIPPED_LANE_PATH = _LANE_PATH
_SHIPPED_WF_APP_PATH = _WF_APP_PATH

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


#: The shipped lane's exact anchors. Mutation teeth apply these to the bytes
#: read from ``_SHIPPED_LANE_PATH`` so a shipped-lane regression reds here, not
#: only a replica one.
_SHIPPED_GATE_JOIN = '''                                     JOIN "{schema}".wf_step_ledger l
                                       ON l.flow_id = $1::uuid AND l.step_key = $2
                                      AND l.job_id = j.id AND l.attempt = j.attempt
                            WHERE j.locked_by_worker = $3::uuid AND j.step_key = $2
                              AND j.status = 'running' AND l.status = 'running'
                              AND j.map_index < $4::bigint)""",'''
_SHIPPED_GATE_JOBS_ONLY = '''                            WHERE j.locked_by_worker = $3::uuid AND j.step_key = $2
                              AND j.status = 'running'
                              AND j.map_index < $4::bigint)""",'''

_SHIPPED_KILL_LEDGER_GUARD = """        assert len(corpse_ledger_claims) > 0, (
            "the corpse's ledger-claimed subset is empty at the kill — the "
            "fence leg would compare ∅ == ∅ (corpse claims: "
            f"{sorted(corpse_records)})"
        )
"""
_SHIPPED_FENCE_LEDGER_GUARD = """        assert len(corpse_ledger_claims) > 0, (
            "the corpse's wf_step_ledger claim subset is empty at the fence "
            "leg: the equality below would be vacuous"
        )
"""

_SHIPPED_CENSUS_ASSERT = """        assert census is not None and census["run_rows"] == census["tagged"] > 0, (
            f"the tagged census does not cover the run's rows: {dict(census) if census else None}"
        )
"""

_SHIPPED_FENCE_SCOPED = (
    "        corpse_scoped_fences = {pair for pair in fenced_pairs if pair[0] in corpse_records}"
)
_SHIPPED_FENCE_ASSERT = "        assert corpse_scoped_fences == corpse_ledger_claims, ("


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
    ``wf_step_ledger`` and filter the ledger row's ``running`` status.

    Both mutants are applied to the SHIPPED lane, so a gate regression there
    reds the pin rather than only a change to the replica."""
    claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 1: drop the wf_step_ledger join from the gate (jobs-only EXISTS).
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_GATE_JOIN,
        new=_SHIPPED_GATE_JOBS_ONLY,
    )
    with pytest.raises((AssertionError, Failed), match=r"(?i)ledger|join"):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 2: keep the ledger join, drop the l.status = 'running' predicate.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=" AND l.status = 'running'",
        new="",
    )
    with pytest.raises((AssertionError, Failed), match=r"(?i)ledger|status|join"):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()


def test_vacuity_pin_rejects_deleted_corpse_ledger_claims_nonempty_asserts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The non-zero claim pin must fail when either or both non-empty asserts
    on ``corpse_ledger_claims`` are deleted, and must accept the same guard
    written as a bare truthiness check: the pin holds the non-vacuity
    contract, not one AST spelling of it.

    The deletions target the SHIPPED lane's two message-carrying asserts; the
    canonical truthiness rewrite is a portability acceptance check."""
    claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 1: delete BOTH non-empty asserts on corpse_ledger_claims.
    both_deleted = _SHIPPED_LANE_PATH.read_text(encoding="utf-8")
    assert both_deleted.count(_SHIPPED_KILL_LEDGER_GUARD) == 1
    assert both_deleted.count(_SHIPPED_FENCE_LEDGER_GUARD) == 1
    both_deleted = both_deleted.replace(_SHIPPED_KILL_LEDGER_GUARD, "").replace(
        _SHIPPED_FENCE_LEDGER_GUARD, ""
    )
    _write_lane(tmp_path, monkeypatch, "both_deleted", both_deleted)
    with pytest.raises((AssertionError, Failed), match=r"(?i)subset|empty|vacu|guard"):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 2: delete ONLY the kill-side (pre-_wait_terminal) non-empty assert.
    first_deleted = _SHIPPED_LANE_PATH.read_text(encoding="utf-8").replace(
        _SHIPPED_KILL_LEDGER_GUARD, "", 1
    )
    _write_lane(tmp_path, monkeypatch, "first_deleted", first_deleted)
    with pytest.raises((AssertionError, Failed), match=r"(?i)subset|empty|vacu|guard"):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Mutant 3: delete ONLY the fence-side (post-_wait_terminal) assert.
    second_deleted = _SHIPPED_LANE_PATH.read_text(encoding="utf-8").replace(
        _SHIPPED_FENCE_LEDGER_GUARD, "", 1
    )
    _write_lane(tmp_path, monkeypatch, "second_deleted", second_deleted)
    with pytest.raises((AssertionError, Failed), match=r"(?i)subset|empty|vacu|guard"):
        claims_mod.test_lane_forces_a_nonzero_corpse_claim_set()

    # Acceptance: a bare truthiness guard on the set is the same non-vacuity
    # contract, so both occurrences must keep the pin green.
    _write_lane(tmp_path, monkeypatch, "canonical", _CANONICAL_LANE)
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
    holds the contract, not one AST spelling of it.

    The mutants rewrite the SHIPPED census assertion; the plain conjunction is
    a portability acceptance check on the canonical lane."""
    claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    shipped = _SHIPPED_LANE_PATH.read_text(encoding="utf-8")
    assert shipped.count(_SHIPPED_CENSUS_ASSERT) == 1

    # Mutant 1: bare presence check.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_CENSUS_ASSERT,
        new="        assert census is not None\n",
    )
    with pytest.raises(
        (AssertionError, Failed), match=r"(?i)census|coverage|positive|equality|vacu"
    ):
        claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    # Mutant 2: equality without strict positivity (allows 0 == 0).
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_CENSUS_ASSERT,
        new='        assert census is not None and census["run_rows"] == census["tagged"]\n',
    )
    with pytest.raises(
        (AssertionError, Failed), match=r"(?i)census|coverage|positive|equality|vacu"
    ):
        claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    # Mutant 3: the comparison survives, but inside a disjunction that can
    # satisfy the assertion without it — a vacuous coverage claim.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_CENSUS_ASSERT,
        new='        assert (census["run_rows"] == census["tagged"] > 0) or True\n',
    )
    with pytest.raises(
        (AssertionError, Failed), match=r"(?i)census|coverage|positive|equality|vacu"
    ):
        claims_mod.test_census_is_restamped_immediately_before_assert_balanced()

    # Acceptance: the same contract as a plain conjunction (a safe rewrite)
    # must keep the pin green.
    real_assert = 'assert census is not None and census["run_rows"] == census["tagged"] > 0'
    assert real_assert in _CANONICAL_LANE
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
    subset query and require the corpse intersection on the fenced side.

    The mutants rewrite the SHIPPED lane's status-unfiltered subset query and
    its named-intermediate fence shape; the canonical variants are
    portability acceptance checks for sound alternative spellings."""
    claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 1: status='fenced' tautology on the corpse ledger subset.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_CORPSE_SUBSET_SQL,
        new='                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
        "                    WHERE flow_id = $1 AND step_key = $2"
        " AND status = 'fenced'"
        '""",',
    )
    with pytest.raises(
        (AssertionError, Failed), match=r"(?i)status\s+predicate|status[- ]filtered|tautolog"
    ):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 2: the same tautology with a table alias — the status rule must
    # reject it regardless of qualification, not misread it as a fenced query.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_CORPSE_SUBSET_SQL,
        new='                f"""SELECT l.job_id, l.attempt FROM "{schema}".wf_step_ledger l\n'
        "                    WHERE l.flow_id = $1 AND l.step_key = $2"
        " AND l.status = 'fenced'"
        '""",',
    )
    with pytest.raises(
        (AssertionError, Failed), match=r"(?i)status\s+predicate|status[- ]filtered|tautolog"
    ):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 3: status-unfiltered subset, but the corpse intersection dropped
    # from the fenced side of the equality.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_FENCE_SCOPED,
        new="        corpse_scoped_fences = set(fenced_pairs)",
    )
    with pytest.raises((AssertionError, Failed), match=r"(?i)scoped|corpse|fenced side"):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 4: the corpse scope hidden behind a named intermediate that does
    # NOT scope to the corpse's rows — the named form is accepted only when its
    # own definition carries the scope.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_FENCE_SCOPED + "\n" + _SHIPPED_FENCE_ASSERT,
        new="        fenced_unscoped = set(fenced_pairs)\n"
        "        assert fenced_unscoped == corpse_ledger_claims, (",
    )
    with pytest.raises((AssertionError, Failed), match=r"(?i)scoped|corpse|fenced side"):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Mutant 5: a two-level chain whose every definition omits the corpse scope
    # — the transitive read must not blanket-accept a named chain.
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_FENCE_SCOPED + "\n" + _SHIPPED_FENCE_ASSERT,
        new="        fenced_unscoped = set(fenced_pairs)\n"
        "        fenced_renamed = set(fenced_unscoped)\n"
        "        assert fenced_renamed == corpse_ledger_claims, (",
    )
    with pytest.raises((AssertionError, Failed), match=r"(?i)scoped|corpse|fenced side"):
        claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    # Acceptance: the canonical inline equality is a sound spelling.
    _write_lane(tmp_path, monkeypatch, "canonical", _CANONICAL_LANE)
    claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()

    pair_eq = """    assert {
        (job_id, attempt) for job_id, attempt in fenced_claims if job_id in corpse_records
    } == corpse_ledger_claims"""
    assert pair_eq in _CANONICAL_LANE

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


_ATTEMPT_COLUMN_RE = re.compile(r"^(?:\w+\s*\.\s*)?attempt$", re.IGNORECASE)

#: A set comprehension that keeps only ``job_id`` from a pair iterable — the
#: fence-side spelling that drops the crashed attempt.
_JOB_ID_ONLY_COMPREHENSION_RE = re.compile(
    r"\{\s*(?:str\s*\(\s*)?job_id\s*(?:\))?\s+for\s+job_id\s*,\s*_\s+in\b"
)


def _unwrap_single_value(node: ast.expr) -> ast.expr:
    """The single value a cast/one-tuple wrapper carries (``str(p[0])``)."""
    while True:
        if isinstance(node, ast.Call) and len(node.args) == 1 and not node.keywords:
            node = node.args[0]
            continue
        if isinstance(node, ast.Tuple) and len(node.elts) == 1:
            node = node.elts[0]
            continue
        return node


def _pair_projection_elements(source: str, node: ast.AST) -> list[str]:
    """The comprehensions that project a ``(job_id, attempt)`` pair to one field.

    ``{pair[0] for pair in fenced_pairs}`` keeps the id and drops the crashed
    attempt, so the equality it feeds is attempt-blind even though BOTH queries
    still select the ``attempt`` column and build real pairs. Only the
    comprehension's ELEMENT is read, never its filter: the shipped
    ``{pair for pair in fenced_pairs if pair[0] in corpse_records}`` scopes by
    id and keeps the whole pair, so it is sound. A single-field read is
    recognised however it is wrapped (``str(pair[0])``, ``(pair[0],)``,
    ``pair[:1]``), and an unpacking comprehension that keeps only its first
    target (``{job_id for job_id, _attempt in ...}``) is refused the same way:
    both compare the id alone. A dynamic subscript (``pairs[key]``) is left
    alone: it indexes a collection, not the pair.
    """
    found: list[str] = []
    for item in ast.walk(node):
        if not isinstance(item, (ast.SetComp, ast.ListComp, ast.GeneratorExp)):
            continue
        element = _unwrap_single_value(item.elt)
        if isinstance(element, ast.Subscript) and isinstance(
            element.slice, (ast.Constant, ast.Slice)
        ):
            found.append(_segment(source, item) or _segment(source, element))
            continue
        target = item.generators[0].target if item.generators else None
        if isinstance(target, ast.Tuple) and len(target.elts) == 2:
            first, second = target.elts
            if isinstance(first, ast.Name) and isinstance(second, ast.Name):
                names = claims_mod._names(item.elt)
                if first.id in names and second.id not in names:
                    found.append(_segment(source, item) or _segment(source, element))
    return found


def _select_list(segment: str) -> list[str] | None:
    """The ``SELECT`` projection expressions of the first query in *segment*."""
    match = re.search(r"SELECT\s+(.*?)\s+FROM\b", segment, re.IGNORECASE | re.DOTALL)
    if match is None:
        return None
    return [part.strip() for part in match.group(1).split(",")]


def _projects_attempt_column(segment: str) -> bool:
    """Whether the query projects a real ``attempt`` column reference.

    ``1 AS attempt`` and other literal expressions are not a column read: the
    fence would then compare against a constant on that side."""
    projections = _select_list(segment)
    return bool(projections) and any(_ATTEMPT_COLUMN_RE.match(expr) for expr in projections)


def _is_literal(expr: ast.AST) -> bool:
    if isinstance(expr, ast.Constant):
        return True
    if isinstance(expr, ast.UnaryOp) and isinstance(expr.op, (ast.USub, ast.UAdd)):
        return _is_literal(expr.operand)
    return False


def test_fence_leg_enforces_pair_level_attempt_equality() -> None:
    """The crash lane's fence query must select ``(job_id, attempt)`` and
    compare pairs directly rather than projecting to ``job_id`` alone.

    The fenced query is located structurally (alias-tolerant, either operand
    order) through the shared provenance helper, so a table alias on correct
    SQL keeps this pin green."""
    source, tree = _parse(_LANE_PATH)

    fenced_assigns = list(claims_mod._fenced_query_assignments(source, tree).values())
    assert fenced_assigns, "no fenced wf_step_ledger assignment found in the crash lane"

    for node in fenced_assigns:
        segment = _segment(source, node)
        assert _projects_attempt_column(segment), (
            "the crash lane's fenced query projects a constant or job_id-only "
            "attempt instead of the attempt column — the fence LHS is attempt-blind: "
            f"{segment.splitlines()[0].strip()}"
        )
        assert any(
            isinstance(item, ast.Subscript)
            and isinstance(item.slice, ast.Constant)
            and item.slice.value == "attempt"
            for item in ast.walk(node.value)  # pyright: ignore[reportArgumentType]  # Why: assignments selected above always carry a value.
        ), (
            "the fenced pairs are not built from the row's attempt field: "
            f"{segment.splitlines()[0].strip()}"
        )

    # Confirm the fence equality does not project corpse_ledger_claims down to
    # job_id-only sets.
    corpse_names = claims_mod._corpse_set_names(source, tree)
    ledger_names = claims_mod._assigned_names_matching(
        source, tree, "wf_step_ledger", *corpse_names
    )
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        if not (_assigned_names := claims_mod._assigned_names(node)) & ledger_names:
            continue
        segment = _segment(source, node)
        assert _projects_attempt_column(segment), (
            "the corpse's ledger-claimed subset projects job_id without attempt "
            f"— the fence RHS is attempt-blind: {segment.splitlines()[0].strip()}"
        )
        for item in ast.walk(node):
            if not isinstance(item, ast.Tuple) or len(item.elts) != 2:
                continue
            assert not _is_literal(item.elts[1]), (
                "the corpse's ledger-claimed subset pairs a job id with a "
                f"constant attempt: {_segment(source, item)}"
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

    definitions: dict[str, ast.Assign | ast.AnnAssign] = {
        name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and node.value is not None
        for name in claims_mod._assigned_names(node)
    }

    def _definition_nodes(names: set[str]) -> list[ast.AST]:
        """The transitive definition nodes of *names* (cycle-safe)."""
        seen: set[str] = set()
        pending = list(names)
        nodes: list[ast.AST] = []
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            node = definitions.get(name)
            if node is None or node.value is None:
                continue
            nodes.append(node)
            pending.extend(claims_mod._names(node.value))
        return nodes

    pair_equalities = 0
    for assertion in claims_mod._asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare) or not claims_mod._is_eq(test):
            continue
        seg = _segment(source, assertion)
        names = claims_mod._names(test)
        if not (names & fenced_derived_names and names & ledger_names):
            continue
        relevant = names & (fenced_derived_names | ledger_names)
        definitions_reachable = [test, *_definition_nodes(relevant)]
        offending = [
            candidate
            for candidate in [
                seg,
                *(_segment(source, node) for node in definitions_reachable),
            ]
            if _JOB_ID_ONLY_COMPREHENSION_RE.search(candidate)
        ]
        assert not offending, (
            "the fence equality projects corpse_ledger_claims to job_id only, "
            f"dropping the crashed attempt: {seg.splitlines()[0].strip()}"
        )
        projected = [
            element
            for node in definitions_reachable
            for element in _pair_projection_elements(source, node)
        ]
        assert not projected, (
            "the fence equality compares a pair projected down to one field "
            "(pair[0]), so a fence at a WRONG attempt on a corpse row still "
            f"passes: {projected} in {seg.splitlines()[0].strip()}"
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

    def _is_direct_release(statement: ast.stmt) -> bool:
        return (
            isinstance(statement, ast.Expr)
            and isinstance(statement.value, ast.Await)
            and isinstance(statement.value.value, ast.Call)
            and _call_name(statement.value.value) == "_await_test_release"
        )

    parks = [
        statement
        for statement in record_body.body
        if isinstance(statement, ast.If) and any(_is_direct_release(s) for s in statement.body)
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

    # If the claim gate bounds the ordinal it samples, it must sample the SAME
    # parked population; a deleted or drifted bound skips the victim's parked
    # claims. Resolve the ``$N`` placeholder to the call's own argument so the
    # shipped ``$4`` binding of BOUNDARY_RECORD_COHORT is accepted and a
    # drifted argument or SQL-side arithmetic is refused.
    gate_calls = [
        (call, call.args[0])
        for call in calls
        if _call_name(call) == "_wait_for"
        and call.lineno < sigkill_line
        and call.args
        and "locked_by_worker" in claims_mod._predicate_segment(lane_source, lane_tree, call)
        and "BOUNDARY_STEP" in claims_mod._predicate_segment(lane_source, lane_tree, call)
    ]
    assert gate_calls, "the crash lane has no pre-kill claim gate; this pin must be re-read"
    for _gate_call, predicate in gate_calls:
        predicate_body: list[ast.AST] = [predicate]
        if isinstance(predicate, ast.Name):
            predicate_def = next(
                (
                    node
                    for node in ast.walk(lane_tree)
                    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and node.name == predicate.id
                ),
                None,
            )
            assert predicate_def is not None, (
                f"the claim gate predicate {predicate.id!r} has no definition; "
                "this pin must be re-read"
            )
            predicate_body = [predicate_def]
        fetches = [
            node
            for body in predicate_body
            for node in ast.walk(body)
            if isinstance(node, ast.Call)
            and _call_name(node) in {"fetch", "fetchval", "fetchrow"}
            and node.args
        ]
        assert fetches, (
            "the claim gate predicate no longer runs a DB query; this pin must be re-read"
        )
        for fetch in fetches:
            sql = _segment(lane_source, fetch.args[0])
            bound = re.search(
                r"\bmap_index\s*<\s*\$(\d+)(?:::[A-Za-z_][A-Za-z_0-9 ]*)?", sql, re.IGNORECASE
            )
            assert bound is not None, (
                "the claim gate does not bound the ordinal it samples by the "
                "parked cohort (map_index < $N): it can observe only claims "
                "outside the parked population and expire"
            )
            tail = sql[bound.end() :].lstrip()
            assert tail[:1] not in {"+", "-", "*", "/"}, (
                "the claim gate's cohort bound carries arithmetic after the "
                f"$N placeholder, so it samples a drifted population: {sql}"
            )
            placeholder = int(bound.group(1))
            assert len(fetch.args) > placeholder, (
                f"the claim gate binds ${placeholder} with no matching argument: "
                f"{_segment(lane_source, fetch)}"
            )
            argument = _segment(lane_source, fetch.args[placeholder]).strip()
            assert argument == "BOUNDARY_RECORD_COHORT", (
                "the claim gate bounds the ordinal it samples differently from "
                f"the parked cohort (BOUNDARY_RECORD_COHORT): it binds ${placeholder} "
                f"to {argument!r}"
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
        with pytest.raises(
            (AssertionError, Failed), match=r"(?i)park|cohort|ordinal|record|release"
        ):
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


#: The shipped lane's two `wf_step_ledger` reads, exactly as written. Mutants
#: anchor on these substrings so a case that stops applying is a loud red, not
#: a silent skip.
_SHIPPED_CORPSE_SUBSET_SQL = (
    '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
    '                    WHERE flow_id = $1 AND step_key = $2""",'
)
_SHIPPED_FENCED_SQL = (
    '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
    "                    WHERE flow_id = $1 AND step_key = $2 AND status = 'fenced'"
    '"""'
)
_SHIPPED_CORPSE_INTERSECTION = "& {(job_id, corpse_records[job_id]) for job_id in corpse_records}"


def _shipped_mutant(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    target: str,
    old: str,
    new: str,
) -> None:
    """Apply *old* -> *new* to the SHIPPED lane/app bytes and point the pins at
    the mutated file. A pin that only holds a replica does not hold the
    shipped contract."""
    source_path = _SHIPPED_LANE_PATH if target == "lane" else _SHIPPED_WF_APP_PATH
    source = source_path.read_text(encoding="utf-8")
    assert old in source, f"mutant anchor moved in the shipped {target}: {old!r}"
    mutant = source.replace(old, new, 1)
    assert mutant != source, f"mutant did not change the shipped {target}: {old!r}"
    path = tmp_path / f"mutated-{target}.py"
    path.write_text(mutant, encoding="utf-8")
    if target == "lane":
        monkeypatch.setattr(claims_mod, "_LANE", path)
        monkeypatch.setattr(sys.modules[__name__], "_LANE_PATH", path)
        # Prove the pins that read this target will see the mutant bytes: a
        # "DID NOT RAISE" must mean the predicate accepted the mutant, never
        # that the source pointer still aimed at the shipped file.
        assert claims_mod._LANE.read_text(encoding="utf-8") == mutant
        assert Path(sys.modules[__name__]._LANE_PATH).read_text(encoding="utf-8") == mutant
    else:
        monkeypatch.setattr(sys.modules[__name__], "_WF_APP_PATH", path)
        assert Path(sys.modules[__name__]._WF_APP_PATH).read_text(encoding="utf-8") == mutant


@pytest.mark.parametrize(
    ("target", "old", "new", "pin", "diagnostic"),
    [
        pytest.param(
            "lane",
            "assert len(corpse_ledger_claims) > 0",
            "assert len(corpse_ledger_claims) >= 0",
            claims_mod.test_lane_forces_a_nonzero_corpse_claim_set,
            r"(?i)empty|vacu|truthy|strict",
            id="nonempty-guard-accepts-ge-zero",
        ),
        pytest.param(
            "lane",
            "assert len(corpse_records) > 0",
            "assert len(corpse_records) >= 0",
            claims_mod.test_lane_forces_a_nonzero_corpse_claim_set,
            r"(?i)empty|vacu|truthy|strict",
            id="corpse-records-guard-accepts-ge-zero",
        ),
        pytest.param(
            "lane",
            "assert len(corpse_ledger_claims) > 0",
            "assert len(corpse_ledger_claims) > 0 or True",
            claims_mod.test_lane_forces_a_nonzero_corpse_claim_set,
            r"(?i)empty|vacu|truthy|strict",
            # Already rejected: the ledger guard requires a top-level Compare,
            # so a disjunction is not accepted. Guard for the repair.
            id="nonempty-guard-inside-disjunction",
        ),
        pytest.param(
            "lane",
            "assert len(corpse_records) > 0",
            "assert len(corpse_records) > 0 or True",
            claims_mod.test_lane_forces_a_nonzero_corpse_claim_set,
            r"(?i)empty|vacu|truthy|strict",
            id="corpse-records-guard-inside-disjunction",
        ),
        pytest.param(
            "lane",
            _SHIPPED_CORPSE_SUBSET_SQL,
            '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
            "                    WHERE flow_id = $1 AND step_key = $2"
            " AND 'fenced' = status"
            '""",',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)status\s+predicate|status[- ]filtered|tautolog",
            id="corpse-subset-reversed-status-tautology",
        ),
        pytest.param(
            "lane",
            _SHIPPED_CORPSE_SUBSET_SQL,
            '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
            "                    WHERE flow_id = $1 AND step_key = $2"
            " AND status IS NOT DISTINCT FROM 'fenced'"
            '""",',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)status\s+predicate|status[- ]filtered|tautolog",
            id="corpse-subset-is-not-distinct-status-tautology",
        ),
        pytest.param(
            # Any status predicate on the subset makes it a tautology, and
            # ``NOT IN`` is the spelling a maintainer reaches for to drop the
            # already-finalized rows: it narrows the subset to the fenced
            # population just as ``= 'fenced'`` does.
            "lane",
            _SHIPPED_CORPSE_SUBSET_SQL,
            '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
            "                    WHERE flow_id = $1 AND step_key = $2"
            " AND status NOT IN ('running')"
            '""",',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)status\s+predicate|status[- ]filtered|tautolog",
            id="corpse-subset-not-in-status-tautology",
        ),
        pytest.param(
            "lane",
            _SHIPPED_CORPSE_SUBSET_SQL,
            '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
            "                    WHERE flow_id = $1 AND step_key = $2"
            " AND status LIKE 'fenced%'"
            '""",',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)status\s+predicate|status[- ]filtered|tautolog",
            id="corpse-subset-like-status-tautology",
        ),
        pytest.param(
            "lane",
            _SHIPPED_CORPSE_SUBSET_SQL,
            '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
            "                    WHERE flow_id = $1 AND step_key = $2"
            " AND status BETWEEN 'fenced' AND 'fenced'"
            '""",',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)status\s+predicate|status[- ]filtered|tautolog",
            id="corpse-subset-between-status-tautology",
        ),
        pytest.param(
            "lane",
            _SHIPPED_FENCED_SQL,
            '                f"""SELECT job_id, 1 AS attempt FROM "{schema}".wf_step_ledger\n'
            "                    WHERE flow_id = $1 AND step_key = $2 AND status = 'fenced'"
            '"""',
            test_fence_leg_enforces_pair_level_attempt_equality,
            r"(?i)attempt",
            id="fenced-side-constant-attempt",
        ),
        pytest.param(
            "lane",
            _SHIPPED_CORPSE_INTERSECTION,
            "& {(job_id, 1) for job_id in corpse_records}",
            test_fence_leg_enforces_pair_level_attempt_equality,
            r"(?i)attempt",
            id="corpse-side-constant-attempt",
        ),
        pytest.param(
            # The comparison-level escape the column-level checks cannot see:
            # both queries still select and build (job_id, attempt) pairs, but
            # the EQUALITY projects each pair down to its id, so a fence at a
            # WRONG attempt on a corpse row lands in both sets and passes.
            "lane",
            _SHIPPED_FENCE_SCOPED + "\n" + _SHIPPED_FENCE_ASSERT,
            _SHIPPED_FENCE_SCOPED
            + "\n        assert {pair[0] for pair in corpse_scoped_fences} == "
            "{pair[0] for pair in corpse_ledger_claims}, (",
            test_fence_leg_enforces_pair_level_attempt_equality,
            r"(?i)projected|attempt",
            id="fence-equality-projects-pairs-to-job-id",
        ),
        pytest.param(
            # The same attempt-blind equality spelled with a NAMED ignored
            # second field: ``{job_id for job_id, _attempt in ...}``. The
            # ``_``-only regex and the constant-index reader both miss it.
            "lane",
            _SHIPPED_FENCE_SCOPED + "\n" + _SHIPPED_FENCE_ASSERT,
            _SHIPPED_FENCE_SCOPED
            + "\n        assert {job_id for job_id, _attempt in corpse_scoped_fences} == "
            "{job_id for job_id, _attempt in corpse_ledger_claims}, (",
            test_fence_leg_enforces_pair_level_attempt_equality,
            r"(?i)projected|attempt",
            id="fence-equality-projects-pairs-to-job-id-named-unpack",
        ),
        pytest.param(
            # The same projection with the id cast before it is kept:
            # ``{str(pair[0]) for pair in ...}``.
            "lane",
            _SHIPPED_FENCE_SCOPED + "\n" + _SHIPPED_FENCE_ASSERT,
            _SHIPPED_FENCE_SCOPED
            + "\n        assert {str(pair[0]) for pair in corpse_scoped_fences} == "
            "{str(pair[0]) for pair in corpse_ledger_claims}, (",
            test_fence_leg_enforces_pair_level_attempt_equality,
            r"(?i)projected|attempt",
            id="fence-equality-projects-pairs-to-cast-job-id",
        ),
        pytest.param(
            "lane",
            "                              AND j.map_index < $4::bigint)",
            "                              )",
            test_parked_record_cohort_forces_a_parked_claim_on_the_victim,
            r"(?i)cohort|map_index|ordinal",
            id="gate-cohort-bound-removed",
        ),
        pytest.param(
            # A present-but-drifted SQL-side bound: the placeholder resolves to
            # the cohort, but arithmetic on it samples a different population.
            "lane",
            "                              AND j.map_index < $4::bigint)",
            "                              AND j.map_index < $4::bigint + 1)",
            test_parked_record_cohort_forces_a_parked_claim_on_the_victim,
            r"(?i)cohort|map_index|ordinal|bound",
            id="gate-cohort-bound-drifted-arithmetic",
        ),
        pytest.param(
            # A present-but-drifted bound: the SQL still carries `map_index <
            # $4`, but the gate's $4 argument is no longer the parked cohort.
            "lane",
            "                    BOUNDARY_RECORD_COHORT,\n",
            "                    BOUNDARY_RECORD_COHORT + 1,\n",
            test_parked_record_cohort_forces_a_parked_claim_on_the_victim,
            r"(?i)cohort|map_index|ordinal|bound",
            id="gate-cohort-bound-drifted-argument",
        ),
        pytest.param(
            "app",
            '    if int(item["record_id"]) < BOUNDARY_RECORD_COHORT:\n'
            "        await _await_test_release(BOUNDARY_RECORD_GATE)",
            '    if int(item["record_id"]) < BOUNDARY_RECORD_COHORT:\n'
            "        if False:\n"
            "            await _await_test_release(BOUNDARY_RECORD_GATE)",
            test_parked_record_cohort_forces_a_parked_claim_on_the_victim,
            r"(?i)park|release|nest|guard",
            id="park-nested-under-if-false",
        ),
        pytest.param(
            # The CORPSE subset (the ledger-claimed side of the fence
            # equality), never the fenced-pairs query: filtering the fenced
            # query by status = 'fenced' is its definition, so that mutant is
            # a semantics-preserving alias, not a tautology. This case
            # therefore mutates the status-UNFILTERED corpse subset into the
            # aliased tautology and requires the status-predicate diagnostic.
            "lane",
            _SHIPPED_CORPSE_SUBSET_SQL,
            '                f"""SELECT l.job_id, l.attempt FROM "{schema}".wf_step_ledger l\n'
            "                    WHERE l.flow_id = $1 AND l.step_key = $2"
            " AND l.status = 'fenced'"
            '""",',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)status\s+predicate|status[- ]filtered|tautolog",
            id="aliased-fenced-status-tautology-names-status",
        ),
        pytest.param(
            # Positive provenance: the fenced assignment MUST keep its
            # status = 'fenced' filter. Dropping it would let ordinary
            # running ledger rows into the fenced comparison. This is an
            # acceptance guard (the literal reader already reds); it exists
            # so the alias-tolerant reader cannot be relaxed into matching
            # any wf_step_ledger SELECT that projects job_id.
            "lane",
            _SHIPPED_FENCED_SQL,
            '                f"""SELECT job_id, attempt FROM "{schema}".wf_step_ledger\n'
            '                    WHERE flow_id = $1 AND step_key = $2"""',
            claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset,
            r"(?i)fenced|provenance|status",
            id="fenced-query-loses-its-status-filter",
        ),
    ],
)
def test_shipped_file_mutants_red_the_pin_that_names_them(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
    old: str,
    new: str,
    pin: Any,
    diagnostic: str,
) -> None:
    """Every semantic mutant of the SHIPPED lane/app must red its own pin.

    Each case applies one mutant to the bytes read from the shipped file,
    points the selected pin at it, and asserts the pin fails with a diagnostic
    naming the mutated property (never an incidental provenance failure). The
    re-anchored mutation tests in this module mutate the same shipped bytes;
    ``_CANONICAL_LANE`` remains only for portability acceptance checks of
    sound alternative spellings.

    Three of these mutants already red their pin before the pin's own repair
    and are guards against a repair that loosens the contract: the alias-qualified corpse
    tautology (the status rule already rejects it), the top-level ledger
    disjunction (the ledger guard already requires a top-level comparison), and
    the fenced query losing its ``status = 'fenced'`` filter (positive
    provenance must survive alias tolerance)."""
    _shipped_mutant(tmp_path, monkeypatch, target=target, old=old, new=new)
    with pytest.raises((AssertionError, Failed), match=diagnostic):
        pin()


def test_aliased_fenced_query_keeps_both_fence_pins_green(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A table alias on the fenced query is a semantics-preserving rewrite.

    The colloquially correct fenced query *must* filter ``status = 'fenced'``
    — that is the set's definition. Both the provenance reader
    (``_fenced_set_names``) and the pair-equality pin identify it by literal
    ``SELECT job_id`` / ``status = 'fenced'`` tokens, so ``SELECT l.job_id,
    l.attempt ... AND l.status = 'fenced'`` makes the provenance assertion
    false-red (and the pair-equality pin find no fenced assignment). The pins
    must identify the query structurally and stay GREEN, because a legitimate
    refactor of correct SQL is not a regression."""
    _shipped_mutant(
        tmp_path,
        monkeypatch,
        target="lane",
        old=_SHIPPED_FENCED_SQL,
        new='                f"""SELECT l.job_id, l.attempt FROM "{schema}".wf_step_ledger l\n'
        "                    WHERE l.flow_id = $1 AND l.step_key = $2"
        " AND l.status = 'fenced'"
        '"""',
    )
    claims_mod.test_fence_set_is_the_corpse_ledger_claim_subset()
    test_fence_leg_enforces_pair_level_attempt_equality()
