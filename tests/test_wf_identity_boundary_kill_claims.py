"""The crash lane's own set/census claims must be backed by its mechanism.

The pins below are the executable statement of the lane's set/census
contract. Each one reds on a shape that claims more than the mechanism
delivers — a shape that false-reds correct behaviour, or that lets a vacuous
assertion pass:

* the fenced-phantom set equated to *every* ``running`` record claim the
  corpse held. A record row is claimed ``running`` by the worker's producer
  (``taskq.worker.run`` / the ``api/_runner`` claim before
  ``claim_step_ledger``) before the workflow ledger has a row for it, and
  ``PHANTOM_REAP_SQL`` fences *ledger* rows only. A claimed-but-unledgered
  record therefore has no phantom to fence, so that equality false-reds
  correct behaviour (measured: ``corpse_records=2 fenced_ids=1``);
* a census tagged once, right after ``create_flow``, while the records are
  emitted later: ``assert_balanced`` then runs over the two pre-emit rows
  only (measured: ``tagged_before_emit=2 total_rows=7 tagged_after_emit=2``),
  so the emitted population is outside the conservation invariants;
* the retry leg equating the re-executed set (``attempt > 1``) with the
  corpse's in-flight claims. The attempt counter increments at claim
  (``backend/_dispatch_sql.py``: ``attempt = LEAST(j.attempt + 1, 32767)``),
  and the machinery's own at-least-once reclaim (a lapsed lease handed back
  mid-flight, a heartbeat-lost isolation, a repend with backoff) re-executes
  rows the corpse never held — so that equality false-reds correct behaviour
  the first time the set legs run with ``n > 0`` (measured: ``retried (3) !=
  corpse_records (2)``, zero lost, one terminal per identity).

The defects those shapes would reintroduce live in the lane's assertions, not
in the product; the lane itself satisfies every contract below. The pins read
the lane's own AST, so a regression to any of those shapes reds here rather
than as a false red or a vacuous green in the lane.

The checks identify the sets by the QUERY they come from, not by their
variable names, so a repair that renames ``corpse_records`` or ``fenced_ids``
still passes as long as the sets' provenance is the same.

WHY EACH PIN CAN FAIL (and what makes it pass):

* ``test_fence_set_is_the_corpse_ledger_claim_subset`` reds on
  ``assert fenced_ids == corpse_records`` and passes only when the fence
  comparison targets the corpse's ``wf_step_ledger``-claimed subset while the
  unledgered claims are allowed to have no phantom. It also fails when the subset's own
  query carries a ``status`` predicate (a ``status = 'fenced'`` tautology —
  the subset can only ever equal whatever the filter selects) or when the
  fenced side of the equality is not scoped to the corpse's rows (a fence at
  a wrong attempt on a corpse row would be filtered away instead of failing);
* ``test_census_is_restamped_immediately_before_assert_balanced`` reds unless
  a ``tag_run_rows`` call follows the terminal wait and sits inside the window
  before ``assert_balanced``, and an assertion reads that stamped census with
  a positive equality (two reads of the census compared equal AND strictly
  above zero — a bare presence check or an unbounded equality discharges
  nothing), and ``assert_balanced`` follows;
* ``test_lane_forces_a_nonzero_corpse_claim_set`` reds unless a pre-kill gate
  waits for a record claim before the SIGKILL and the lane asserts the
  snapshot is non-empty, so both set legs are exercised. The gate itself must
  join
  ``wf_step_ledger`` and filter the ledger row's ``running`` status on the
  join's own alias — a jobs-only gate or a status-blind join accepts a claim
  that can already have finalized — and the lane must guard the corpse's
  ledger-claimed subset non-empty both before ``_wait_terminal`` (the
  snapshot leg's guard) and after it (the fence leg's guard), so neither leg
  can run on an empty subset;
* ``test_retry_leg_does_not_equate_attempt_gt_1_to_the_corpse_set`` reds on
  ``assert retried == corpse_records`` and passes only when the retry leg
  demands the zero-loss direction (``set(corpse_records) - retried`` empty)
  and bounds the machinery's extras instead of equating the two sets. It
  names the instructed equality shape; a set-equality written in another
  form is not detected by it.
"""

from __future__ import annotations

import ast
import re
from itertools import pairwise
from pathlib import Path

import pytest

_LANE = Path(__file__).with_name("system_e2e") / "test_wf_identity_boundary_kill.py"

#: A ``wf_step_ledger`` join and its alias (``wf_step_ledger l``), read out of
#: the join text so an alias rename survives.
_LEDGER_JOIN_RE = re.compile(r"\bwf_step_ledger(?:\s+(?:AS\s+)?(\w+))?", re.IGNORECASE)

#: A status predicate in SQL text (``status = 'fenced'``, ``l.status IN (...)``
#: — a comparison or membership test, never the bare word, so prose inside the
#: expression cannot trip it).
_STATUS_PREDICATE_RE = re.compile(
    r"\bstatus\b\s*(?:::\s*[\w\s]+?)?\s*(?:=|<>|!=|IN\b)", re.IGNORECASE
)

#: The maximum gap between the census stamp and ``assert_balanced``. The
#: harness requires the stamp "immediately before" the invariant read; this
#: window leaves room for the coverage assertion between them.
_CENSUS_WINDOW_LINES = 30


def _lane_source() -> tuple[str, ast.Module]:
    source = _LANE.read_text(encoding="utf-8")
    return source, ast.parse(source, filename=str(_LANE))


def _segment(source: str, node: ast.AST) -> str:
    return ast.get_source_segment(source, node) or ""


def _names(node: ast.AST) -> set[str]:
    return {n.id for n in ast.walk(node) if isinstance(n, ast.Name)}


def _assigned_names(node: ast.Assign | ast.AnnAssign) -> set[str]:
    targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    names: set[str] = set()
    for target in targets:
        for item in ast.walk(target):
            if isinstance(item, ast.Name):
                names.add(item.id)
    return names


def _assigned_names_matching(source: str, tree: ast.Module, *tokens: str) -> set[str]:
    """Names assigned from an expression whose source contains every *token*."""
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            segment = _segment(source, node)
            if all(token in segment for token in tokens):
                names |= _assigned_names(node)
    return names


def _call_name(call: ast.Call) -> str:
    func = call.func
    if isinstance(func, ast.Name):
        return func.id
    if isinstance(func, ast.Attribute):
        return func.attr
    return ""


def _asserts(tree: ast.Module) -> list[ast.Assert]:
    return [node for node in ast.walk(tree) if isinstance(node, ast.Assert)]


def _predicate_segment(source: str, tree: ast.Module, call: ast.Call) -> str:
    """The wait's predicate source, following a named predicate to its def."""
    if not call.args:
        return ""
    predicate = call.args[0]
    segment = _segment(source, predicate)
    if isinstance(predicate, ast.Name):
        for node in ast.walk(tree):
            if (
                isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                and node.name == predicate.id
            ):
                segment += "\n" + _segment(source, node)
    return segment


def _eq_sides(compare: ast.Compare) -> list[tuple[ast.expr, ast.expr]]:
    """Every adjacent operand pair of a chained comparison."""
    operands = [compare.left, *compare.comparators]
    return list(pairwise(operands))


def _is_eq(compare: ast.Compare) -> bool:
    return any(isinstance(op, ast.Eq) for op in compare.ops)


def _corpse_set_names(source: str, tree: ast.Module) -> set[str]:
    """The snapshot of the victim's running record rows, by query provenance."""
    return _assigned_names_matching(
        source,
        tree,
        "SELECT id",
        "locked_by_worker",
        "status = 'running'",
    )


def _fenced_set_names(source: str, tree: ast.Module) -> set[str]:
    """The fenced ``wf_step_ledger`` job ids, by query provenance."""
    return _assigned_names_matching(
        source,
        tree,
        "SELECT job_id",
        "wf_step_ledger",
        "status = 'fenced'",
    )


def _retried_set_names(source: str, tree: ast.Module) -> set[str]:
    """The re-executed record set (``attempt > 1``), by query provenance."""
    return _assigned_names_matching(source, tree, "SELECT id", "attempt > 1")


def test_fence_set_is_the_corpse_ledger_claim_subset() -> None:
    source, tree = _lane_source()
    corpse_names = _corpse_set_names(source, tree)
    assert corpse_names, (
        "the lane no longer snapshots the corpse's running record claims "
        "(a query on locked_by_worker + status = 'running' + BOUNDARY_STEP)"
    )
    fenced_names = _fenced_set_names(source, tree)
    assert fenced_names, (
        "the lane no longer reads the fenced wf_step_ledger job ids "
        "(SELECT job_id ... status = 'fenced')"
    )

    # The subset's own contract: derived from the corpse's ledger rows, and
    # carrying no status predicate — a status-filtered subset is a tautology
    # (it can only ever equal whatever the filter selects, so the fence
    # equality proves nothing).
    ledger_names = _assigned_names_matching(source, tree, "wf_step_ledger", *corpse_names)
    assert ledger_names, (
        "the lane no longer derives a ledger-claimed subset from the corpse's "
        "own wf_step_ledger rows; the fence equality has no sound target"
    )
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and (_assigned_names(node) & ledger_names):
            status_match = _STATUS_PREDICATE_RE.search(_segment(source, node))
            if status_match:
                pytest.fail(
                    "the corpse's wf_step_ledger claim subset is status-filtered "
                    f"({status_match.group(0).strip()}) — a tautology: the subset "
                    "can only ever equal whatever the filter selects, so the "
                    "fence equality proves nothing"
                )

    # A named intermediate set is the same contract as the inline
    # comprehension: ``corpse_scoped_fences = {... fenced ... if ... corpse
    # ...}`` then compared. Read the fenced side through its own definition
    # (transitively) so extracting the comprehension into a name — the natural
    # cleanup of the duplicated inline form — is not a red on sound code.
    fenced_derived: dict[str, ast.Assign | ast.AnnAssign] = {}
    fenced_all = set(fenced_names)
    while True:
        added = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)) or node.value is None:
                continue
            if not (_names(node.value) & fenced_all):
                continue
            for name in _assigned_names(node) - fenced_all:
                fenced_derived[name] = node
                fenced_all.add(name)
                added = True
        if not added:
            break

    def _derived_segments(expr: ast.expr) -> list[str]:
        """The definition segments reachable from *expr*'s fenced-derived names.

        Transitive for the same reason the name set is: an extraction of an
        extraction is the same contract, so the corpse scope may sit on the
        intermediate's own definition.
        """
        seen: set[str] = set()
        pending = [name for name in _names(expr) if name in fenced_derived]
        segments: list[str] = []
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            node = fenced_derived[name]
            segments.append(_segment(source, node))
            pending.extend(
                nested
                for nested in _names(node.value)  # pyright: ignore[reportArgumentType]  # Why: entries are assigned nodes with a value (guarded above).
                if nested in fenced_derived and nested not in seen
            )
        return segments

    def _corpse_scoped(expr: ast.expr) -> bool:
        segment = _segment(source, expr)
        if _names(expr) & corpse_names or any(name in segment for name in corpse_names):
            return True
        return any(
            corpse_name in definition
            for definition in _derived_segments(expr)
            for corpse_name in corpse_names
        )

    # The unsound equality: the fenced ledger rows are the corpse's *record
    # claims*, but a claimed-but-unledgered claim has no ledger row to fence.
    for assertion in _asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare) or not _is_eq(test):
            continue
        for left, right in _eq_sides(test):
            if _names(left) & fenced_all and _names(right) & corpse_names:
                pytest.fail(
                    "the lane equates the fenced phantom set to EVERY corpse "
                    "record claim. A row claimed running before the ledger "
                    "claim has no wf_step_ledger row for PHANTOM_REAP_SQL to "
                    "fence, so this false-reds correct behaviour: "
                    f"{_segment(source, assertion).splitlines()[0].strip()}"
                )
            if _names(left) & corpse_names and _names(right) & fenced_all:
                pytest.fail(
                    "the lane equates the corpse's every record claim to the "
                    "fenced phantom set; claimed-but-unledgered records have "
                    "no phantom: "
                    f"{_segment(source, assertion).splitlines()[0].strip()}"
                )

    # The sound target: a subset derived from the corpse's own ledger rows,
    # either assigned and then compared, or selected inline in the assert —
    # and the fenced side must itself be scoped to the corpse's rows, so a
    # fence at a wrong attempt on a corpse row lands in the compared set
    # and fails loudly instead of being filtered away.
    for assertion in _asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare) or not _is_eq(test):
            continue
        for left, right in _eq_sides(test):
            for fence_side, other in ((left, right), (right, left)):
                if not (_names(fence_side) & fenced_all):
                    continue
                if not _corpse_scoped(fence_side):
                    pytest.fail(
                        "the fenced side of the fence equality is not scoped to "
                        "the corpse's rows — a fence at a wrong attempt on a "
                        "corpse row (or a fence of a non-corpse row) would never "
                        "land in the compared set: "
                        f"{_segment(source, assertion).splitlines()[0].strip()}"
                    )
                other_segment = _segment(source, other)
                if _names(other) & ledger_names or (
                    "wf_step_ledger" in other_segment
                    and (
                        _names(other) & corpse_names
                        or any(name in other_segment for name in corpse_names)
                    )
                ):
                    return

    pytest.fail(
        "the lane has no fence assertion against the corpse's wf_step_ledger "
        "claim subset; fenced ids must be compared to the ledger rows the "
        "corpse actually held, not to every running record claim"
    )


def test_census_is_restamped_immediately_before_assert_balanced() -> None:
    source, tree = _lane_source()
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]

    balanced = [call for call in calls if _call_name(call) == "assert_balanced"]
    assert len(balanced) == 1, (
        f"expected exactly one assert_balanced call in the lane, found {len(balanced)}"
    )
    balanced_line = balanced[0].lineno

    terminal_waits = [call for call in calls if _call_name(call) == "_wait_terminal"]
    assert terminal_waits, "the lane no longer calls _wait_terminal; this pin must be re-read"

    stamps = [call for call in calls if _call_name(call) == "tag_run_rows"]
    before = [call for call in stamps if call.lineno < balanced_line]
    assert before, (
        "the lane never calls tag_run_rows before assert_balanced, so the "
        "invariant population is whatever an earlier stamp happened to catch"
    )
    nearest = max(before, key=lambda call: call.lineno)
    assert nearest.lineno > max(call.lineno for call in terminal_waits), (
        "the census stamp precedes the run's terminal wait; records emitted "
        "after the stamp stay outside the invariant population"
    )
    assert balanced_line - nearest.lineno <= _CENSUS_WINDOW_LINES, (
        "the census stamp is not immediately before assert_balanced: the "
        f"stamp is at line {nearest.lineno}, the invariant read at {balanced_line} "
        f"(window {_CENSUS_WINDOW_LINES})"
    )

    # The stamp must be accompanied by a read that proves the population
    # actually covers the run — a positive equality over the tagged census:
    # two reads of the census compared equal AND bounded strictly above
    # zero. A bare presence check or an unbounded equality discharges
    # nothing (0 == 0 passes both).
    tagged_targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.Assign, ast.AnnAssign)) and "tags @>" in _segment(source, node):
            tagged_targets |= _assigned_names(node)
    assert tagged_targets, (
        "the lane never assigns the tagged census (a query on tags @>); "
        "the coverage assertion has nothing to read"
    )

    def _conjunctive_compares(node: ast.AST) -> list[ast.Compare]:
        """The ``Compare`` leaves of an AND-conjunction.

        A disjunction yields none: a branch that can satisfy the assertion
        without the equality (``... or True``) proves nothing, so it must
        not discharge the coverage contract. Flattening the conjunction
        keeps a semantically identical rewrite — ``a == b and b > 0`` —
        accepted beside the chained ``a == b > 0``.
        """
        if isinstance(node, ast.BoolOp) and isinstance(node.op, ast.And):
            leaves: list[ast.Compare] = []
            for value in node.values:
                leaves.extend(_conjunctive_compares(value))
            return leaves
        if isinstance(node, ast.Compare):
            return [node]
        return []

    def _covers_run(assertion: ast.Assert) -> bool:
        eq_targets: set[str] = set()
        positive = False
        for node in _conjunctive_compares(assertion.test):
            operands = [node.left, *node.comparators]
            for left, op, right in zip(operands[:-1], node.ops, operands[1:], strict=True):
                left_targets = _names(left) & tagged_targets
                right_targets = _names(right) & tagged_targets
                if isinstance(op, ast.Eq) and left_targets and right_targets:
                    eq_targets |= left_targets | right_targets
                if (
                    isinstance(op, ast.Gt)
                    and left_targets
                    and isinstance(right, ast.Constant)
                    and right.value == 0
                ):
                    positive = True
        return bool(eq_targets) and positive

    for assertion in _asserts(tree):
        if _covers_run(assertion):
            return

    pytest.fail(
        "the lane re-stamps the census but never asserts that the tagged "
        "population covers the run's rows with a positive equality "
        "(tagged == run_rows > 0); a bare presence check or an unbounded "
        "equality discharges nothing"
    )


def test_lane_forces_a_nonzero_corpse_claim_set() -> None:
    source, tree = _lane_source()
    calls = [node for node in ast.walk(tree) if isinstance(node, ast.Call)]
    corpse_names = _corpse_set_names(source, tree)
    assert corpse_names, "the lane no longer snapshots the corpse's record claims"

    sigkill_calls = [
        call
        for call in calls
        if _call_name(call) == "send_signal" and "SIGKILL" in _segment(source, call)
    ]
    assert sigkill_calls, "the lane no longer SIGKILLs the holder; this pin must be re-read"
    sigkill_line = min(call.lineno for call in sigkill_calls)

    # A test-side gate must make the victim hold a record when it dies. The
    # gate is a wait before the signal, reading the record claim by worker
    # — and it must be a claim that cannot already have finalized: the gate
    # joins wf_step_ledger and filters the ledger row's 'running' status on
    # the join's own alias (read from the join text, so an alias rename
    # survives; a jobs-only gate or a status-blind join does not).
    def _is_claim_candidate(node: ast.AST) -> str | None:
        if isinstance(node, ast.Call) and _call_name(node) == "_wait_for":
            return _predicate_segment(source, tree, node)
        if isinstance(node, (ast.While, ast.For, ast.AsyncFor)):
            return _segment(source, node)
        return None

    def _has_claim_tokens(segment: str) -> bool:
        return "locked_by_worker" in segment and "BOUNDARY_STEP" in segment

    def _has_ledger_running_predicate(segment: str) -> bool:
        for match in _LEDGER_JOIN_RE.finditer(segment):
            alias = match.group(1) or ""
            if not alias or alias.upper() in {"AS", "ON", "WHERE", "JOIN", "LEFT", "INNER"}:
                alias = "wf_step_ledger"
            if re.search(rf"\b{re.escape(alias)}\.status\s*=\s*'running'", segment, re.IGNORECASE):
                return True
        return False

    candidates = [
        (node, segment)
        for node in ast.walk(tree)
        if getattr(node, "lineno", sigkill_line) < sigkill_line
        and (segment := _is_claim_candidate(node)) is not None
        and _has_claim_tokens(segment)
    ]
    assert candidates, (
        "no test-side gate waits for the victim to hold a record claim before "
        "the SIGKILL; every measured green run killed a corpse with zero "
        "in-flight record claims, so the set legs never ran"
    )
    gates = [
        (node, segment) for node, segment in candidates if _has_ledger_running_predicate(segment)
    ]
    assert gates, (
        "the pre-kill claim gate no longer joins wf_step_ledger and filters "
        "the ledger row's own 'running' status on the join's alias — a "
        "jobs-only gate or a status-blind join accepts a claim that can "
        "already have finalized, so the SIGKILL can miss every claim"
    )

    # And the lane must assert the snapshot is non-empty, not just print it.
    def _has_nonempty_corpse_guard() -> bool:
        for assertion in _asserts(tree):
            test = assertion.test
            segment = _segment(source, assertion)
            if not (_names(test) & corpse_names or any(name in segment for name in corpse_names)):
                continue
            if isinstance(test, ast.Name) and test.id in corpse_names:
                return True
            for node in ast.walk(test):
                if (
                    isinstance(node, ast.Compare)
                    and _names(node) & corpse_names
                    and any(isinstance(op, (ast.Gt, ast.GtE, ast.NotEq)) for op in node.ops)
                ):
                    return True
        return False

    assert _has_nonempty_corpse_guard(), (
        "the lane snapshots the corpse's record claims but never asserts the "
        "set is non-empty; a green that only ever sees n=0 does not exercise "
        "the retry or fence legs"
    )

    # Both non-empty guards on the corpse's ledger-claimed subset, anchored
    # on the legs they protect: one before _wait_terminal (the snapshot
    # leg's guard) and one after it (the fence leg's guard). Anchoring beats
    # a bare count of two: two guards in one leg can leave the fence
    # equality unguarded — the ∅ == ∅ the guard exists to prevent.
    ledger_names = _assigned_names_matching(source, tree, "wf_step_ledger", *corpse_names)
    assert ledger_names, (
        "the lane no longer derives a ledger-claimed subset from the corpse's "
        "own wf_step_ledger rows; the non-empty guards have nothing to anchor on"
    )
    terminal_calls = [call for call in calls if _call_name(call) == "_wait_terminal"]
    assert terminal_calls, "the lane no longer calls _wait_terminal; this pin must be re-read"
    terminal_line = min(call.lineno for call in terminal_calls)

    def _is_nonempty_ledger_guard(assertion: ast.Assert) -> bool:
        test = assertion.test
        # A bare truthiness check names the set and is equally non-vacuous.
        if isinstance(test, ast.Name) and test.id in ledger_names:
            return True
        if not isinstance(test, ast.Compare) or not (_names(test) & ledger_names):
            return False
        return any(
            isinstance(op, (ast.Gt, ast.GtE, ast.NotEq))
            for node in ast.walk(test)
            if isinstance(node, ast.Compare)
            for op in node.ops
        )

    before = [
        assertion
        for assertion in _asserts(tree)
        if assertion.lineno < terminal_line and _is_nonempty_ledger_guard(assertion)
    ]
    after = [
        assertion
        for assertion in _asserts(tree)
        if assertion.lineno > terminal_line and _is_nonempty_ledger_guard(assertion)
    ]
    assert before, (
        "the lane's snapshot leg no longer guards the corpse's ledger-claimed "
        "subset non-empty before _wait_terminal; the fence leg could compare "
        "an empty subset (∅ == ∅) without this pin redding"
    )
    assert after, (
        "the lane's fence leg no longer re-asserts the corpse's ledger-claimed "
        "subset non-empty after _wait_terminal; the fence equality below it "
        "could be vacuous"
    )


def test_retry_leg_does_not_equate_attempt_gt_1_to_the_corpse_set() -> None:
    source, tree = _lane_source()
    corpse_names = _corpse_set_names(source, tree)
    assert corpse_names, "the lane no longer snapshots the corpse's record claims"
    retried_names = _retried_set_names(source, tree)
    assert retried_names, (
        "the lane no longer derives the re-executed record set from its own "
        "attempt > 1 query; this pin must be re-read"
    )

    for assertion in _asserts(tree):
        test = assertion.test
        if not isinstance(test, ast.Compare):
            continue
        operands = [test.left, *test.comparators]
        for left, op, right in zip(operands[:-1], test.ops, operands[1:], strict=True):
            left_retried = bool(_names(left) & retried_names)
            right_retried = bool(_names(right) & retried_names)
            left_corpse = bool(_names(left) & corpse_names)
            right_corpse = bool(_names(right) & corpse_names)
            over_claims = (
                (
                    isinstance(op, ast.Eq)
                    and ((left_retried and right_corpse) or (left_corpse and right_retried))
                )
                or (isinstance(op, ast.LtE) and left_retried and right_corpse)
                or (isinstance(op, ast.GtE) and left_corpse and right_retried)
            )
            if not over_claims:
                continue
            pytest.fail(
                "the lane demands that the re-executed record set (attempt > 1) "
                "be exactly the corpse's in-flight claims. The reclaim "
                "machinery's own at-least-once path (a lapsed lease handed "
                "back mid-flight, a heartbeat-lost isolation, a repend) "
                "re-executes rows the corpse never held — the attempt counter "
                "increments at claim — so this false-reds correct behaviour "
                "(measured: retried (3) != corpse_records (2), zero lost). Keep "
                "the zero-loss direction (corpse claims ⊆ retried) and bound "
                "the machinery's extras: "
                f"{_segment(source, assertion).splitlines()[0].strip()}"
            )
