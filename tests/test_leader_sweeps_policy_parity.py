"""The parity pin for the prune-family leader sweeps' shared POLICY SCAFFOLD.

``_prune_loop`` and ``_archive_expiry_loop`` are two instantiations of ONE
policy: a once-per-successful-attempt-per-day date latch, a failure-only
retry ladder, a batch-size circuit breaker sized from the dispatcher pool,
a drain gate with a demotion clause, a leaderless-miss arm, a session
advisory lock, and a backstop guard — each holding the same constants, in
the same wiring order, at the same control-flow sites. The two loops
differ ONLY in the sweep operation they drive (the retention delete vs
the archive expiry) and in that sweep's own preps and post-success
cleanup, which are by design.

The alternative — extracting the shared scaffold into one parameterized
loop — was REJECTED: the demotion-cut / unfinished-day latch semantics and
the ``no await between the gate's last False and the ``leading()`` read''
invariant are pinned by chaos tests
(``tests/test_attack_leader_gaps.py``), and a parameterization that
subtly reorders one await would pass every unit test while breaking the
race contract. So the doctrine is mirror-maintenance, and mirror-
maintenance fails silently: the next edit to one loop that forks a
constant, drops an arm, or moves an await would diverge the policies
without any test noticing.

These tests are that noticing. They parse both loops' AST, normalize the
loop-specific names (``last_pruned_date`` vs ``last_expiry_date``,
``prune_terminal_jobs`` vs ``archive_expiry_sweep``, the event names, ...)
to canonical placeholders, and assert the scaffolds are line-for-line
IDENTICAL modulo the whitelisted by-design segments. An edit that forks
the policies reds here; the fix is to mirror the edit (or, if genuinely
by design, extend the whitelist explicitly — a decision that then shows
up in review).
"""

import ast
import copy
from pathlib import Path

import pytest

from taskq.worker import _leader_sweeps

_MODULE_PATH = Path(_leader_sweeps.__file__)

# Loop-specific identifiers → canonical placeholders. Anything not renamed
# here is scaffold the pin requires to be token-identical across the loops.
_PRUNE_RENAMES: dict[str, str] = {
    "last_pruned_date": "LAST_DONE_DATE",
    "prune_sizer": "SWEEP_SIZER",
    "prune_terminal_jobs": "SWEEP_OP",
    "prune_cron_expr": "SWEEP_CRON_EXPR",
    "prune_schedule_utc": "SWEEP_SCHEDULE_UTC",
    "retention_per_status": "SWEEP_PREP_ARG_1",
    "actor_overrides": "SWEEP_PREP_ARG_2",
    "_build_retention_per_status": "SWEEP_PREP_1",
    "_load_actor_retention_overrides": "SWEEP_PREP_2",
    "prune_old_batches": "POST_SUCCESS_CLEANUP",
    "batch_count": "POST_SUCCESS_CLEANUP_COUNT",
    "cutoff": "COMPLETED_DETAIL",
    "cutoffs": "COMPLETED_CUTOFFS",
    "pruned_date": "DONE_DATE",
}
_ARCHIVE_RENAMES: dict[str, str] = {
    "last_expiry_date": "LAST_DONE_DATE",
    "expiry_sizer": "SWEEP_SIZER",
    "archive_expiry_sweep": "SWEEP_OP",
    "archive_expiry_cron_expr": "SWEEP_CRON_EXPR",
    "archive_expiry_schedule_utc": "SWEEP_SCHEDULE_UTC",
    "expired_date": "DONE_DATE",
    "expire_before": "COMPLETED_DETAIL",
}
# Loop-specific string literals (loop names, kinds, event names) →
# canonical placeholders, same rule as the renames above.
_PRUNE_STRINGS: dict[str, str] = {
    "leader.prune": "LOOP_NAME",
    "prune": "SWEEP_KIND",
    "prune-fire-missed-leaderless": "EV_MISSED_LEADERLESS",
    "prune-drain-cut-demotion": "EV_DRAIN_CUT",
    "prune-completed": "EV_COMPLETED",
    "prune-failed": "EV_FAILED",
    "prune-skipped-advisory-lock-held": "EV_LOCK_HELD",
    "prune-skipped-day-latch": "EV_LATCH_SKIPPED",
    "prune-lock-attempt-failed": "EV_LOCK_ATTEMPT_FAILED",
    "prune_lock_failed": "KIND_LOCK_ATTEMPT_FAILED",
    "batches pruned": "EV_POST_SUCCESS",
    "batch-prune-failed": "EV_POST_SUCCESS_FAILED",
    "batch": "KIND_POST_SUCCESS",
}
_ARCHIVE_STRINGS: dict[str, str] = {
    "leader.archive_expiry": "LOOP_NAME",
    "archive_expiry": "SWEEP_KIND",
    "archive-expiry-fire-missed-leaderless": "EV_MISSED_LEADERLESS",
    "archive-expiry-drain-cut-demotion": "EV_DRAIN_CUT",
    "archive-expiry-completed": "EV_COMPLETED",
    "archive-expiry-failed": "EV_FAILED",
    "archive-expiry-skipped-advisory-lock-held": "EV_LOCK_HELD",
    "archive-expiry-skipped-day-latch": "EV_LATCH_SKIPPED",
    "archive-expiry-lock-attempt-failed": "EV_LOCK_ATTEMPT_FAILED",
    "archive_expiry_lock_failed": "KIND_LOCK_ATTEMPT_FAILED",
}

# The single retry ladder both loops ride: 60 s doubling, capped at
# 30 min, routed ONLY through _next_retry_backoff (the loops never touch
# the constants directly). Pinned verbatim so a fork of the ladder into a
# second function reds the parity pin too, not just the ladder pin.
_LADDER_SOURCE = (
    "if current is None:\n"
    "    return _PRUNE_RETRY_BACKOFF_INITIAL_SECS\n"
    "return min(current * 2.0, _PRUNE_RETRY_BACKOFF_CAP_SECS)"
)

# The shared SweepBatchSizer construction: identical kwarg→setting pairs
# in both loops (the prune family shares one batch bound and one breaker).
_SIZER_SOURCE = (
    "SweepBatchSizer(default_size=ctx.deps.settings.prune_batch_size, "
    "divisor=ctx.deps.settings.event_writer_reduced_batch_divisor, "
    "failure_threshold=ctx.deps.settings.sweep_breaker_failure_threshold, "
    "window_secs=ctx.deps.settings.sweep_breaker_window_secs)"
)

# The shared gate wiring: the per-batch server-side bound derived from the
# dispatcher pool's command timeout, then the gate whose liveness period
# IS that bound, in that order.
_TIMEOUT_SOURCE = (
    "statement_timeout_ms = "
    "_prune_statement_timeout_ms(ctx.deps.settings.dispatcher_command_timeout)"
)
_GATE_SOURCE = (
    "drain_gate = _batch_drain_gate(ctx, shutdown, loop_name='LOOP_NAME', "
    "period_secs=statement_timeout_ms / 1000.0)"
)


def _load_loops() -> tuple[ast.AsyncFunctionDef, ast.AsyncFunctionDef]:
    """Parse the sweeps module and return (_prune_loop, _archive_expiry_loop)."""
    tree = ast.parse(_MODULE_PATH.read_text(encoding="utf-8"))
    fns = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name in ("_prune_loop", "_archive_expiry_loop")
    }
    if set(fns) != {"_prune_loop", "_archive_expiry_loop"}:
        pytest.fail(
            f"the parity pin could not find both policy loops in {_MODULE_PATH}; "
            f"found {sorted(fns)} — if a loop was renamed or moved, repoint the pin"
        )
    return fns["_prune_loop"], fns["_archive_expiry_loop"]


def _norm(node: ast.AST, renames: dict[str, str], strings: dict[str, str]) -> str:
    """Token-normalized unparse: loop-specific names and strings mapped to
    their canonical placeholders, everything else left verbatim."""
    node = copy.deepcopy(node)
    for sub in ast.walk(node):
        if isinstance(sub, ast.Name) and sub.id in renames:
            sub.id = renames[sub.id]
        if isinstance(sub, ast.Attribute) and sub.attr in renames:
            sub.attr = renames[sub.attr]
        if isinstance(sub, ast.arg) and sub.arg in renames:
            sub.arg = renames[sub.arg]
        # The log lines' keyword args (ast.keyword, not ast.arg) — the
        # surfaced day-latch event's per-loop detail field renames through
        # the same map.
        if isinstance(sub, ast.keyword) and sub.arg in renames:
            sub.arg = renames[sub.arg]
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str) and sub.value in strings:
            sub.value = strings[sub.value]
    return ast.unparse(node)


def _by_design(stmt: ast.AST, renames: dict[str, str]) -> bool:
    """Whether a statement (and its subtree) is a whitelisted by-design
    difference: the sweep operation itself and its private preps, the
    per-status completed-log detail field, and the prune-only post-success
    batch-cleanup. Everything else must match across the loops."""
    text = _norm(stmt, renames, {})
    if text.startswith("result = await SWEEP_OP("):
        return True  # the sweep operations — the whole reason there are two loops
    if text.startswith("SWEEP_PREP"):
        return True  # the retention/override preps only the prune family needs
    if text.startswith("max_cutoff = max("):
        return True  # prune's batch-cutoff anchor
    if text.startswith("POST_SUCCESS_CLEANUP_COUNT = "):
        return True  # prune's post-success batch cleanup, archive has none
    return (  # the cleanup try-block itself (its handler family is the
        # batch-cleanup's own; the attempt-body tries have no such handler)
        isinstance(stmt, ast.Try)
        and bool(stmt.handlers)
        and stmt.handlers[0].type is not None
        and "NotImplementedError" in _norm(stmt.handlers[0].type, renames, {})
    )


def _flatten(
    stmts: list[ast.stmt],
    renames: dict[str, str],
    strings: dict[str, str],
    out: list[str],
) -> None:
    """Flatten a statement list to structural lines: compound headers,
    leaf statements, recursion into bodies/handlers/finally. Docstrings
    and comments are dropped (comments are the mirror-maintenance itself;
    their wording is not policy)."""
    for stmt in stmts:
        if (
            isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        ):
            continue  # docstring
        if _by_design(stmt, renames):
            continue
        if isinstance(stmt, ast.While):
            out.append(f"while {_norm(stmt.test, renames, strings)}:")
            _flatten(stmt.body, renames, strings, out)
            _flatten(stmt.orelse, renames, strings, out)
        elif isinstance(stmt, (ast.With, ast.AsyncWith)):
            items = ", ".join(_norm(item, renames, strings) for item in stmt.items)
            kind = "async with" if isinstance(stmt, ast.AsyncWith) else "with"
            out.append(f"{kind} {items}:")
            _flatten(stmt.body, renames, strings, out)
        elif isinstance(stmt, ast.If):
            out.append(f"if {_norm(stmt.test, renames, strings)}:")
            _flatten(stmt.body, renames, strings, out)
            if stmt.orelse:
                out.append("else:")
                _flatten(stmt.orelse, renames, strings, out)
        elif isinstance(stmt, ast.Try):
            out.append("try:")
            _flatten(stmt.body, renames, strings, out)
            for handler in stmt.handlers:
                type_txt = _norm(handler.type, renames, strings) if handler.type else ""
                name_txt = f" as {handler.name}" if handler.name else ""
                out.append(f"except {type_txt}{name_txt}:")
                _flatten(handler.body, renames, strings, out)
            if stmt.finalbody:
                out.append("finally:")
                _flatten(stmt.finalbody, renames, strings, out)
        elif isinstance(stmt, ast.For) and _norm(stmt.iter, renames, strings) == (
            "result.by_status.items()"
        ):
            # The completed-log loop: its HEADER is scaffold (both loops
            # log per-status counts on success); its detail FIELD is sweep-
            # specific by design (prune reports per-status cutoffs, archive
            # a single expire_before), so the body is not compared.
            out.append(
                f"for {_norm(stmt.target, renames, strings)} in "
                f"{_norm(stmt.iter, renames, strings)}:"
            )
        else:
            out.append(_norm(stmt, renames, strings))


def _scaffold_lines(
    fn: ast.AsyncFunctionDef, renames: dict[str, str], strings: dict[str, str]
) -> list[str]:
    out: list[str] = []
    _flatten(fn.body, renames, strings, out)
    return out


def _find_call(fn: ast.AsyncFunctionDef, name: str) -> ast.Call:
    """The single call to *name* inside the loop (the sizer / gate wiring)."""
    calls = [
        sub
        for sub in ast.walk(fn)
        if isinstance(sub, ast.Call)
        and (
            (isinstance(sub.func, ast.Name) and sub.func.id == name)
            or (isinstance(sub.func, ast.Attribute) and sub.func.attr == name)
        )
    ]
    assert len(calls) == 1, f"expected exactly one {name}(...) call in the loop, found {len(calls)}"
    return calls[0]


# ── the pins ────────────────────────────────────────────────────────────


def test_prune_and_archive_expiry_loops_share_the_policy_scaffold() -> None:
    """THE PARITY PIN. Both loops' scaffolds must be token-identical after
    loop-specific renaming, except the whitelisted by-design segments (the
    sweep operation, its preps, its completed-log detail, prune's batch
    cleanup). An edit to ONE loop that forks the shared policy — a changed
    ladder arm, a dropped gate argument, a moved latch stamp — lands here
    as a first mismatch and reds."""
    prune, arch = _load_loops()
    p = _scaffold_lines(prune, _PRUNE_RENAMES, _PRUNE_STRINGS)
    a = _scaffold_lines(arch, _ARCHIVE_RENAMES, _ARCHIVE_STRINGS)
    for i in range(min(len(p), len(a))):
        assert p[i] == a[i], (
            f"the prune-family policy scaffold FORKED at shared segment {i}:\n"
            f"  _prune_loop        : {p[i]}\n"
            f"  _archive_expiry_loop: {a[i]}\n"
            "the two loops are one policy in two instantiations; mirror the edit "
            "across both loops (or, if the difference is genuinely by design, "
            "extend _by_design's whitelist explicitly — and say why in review)"
        )
    assert len(p) == len(a), (
        f"the loops' scaffolds diverged in LENGTH: _prune_loop has {len(p)} "
        f"shared segments, _archive_expiry_loop {len(a)} — one loop gained or "
        "lost a policy arm the other did not"
    )


def test_both_loops_ride_the_same_retry_ladder() -> None:
    """One ladder, one function, five arm sites per loop, zero direct
    constant reads. A loop that arms the ladder differently — its own
    backoff function, its own constants, a dropped arm — breaks the
    once-per-day/miss/failure contract symmetry the doctrine pins."""
    prune, arch = _load_loops()
    ladder = next(
        node
        for node in ast.parse(_MODULE_PATH.read_text(encoding="utf-8")).body
        if isinstance(node, ast.FunctionDef) and node.name == "_next_retry_backoff"
    )
    ladder_stmts = [
        stmt
        for stmt in ladder.body
        if not (
            isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        )
    ]
    ladder_src = "\n".join(ast.unparse(stmt) for stmt in ladder_stmts)
    assert ladder_src == _LADDER_SOURCE, (
        f"the retry ladder changed shape:\n{ladder_src}\n— both loops ride it; "
        "if the constants moved, mirror it in the pin's _LADDER_SOURCE"
    )
    for label, fn, renames, strings in (
        ("_prune_loop", prune, _PRUNE_RENAMES, _PRUNE_STRINGS),
        ("_archive_expiry_loop", arch, _ARCHIVE_RENAMES, _ARCHIVE_STRINGS),
    ):
        lines = _scaffold_lines(fn, renames, strings)
        joined = "\n".join(lines)
        arms = joined.count("_next_retry_backoff(retry_backoff)")
        assert arms == 5, (
            f"{label} arms the retry ladder {arms} times, the shared policy's "
            "shape is five (leaderless miss, demotion cut, attempt failure, "
            "lock-attempt failure, backstop) — a forked arm site reds here"
        )
        assert "_PRUNE_RETRY_BACKOFF" not in joined, (
            f"{label} reads the backoff constants directly; the ladder must "
            "stay routed through _next_retry_backoff so both loops cannot "
            "drift onto different rungs"
        )


def test_both_loops_build_the_batch_sizer_identically() -> None:
    """One breaker construction, one set of settings, verbatim in both
    loops: the prune family shares one batch bound and one failure
    latch — a loop that sizes, divides, or latches differently splits the
    family's loaded-database behaviour in two."""
    prune, arch = _load_loops()
    for label, fn in (("_prune_loop", prune), ("_archive_expiry_loop", arch)):
        sizer = _find_call(fn, "SweepBatchSizer")
        src = ast.unparse(sizer)
        assert src == _SIZER_SOURCE, (
            f"{label} constructs SweepBatchSizer as:\n{src}\n— the shared "
            "policy is: default_size ← prune_batch_size, divisor ← "
            "event_writer_reduced_batch_divisor, failure_threshold ← "
            "sweep_breaker_failure_threshold, window_secs ← "
            "sweep_breaker_window_secs"
        )


def test_both_loops_wire_the_drain_gate_identically() -> None:
    """The gate wiring order IS the doctrine: the per-batch server-side
    bound derives from the dispatcher pool's command timeout, the gate's
    liveness period IS that bound, and the sweep call receives
    drain_gate / statement_timeout_ms / sizer together — the trio that
    makes a loaded database abort server-side and retry smaller."""
    prune, arch = _load_loops()
    for label, fn, renames, strings in (
        ("_prune_loop", prune, _PRUNE_RENAMES, _PRUNE_STRINGS),
        ("_archive_expiry_loop", arch, _ARCHIVE_RENAMES, _ARCHIVE_STRINGS),
    ):
        lines = _scaffold_lines(fn, renames, strings)
        joined = "\n".join(lines)
        assert _TIMEOUT_SOURCE in joined, (
            f"{label} no longer derives statement_timeout_ms from the "
            "dispatcher pool's command_timeout via _prune_statement_timeout_ms"
        )
        assert _GATE_SOURCE in joined, (
            f"{label} wires _batch_drain_gate differently: the shared policy "
            "is loop_name=<the loop's own name> and "
            "period_secs=statement_timeout_ms / 1000.0 (a wedged drain trips "
            "detector 2 within a few batches' budget)"
        )
        sizer_idx = next(
            i for i, line in enumerate(lines) if line.startswith("SWEEP_SIZER = SweepBatchSizer(")
        )
        gate_idx = lines.index(_GATE_SOURCE)
        timeout_idx = lines.index(_TIMEOUT_SOURCE)
        assert sizer_idx < timeout_idx < gate_idx, (
            f"{label} changed the wiring order; the policy builds the sizer, "
            "then the batch bound, then the gate that consumes both"
        )
        sweep_calls = [
            line
            for line in _scaffold_lines(fn, renames, strings)
            + [_norm(s, renames, strings) for s in ast.walk(fn) if isinstance(s, ast.stmt)]
            if line.startswith("result = await SWEEP_OP(")
        ]
        assert sweep_calls, f"{label}'s sweep call vanished from the attempt body"
        for line in sweep_calls:
            for kw in (
                "drain_gate=drain_gate",
                "statement_timeout_ms=statement_timeout_ms",
                "sizer=SWEEP_SIZER",
            ):
                assert kw in line, (
                    f"{label}'s sweep call dropped {kw!r} — the gate, the batch "
                    "bound and the breaker travel together into every sweep op"
                )


def test_both_loops_keep_the_date_latch_unstamped_on_a_demotion_cut() -> None:
    """The unfinished-day latch semantics, in both loops: the demotion-cut
    arm arms the ladder and does NOT stamp the date latch; the success arm
    stamps it, clears the ladder, and resets the backstop streak; the
    day-gate clears a stale ladder. Forking any half of this turns a
    demotion cut into a done day (or a done day into a retry loop)."""
    prune, arch = _load_loops()
    for label, fn, renames, strings in (
        ("_prune_loop", prune, _PRUNE_RENAMES, _PRUNE_STRINGS),
        ("_archive_expiry_loop", arch, _ARCHIVE_RENAMES, _ARCHIVE_STRINGS),
    ):
        lines = _scaffold_lines(fn, renames, strings)
        joined = "\n".join(lines)
        day_gate = "if not check_lane and LAST_DONE_DATE == today_utc:"
        assert day_gate in joined, (
            f"{label} lost the once-per-day date gate (the DAILY lane's — "
            "the check_lane bypass is the retention-cadence decoupling's "
            "own: a sub-daily schedule is a retention policy, checked "
            "every tick, never latched — the D2 soak's at-most-daily "
            "conviction)"
        )
        gate_idx = lines.index(day_gate)
        # Invariants, not token shapes: a mirrored edit inside the gate
        # (a debug line, an extra field) must stay green; what may not
        # change is clear-the-ladder-then-skip, in that order.
        gate_clear_idx = lines.index("retry_backoff = None", gate_idx)
        gate_skip_idx = lines.index("continue", gate_idx)
        assert gate_clear_idx < gate_skip_idx, (
            f"{label}'s date gate must clear the ladder and skip, in that "
            f"order, it does: {lines[gate_idx : gate_skip_idx + 1]}"
        )
        # The demotion-cut If: ladder arm + drain-cut warn, NO latch stamp —
        # checked over the arm's WHOLE body, not a leading window: the stamp
        # is the bug wherever in the arm it lands (a mirrored stamp after the
        # warn line shipped green under the windowed check this replaced).
        cut_idx = lines.index("if not ctx.deps.leading():", gate_idx)
        cut_end = lines.index("else:", cut_idx)
        cut_arm = lines[cut_idx + 1 : cut_end]
        assert "retry_backoff = _next_retry_backoff(retry_backoff)" in cut_arm, (
            f"{label}'s demotion-cut arm must arm the ladder: {cut_arm}"
        )
        assert any("EV_DRAIN_CUT" in line for line in cut_arm), (
            f"{label}'s demotion-cut arm must log the drain-cut event: {cut_arm}"
        )
        assert "LAST_DONE_DATE = today_utc" not in cut_arm, (
            f"{label} stamps the date latch on a DEMOTION CUT — that turns an "
            "unfinished day into a done one and defers the remainder to the "
            "next cron fire, up to 24h away (pinned by "
            "test_demoted_leader_stops_pruning_mid_drain)"
        )
        stamp_idx = lines.index("LAST_DONE_DATE = today_utc", cut_end)
        # The success arm's half: stamp, clear the ladder, reset the backstop
        # streak — the ORDER is the contract, the neighbouring lines are not
        # (a mirrored log line between them is parity, not a fork).
        clear_idx = lines.index("retry_backoff = None", stamp_idx)
        ok_idx = lines.index("guard.ok()", stamp_idx)
        assert stamp_idx < clear_idx < ok_idx, (
            f"{label}'s success arm must stamp the day, clear the ladder, and "
            f"reset the backstop streak (the reset-on-success contract: a "
            f"skipped or failed day must not buy the fault more time), in that "
            f"order: {lines[stamp_idx : ok_idx + 1]}"
        )


def test_no_await_between_the_gates_last_false_and_the_leading_read() -> None:
    """The race invariant, structurally: the demotion-cut check is the
    IMMEDIATE successor of the sweep await, and the cut arm is synchronous
    UP TO THE LADDER WRITE — so no await can run between the gate's last
    False (inside the drain) and the loop's ``leading()`` read, and a
    re-election cannot flip leadership in between. (An await AFTER the
    ladder write is benign: the arm's only remaining work is the skip, and
    the armed ladder is already committed.) Reordering one await here
    passes every behavioural test and still breaks the contract; that is
    why the pin is structural."""
    prune, arch = _load_loops()
    for label, fn, renames, strings in (
        ("_prune_loop", prune, _PRUNE_RENAMES, _PRUNE_STRINGS),
        ("_archive_expiry_loop", arch, _ARCHIVE_RENAMES, _ARCHIVE_STRINGS),
    ):
        # The attempt body: the inner try inside the advisory-lock's
        # async-with, inside the while. Navigated structurally (by node
        # type), not by magic statement indices.
        while_stmt = next(s for s in fn.body if isinstance(s, ast.While))
        attempt_try = next(s for s in while_stmt.body if isinstance(s, ast.Try))
        async_with = next(s for s in attempt_try.body if isinstance(s, ast.AsyncWith))
        inner_try = next(s for s in async_with.body if isinstance(s, ast.Try))
        sweep_idx = next(
            i
            for i, stmt in enumerate(inner_try.body)
            if any(
                isinstance(sub, ast.Await) and "SWEEP_OP" in _norm(sub, renames, strings)
                for sub in ast.walk(stmt)
            )
        )
        cut_idx = next(
            i
            for i, stmt in enumerate(inner_try.body)
            if isinstance(stmt, ast.If)
            and _norm(stmt.test, renames, strings) == "not ctx.deps.leading()"
            and any("EV_DRAIN_CUT" in _norm(sub, renames, strings) for sub in ast.walk(stmt))
        )
        assert cut_idx == sweep_idx + 1, (
            f"{label} runs {cut_idx - sweep_idx - 1} statement(s) between the "
            "sweep await and the demotion-cut leading() read — the invariant "
            "is ZERO: no await may run between the gate's last False and the "
            "leading() read, so a re-election cannot flip leadership in between"
        )
        cut_arm = inner_try.body[cut_idx]
        assert isinstance(cut_arm, ast.If)  # for pyright: the cut arm is the If above
        # Synchronous up to the ladder write, wherever the arm's other
        # statements sit: walk the arm's statements in order, every one
        # before (and including) the ladder write must be await-free. The
        # first-statement-only walk this replaced missed an await inserted
        # after a reordered leading log line.
        for stmt in cut_arm.body:
            stmt_txt = _norm(stmt, renames, strings)
            has_await = any(isinstance(sub, ast.Await) for sub in ast.walk(stmt))
            assert not has_await, (
                f"{label}'s demotion-cut arm awaits before arming the ladder "
                f"({stmt_txt}); the arm must be synchronous up to the ladder write"
            )
            if stmt_txt.startswith("retry_backoff = _next_retry_backoff("):
                break


def test_both_loops_arm_the_leaderless_miss() -> None:
    """A wake that finds the pod leaderless is a MISSED fire, not a done
    day: both loops must arm the ladder, warn, and recompute the next fire
    from the cron — and the arm must sit between the scheduled-fire reset
    and the date gate. Dropping it silently defers retention work by 24h
    across a seconds-scale leadership flap."""
    prune, arch = _load_loops()
    for label, fn, renames, strings in (
        ("_prune_loop", prune, _PRUNE_RENAMES, _PRUNE_STRINGS),
        ("_archive_expiry_loop", arch, _ARCHIVE_RENAMES, _ARCHIVE_STRINGS),
    ):
        lines = _scaffold_lines(fn, renames, strings)
        fire_reset = lines.index("if not woke_for_retry:")
        miss_idx = lines.index("if not ctx.deps.leading():", fire_reset)
        # Invariants, not token shapes: the arm must arm the ladder, warn
        # the missed-fire event, stamp NOTHING (a miss is not a done day),
        # and skip — a mirrored edit to the warn's fields stays green.
        miss_cont = lines.index("continue", miss_idx)
        arm = lines[miss_idx + 1 : miss_cont]
        assert "retry_backoff = _next_retry_backoff(retry_backoff)" in arm, (
            f"{label}'s leaderless-miss arm must arm the ladder: {arm}"
        )
        assert any("EV_MISSED_LEADERLESS" in line for line in arm), (
            f"{label}'s leaderless-miss arm must warn the missed-fire event: {arm}"
        )
        assert not any("LAST_DONE_DATE" in line for line in arm), (
            f"{label}'s leaderless-miss arm stamps the date latch — a MISSED "
            f"fire is not a done day: {arm}"
        )
        day_gate = lines.index("if not check_lane and LAST_DONE_DATE == today_utc:", miss_idx)
        assert fire_reset < miss_idx < day_gate, (
            f"{label}'s leaderless-miss arm must sit between the "
            "scheduled-fire ladder reset and the date gate"
        )
