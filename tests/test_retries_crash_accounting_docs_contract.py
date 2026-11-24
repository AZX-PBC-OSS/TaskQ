"""Docs-contract pins for the crash-vs-shutdown attempt accounting.

``docs/guides/retries.md`` is where a newcomer lands to learn what an
"attempt" costs, and it must state the two things an adopter gets wrong
by default: a crash mid-execution (SIGKILL)
SPENDS the attempt and a graceful shutdown (SIGTERM) spends it too, and
a retry budget ported from another queue library does not carry its
wall-clock coverage with it. A doc edit that drifts from the shipped
behaviour fails these pins (the pattern the docs-contract suite
established; see tests/test_outbox_exemption_docs_contract.py).
"""

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DOCS = _ROOT / "docs"


def _normalized(path: Path) -> str:
    return " ".join(path.read_text().split())


def test_retries_guide_names_crash_and_shutdown_accounting() -> None:
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "SIGKILL" in text and "crash" in text.lower(), (
        "retries.md must say 'crash' and 'SIGKILL' plainly - the guide a "
        "newcomer reads cannot leave the costliest attempt-accounting "
        "surprise undiscoverable"
    )
    assert "outcome='crashed'" in text and "WorkerCrashed" in text, (
        "retries.md must pin the crash half of the accounting: the claim's "
        "attempt is spent and the job_attempts audit row records "
        "outcome='crashed' with error_class='WorkerCrashed'"
    )
    assert "spent, not refunded" in text and "interrupt_count" in text, (
        "retries.md must pin the shutdown half: the interrupted claim is "
        "spent, not refunded (no attempt row; interrupt_count carries the "
        "aggregate) - a deploy costs one attempt, the price of never "
        "sharing an attempt epoch between a dying process and its re-run"
    )
    assert "heartbeat_interval" in text and "lock_lease" in text, (
        "retries.md must point at the operator controls for crash-detection "
        "latency (heartbeat_interval sizing, the lock_lease invariant), not "
        "just name the behaviour"
    )


def test_retries_guide_carries_the_porting_guidance() -> None:
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "Porting a retry budget from another queue library" in text, (
        "retries.md must carry the porting section: an adopter arriving "
        "from another queue library lands here first"
    )
    assert "2^(N-1)" in text and "attempt count" in text, (
        "retries.md's porting section must state TaskQ's curve "
        "(base-2^(N-1) exponential capped at 1h with jitter) and warn "
        "that the attempt count does not carry the wall-clock coverage "
        "with it"
    )
    assert "wall-clock" in text and "time_budget" in text, (
        "retries.md's porting section must direct the adopter to decide "
        "the wall-clock window and set base/cap (or a time_budget) for it"
    )


def test_retries_guide_marks_indefinite_max_attempts_as_inert() -> None:
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "ignored entirely" in text and "inert" in text, (
        "retries.md must state that retry_kind='indefinite' ignores "
        "max_attempts entirely - the row still carries the configured value, "
        "and it is inert"
    )
    assert "— (indefinite)" in text, (
        "retries.md must tell the operator how the admin UI renders the "
        "inert max_attempts on an indefinite-kind job: — (indefinite)"
    )


def test_retries_guide_states_the_workflow_step_budgets() -> None:
    """#746: a workflow step keeps two budgets. The lease reclaim re-pends a
    crash only while ``attempt < max_attempts`` (every claim counts), while
    the runner's ladder counts only the step's ``failed`` ledger rows, so a
    crash never spends the ladder and the two stack. The shipped behaviour
    is pinned by ``tests/test_wf_step_crash_retry_budget_pins.py``; the
    retries guide must state it, and every guide line that called
    ``attempt`` vs ``max_attempts`` "the ladder" (or bounded crashes by the
    ladder) must say which budget it means. One substring per guide claim, so
    reverting or dropping any one of them fails here."""
    retries = _normalized(_DOCS / "guides" / "retries.md")
    for phrase, why in (
        (
            "for a plain actor with the default `max_attempts=3`, two crashes",
            "§12's 'two crashes leave one attempt' arithmetic is the plain actor's",
        ),
        (
            "counts crashes and body failures separately",
            "a workflow step counts crashes and body failures separately: the "
            "plain-actor arithmetic ('two crashes leave one attempt') is wrong "
            "for workflow steps",
        ),
        (
            "For the default `transient` kind",
            "the two-budget arithmetic holds for the transient kind; permanent "
            "and indefinite differ",
        ),
        (
            "run the step body five times, and the step ends `failed` with `attempt=5`",
            "the worked example: two crashes then three body failures (CCFFF pin)",
        ),
        (
            "`max_attempts - 1` crashes followed by `max_attempts` body failures",
            "the workflow step's worst case: max_attempts - 1 crashes followed "
            "by max_attempts body failures",
        ),
        ("`2 * max_attempts - 1`", "the worst case's execution count"),
        (
            "two body failures followed by a crash end the step `crashed`",
            "the order effect: a crash at the claim ceiling is terminal with ladder left",
        ),
        (
            "each resume after a hold is a claim",
            "a hold's resume spends crash headroom",
        ),
        (
            "a step that held twice under `max_attempts=3` ends `crashed` on its first crash",
            "the hold arithmetic (the hold-then-crash pin)",
        ),
        (
            "re-pends the step with no `attempt` ceiling",
            "the post-body infrastructure fault re-pends past the claim ceiling",
        ),
        (
            "the re-run is still a claim, so like a hold's resume it spends crash headroom",
            "the infrastructure fault's re-run spends crash headroom (the infra-then-crash pin)",
        ),
        (
            "a node whose ladder ran out gets one more body attempt and no crash headroom",
            "a manual flows retry resets neither count (the manual-retry pin)",
        ),
        (
            "a `permanent` step runs once, since its first crash or body failure is terminal",
            "the permanent kind (the permanent pins)",
        ),
        (
            "an `indefinite` step has no crash ceiling while its ladder still stops at "
            "the `max_attempts`-th body failure",
            "the indefinite kind (the indefinite pin)",
        ),
        (
            "a workflow step keeps the two budgets apart (§12)",
            "§13's 'a crashed worker spends an attempt' must point workflow steps at §12",
        ),
        (
            "an `indefinite` workflow step's body-failure ladder still stops at the "
            "`max_attempts`-th failure",
            "§13's 'indefinite ignores max_attempts entirely' is the plain actor's",
        ),
        (
            "it counts failed attempts only, so crashes are budgeted apart",
            "§14 must say the workflow ladder counts failed attempts only",
        ),
    ):
        assert phrase in retries, f"retries.md must say {phrase!r}: {why}"
    for phrase in (
        "without spending either budget",
        "is the SAME ladder this guide documents",
    ):
        assert phrase not in retries, (
            f"retries.md must not say {phrase!r}: a workflow step's re-runs are "
            "claims that spend crash headroom, and its ladder counts failed "
            "attempts where the plain actor's counts claims"
        )

    workflows = _normalized(_DOCS / "guides" / "workflows.md")
    assert "retries.md#12-crash-vs-shutdown-what-happens-to-the-attempt-count" in workflows, (
        "workflows.md must point at retries.md §12 for the step's two budgets"
    )
    for phrase, why in (
        ("`2 * max_attempts - 1`", "the retry knob must state the step's execution bound"),
        (
            "counting crashes and body failures alone",
            "the retry knob's bound must be qualified: holds, defers and the "
            "machinery re-pend fall outside it",
        ),
        (
            "a `permanent` step runs once: its first crash or body failure is terminal",
            "the retry knob's permanent kind",
        ),
        (
            "re-pends with no claim ceiling and never spends the ladder, though the "
            "re-run's claim still spends crash headroom",
            "the retry knob's machinery re-pend",
        ),
        (
            "the body-failure ladder counts the node's",
            "§7.2's audit recipe must name the ladder's own count",
        ),
        (
            "the crash reclaim never writes the ledger",
            "§7.2's audit recipe: a lease-reclaimed crash is a job_attempts row, not a ledger outcome",
        ),
        (
            "its claim headroom (`attempt` vs `max_attempts`",
            "§7 step 3 must call `attempt` vs `max_attempts` the claim headroom",
        ),
        (
            "neither count resets (a transient node whose ladder ran out gets one more "
            "body attempt, and its first crash is terminal)",
            "§7 step 3's manual resume (the manual-retry pin)",
        ),
        (
            "each resume is still a claim, so it spends crash headroom",
            "the 'My holds ate the retry budget' row must say a resume spends crash headroom",
        ),
        (
            "a crash's re-run and a hold's resume each advance it, so it is not the ladder's count",
            "the ctx.attempt row (the CCFFF and hold-then-crash pins)",
        ),
    ):
        assert phrase in workflows, f"workflows.md must say {phrase!r}: {why}"
    for phrase in (
        "is the ladder; the LEDGER is the per-attempt record",
        "the ladder's headroom (`attempt` vs `max_attempts`)",
        "the ladder's count; holds do not advance it",
        "re-pends outside both budgets",
        "the ceiling raises past the spent attempt",
    ):
        assert phrase not in workflows, (
            f"workflows.md must not say {phrase!r}: the ladder counts the "
            "step's failed ledger rows, `attempt` counts claims, and neither resets"
        )

    runbooks = _normalized(_DOCS / "guides" / "runbooks.md")
    for phrase, why in (
        (
            "bounded by the child's claim counter",
            "the kill runbook must bound the kill-retry cycle by the claim counter",
        ),
        (
            "the next claim advances `attempt` (the body-failure ladder is untouched)",
            "the kill runbook's step 3 must name the claim counter",
        ),
        (
            "the failures name the claim headroom",
            "the stuck-run walk-through must name the claim headroom",
        ),
        (
            "neither count resets, so a transient node whose ladder ran out gets one more "
            "body attempt and its first crash is terminal",
            "the stuck-run remedy's manual retry (the manual-retry pin)",
        ),
        (
            "is live (not terminal-failed), unknown to run",
            "the kill runbook must quote the retry verb's real refusal",
        ),
    ):
        assert phrase in runbooks, f"runbooks.md must say {phrase!r}: {why}"
    for phrase in (
        "bounded by the child's ladder",
        "the attempt ladder continues",
        "A node failed with ladder headroom",
        "the ceiling raises;",
        "spent past the attempt ceiling",
    ):
        assert phrase not in runbooks, (
            f"runbooks.md must not say {phrase!r}: crashes never spend the "
            "ladder, and a manual retry resets neither count"
        )

    task_stacks = _normalized(_DOCS / "guides" / "task-stacks.md")
    assert "is the body's own view of the ladder" not in task_stacks, (
        "task-stacks.md must not call ctx.attempt the ladder: crashes and hold "
        "resumes advance it, the ladder counts failed attempts"
    )
