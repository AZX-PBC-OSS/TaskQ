"""Docs-contract pins for the graceful-shutdown interruption design.

The operator-facing surfaces named here carry the behaviour contract for
what a deploy does to in-flight work: release (interrupt) it back to the
fleet with the spent attempt standing, never terminalise it, and never
lose an operator cancel inside one. A doc edit that drifts from the
shipped behaviour fails these pins (the pattern the docs-contract suite
established; see tests/test_outbox_exemption_docs_contract.py).

Surfaces pinned: ``docs/guides/cancellation.md``, ``docs/guides/ops.md``,
``docs/guides/deployment.md``, ``docs/guides/jobs-clients.md`` (the four
guide surfaces), plus the upgrade note in ``docs/guides/upgrading.md``
and the phase tables in ``docs/architecture.md`` / ``docs/guides/cli.md``.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DOCS = _ROOT / "docs"


def _normalized(path: Path) -> str:
    return " ".join(path.read_text().split())


def test_cancellation_guide_teaches_shutdown_origin_and_the_no_refund() -> None:
    text = _normalized(_DOCS / "guides" / "cancellation.md")
    assert "cancel_origin" in text and "CancelOrigin.SHUTDOWN" in text, (
        "cancellation.md must document ctx.cancel_origin so an actor can tell "
        "a deploy (checkpoint and raise - the fleet re-runs the attempt) from "
        "an operator cancel (the partial result returned is the one kept)"
    )
    assert "released" in text and "not refunded" in text, (
        "cancellation.md must say what a deploy does to a running job: the "
        "attempt is released back to the fleet and its increment is NOT "
        "refunded; the attempt started executing, so it is spent"
    )
    assert "never fabricates" in text and (
        "The one terminal write shutdown adds for a cancelled row is the operator's OWN verdict landing"
        in text
    ), (
        "cancellation.md must teach the reconciled shutdown rule, not the old "
        "absolutes: shutdown never fabricates a verdict (an interruption with no "
        "operator cancel behind it is released back to the fleet, never written "
        "cancelled or abandoned), and the one terminal write it adds for a "
        "cancelled row is the operator's OWN verdict (a row already carrying an "
        "operator's cancel when the process dies is handed to mark_cancelled at "
        "release time - the same fenced write the unwinding consumer races - so "
        "the row reads cancelled, never abandoned). A regression that resurrects "
        "the RELEASING mark_abandoned verdict (an operator's cancelled job could "
        "show `abandoned`) or drops the reconciliation must red here"
    )


def test_cancellation_guide_abandoned_row_carries_the_operator_verdict_contract() -> None:
    text = _normalized(_DOCS / "guides" / "cancellation.md")
    assert "Shutdown never writes `abandoned`" in text, (
        "cancellation.md must state the ownership contract flatly: shutdown "
        "never writes `abandoned` - an operator-cancelled row the process dies "
        "with is terminalised with the operator's OWN verdict (mark_cancelled, "
        "forced), the same fenced write the unwinding consumer races"
    )
    assert "is abandoned during release" not in text, (
        "cancellation.md's §8 `abandoned` status row still teaches the "
        "PRE-#596 contract (a row whose operator cancel was in flight when "
        "the process died 'is abandoned during release'). The shipped code "
        "writes mark_cancelled at RELEASING (the operator's own verdict, the "
        "same fenced write the unwinding consumer races), so the row reads "
        "`cancelled` - a doc edit that resurrects the old mark_abandoned-at-"
        "release story contradicts both the code and this guide's own "
        "'Shutdown never writes abandoned' paragraph and must red here"
    )
    assert "run_post_tx()` drains the queue: for each entry it writes" in text and (
        "a held entry while the shutdown orchestration is active gets "
        "`backend.mark_cancelled()`" in text
    ), (
        "cancellation.md's phase-3 drain paragraph must carry the #596 "
        "handover: the drain writes mark_abandoned for the UNHELD orphan "
        "class and for held entries while the worker runs normally, but "
        "mark_cancelled (the operator's own verdict) for a held entry while "
        "the shutdown orchestration is active"
    )
    workers_text = _normalized(_DOCS / "guides" / "workers.md")
    assert "jobs under an operator cancel reach `abandoned` here instead" not in workers_text, (
        "workers.md's RELEASING phase-table row still teaches the PRE-#596 "
        "contract (operator-cancelled jobs reach `abandoned` at RELEASING). "
        "The shipped code writes mark_cancelled there (the operator's own "
        "verdict, the same fenced write the unwinding consumer races); the "
        "matching row in architecture.md's phase table was already corrected"
    )
    assert "never `abandoned`" in workers_text, (
        "workers.md's RELEASING row must carry the ownership contract's "
        "verdict: operator-cancelled jobs are terminalised with "
        "mark_cancelled, never abandoned"
    )


def test_ops_guide_abandoned_definition_excludes_shutdown() -> None:
    text = _normalized(_DOCS / "guides" / "ops.md")
    assert "shutdown never produces it either" in text, (
        "ops.md's `abandoned` definition must say shutdown never produces it - "
        "a deploy releases (interrupts) the job back to the fleet instead"
    )
    assert "interrupt_count" in text, (
        "ops.md's footgun registry must carry the interrupt-loop footgun: an "
        "actor longer than cancellation_grace_period + cleanup_grace_period is "
        "interrupted on every deploy and re-run from scratch, bounded only by "
        "schedule_to_close or progress-state checkpointing"
    )


def test_deployment_guide_names_the_release_phase_and_write() -> None:
    text = _normalized(_DOCS / "guides" / "deployment.md")
    assert "FORCING → RELEASING" in text, (
        "deployment.md's shutdown-phase list must name the RELEASING phase - "
        "the phase no longer abandons anything"
    )
    assert "ABANDONING" not in text, (
        "deployment.md still names ABANDONING; the phase is RELEASING (value 4 unchanged)"
    )
    assert "mark_interrupted" in text, (
        "deployment.md's SIGKILL-mid-unwind paragraph must name the release "
        "write (mark_interrupted) among the writes that may not land"
    )


def test_jobs_clients_guide_corrects_the_abandoned_definition() -> None:
    text = _normalized(_DOCS / "guides" / "jobs-clients.md")
    # The pre-existing definition described `crashed` under the `abandoned`
    # label - the broken window this design's docs pass corrects.
    assert "Heartbeat expired and no worker reclaimed" not in text, (
        "jobs-clients.md's `abandoned` definition described the crashed shape; "
        "`abandoned` is the operator-cancel terminal (escalation past the "
        "grace periods), and shutdown never produces it"
    )
    assert "operator" in text and "never produced by a worker shutdown" in text.lower(), (
        "jobs-clients.md's status table must define `abandoned` as the "
        "operator-cancel terminal and say shutdown never produces it"
    )
    assert "interrupt_count" in text, (
        "jobs-clients.md must document the interrupt_count row counter and "
        "the 'interrupted' timeline transition"
    )


def test_upgrading_guide_carries_the_interrupt_entry() -> None:
    text = _normalized(_DOCS / "guides" / "upgrading.md")
    assert "interrupt_count" in text and "RELEASING" in text, (
        "upgrading.md must carry the new column, the phase rename "
        "(ABANDONING → RELEASING, value 4 unchanged), and the release "
        "semantics"
    )
    assert "ctx.cancel_origin" in text, (
        "upgrading.md must name the ctx.cancel_origin audit surface: actors "
        "that return early on a deploy's cancel keep the partial result"
    )


def test_upgrading_guide_carries_the_grace_default_change() -> None:
    text = _normalized(_DOCS / "guides" / "upgrading.md")
    assert "default termination grace period rose from 75 seconds to 85" in text, (
        "upgrading.md must carry the TASKQ_TERMINATION_GRACE_PERIOD default "
        "change (75s to 85s): a deployment whose platform grace was sized "
        "against the old number SIGKILLs the worker mid-teardown after "
        "upgrading, and the docs are the only place that names the action"
    )
    assert "crash-reclaim" in text, (
        "upgrading.md must say what a short platform grace degrades to: "
        "leases expire and the leader's crash-reclaim sweep re-runs the "
        "work instead of the shutdown finishing cleanly"
    )
    assert "deployment.md" in text, (
        "upgrading.md must point at the deployment recipes that carry the "
        "sized platform graces and the worst-case formula"
    )


def test_deployment_guide_sizes_the_platform_grace_from_the_new_tail() -> None:
    text = _normalized(_DOCS / "guides" / "deployment.md")
    assert "8 sequential bounded closes" in text and "42s of tail" in text, (
        "deployment.md's grace notes must carry the new teardown-tail "
        "arithmetic (8 closes, ~42s) so a manifest sized from them fits the "
        "shipped teardown"
    )
    assert "85 / 30 / 10" in text, (
        "deployment.md must state the new default grace combination the "
        "safe platform value is computed from"
    )


def test_phase_labels_in_architecture_and_cli_tables() -> None:
    architecture = _normalized(_DOCS / "architecture.md")
    cli = _normalized(_DOCS / "guides" / "cli.md")
    for name, text in (("architecture.md", architecture), ("cli.md", cli)):
        assert "RELEASING" in text, (
            f"{name}'s shutdown-phase table must name RELEASING for the value-4 phase"
        )
        assert "ABANDONING" not in text, (
            f"{name} still names ABANDONING; the phase is RELEASING (value 4 "
            "unchanged - /health JSON and the CLI keep their numbers)"
        )
