"""Docs-contract pins for the graceful-shutdown interruption design.

The operator-facing surfaces named here carry the behaviour contract for
what a deploy does to in-flight work: release (interrupt) it back to the
fleet with the attempt refunded, never terminalise it, and never lose an
operator cancel inside one. A doc edit that drifts from the shipped
behaviour fails these pins — the pattern the docs-contract suite
established (see tests/test_outbox_exemption_docs_contract.py).

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


def test_cancellation_guide_teaches_shutdown_origin_and_the_refund() -> None:
    text = _normalized(_DOCS / "guides" / "cancellation.md")
    assert "cancel_origin" in text and "CancelOrigin.SHUTDOWN" in text, (
        "cancellation.md must document ctx.cancel_origin so an actor can tell "
        "a deploy (checkpoint and raise — the fleet re-runs the attempt) from "
        "an operator cancel (the partial result returned is the one kept)"
    )
    assert "released" in text and "refunded" in text, (
        "cancellation.md must say what a deploy does to a running job: the "
        "attempt is released back to the fleet with its budget refunded"
    )
    assert "never produces" in text or "never writes" in text, (
        "cancellation.md must state that shutdown never writes the operator-ladder terminal states"
    )


def test_ops_guide_abandoned_definition_excludes_shutdown() -> None:
    text = _normalized(_DOCS / "guides" / "ops.md")
    assert "shutdown never produces it either" in text, (
        "ops.md's `abandoned` definition must say shutdown never produces it — "
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
        "deployment.md's shutdown-phase list must name the RELEASING phase — "
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
    # label — the broken window this design's docs pass corrects.
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
            "unchanged — /health JSON and the CLI keep their numbers)"
        )
