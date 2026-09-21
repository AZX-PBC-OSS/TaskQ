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
