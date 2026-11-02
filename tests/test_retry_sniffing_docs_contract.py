"""Docs-contract pins for the retry-sniffing feature (§5 extension).

``docs/guides/retries.md`` §5 must keep the two things the retry-sniffing
feature promises its users: the signal → override → bounds table (the
"what bounds it" column is the haunt lesson rendered as documentation —
every indefinite-returning path's docstring restates the deadline hazard,
and the docs table is its user-facing twin) and the taxonomy's
conservative-default contract. A doc edit that drifts from the shipped
behaviour fails these pins (the pattern the docs-contract suite
established; see tests/test_retries_crash_accounting_docs_contract.py).
"""

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DOCS = _ROOT / "docs"


def _normalized(path: Path) -> str:
    return " ".join(path.read_text().split())


def test_retries_guide_keeps_the_signal_override_bounds_table() -> None:
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "What bounds it" in text and "curve fallback" in text, (
        "retries.md §5 must keep the signal → override → bounds table: the "
        "header-sniffing feature's contract (what a Retry-After hint "
        "changes, what still bounds it) must stay discoverable"
    )
    assert "X-Retry-After" in text and "parsedate_to_datetime" in text, (
        "retries.md must name both header forms and the HTTP-date parse"
    )
    assert "the delay does not move the deadline" in text, (
        "retries.md must restate the deadline hazard on the delay path: a "
        "parsed Retry-After schedules when the next attempt lands, it does "
        "not extend schedule_to_close"
    )


def test_retries_guide_keeps_the_taxonomy_conservative_default_contract() -> None:
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "failure_taxonomy_classifier" in text, (
        "retries.md §5 must document the taxonomy classifier"
    )
    assert "over-claiming is the haunt class" in text, (
        "retries.md must state the conservative default: unsure → None"
    )
    assert "never returns" in text and "indefinite" in text, (
        "retries.md must pin the anti-haunt contract: the taxonomy claims "
        "transient/non_retryable/None, never indefinite, so composing it "
        "needs no deadline argument"
    )
    assert "DEFAULT_TRANSIENT_STATUSES" in text and "DEFAULT_NON_RETRYABLE_STATUSES" in text, (
        "retries.md must name the documented module constants the sets default to"
    )


def test_retries_guide_keeps_the_claim_kind_knob_contract() -> None:
    """The claim-kind knob's contract (the §5 extension): the factory
    name, the three modes with their bounds, and the haunt hazard per
    mode must stay discoverable — a doc edit that drifts from the
    shipped modes fails here."""
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "make_rate_limit_aware_classifier" in text, (
        "retries.md §5 must document the factory behind the built-in"
    )
    assert 'claim_kind="transient"' in text and "max_attempts" in text, (
        "retries.md must document the bounded mode: max_attempts stays the "
        "stopper, the hint sets when"
    )
    assert "claim_kind=None" in text, "retries.md must document the never-claiming identity mode"

    # The #658 docs↔code sniff pattern: the doc's named kwarg must exist on
    # the factory's signature, and the doc's "default" claim ("the built-in
    # is the factory's claim_kind='indefinite' instance") must match the
    # shipped default. Sniffed by PARAMETER NAME, not position: a refactor
    # of kwarg order cannot silently break (or unbreak) this pin — only a
    # rename or a default drift does, together with the doc it documents.
    import inspect

    import taskq.retry

    parameters = inspect.signature(taskq.retry.make_rate_limit_aware_classifier).parameters
    assert "claim_kind" in parameters, (
        "retries.md §5 names claim_kind as the factory's knob: renaming the "
        "parameter without updating the guide fails this pin rather than "
        "shipping a doc that promises a keyword that isn't there"
    )
    assert parameters["claim_kind"].default == "indefinite", (
        "the doc's claim that the built-in is the factory's "
        'claim_kind="indefinite" instance pins the default: a default drift '
        "changes the built-in's bounded-ness for every existing registration "
        "and must update the guide in the same commit"
    )


def test_retries_guide_keeps_the_parser_alignment_contract() -> None:
    """The parser's finite-hint rules (the §5 extension): a finite hint
    clamps to the operator's ceiling (never garbage), the decimal-fraction
    grammar is documented, and the HTTP-date divergence choice point is
    stated."""
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "A finite hint is never garbage" in text, (
        "retries.md must state the clamp rule: an oversized-but-finite "
        "hint is the operator's max_retry_backoff ceiling's input, not garbage"
    )
    assert "0.5" in text and "1,5" in text and "1e3" in text, (
        "retries.md must document the decimal-fraction grammar and its "
        "deliberate limits (comma decimals and scientific notation are garbage)"
    )
    assert (
        "fences HTTP-date forms in their own classifier first" in text
        and "parsedate_to_datetime" in text
    ), (
        "retries.md must state the divergence choice point: a consumer "
        "preferring fallback-over-parse fences HTTP-date forms in their own "
        "classifier first"
    )


def test_retries_guide_keeps_the_taxonomy_matching_rule_contract() -> None:
    """§5's matching rule is a CONTRACT, not prose: exact curated names by
    default, the suffix inference opt-in behind
    ``infer_timeout_by_suffix`` with the ``ExecutionTimeout``
    counterexample, and the curated constant named. The code-side sniff
    keeps the doc honest — a rename or removal of the flag fails this pin
    together with the doc it documents (the docs-contract pin pattern)."""
    text = _normalized(_DOCS / "guides" / "retries.md")
    assert "exact curated names by default" in text and "infer_timeout_by_suffix" in text, (
        "retries.md §5 must state the taxonomy's matching rule as the shipped "
        "contract: the default path matches exact curated names only, and the "
        "suffix inference is opt-in — a doc edit that drifts back to describing "
        "suffix-on-by-default behavior (or drops the flag's existence) fails here"
    )
    assert "DEFAULT_TRANSIENT_EXCEPTION_NAMES" in text, (
        "retries.md must name the curated constant the default matching rule "
        "reads: the provenance audit lives on the constant, and the doc's "
        "promise is that exact name"
    )
    assert "ExecutionTimeout" in text, (
        "retries.md must keep the counterexample: the suffix flag's documented "
        "cost is the deadline-exceeded that MEANS failure — without it the "
        "opt-in reads as a free convenience, not a trade"
    )

    import taskq.retry

    assert "infer_timeout_by_suffix" in (
        taskq.retry.failure_taxonomy_classifier.__kwdefaults__ or {}
    ), (
        "the doc's named flag must exist on the factory: the contract pin is "
        "docs ↔ code, so renaming the kwarg without updating the guide fails "
        "here rather than shipping a doc that promises a keyword that isn't there"
    )
