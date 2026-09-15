"""Denial docs must describe admission denials as HTTP-429 semantics.

An admission denial -- rate limit or reservation -- means "come back later",
exactly like an HTTP 429 with `Retry-After`. It never consumes the job's
retry budget and never by itself fails a job terminally: a denied job is
rescheduled with backoff indefinitely until capacity frees or its
`schedule_to_close` deadline expires, at which point the normal deadline
path fails it. This matters because a queue or rate-limit misconfiguration
must not be able to kill work that simply never got a slot -- a job with a
retry budget of 3 should not die because the fleet was busy four times.

Because per-denial `job_events`/`job_attempts` rows are also gone (they were
an unbounded-growth vector), the aggregated denial counter on the job row is
the only remaining way an operator sees contention, so the guides must point
at it.

These tests pin the operator-facing guides to that behavior so no surface
can drift back to describing denials as budget-consuming or terminal.
"""

from __future__ import annotations

from pathlib import Path

_DOCS = Path(__file__).resolve().parent.parent / "docs"


def _read(*parts: str) -> str:
    return (_DOCS.joinpath(*parts)).read_text()


def test_ops_guide_states_denials_consume_no_retry_budget() -> None:
    """ops.md must say a denial costs no retry budget, per 429 semantics."""
    text = _read("guides", "ops.md")
    assert "no retry budget is consumed" in text, (
        "an admission denial is 'come back later': it must never spend the "
        "job's retry budget, so ops.md must say so explicitly"
    )


def test_ops_guide_does_not_claim_denials_can_exhaust_the_budget() -> None:
    """A denial must never be documented as a terminal MaxAttemptsExceeded."""
    text = _read("guides", "ops.md")
    denial_section = text.split("### Rate-limit denial is a snooze, not a failure", 1)
    assert len(denial_section) == 2, (
        "ops.md must keep a section framing rate-limit denial as a snooze rather than a failure"
    )
    body = denial_section[1].split("\n### ", 1)[0]
    assert "MaxAttemptsExceeded" not in body, (
        "a denial never terminally fails a job by itself; only the "
        "schedule-to-close deadline path can end a perpetually denied job, "
        "so the denial section must not promise MaxAttemptsExceeded"
    )
    assert "The attempt row records" not in body, (
        "a denial writes no job_attempts row -- per-denial rows are an "
        "unbounded-growth vector and were replaced by an aggregated denial "
        "counter on the job row, which is what this section must point at"
    )


def test_ops_guide_snooze_table_does_not_say_max_attempts_is_bumped() -> None:
    """`max_attempts` is immutable; a denial leaves the budget untouched."""
    text = _read("guides", "ops.md")
    assert "max_attempts` is bumped to keep the invariant" not in text, (
        "max_attempts is immutable -- nothing raises it. A deferral leaves "
        "the retry budget unspent instead of inflating the ceiling"
    )


def test_ops_guide_bounds_perpetual_denial_by_schedule_to_close() -> None:
    """The only end state for a never-admitted job is deadline expiry."""
    text = _read("guides", "ops.md")
    assert "schedule_to_close" in text, (
        "ops.md must name schedule_to_close as the single bound on a job "
        "that is denied indefinitely"
    )


def test_rate_limiting_guide_states_denials_are_retry_free() -> None:
    """rate-limiting.md must keep the retry-free framing of denials."""
    text = _read("guides", "rate-limiting.md")
    assert "They do not consume retry budget." in text, (
        "sustained rate limiting must never burn retry budget; the "
        "queue-depth section is where operators look for that guarantee"
    )


def test_rate_limiting_guide_does_not_promise_terminal_failure() -> None:
    """A rate-limited job is rescheduled, never failed for being limited."""
    text = _read("guides", "rate-limiting.md")
    assert "MaxAttemptsExceeded" not in text, (
        "rate limiting is backpressure, not failure: a rate-limited job is "
        "rescheduled until capacity frees or its schedule-to-close expires"
    )


def test_rate_limiting_guide_does_not_claim_snoozed_job_status() -> None:
    """`job_status` has no `snoozed` value; denied jobs read as scheduled."""
    text = _read("guides", "rate-limiting.md")
    assert "transitions to `snoozed` status" not in text, (
        "job_status has no 'snoozed' value -- a denied job is `scheduled` "
        "with a future run_at, and operators querying the table need the "
        "status name that actually exists"
    )


def test_troubleshooting_guide_states_denials_cost_no_retry_budget() -> None:
    """troubleshooting.md must not teach operators that denials cost retries."""
    text = _read("guides", "troubleshooting.md")
    assert "no retry budget consumed" in text, (
        "an operator diagnosing a rate-limit backlog must be told the "
        "backlog is not silently eating retry budget"
    )


def test_troubleshooting_guide_points_at_the_aggregated_denial_counter() -> None:
    """Per-denial rows are gone; the counter on the job row is the signal."""
    text = _read("guides", "troubleshooting.md")
    assert "rate_limit_blocked_count" in text, (
        "denials write no per-denial job_events or job_attempts rows, so "
        "the aggregated denial counter on the job row is the only way "
        "contention stays visible -- troubleshooting.md must name it"
    )


def test_upgrading_guide_describes_denials_as_429_semantics() -> None:
    """The upgrade guide must describe the shipping 429 denial semantics."""
    text = _read("guides", "upgrading.md")
    assert "Admission denials are budget-bounded." not in text, (
        "denials are not budget-bounded: they never consume retry budget "
        "and never terminally fail a job on their own"
    )
    assert "schedule_to_close" in text, (
        "the upgrade guide must state that a perpetually denied job is "
        "bounded only by its schedule-to-close deadline"
    )


def test_observability_guide_names_the_per_job_denial_counter() -> None:
    """The durable denial counter must be findable in the metrics guide.

    The OTel denial counters are fleet-wide rates: they say the fleet is
    shedding admissions, not which job has been starving. Because a denial
    now writes no `job_events` and no `job_attempts` row, the aggregated
    `rate_limit_blocked_count` on the job row is the only per-job record
    of the contention a single job absorbed -- the column an operator
    queries when one job is mysteriously slow while the fleet looks
    healthy. observability.md is where operators go to find a signal, so
    the column must be named there alongside the counters it complements.
    """
    text = _read("guides", "observability.md")
    assert "rate_limit_blocked_count" in text, (
        "an admission denial leaves no per-denial event or attempt row, so "
        "the aggregated denial counter on the job row is the only durable, "
        "per-job view of contention. observability.md documents the "
        "fleet-wide denial counters but never names the column that carries "
        "the per-job signal, leaving an operator with no documented way to "
        "tell which job is starving for capacity."
    )


def test_ops_guide_denial_section_points_at_the_aggregated_counter() -> None:
    """The denial section must hand operators the replacement signal.

    The section that tells an operator a denial is a snooze rather than a
    failure is exactly where the removal of per-denial rows bites: someone
    who went looking for an attempt row or an event and found nothing needs
    the section to say what replaced them. Naming the counter in place
    keeps the docs from describing a removal without describing the
    substitute.
    """
    text = _read("guides", "ops.md")
    denial_section = text.split("### Rate-limit denial is a snooze, not a failure", 1)
    assert len(denial_section) == 2, (
        "ops.md must keep a section framing rate-limit denial as a snooze rather than a failure"
    )
    body = denial_section[1].split("\n### ", 1)[0]
    assert "rate_limit_blocked_count" in body, (
        "the denial section describes a denial that writes no attempt row "
        "and no event row, so it must name the aggregated denial counter on "
        "the job row as the surviving record of contention -- otherwise the "
        "guide documents what was taken away and not what replaced it"
    )
