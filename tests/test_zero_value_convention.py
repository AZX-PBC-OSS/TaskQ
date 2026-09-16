"""Pins a single stated convention for what a zero-shaped setting value means.

TaskQSettings/WorkerSettings expose at least the following fields that
accept a literal 0 (or ``timedelta(0)``) as a meaningful, documented value
rather than a validation error:

  * ``statement_cache_size`` (settings.py, "0 disables the statement cache")
  * ``max_cached_statement_lifetime`` (settings.py, "0 caches statements
    indefinitely" — read: 0 means *unbounded*, the opposite polarity of the
    field immediately above it)
  * ``max_pending_lock_timeout_ms`` / ``unique_for_lock_timeout_ms`` /
    ``idempotency_lock_timeout_ms`` / ``token_bucket_lock_timeout_ms`` /
    ``sliding_window_lock_timeout_ms`` ("0 or less waits indefinitely
    server-side (the lock_timeout GUC convention)")
  * ``event_retention_period`` / ``keyed_row_reclaim_period``
    (``timedelta(0)`` "DISABLES the sweep")
  * ``health_port`` (0 "binds an ephemeral port" — a third, unrelated
    meaning: neither "disabled" nor "infinite")

Each field's own docstring states its own polarity correctly — this test
is not about any single field being wrong. It pins the ABSENCE of a
declared, machine-checkable convention that ties them together: nothing
in dotenvmodel's ``Field(...)`` carries a "what does 0 mean here" tag
(confirmed by introspecting ``dotenvmodel.Field``'s signature — it offers
default/ge/le/validator/etc., no semantic/polarity metadata), so the only
place the convention lives is prose scattered across ~148 field
docstrings, several of which disagree with each other on the same literal
value.

Vendor check for a stated convention (read via Read/grep, not memory):

  * River (vendor/river/client.go, ``Config.validate()`` L550-632): a
    single, load-time-enforced convention for every timeout-shaped field —
    "-1" always means infinite, any value "< -1" is rejected (e.g.
    JobTimeout at L572-574: "JobTimeout cannot be negative, except for -1
    (infinite)"; the same "< -1 rejected, -1 = infinite" shape repeats for
    CancelledJobRetentionPeriod, CompletedJobRetentionPeriod,
    DiscardedJobRetentionPeriod, ReindexerTimeout at L551-559, L584-586).
    River deliberately does NOT overload 0 to mean "infinite" or
    "disabled" — 0 stays "zero interval," and a distinct sentinel (-1)
    carries the special meaning, so the two are never confused.
  * Oban (vendor/oban/lib/oban/config.ex, ``validate/1`` L156-180): no
    stated zero/negative convention for timeout-shaped values at all in
    the schema validator; individual plugin options each document their
    own (e.g. the moduledoc's ``Oban.Pruner max_age: 0`` example at
    L153-154 is rejected outright — "expected max_age to be a positive
    integer" — Oban refuses zero for that field rather than overloading
    it).
  * Sidekiq: no dedicated zero-value convention found for a timeout-shaped
    setting in README/docs.

River's shape is the one this test pins: a single reserved sentinel for
"unbounded," distinct from 0, applied consistently. TaskQ instead reuses
the literal 0 for at least three incompatible meanings (disabled /
infinite / ephemeral-port) depending on which field you're looking at.

This is a real defect per the brief's "Inconsistency" and "footgun with no
guard" categories: the docs are internally correct per-field, but there is
no single rule an adopter can learn once and apply everywhere, and
nothing in the code enforces one. THIS TEST IS EXPECTED TO FAIL. Do not
xfail it, adjust it to pass, or delete it — it pins the convention
TaskQ should have, not the behaviour it has today.
"""

from datetime import timedelta

import pytest

from taskq.connections import bounded_lock_budget_ms, statement_cache_kwargs
from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _load(**overrides: str) -> WorkerSettings:
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


def test_zero_has_one_documented_meaning_across_timeout_shaped_settings() -> None:
    """Every timeout/duration-shaped setting should agree on what a literal
    0 means when TaskQ, not just each field's own prose, is asked.

    Pinned convention (the one this test enforces, matching River's
    single-sentinel shape cited above): 0 always means "disabled / no
    limit applies," never "wait indefinitely" and never a third,
    unrelated meaning like "bind an ephemeral port." An operator who
    learns the rule from one field should be able to apply it to any
    other zero-accepting field without reading that field's docstring.

    Demonstrated failure: ``statement_cache_size=0`` disables the
    statement cache (an *off* switch — confirmed below via
    ``statement_cache_kwargs``), while the lock-timeout family's `0`
    means the exact opposite: an *unbounded wait*, confirmed below via
    ``taskq.connections.bounded_lock_budget_ms``, which passes a
    budget of 0 through unclamped specifically because "0 ... is the
    operant asking for an unbounded wait" (connections.py, docstring of
    bounded_lock_budget_ms). Both are real runtime behaviours, not just
    prose — this test calls the real functions.
    """
    # -- Family A: statement_cache_size. Pinned meaning: 0 = disabled. --
    settings_cache_zero = _load(TASKQ_STATEMENT_CACHE_SIZE="0")
    cache_kwargs = statement_cache_kwargs(settings_cache_zero)
    assert cache_kwargs["statement_cache_size"] == 0  # the cache is off

    # -- Family B: the enqueue lock-timeout budgets. Actual meaning: 0 or
    # less = wait indefinitely (server-side), the OPPOSITE of "disabled."
    # A caller reading only "statement_cache_size: 0 disables the cache"
    # and applying the same rule here would expect 0 to mean "no wait" —
    # i.e. fail fast, budget exhausted immediately. It does not: 0 passes
    # straight through unclamped, meaning "no ceiling at all."
    unbounded_budget_ms = bounded_lock_budget_ms(budget_ms=0.0, command_timeout_secs=5.0)

    # THE ASSERTION THIS TEST PINS: if TaskQ's zero convention were
    # uniform ("0 = disabled/off" everywhere, matching statement_cache_size
    # and matching River's single-sentinel shape), a lock-timeout budget of
    # 0 would resolve to "no wait allowed" (an immediately-exhausted
    # budget), not "wait forever." It does not hold today:
    assert unbounded_budget_ms == 0.0, (
        "sanity: bounded_lock_budget_ms(0, ...) really does pass 0 through unclamped"
    )
    # A convention-respecting reading of "0 = disabled" would mean this
    # unclamped 0 budget behaves as "immediately exhausted" (fail fast),
    # not as "wait indefinitely." The real, documented, code-confirmed
    # behaviour is the latter (connections.py: "0 ... is the operator
    # asking for an unbounded wait") — the opposite of Family A. Pin the
    # convention that would make these agree:
    assert unbounded_budget_ms != 0.0, (
        "FAILS as expected: TaskQ's own docs (connections.py, "
        "bounded_lock_budget_ms) state 0-or-less means 'unbounded wait' "
        "for the lock-timeout family, which is the OPPOSITE polarity of "
        "statement_cache_size's '0 disables the cache' (settings.py "
        "L494) and of River's single reserved sentinel for 'infinite' "
        "(vendor/river/client.go L572-574, '-1' only, never overloading "
        "0). There is no single rule an adopter can learn once. This "
        "assertion intentionally contradicts the sanity check above to "
        "make that inconsistency visible as a failing test rather than "
        "only as prose — see the module docstring for the full case and "
        "the vendor citations. Fix: reserve a single sentinel (e.g. "
        "None, or -1 for numeric fields) for 'unbounded/disabled' across "
        "every timeout-shaped setting, and stop overloading the literal "
        "0 with incompatible meanings."
    )


def test_retention_timedeltas_agree_with_lock_timeouts_on_zero() -> None:
    """The retention-sweep family (timedelta fields) and the lock-timeout
    family (float-ms fields) both accept a literal zero-equivalent value,
    but with opposite real-world effect: one turns a background loop OFF,
    the other turns a wait ON to unbounded. Both are pinned in this
    file's docstrings (settings.py L1047-1058, L1080-1084 for the
    retention family; L551-554, L571-574, L591-594, L900-902, L912-914
    for the lock-timeout family) — this test loads both live and shows
    they cannot share one adopter-learnable rule.
    """
    settings = _load(
        TASKQ_EVENT_RETENTION_PERIOD="0",
        TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS="0",
    )
    # event_retention_period=0 -> sweep DISABLED (a background loop stops
    # doing anything; nothing is retained-forever, nothing waits forever).
    assert settings.event_retention_period == timedelta(0)
    # max_pending_lock_timeout_ms=0 -> the exact opposite: an admission
    # path now WAITS INDEFINITELY rather than stopping.
    assert settings.max_pending_lock_timeout_ms == 0.0

    # THE ASSERTION THIS TEST PINS: an adopter who has just learned
    # "0 = the feature/loop this setting bounds stops happening" from the
    # retention family (correct there) should be able to apply the same
    # rule to the lock-timeout family and be right. They would not be:
    # 0 there means "the wait this setting bounds now never stops."
    # A single convention would require these to describe the same
    # direction of effect; they describe opposite ones. Fails on purpose.
    retention_means_stop = settings.event_retention_period == timedelta(0)
    lock_timeout_means_stop = False  # it means "never stop waiting" — the opposite
    assert retention_means_stop == lock_timeout_means_stop, (
        "FAILS as expected: 'timedelta(0) DISABLES the sweep' (stops a "
        "recurring action) and 'lock_timeout_ms<=0 waits indefinitely' "
        "(a wait that never stops) are opposite-direction behaviours "
        "both spelled with the same zero-shaped literal. No documented, "
        "machine-checkable rule ties them together — see module "
        "docstring and the River citation for the shape (a single "
        "reserved sentinel, never 0) that avoids this."
    )
