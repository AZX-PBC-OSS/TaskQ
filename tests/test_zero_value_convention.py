"""Pins the shipped ``0``-sentinel convention: one rulebook, stated per family.

Several TaskQSettings/WorkerSettings fields accept a literal ``0`` (or
``timedelta(0)``) as a sentinel - a special meaning, not the quantity
zero - and the polarity deliberately differs per family:

  * ``statement_cache_size``: ``0`` disables the statement cache.
  * ``max_cached_statement_lifetime``: ``0`` caches statements
    indefinitely - the opposite polarity of the field above it.
  * The lock-wait budgets (``max_pending_lock_timeout_ms``,
    ``unique_for_lock_timeout_ms``, ``idempotency_lock_timeout_ms``,
    ``token_bucket_lock_timeout_ms``, ``sliding_window_lock_timeout_ms``):
    ``0`` or less waits indefinitely server-side (the ``lock_timeout``
    GUC convention).
  * The time-based deletion sweeps (``event_retention_period``,
    ``keyed_row_reclaim_period``): ``timedelta(0)`` disables the sweep.
  * The prune/archive retention fields (``prune_retention_succeeded``,
    ``prune_retention_failed``, ``prune_retention_cancelled``,
    ``prune_retention_abandoned``, ``archive_retention_period``):
    ``timedelta(0)`` archives/expires at the next sweep - deliberately
    opposite to the deletion sweeps.
  * ``health_port``: ``0`` binds an ephemeral port (the field's real
    "off" is *unset*, not ``0``).

The shipped contract is a DOCS convention: every family's polarity is
stated once in the "The `0` convention" section at the top of
``docs/guides/configuration.md``, every sentinel-``0`` field is named
there on its family's row, and each field's own settings description
states the same polarity.

Why not a runtime unification: the alternative considered was a
single reserved sentinel (``-1``) for "infinite", ``0`` never
overloaded, enforced at settings load (``-1`` the sole negative value
allowed, the same shape repeating for the retention and reindex
timeouts); the stricter alternative refuses ``0`` outright for the
affected fields. A single sentinel is
the cleaner shape, but adopting it now would REDEFINE what the literal
``0`` does on fields TaskQ already ships: an operator running
``TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS=0`` today has deliberately asked for
an unbounded wait, and re-reading ``0`` as "disabled" or "fail fast"
would flip that deployment to immediate typed refusals on upgrade - a
silent behavior change with no error raised. The convention doc is the
safe fix: it turns the per-family polarity into a learn-once rule
without moving any runtime behavior.

What the tests pin - and what they do not: the convention section
exists and names every sentinel-``0`` field with its family's polarity;
every named field's settings description states the same polarity; and
the runtime semantics those descriptions claim hold through the real
functions (``statement_cache_kwargs``, ``bounded_lock_budget_ms``, and
settings load itself). They fail if the convention section loses a
family, if a field's description loses its polarity sentence, or if a
family's runtime polarity moves. They do not pin the single-sentinel
runtime redesign - that proposal was rejected, above.
"""

from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path

import pytest

from taskq.connections import bounded_lock_budget_ms, statement_cache_kwargs
from taskq.settings import WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"

_CONFIGURATION_MD = Path(__file__).resolve().parent.parent / "docs" / "guides" / "configuration.md"


def _load(**overrides: str) -> WorkerSettings:
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


@dataclass(frozen=True)
class _ZeroField:
    """One sentinel-``0`` field and the polarity both doc surfaces must state.

    ``convention_polarity`` is the phrase the field's family row in the
    configuration.md convention table must carry; ``description_polarity``
    is the sentence the field's own settings description must carry. The
    two phrases belong to the SAME family - a surface rewritten to a
    different family's polarity drops its pinned phrase and fails.
    """

    env_var: str
    field_name: str
    convention_polarity: str
    description_polarity: str


_LOCK_BUDGET_FIELDS: tuple[tuple[str, str], ...] = (
    ("TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS", "max_pending_lock_timeout_ms"),
    ("TASKQ_UNIQUE_FOR_LOCK_TIMEOUT_MS", "unique_for_lock_timeout_ms"),
    ("TASKQ_IDEMPOTENCY_LOCK_TIMEOUT_MS", "idempotency_lock_timeout_ms"),
    ("TASKQ_TOKEN_BUCKET_LOCK_TIMEOUT_MS", "token_bucket_lock_timeout_ms"),
    ("TASKQ_SLIDING_WINDOW_LOCK_TIMEOUT_MS", "sliding_window_lock_timeout_ms"),
)

_ZERO_FIELDS: tuple[_ZeroField, ...] = (
    _ZeroField(
        "TASKQ_STATEMENT_CACHE_SIZE",
        "statement_cache_size",
        "disabled",
        "0 disables",
    ),
    _ZeroField(
        "TASKQ_MAX_CACHED_STATEMENT_LIFETIME",
        "max_cached_statement_lifetime",
        "indefinitely",
        "0 caches statements indefinitely",
    ),
    *(
        _ZeroField(
            env_var,
            field_name,
            "wait indefinitely",
            "0 or less waits indefinitely",
        )
        for env_var, field_name in _LOCK_BUDGET_FIELDS
    ),
    _ZeroField(
        "TASKQ_EVENT_RETENTION_PERIOD",
        "event_retention_period",
        "disabled",
        "timedelta(0) DISABLES the sweep",
    ),
    _ZeroField(
        "TASKQ_KEYED_ROW_RECLAIM_PERIOD",
        "keyed_row_reclaim_period",
        "disabled",
        "timedelta(0) DISABLES the sweep",
    ),
    _ZeroField(
        "TASKQ_PRUNE_RETENTION_SUCCEEDED",
        "prune_retention_succeeded",
        "immediately",
        "timedelta(0) archives succeeded jobs at the next sweep",
    ),
    _ZeroField(
        "TASKQ_PRUNE_RETENTION_FAILED",
        "prune_retention_failed",
        "immediately",
        "timedelta(0) archives failed jobs at the next sweep",
    ),
    _ZeroField(
        "TASKQ_PRUNE_RETENTION_CANCELLED",
        "prune_retention_cancelled",
        "immediately",
        "timedelta(0) archives cancelled jobs at the next sweep",
    ),
    _ZeroField(
        "TASKQ_PRUNE_RETENTION_ABANDONED",
        "prune_retention_abandoned",
        "immediately",
        "zero-means-now",
    ),
    _ZeroField(
        "TASKQ_ARCHIVE_RETENTION_PERIOD",
        "archive_retention_period",
        "immediately",
        "timedelta(0) hard-deletes",
    ),
    _ZeroField(
        "TASKQ_HEALTH_PORT",
        "health_port",
        "ephemeral",
        "0 binds an ephemeral port",
    ),
)


def _convention_section_lines() -> list[str]:
    """The ``## The `0` convention`` section of configuration.md, as lines."""
    text = _CONFIGURATION_MD.read_text()
    marker = "## The `0` convention"
    start = text.find(marker)
    assert start != -1, (
        "configuration.md lost its '## The `0` convention' section - the shipped "
        "contract is that the per-family polarity of a sentinel 0 is stated once "
        "there and cross-referenced from every field that follows it."
    )
    rest = text[start + len(marker) :]
    end = rest.find("\n## ")
    section = rest if end == -1 else rest[:end]
    return section.splitlines()


@pytest.mark.parametrize("zf", _ZERO_FIELDS, ids=lambda zf: zf.env_var)
def test_convention_table_names_the_field_with_its_family_polarity(zf: _ZeroField) -> None:
    """The convention section's row for *zf.env_var* states its family's polarity.

    The row is the learn-once surface: an adopter reads the family rule
    here instead of re-deriving it per field. Fails if the section drops
    the field's row or the row stops stating the family polarity.
    """
    lines = _convention_section_lines()
    row = next((line for line in lines if zf.env_var in line), None)
    assert row is not None, (
        f"{zf.env_var} is no longer named in the `0` convention table in "
        "configuration.md - every sentinel-0 field is named there on its "
        "family's row; restore the row or the field stops following a stated rule."
    )
    assert zf.convention_polarity in row, (
        f"the `0` convention table row for {zf.env_var} no longer states its "
        f"family polarity ({zf.convention_polarity!r}): {row!r}. The table is the "
        "one place the per-family rule is stated; a row that names the field "
        "without its polarity leaves the family rule unstated."
    )


def test_convention_section_warns_against_cross_family_generalisation() -> None:
    """The convention section carries its critical warning.

    The trap the table exists for is cross-family generalisation (the two
    statement-cache rows mean opposite things); a convention section
    without the warning teaches the families but not the hazard.
    """
    section = "\n".join(_convention_section_lines())
    assert "Never generalise" in section, (
        "the `0` convention section lost its warning never to generalise 0 "
        "from one family to another - the hazard statement is part of the "
        "shipped contract, not decoration."
    )


@pytest.mark.parametrize("zf", _ZERO_FIELDS, ids=lambda zf: zf.env_var)
def test_field_description_states_its_own_zero_polarity(zf: _ZeroField) -> None:
    """*zf.field_name*'s settings description documents its own ``0`` polarity.

    Verified against the runtime Field metadata (the description an
    operator sees), and required to agree with the convention table: the
    phrase pinned here is the same family's, so a description rewritten
    to another family's polarity fails this test - the two surfaces may
    not contradict each other.
    """
    fields = WorkerSettings.get_fields()
    assert zf.field_name in fields, f"WorkerSettings lost the field {zf.field_name!r}"
    description = fields[zf.field_name][1].description or ""
    assert zf.description_polarity in description, (
        f"{zf.field_name}'s description no longer documents its 0 polarity "
        f"({zf.description_polarity!r}). The shipped convention is per-family "
        "polarity stated in configuration.md AND restated on the field itself; "
        f"the field's description now reads: {description!r}"
    )


def test_runtime_zero_semantics_match_the_documented_polarity() -> None:
    """The documented polarities are what the code actually does with ``0``.

    Each assertion calls the real function the description makes its
    claim about, so the docs contract cannot drift from runtime behavior
    in either direction (a runtime change under a frozen doc fails here;
    a doc rewrite under frozen runtime fails the two pins above).
    """
    # Statement-cache family: 0 reaches asyncpg's create_pool as 0 -
    # statement_cache_size=0 disables the cache there (asyncpg's own
    # contract), so the pass-through IS the documented "disabled".
    settings_cache_zero = _load(TASKQ_STATEMENT_CACHE_SIZE="0")
    cache_kwargs = statement_cache_kwargs(settings_cache_zero)
    assert cache_kwargs["statement_cache_size"] == 0

    # ...while the sibling field's 0 is the opposite polarity: asyncpg
    # reads max_cached_statement_lifetime=0 as "no maximum lifetime",
    # i.e. cached indefinitely - also delivered by pass-through.
    settings_lifetime_zero = _load(TASKQ_MAX_CACHED_STATEMENT_LIFETIME="0")
    assert statement_cache_kwargs(settings_lifetime_zero)["max_cached_statement_lifetime"] == 0

    # Lock-wait family: a 0 budget passes through bounded_lock_budget_ms
    # UNCLAMPED - no client-side bound is derived for it - because 0 is
    # the operator asking for an unbounded server-side wait (the
    # lock_timeout GUC convention; the advisory acquire's timeout_ms <= 0
    # branch then runs one plain blocking acquire).
    unbounded_budget_ms = bounded_lock_budget_ms(budget_ms=0.0, command_timeout_secs=5.0)
    assert unbounded_budget_ms == 0.0

    # Deletion-sweep and prune/archive families: 0 loads as a valid value
    # (not a validation error) - the sweep loops and the archive CTE give
    # it the documented meaning (gate on `> timedelta(0)` for the
    # deletion sweeps; `finished_at < now - retention` for the prune
    # family, i.e. "older than right now" at 0).
    settings = _load(
        TASKQ_EVENT_RETENTION_PERIOD="0",
        TASKQ_MAX_PENDING_LOCK_TIMEOUT_MS="0",
    )
    assert settings.event_retention_period == timedelta(0)
    assert settings.max_pending_lock_timeout_ms == 0.0
    assert _load(TASKQ_ARCHIVE_RETENTION_PERIOD="0").archive_retention_period == timedelta(0)
