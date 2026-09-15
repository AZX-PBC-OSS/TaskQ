"""Docs/docstrings must not promise the crash-reclaim outbox slice
(``kind='state_change'`` AND ``detail->>'reason'='lock_expired'``) is
"exempt from the sweep at any setting" -- it is not.

``_sweeps.py``'s ``expired_outbox`` CTE deletes those rows once
``occurred_at`` is older than ``retention * RECLAIM_OUTBOX_RETENTION_MULTIPLIER``
(100x the configured ``event_retention_period``; see
``src/taskq/constants.py``). That is a deliberate, documented-in-source
design (see the ``expired_outbox`` CTE comment and the
``RECLAIM_OUTBOX_RETENTION_MULTIPLIER`` docstring in constants.py), but the
operator-facing text in three places overstates the guarantee to "exempt at
any setting" with no mention of the 100x age cap:

- ``settings.py``'s ``event_retention_period`` field description
- ``docs/guides/upgrading.md``'s "job_events rows past the retention
  period are deleted" section
- ``client/_taskq.py``'s ``watch_reclaims`` docstring, which tells
  operators to prune by their slowest consumer's cursor without ever
  mentioning that the built-in sweep ALSO prunes by age alone (100x
  retention) -- exactly the operational trap in issue #198: a
  short-retention deployment with a consumer lagging past 100x retention
  silently loses reclaim events with no error.

These are "red" tests: they currently FAIL because the unqualified
"exempt ... at any setting" wording is still present verbatim, and/or the
100x age-cap is not mentioned anywhere an operator reading these surfaces
would see it. They should start passing only once the docs are corrected
to describe the actual (bounded) exemption -- at which point they become a
regression pin.
"""

from __future__ import annotations

from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
_DOCS = _ROOT / "docs"
_SRC = _ROOT / "src" / "taskq"


def test_settings_py_does_not_overstate_outbox_exemption() -> None:
    text = (_SRC / "settings.py").read_text()
    idx = text.find("event_retention_period")
    field_block = text[idx : idx + 2000]
    normalized = " ".join(field_block.split())
    # The current (wrong) claim: unconditional exemption, no cap mentioned.
    assert "is exempt from the sweep at any setting" not in normalized, (
        "settings.py still claims the lock_expired outbox slice is exempt "
        "from the retention sweep 'at any setting' -- it is deleted once "
        "occurred_at exceeds retention * RECLAIM_OUTBOX_RETENTION_MULTIPLIER "
        "(100x). The docstring must say so."
    )
    # Once corrected, the field description should actually name the cap.
    assert "RECLAIM_OUTBOX_RETENTION_MULTIPLIER" in normalized or "100x" in normalized, (
        "event_retention_period's description should mention the "
        "lock_expired outbox slice's 100x age cap, not just call it exempt"
    )


def test_upgrading_guide_does_not_overstate_outbox_exemption() -> None:
    text = (_DOCS / "guides" / "upgrading.md").read_text()
    section = text.split("### `job_events` rows past the retention period are deleted", 1)[1][:2000]
    normalized = " ".join(section.split())
    assert "is exempt at any setting" not in normalized, (
        "upgrading.md still claims the lock_expired outbox slice is "
        "'exempt at any setting' -- it is deleted at 100x the retention "
        "period. Docs must disclose the age cap so operators with a "
        "short retention period and a lagging consumer aren't surprised "
        "by silent reclaim-event loss."
    )
    assert "RECLAIM_OUTBOX_RETENTION_MULTIPLIER" in normalized or "100x" in normalized, (
        "the corrected section should state the multiplier (100x retention) "
        "at which the outbox slice IS eventually deleted"
    )


def test_watch_reclaims_docstring_mentions_the_age_based_sweep() -> None:
    """The docstring tells operators to prune by their slowest consumer's
    cursor, but never warns that the built-in sweep also deletes
    lock_expired rows by age alone (100x retention) regardless of cursor
    position -- the exact trap in issue #198.

    Note: the docstring DOES contain unrelated uses of the words "sweep"
    (referring to transaction-commit sweep timing, for the
    visibility-delay-risk diagnostic) and "age" (in "drains at query
    speed", of an outage) that are false-positive matches for a naive
    substring check -- so this test requires the specific multiplier
    concept, not those loose words.
    """
    text = (_SRC / "client" / "_taskq.py").read_text()
    idx = text.find("async def watch_reclaims")
    assert idx != -1, "watch_reclaims not found in client/_taskq.py"
    docstring = text[idx : idx + 4000]
    assert (
        "RECLAIM_OUTBOX_RETENTION_MULTIPLIER" in docstring
        or "100x" in docstring
        or "100 x" in docstring
        or "100 times" in docstring
    ), (
        "watch_reclaims docstring tells operators to prune by their "
        "slowest consumer's cursor but never mentions that the retention "
        "sweep independently deletes lock_expired rows once they exceed "
        "100x the retention period -- a lagging consumer past that age "
        "silently loses reclaim events with no error. The docstring "
        "should warn about this explicitly (not just contain the loose "
        "words 'sweep' or 'age' in an unrelated context)."
    )
