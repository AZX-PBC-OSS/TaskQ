"""Docs that PR #170 changed must describe the shipped behavior.

Three surfaces drifted from the code after PR #170 landed:

- `upgrading.md` claims `taskq.cron.consecutive_failures` keeps a
  `schedule_id` dimension. The instrument is labeled by `actor` (see
  `_otel.py`'s own module docstring and the instrument's own
  `description=`) -- a dashboard grouped by `schedule_id` loses its
  series entirely, because that label was never emitted.
- `ops.md` and `deployment.md` describe `taskq.backpressure.errors` as a
  pure producer/capacity-pressure signal with no `kind` filter called
  out. The counter also increments for `unique_for_lock_timeout` and
  `idempotency_lock_timeout` -- identity-serialization refusals that
  are not capacity signals (an operator alerting on raw counter growth
  gets paged by contention that has nothing to do with `max_pending` or
  rate limits).
- `jobs-clients.md` says the `unique_for` lock timeout bumps no
  `taskq.backpressure.errors` counter. It does, with
  `kind="unique_for_lock_timeout"`.

These pins hold the docs to the code that actually ships, scoped to the
surfaces PR #170 touched.
"""

from __future__ import annotations

from pathlib import Path

_DOCS = Path(__file__).resolve().parent.parent / "docs"
_SRC = Path(__file__).resolve().parent.parent / "src" / "taskq"


def test_upgrading_guide_does_not_claim_schedule_id_dimension_for_cron_failures() -> None:
    text = (_DOCS / "guides" / "upgrading.md").read_text()
    assert "`taskq.cron.consecutive_failures` keeps its `schedule_id` dimension" not in text, (
        "the gauge is labeled by actor, not schedule_id -- a dashboard grouped by "
        "schedule_id would lose its series"
    )


def test_otel_instrument_confirms_actor_not_schedule_id_label() -> None:
    """Guards the premise the doc pin above relies on: the instrument's own
    description names its actual dimension, so this fails loudly if the
    label is ever widened back to schedule_id without the doc following."""
    otel_src = (_SRC / "obs" / "_otel.py").read_text()
    section = otel_src.split('"taskq.cron.consecutive_failures"', 1)[1][:800]
    assert "actor label is capped" in section
    assert "schedule_id" not in section


def test_ops_guide_backpressure_errors_entry_names_the_non_capacity_kinds() -> None:
    text = (_DOCS / "guides" / "ops.md").read_text()
    section = text[text.index("`taskq.backpressure.errors`") :][:400]
    assert "kind" in section, (
        "ops.md's backpressure.errors watchlist entry describes it as a pure "
        "producer-pressure signal but the counter also fires for identity-lock "
        "timeouts that are not capacity pressure -- the entry must point operators "
        "at the kind label"
    )


def test_deployment_guide_backpressure_errors_mention_names_the_kind_dimension() -> None:
    text = (_DOCS / "guides" / "deployment.md").read_text()
    section = text[text.index("`taskq.backpressure.errors`") :][:400]
    assert "kind" in section, (
        "deployment.md tells operators to monitor taskq.backpressure.errors for "
        "'sustained producer pressure' without naming the kind label that "
        "separates capacity denials from identity-lock timeouts"
    )


def test_jobs_clients_guide_unique_for_lock_timeout_admits_it_bumps_the_counter() -> None:
    text = (_DOCS / "guides" / "jobs-clients.md").read_text()
    section = text[text.index("bounded") : text.index("bounded") + 2000]
    assert (
        "bumps no" not in section
        or "backpressure.errors" not in section.split("bumps no", 1)[1][:80]
    ), (
        "the unique_for lock-timeout paragraph must not claim it bumps no "
        "taskq.backpressure.errors counter -- record_backpressure_error is called "
        "with kind='unique_for_lock_timeout' on that path"
    )


def test_enqueue_source_confirms_unique_for_lock_timeout_records_backpressure() -> None:
    """Guards the premise: the enqueue path really does record this kind,
    so the doc pin above is pinning shipped behavior, not a hypothetical."""
    enqueue_src = (_SRC / "backend" / "_enqueue.py").read_text()
    assert 'record_backpressure_error(actor, kind="unique_for_lock_timeout")' in enqueue_src
    assert 'record_backpressure_error(args.actor, kind="idempotency_lock_timeout")' in enqueue_src
