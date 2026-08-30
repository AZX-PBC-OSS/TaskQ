"""`max_concurrent` must not be documented as a hard fleet-wide cap.

It is a per-round admission damper. Dispatch reads `running_per_actor` once,
before `locked` takes its FOR UPDATE SKIP LOCKED row locks, and never rechecks
it -- so two dispatchers each see the same `in_flight`, each admit up to the
cap, and lock disjoint rows. Both succeed, and the over-dispatched jobs really
run; reclaiming stale locks does not undo an over-dispatch.

`deployment.md` claimed it caps execution "across all workers", contradicting
`architecture.md`, `actors.md`, and the `@actor` docstring. An operator sizing a
memory-bound actor from that sentence gets an OOMKill. These tests pin the
correction so it cannot regress to the confident-and-wrong wording.
"""

from __future__ import annotations

from pathlib import Path

_DOCS = Path(__file__).resolve().parent.parent / "docs"
_SRC = Path(__file__).resolve().parent.parent / "src" / "taskq"


def test_deployment_guide_does_not_promise_a_fleet_wide_cap() -> None:
    text = (_DOCS / "guides" / "deployment.md").read_text()
    section = text.split("### max_concurrent and max_pending", 1)[1][:3000]
    assert "run simultaneously **across all workers**" not in section, (
        "the unqualified fleet-wide claim is back"
    )
    assert "best-effort" in section
    assert "(num_producers - 1) * max_concurrent" in section
    # It must also say what to use instead, or the correction is not actionable.
    assert "set-max-concurrent" in section


def test_rate_limiting_guide_does_not_call_it_per_worker() -> None:
    """The third contradictory framing: neither a hard fleet cap nor per-worker."""
    text = (_DOCS / "guides" / "rate-limiting.md").read_text()
    assert "caps concurrency per\nactor, per worker" not in text
    assert "best-effort" in text


def test_rate_limiting_guide_upserts_rather_than_updates() -> None:
    """A plain UPDATE silently matches zero rows on a fresh deployment."""
    text = (_DOCS / "guides" / "rate-limiting.md").read_text()
    assert 'UPDATE "taskq".queues SET max_concurrent = 20 WHERE name' not in text
    assert "ON CONFLICT (name) DO UPDATE SET max_concurrent" in text


def test_dispatch_sql_documents_the_toctou_for_max_concurrent() -> None:
    """The source documented this window for `identity_key` but not for
    `max_concurrent` -- the one operators actually tune."""
    sql = (_SRC / "backend" / "_dispatch_sql.py").read_text()
    head, _, _ = sql.partition("running_identities AS (")
    preamble = head[: head.index("running_per_actor AS (")]
    assert "NOT a hard fleet-wide cap" in preamble
    assert "(num_producers - 1) * max_concurrent" in preamble
    assert "ConcurrencyReservation" in preamble, "must name the mechanism that IS a hard cap"
