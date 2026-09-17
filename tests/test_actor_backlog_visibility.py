"""Pins for making an unconsumed actor visible in the emitted series.

The deployment failure this guards against is silent by construction.
No worker refuses to start because an actor's queue has no consumer —
the governing rule is that a worker able to do work never fails to boot,
and in a multi-worker or workgroup fleet no single supervisor can know
what consumes a queue. So a misrouted actor produces no refusal, no
error, and no failed job: its rows simply pile up pending forever while
every health probe stays green and every worker reports healthy.

Monitoring is the only place that condition can surface, and it surfaces
only at the right granularity. A queue-level depth gauge cannot
distinguish "one actor on this queue is never consumed" from "this queue
is busy": the shared queue's depth looks like ordinary load, and the
fleet-wide oldest-due-age gauge is about promotion (scheduled to
pending), not about pending work no consumer takes. Backlog depth and
oldest-pending-age must therefore be attributable at BOTH queue and
actor granularity in the series the bridge emits. The alert-rule half of
that contract lives beside the other rule-file drift pins; these pins
are about the series those rules have to read.
"""

from __future__ import annotations

from opentelemetry.metrics import CallbackOptions

import taskq.obs as obs_mod
import taskq.obs._otel as otel_mod


def test_backlog_depth_is_attributable_per_actor_and_queue() -> None:
    """Pending backlog depth is emitted per (actor, queue) pair, not only
    per queue.

    An actor sharing a queue with healthy actors is invisible in a
    queue-summed depth: the queue keeps moving, so nothing crosses a
    depth threshold and nothing rises. Split by actor, the stuck actor's
    own series is the one that rises without bound while its siblings
    stay flat — the shape that distinguishes an unconsumed actor from
    load.
    """
    update = getattr(obs_mod, "update_actor_backlog_cache", None)
    assert update is not None, (
        "taskq.obs exposes no per-actor backlog-depth cache update — backlog "
        "depth is attributable per queue only, so an actor whose jobs are "
        "never consumed is indistinguishable from a busy shared queue"
    )

    update({("emails", "default"): 3, ("reports", "default"): 4021})

    observe = otel_mod._observe_actor_backlog  # pyright: ignore[reportAttributeAccessIssue]  # Why: the gauge callback is the only way to read a synchronous observable gauge without a full SDK scrape, the pattern every gauge pin in this suite uses.
    observations = list(observe(CallbackOptions()))

    by_labels = {
        (
            str(dict(o.attributes or {}).get("actor")),
            str(dict(o.attributes or {}).get("queue")),
        ): o.value
        for o in observations
    }
    assert by_labels == {("emails", "default"): 3, ("reports", "default"): 4021}, (
        f"each (actor, queue) pair must carry its own depth observation; got {by_labels!r}"
    )


def test_oldest_pending_age_is_attributable_per_actor_and_queue() -> None:
    """Oldest-pending-age is emitted per (actor, queue) pair.

    Depth alone is ambiguous: a deep queue that drains is healthy
    throughput. Age is what separates the two — an actor nobody consumes
    has a pending job whose age grows monotonically with wall clock,
    while a busy actor's oldest pending job stays bounded by its drain
    rate no matter how deep the queue gets. This is distinct from the
    fleet-wide oldest-DUE-age gauge, which measures promotion from
    scheduled to pending and reads 0.0 for work that is already pending
    and simply never taken.
    """
    update = getattr(obs_mod, "update_actor_oldest_pending_age_cache", None)
    assert update is not None, (
        "taskq.obs exposes no per-actor oldest-pending-age cache update — "
        "without it, pending work that no consumer ever takes reads as 0.0 on "
        "the oldest-due-age gauge (that gauge measures promotion, not pickup) "
        "and nothing in the emitted series ages"
    )

    update({("emails", "default"): 2.5, ("reports", "default"): 86_400.0})

    observe = otel_mod._observe_actor_oldest_pending_age  # pyright: ignore[reportAttributeAccessIssue]  # Why: same gauge-callback observation pattern as above.
    observations = list(observe(CallbackOptions()))

    by_labels = {
        (
            str(dict(o.attributes or {}).get("actor")),
            str(dict(o.attributes or {}).get("queue")),
        ): o.value
        for o in observations
    }
    assert by_labels == {("emails", "default"): 2.5, ("reports", "default"): 86_400.0}, (
        f"each (actor, queue) pair must carry its own age observation; got {by_labels!r}"
    )


def test_actor_backlog_series_carry_only_actor_and_queue_dimensions() -> None:
    """The per-actor backlog series carry exactly {actor, queue} and no
    identity-like dimension.

    Actor names and queue names are bounded by the deployment's own
    registration; job ids, worker ids and schedule ids are not, and one
    of them riding along here would make the series that exists to be
    alerted on the series that cannot be stored.
    """
    obs_mod.update_actor_backlog_cache({("emails", "default"): 1})  # pyright: ignore[reportAttributeAccessIssue]  # Why: the desired public seam; its absence is asserted with a readable message by the pins above.
    obs_mod.update_actor_oldest_pending_age_cache({("emails", "default"): 1.0})  # pyright: ignore[reportAttributeAccessIssue]  # Why: see above.

    for callback_name in ("_observe_actor_backlog", "_observe_actor_oldest_pending_age"):
        observe = getattr(otel_mod, callback_name)
        dimension_sets = {
            tuple(sorted(dict(o.attributes or {}))) for o in observe(CallbackOptions())
        }
        assert dimension_sets == {("actor", "queue")}, (
            f"{callback_name} emits dimensions {dimension_sets!r}; the contract is "
            "exactly (actor, queue) — enough to name the stuck actor, and nothing "
            "unbounded"
        )


def test_backlog_sampler_feeds_the_per_actor_caches() -> None:
    """The backlog sampler's query groups pending work by actor and queue
    and computes the oldest pending age from the same snapshot.

    A gauge with no sampler feeding it reads 0 forever, which is exactly
    the reading a healthy fleet produces — the most dangerous possible
    failure for a detector, because it is indistinguishable from success.
    """
    from taskq.worker import _leader_shared, _leader_sweeps

    sql_template = getattr(_leader_shared, "_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE", None) or getattr(
        _leader_sweeps, "_QUERY_ACTOR_BACKLOG_SQL_TEMPLATE", None
    )
    assert sql_template is not None, (
        "no per-actor backlog query exists in the leader sweep SQL — the "
        "per-actor gauges have nothing feeding them and would read 0 forever, "
        "which is what a healthy fleet reads too"
    )

    sql = str(sql_template).format(schema="taskq")
    normalized = " ".join(sql.split()).lower()
    assert "group by" in normalized and "actor" in normalized and "queue" in normalized, (
        f"the per-actor backlog query must group by actor and queue; got: {sql}"
    )
    assert "'pending'" in normalized, (
        "the per-actor backlog query must select PENDING work: the unconsumed-actor "
        "condition is pending rows no consumer takes, not scheduled rows awaiting "
        f"promotion; got: {sql}"
    )
    assert "min(" in normalized, (
        "the per-actor backlog query must carry the oldest-pending timestamp from "
        "the same snapshot as the depth, so depth and age can never describe two "
        f"different moments; got: {sql}"
    )
