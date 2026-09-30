"""The ``taskq doctor`` analysis engine: the finding families.

Every function here is pure analysis — fetched rows, a window, and an
environment scan in; report lines out. Nothing in this module touches
Typer, a connection, or any CLI state: the asyncpg fetchers and the
``typer.echo`` rendering live in ``taskq.cli`` (``_doctor``), which is
this module's only importer.
"""

import difflib
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final, cast

from taskq.actor import ActorRef
from taskq.actor_config_ops import ActorConfigRow
from taskq.insights import INSIGHTS_WINDOWS
from taskq.settings import OIDCSettings, SAMLSettings, TaskQSettings, WorkerSettings
from taskq.timescale import STORAGE_MODE_SUMMARY, StorageMode
from taskq.worker._stall_tally import remedy_for_kind
from taskq.worker.queue_ops import QueueRow

# The engine's consumer is taskq.cli (the ``_doctor`` orchestrator): these are
# the names it imports (and the tests import through it). Declared here so the
# cross-module seam is explicit without decorating the moved definitions.
__all__ = [
    "_DOCTOR_INSIGHTS_WINDOW",
    "_StrandedActorJobs",
    "_doctor_findings",
    "_unknown_taskq_env_vars",
]


@dataclass(frozen=True, slots=True)
class _StrandedActorJobs:
    """One actor's stranded pending/scheduled rows, by strand shape.

    Mirrors the two shapes the leader's stranded-jobs sweep computes
    (``_stranded_jobs_loop`` in ``taskq/worker/_leader_sweeps.py``):
    ``no_actor_config`` rows can never become dispatch candidates, and
    ``unserved_queue`` rows route to a queue no live worker serves.
    """

    actor: str
    no_actor_config: int
    unserved_queue: int
    unserved_queues: tuple[str, ...]


def _taskq_env_var_names() -> frozenset[str]:
    """Every ``TASKQ_*`` env-var name the settings models define.

    Derived from the models' own field registries (``get_fields``) with the
    same mapping ``WorkerSettings.post_load`` uses for its empty-env
    messages: ``FieldInfo.alias`` when one is set, else
    ``env_prefix + FIELD_NAME.upper()``. ``WorkerSettings``' registry
    includes every inherited ``TaskQSettings`` field; the base model and
    the SSO sub-configs are unioned in explicitly so a legitimate
    ``TASKQ_OIDC_*`` / ``TASKQ_SAML_*`` variable is never flagged unknown.
    """
    names: set[str] = set()
    for model in (TaskQSettings, WorkerSettings, OIDCSettings, SAMLSettings):
        for field_name, (_field_type, field_info) in model.get_fields().items():
            names.add(
                field_info.alias if field_info.alias else f"{model.env_prefix}{field_name.upper()}"
            )
    return frozenset(names)


def _unknown_taskq_env_vars(env: Mapping[str, str] | None = None) -> list[str]:
    """The ``TASKQ_``-prefixed names in *env* (default ``os.environ``) that
    match no settings field.

    This is the config-drift trap the settings loader cannot catch: it
    reads only the names it defines, so a typo'd variable
    (``TASKQ_MAX_PENDNG_LOCK_TIMEOUT_MS``) loads nothing, raises nothing,
    and the intended field applies its documented default. ``doctor`` is
    the loud surface for it. Sorted for a deterministic report.
    """
    known = _taskq_env_var_names()
    names = os.environ if env is None else env
    return sorted(name for name in names if name.startswith("TASKQ_") and name not in known)


def _unknown_env_findings(unknown_env_vars: Sequence[str]) -> list[str]:
    """The report lines for the unknown-``TASKQ_-variable`` family.

    Each line names the offender, states the silent-default consequence,
    and offers the closest real setting name (``difflib``) as the remedy
    hint - a typo is nearly always a one-character drift from its target.
    The similarity cutoff is deliberately above ``difflib``'s default: a
    name with no real setting behind it must get the "no similar name"
    remedy, not a bogus suggestion.
    """
    known = _taskq_env_var_names()
    findings: list[str] = []
    for name in unknown_env_vars:
        closest = difflib.get_close_matches(name, known, n=1, cutoff=0.75)
        remedy = (
            f"did you mean {closest[0]}?"
            if closest
            else "no similar setting name exists; check the field tables in "
            "docs/guides/configuration.md"
        )
        findings.append(
            f"unknown TASKQ_ setting: {name} - a typo applies defaults silently, "
            f"the intended field loads its documented default. {remedy}"
        )
    return findings


# ── Doctor's operational-insight families ──────────────────────────────
#
# Every threshold below is derived in place, and each derivation is the
# same three-part argument: what the metric honestly measures, what the
# confound is, and why the number sits past the confound.  The families
# read the already-merged taskq.insights statements — pure SELECTs, so
# the read-only contract holds family by family.

#: The window every insights read runs over, and the label the findings
#: quote.  Derivation: 24h spans one full diurnal traffic cycle — the
#: shortest window that cannot mistake a nightly lull for a fleet to
#: shrink — while staying far inside the default 30-day prune retention,
#: so both UNION tiers (live + archive) can answer every statement.  It
#: is a member of the insights layer's own closed window set
#: (INSIGHTS_WINDOWS), not a doctor-private value the docs cannot
#: reproduce.
_DOCTOR_INSIGHTS_WINDOW: Final[timedelta] = INSIGHTS_WINDOWS["24h"]

#: STARVED: the utilization (due depth / effective capacity) above which
#: the imbalance family reports.  Derivation: effective capacity is the
#: jobs ONE claim wave can absorb (sum(max_concurrent) over the actors
#: routed to the queue x live workers).  Crossing 1.0 means more due work
#: than one wave — but any enqueue burst crosses 1.0 transiently, and the
#: dispatcher is built to absorb exactly that.  2.0 means the queue still
#: holds a full SECOND wave after the first drains entirely: a burst that
#: size is a fleet too small for its arrival rate.  The number is also
#: the lever's own granularity — one added worker (or one max_concurrent
#: step) adds exactly one wave of capacity — so a 2x finding is one
#: lever away from resolved.
_IMBALANCE_UTILIZATION_HIGH: Final[float] = 2.0

#: STARVED (strand arm): the oldest due job's age must exceed the
#: queue's own p95 wait by this factor.  Derivation: the p95 wait is
#: computed over the SAME window the pathological depth would inflate —
#: a burst raises the p95 with the depth, so a factor arm is what
#: survives that confound.  4x the tail means the row has outlived the
#: entire observed distribution INCLUDING its tail by a full factor: 95%
#: of the queue's recent deliveries waited less than a quarter of this
#: row's age, which no reading of the same window calls healthy.
_IMBALANCE_STRAND_FACTOR: Final[float] = 4.0

#: STARVED (strand arm) and the family-wide persistence floor: the oldest
#: due job's age must exceed the queue's own p95 wait by this factor.  Derivation: the p95 wait is
#: computed over the SAME window the pathological depth would inflate —
#: a burst raises the p95 with the depth, so a factor arm is what
#: survives that confound.  4x the tail means the row has outlived the
#: entire observed distribution INCLUDING its tail by a full factor: 95%
#: of the queue's recent deliveries waited less than a quarter of this
#: row's age, which no reading of the same window calls healthy.
#: The floor ALSO gates the utilization arm and the drain family's
#: depth claim (measured against the imbalance read's own
#: ``oldest_due_age_s``): a depth snapshot younger than the floor is a
#: burst the dispatcher may absorb before the report is read — the same
#: in-flight-claim logic the strand arm's derivation states — so no
#: depth-derived verdict may rest on a snapshot younger than the
#: claim-in-flight window.  (Measured: a healthy queue's 9-job burst
#: against capacity 4 read as a 2.2x utilization and a healthy idle-
#: capacity fleet's 51-job burst read as a 1.0-day drain eta without
#: this floor; both bursts were younger than the floor and gone before
#: it elapsed.)
_IMBALANCE_STRAND_FLOOR_S: Final[float] = 60.0


def _insights_window_label(window: timedelta) -> str:
    """The window as the findings quote it ("24h"), derived from the
    timedelta so the label cannot drift from the constant it names.
    Hours win below two days — the window's own docs quote 24h, not 1d."""
    seconds = window.total_seconds()
    if seconds >= 172800 and seconds % 86400 == 0:
        return f"{int(seconds // 86400)}d"
    if seconds % 3600 == 0:
        return f"{int(seconds // 3600)}h"
    return f"{int(seconds)}s"


def _format_eta(seconds: float) -> str:
    """An eta in the unit the operator would say it out loud in."""
    if seconds >= 86400:
        return f"{seconds / 86400:.1f} days"
    if seconds >= 3600:
        return f"{seconds / 3600:.1f} hours"
    if seconds >= 60:
        return f"{seconds / 60:.0f} minutes"
    return f"{seconds:.0f}s"


def _capacity_remedy(queue: str, stored_by_actor: Mapping[str, ActorConfigRow]) -> str:
    """The capacity sentence the imbalance findings end with: the only
    two levers that add dispatch capacity, with the actors actually
    serving the queue named when the stored config knows them."""
    serving = sorted(a for a, row in stored_by_actor.items() if row.queue == queue)
    actors = f" (actors serving it: {', '.join(repr(a) for a in serving)})" if serving else ""
    return (
        f"The levers that add capacity are the product's own two: start another worker "
        f"serving {queue!r}, or raise the serving actor's max_concurrent{actors}."
    )


def _imbalance_findings(
    imbalance_rows: Sequence[Mapping[str, Any]],
    wait_rows: Sequence[Mapping[str, Any]],
    stored_by_actor: Mapping[str, ActorConfigRow],
    window: timedelta,
) -> list[str]:
    """The IMBALANCE family, read from ``fetch_queue_imbalance`` (the
    per-queue depth / armed wave / live workers / effective capacity /
    utilization / oldest-due-age row) with ``fetch_wait_distribution``'s
    per-queue p95 as the strand arm's baseline.

    Two arms, each with its derivation at the threshold constants:

    * utilization (depth / effective capacity) above
      ``_IMBALANCE_UTILIZATION_HIGH``, HELD past the persistence floor
      (``_IMBALANCE_STRAND_FLOOR_S`` — the oldest due row must have
      outlived the claim-in-flight window): the starved queue.  The
      persistence gate is what separates a fleet too small for its
      arrival rate from a burst the dispatcher is absorbing at sampling
      time — a depth snapshot younger than the floor cannot support an
      arrival-rate claim.
    * oldest-due age above ``max(_IMBALANCE_STRAND_FACTOR x p95,
      _IMBALANCE_STRAND_FLOOR_S)`` — the strand.  The p95 is the CLEAN
      segment's (first-delivery rows only): the deferred segment's wait
      excludes its deferral by construction, so its percentile is not a
      queue-latency baseline.  A queue with no clean wait history has no
      honest baseline and the arm stays silent.

    ``utilization IS NULL`` (due work, nothing can serve it) has two
    shapes, reported by different lines: no LIVE worker on the queue is
    the unserved-queue starvation shape the stranded-jobs families above
    report; a live worker behind a stored ``max_concurrent = 0`` is the
    deliberately-stopped drain mode, which the stored-capacity section's
    own line names (``max_concurrent=0, DRAIN MODE``) — neither is
    reported twice.
    """
    label = _insights_window_label(window)
    p95_by_queue = {
        str(r["queue"]): float(r["p95_wait_s"])
        for r in wait_rows
        if r.get("segment") == "clean" and r.get("p95_wait_s") is not None
    }
    findings: list[str] = []
    for row in imbalance_rows:
        queue = str(row["queue"])
        utilization = row.get("utilization")
        age = row.get("oldest_due_age_s")
        if (
            utilization is not None
            and float(utilization) > _IMBALANCE_UTILIZATION_HIGH
            and age is not None
            and float(age) > _IMBALANCE_STRAND_FLOOR_S
        ):
            findings.append(
                f"queue {queue!r}: STARVED, {row['depth']} due job(s) against effective "
                f"capacity {row['effective_capacity']} (utilization {float(utilization):.1f}x, "
                f"threshold {_IMBALANCE_UTILIZATION_HIGH:.0f}x — a full second claim-wave of "
                f"due work survives after the first drains). {_capacity_remedy(queue, stored_by_actor)}"
            )
        p95 = p95_by_queue.get(queue)
        if (
            age is not None
            and p95 is not None
            and float(age) > max(_IMBALANCE_STRAND_FACTOR * p95, _IMBALANCE_STRAND_FLOOR_S)
        ):
            findings.append(
                f"queue {queue!r}: STRANDED WORK, the oldest due job has waited "
                f"{float(age):.0f}s — {float(age) / p95:.0f}x the queue's own p95 wait of "
                f"{p95:.0f}s over the last {label} (threshold: "
                f"{_IMBALANCE_STRAND_FACTOR:.0f}x p95 with a {_IMBALANCE_STRAND_FLOOR_S:.0f}s "
                "floor — the floor is 2x the 30s worker-liveness window, so a claim already "
                f"in flight is never reported). {_capacity_remedy(queue, stored_by_actor)}"
            )
    return findings


def _overprovisioning_findings(
    overprovisioning_rows: Sequence[Mapping[str, Any]],
    window: timedelta,
) -> list[str]:
    """The OVERPROVISIONING family, read from ``fetch_overprovisioning``:
    the statement's own verdict (live workers, ZERO due depth, fewer
    terminalisations across the whole window than workers — fewer than
    one completion per worker).  The remedy is consolidation in the
    workgroup config; a destructive suggestion (drop the queue, purge the
    rows) would destroy the very history that proves the verdict, so the
    finding says explicitly that nothing is deleted."""
    label = _insights_window_label(window)
    return [
        f"queue {row['queue']!r}: OVERPROVISIONED, {row['live_workers']} live worker(s) with "
        f"{row['depth']} due job(s) and {row['terminalisations']} completion(s) in the last "
        f"{label} — fewer than one completion per worker across the whole window. "
        "Consolidate the workers serving this queue in the workgroup config (fewer "
        "[[workers]] entries or a smaller fleet on the queue's subscriber list); nothing "
        "is deleted — the queue's rows and history are untouched."
        for row in overprovisioning_rows
        if row.get("overprovisioned")
    ]


def _drain_findings(
    drain_rows: Sequence[Mapping[str, Any]],
    imbalance_rows: Sequence[Mapping[str, Any]],
    window: timedelta,
) -> list[str]:
    """The DRAIN family, read from ``fetch_drain_estimates``.

    Threshold derivation — the eta must exceed the observation window
    itself.  The eta is depth / (terminalisations / window): a
    throughput extrapolation whose only honest input is the traffic the
    window actually carried (``has_traffic``).  An eta beyond the window
    means the due depth exceeds everything the ENTIRE window completed —
    the queue needs more than the full observation period to drain,
    which is the longest horizon the rate has any evidence for, and
    exactly the "will this be done by tomorrow?" period the operator
    recognizes.  Below the window the extrapolation still has support;
    above it the finding fires.

    The finding also requires the depth to have PERSISTED past the
    persistence floor (``_IMBALANCE_STRAND_FLOOR_S``, read from the
    imbalance rows' ``oldest_due_age_s`` — the same read the imbalance
    family runs): the observed rate of an idle-capacity fleet is
    demand-limited, so a depth snapshot younger than the claim-in-flight
    window says nothing about the fleet's drain rate.  A healthy fleet's
    burst is gone before the floor elapses; a depth that outlives it is
    a rate the extrapolation can honestly claim.

    ``has_traffic = false`` (no terminalisations in the window) means the
    estimate is honestly NULL — "already drained" would be a lie — so a
    no-traffic window renders NO drain finding; the widening-the-window
    advice lives in docs/guides/insights.md, and the empty queue is what
    the overprovisioning family exists to catch.
    """
    label = _insights_window_label(window)
    window_s = window.total_seconds()
    age_by_queue = {
        str(r["queue"]): r.get("oldest_due_age_s")
        for r in imbalance_rows
        if r.get("oldest_due_age_s") is not None
    }
    findings: list[str] = []
    for row in drain_rows:
        if not row.get("has_traffic"):
            continue
        eta = row.get("eta_seconds")
        if eta is None or float(eta) <= window_s:
            continue
        age = age_by_queue.get(str(row["queue"]))
        if age is None or float(age) <= _IMBALANCE_STRAND_FLOOR_S:
            continue  # a depth younger than the claim-in-flight window: no rate claim
        queue = str(row["queue"])
        armed = (
            f" The armed wave ({row['scheduled_depth']} scheduled job(s)) is not included "
            "in the eta."
            if row.get("scheduled_depth")
            else ""
        )
        findings.append(
            f"queue {queue!r}: SLOW DRAIN, {row['depth']} due job(s) at the observed rate of "
            f"{float(row['completions_per_second']):.3g} completion(s)/s drains in "
            f"~{_format_eta(float(eta))}, longer than the {label} window the rate was measured "
            "over (threshold: the eta exceeds the observation window — the depth exceeds "
            f"everything the entire window completed). Confidence caveat: a throughput "
            f"extrapolation resting on {row['terminalisations']} completion(s) of realised "
            "traffic (has_traffic) — it assumes the next window looks like the last one, "
            "the workers stay up, and nothing enqueues behind the current depth." + armed
        )
    return findings


def _cron_lag_findings(
    cron_rows: Sequence[Mapping[str, Any]],
    window: timedelta,
) -> list[str]:
    """The CRON LAG family, read from ``fetch_cron_ledger``: one finding
    per schedule whose fan-out outruns its clearance, naming the
    schedule, the backlog, and the two honest remedies (slow the cron /
    add workers for the actor).

    Two trigger arms:

    * ``runaway_trending`` — the statement's own verdict: fires >
      cleared in BOTH the current and the prior window.  One window is a
      burst; two consecutive is the runaway shape.
    * outstanding above the catch-up window's slot capacity.  The
      catch-up window is the same insights window, and its slot capacity
      is the schedule's OWN demonstrated clearance — the better of the
      current and the prior equal window (``max(cleared_window,
      cleared_prior)``), REQUIRED POSITIVE: what the fleet actually
      cleared in one window at its best, not a theoretical ceiling it has
      never been observed to reach, and never zero — a schedule with no
      demonstrated clearance has nothing to compare against, and its
      in-flight fires are honest work in flight, not a lag (a capacity
      of zero would read every first in-flight fire as an uncatchable
      backlog).  The best-of-two guards the ledger's documented
      right-edge confound (clearance lags fires at the window's edge, so
      a burst of fresh fires must not read as an uncatchable backlog).
      The arm also requires the schedule to have actually fired within
      the two-window trend horizon (``fires_window > 0 or fires_prior >
      0``): a weekly cron with one long-running fire has no measured
      clearance capacity to compare against, and its outstanding row is
      honest work in flight, not a lag.
    """
    label = _insights_window_label(window)
    findings: list[str] = []
    for row in cron_rows:
        if not row.get("enabled"):
            continue  # A disabled schedule fires nothing new; its ledger is historical.
        runaway = bool(row.get("runaway_trending"))
        outstanding = int(row["outstanding"])
        capacity = max(int(row["cleared_window"]), int(row["cleared_prior"]))
        fired_in_horizon = int(row["fires_window"]) > 0 or int(row["fires_prior"]) > 0
        backlog = fired_in_horizon and capacity > 0 and outstanding > capacity
        if not runaway and not backlog:
            continue
        triggers: list[str] = []
        if runaway:
            triggers.append(
                "the fan-out is trending: fires outran clearances in both the current "
                "and the prior window"
            )
        if backlog:
            triggers.append(
                f"the backlog exceeds one {label} catch-up window's slot capacity (the "
                f"schedule's own demonstrated clearance: {capacity} in its best window)"
            )
        findings.append(
            f"cron schedule {row['schedule_id']} (actor {row['actor']!r}, "
            f"cron {row['cron_expr']}): CRON LAG, {outstanding} fire(s) outstanding with "
            f"{row['cleared_window']} of {row['fires_window']} cleared in the last {label} — "
            f"{' and '.join(triggers)}. Two honest remedies: slow the cron (widen the "
            "interval or raise its budget), or add workers for the actor — raise its "
            "max_concurrent so more fires clear per wave."
        )
    return findings


def _storage_mode_findings(
    mode: StorageMode,
    *,
    flag_on: bool,
    downgraded_policies: Sequence[str] = (),
) -> list[str]:
    """The report lines for the storage-mode family, the FIRST family in
    every doctor report: the mode the connected server was DETECTED in
    (``taskq.timescale.detect_storage_mode`` — the extension catalog, then
    ``timescaledb.license``, never the settings) plus that mode's
    capability consequences in one glance, from
    ``taskq.timescale.STORAGE_MODE_SUMMARY``.

    Green line: every mode is a supported configuration — the line is
    information, not a defect. The two red arms: the flag on in an
    environment whose server detects vanilla, because that contradiction
    is not workable — the next ``taskq migrate up`` refuses; and the
    license DOWNGRADE — a server converted under the full TSL license
    whose license was later downgraded to apache: the conversion-era
    policy jobs are still registered (measured on 2.30.1: they fail on
    every background run under the downgraded license) and the row-level
    sweeps defer the aged end to them, so rows older than their horizon
    strand — a healthy apache summary alone would be a lie on that server.
    """
    findings = [
        f"storage mode: {mode.value} - {STORAGE_MODE_SUMMARY[mode]} "
        "(the mode x capability matrix: docs/guides/timescaledb.md)"
    ]
    if flag_on and mode is StorageMode.VANILLA:
        findings.append(
            "storage mode drift: TASKQ_TIMESCALEDB_HYPERTABLES=true but this "
            "server detects vanilla (no timescaledb extension installed) - the "
            "next `taskq migrate up` refuses with TimescaleDBUnavailableError. "
            "Unset the flag in this environment, or enable the extension on the "
            "server first (docs/guides/timescaledb.md)."
        )
    if mode is StorageMode.TIMESCALE_APACHE and downgraded_policies:
        findings.append(
            f"storage mode drift: this server's timescaledb.license is 'apache' but "
            f"{len(downgraded_policies)} TimescaleDB policy job(s) from an earlier "
            f"timescale-license deployment are still registered "
            f"({', '.join(downgraded_policies)}): they fail on every background run "
            "under this license, and the row-level sweeps defer the aged end to them, "
            "so rows older than their horizon strand - nothing deletes them. Restore "
            "the timescale license (ALTER SYSTEM SET timescaledb.license = 'timescale' "
            "and reload), re-run `taskq migrate up` to converge, then flip and disable "
            "properly (docs/guides/timescaledb.md)."
        )
    return findings


def _doctor_findings(
    registry: Mapping[str, ActorRef[Any, Any]],
    rows: list[ActorConfigRow],
    queues: list[QueueRow],
    stranded: list[_StrandedActorJobs],
    worker_stalls: list[tuple[str, dict[str, object]]] | None = None,
    unknown_env_vars: Sequence[str] | None = None,
    storage_mode: StorageMode | None = None,
    timescaledb_flag: bool = False,
    downgraded_policies: Sequence[str] = (),
    *,
    imbalance_rows: list[dict[str, Any]] | None = None,
    wait_rows: list[dict[str, Any]] | None = None,
    overprovisioning_rows: list[dict[str, Any]] | None = None,
    drain_rows: list[dict[str, Any]] | None = None,
    cron_rows: list[dict[str, Any]] | None = None,
    insights_window: timedelta = _DOCTOR_INSIGHTS_WINDOW,
) -> list[str]:
    """Every condition worth an operator's attention, as report lines.

    Every family is a condition TaskQ has decided a worker keeps
    running through, which is why they surface here rather than at boot:
    each one produces no error anywhere, and its only symptom is work
    that quietly does not happen.

    ``worker_stalls`` carries each live worker's stall tally as read from
    its ``workers`` row metadata (``(worker_id, loop_stalls)``): the
    attributed event-loop stalls that worker's lag watchdog recorded.

    ``unknown_env_vars`` carries the ``TASKQ_``-prefixed environment names
    that match no settings field (``_unknown_taskq_env_vars``): each is a
    configuration typo applying its intended setting's default silently.

    ``storage_mode`` is the server's detected storage mode
    (:func:`taskq.timescale.detect_storage_mode`, read while the report's
    connection is open) — the first finding family, the mode and its
    capability consequences in one glance. ``timescaledb_flag`` is the
    flag as this environment loaded it, for the family's red arms: the
    flag on against a vanilla-detected server, and the license downgrade
    (``downgraded_policies`` — :func:`taskq.timescale.probe_registered_policy_jobs`,
    read on the same connection when the mode detected apache) naming the
    conversion-era policies a downgraded license strands.

    The four ``*_rows`` insight parameters carry the operational-insight
    families as the ``taskq.insights`` fetchers returned them
    (``fetch_queue_imbalance`` / ``fetch_wait_distribution`` ->
    ``imbalance_rows`` + ``wait_rows``, ``fetch_overprovisioning``,
    ``fetch_drain_estimates``, ``fetch_cron_ledger``), all over
    ``insights_window``: the IMBALANCE, OVERPROVISIONING, DRAIN and CRON
    LAG families.  They are keyword-only and default to empty — the
    healthy fleet — so a caller that cannot read the insights layer
    still gets every stored-config family.
    """
    stored_by_actor = {row.actor: row for row in rows}
    findings: list[str] = []

    # The storage mode leads every report: it is the ground the rest of
    # the report stands on (which retention mechanisms exist, what the
    # sweeps own), it is detected from the server rather than read from
    # settings, and its one contradiction (the flag on, the server
    # vanilla) is upstream of the deploy step's own refusal.
    if storage_mode is not None:
        findings.extend(
            _storage_mode_findings(
                storage_mode,
                flag_on=timescaledb_flag,
                downgraded_policies=downgraded_policies,
            )
        )

    # The environment family next: a typo'd TASKQ_ variable is upstream of
    # every stored-row condition below - the wrong value was in force before
    # any worker registered. Nothing errors at load (the loader reads only
    # the names it defines), so this report is the only surface that names it.
    findings.extend(_unknown_env_findings(unknown_env_vars or []))

    # The dispatch capacity gate joins actor_config, so a registered actor
    # with no row is not merely uncapped, it is never a candidate.
    for name in sorted(set(registry) - set(stored_by_actor)):
        findings.append(
            f"{name}: no stored actor_config row, NEVER DISPATCHES. The dispatch "
            "capacity gate reads only stored rows, so jobs accumulate pending "
            "with no error anywhere. A worker startup seeds the row."
        )

    # The same gate seen from the jobs side: rows already pending/scheduled
    # whose actor has no stored config row (a renamed or removed actor that
    # old producers or old rows still reference) never dispatch either, and
    # no registry walk can name them, the registry no longer knows the name.
    # The unserved-queue arm is the fleet-liveness twin: the row's routing
    # queue (its actor's stored assignment once re-pended) has no live
    # worker subscribed, so every dispatch round annihilates the pair.
    for entry in sorted(stranded, key=lambda e: e.actor):
        if entry.no_actor_config:
            registry_note = (
                " and no entry in the loaded registry" if entry.actor not in registry else ""
            )
            findings.append(
                f"{entry.actor}: {entry.no_actor_config} pending/scheduled job(s) whose "
                f"actor has no stored actor_config row{registry_note}, NEVER DISPATCHES. "
                "The dispatch capacity gate reads only stored rows, so these jobs wait "
                "forever with no error anywhere. Re-register the actor and seed its row "
                "(a worker startup does this), or purge the jobs if the actor was retired."
            )
        if entry.unserved_queue:
            queue_names = ", ".join(repr(q) for q in entry.unserved_queues)
            findings.append(
                f"{entry.actor}: {entry.unserved_queue} pending/scheduled job(s) routed to "
                f"queue(s) {queue_names} that no live worker serves, they wait while "
                "nothing consumes them. Start a worker subscribed to the queue or move "
                "the actor onto a served one."
            )

    # A queues row whose queue no actor is assigned to is inert until an
    # actor is moved onto that name and silently inherits its cap.
    assigned = {row.queue for row in stored_by_actor.values()}
    for queue in sorted(
        (q for q in queues if q.name not in assigned and q.max_concurrent is not None),
        key=lambda q: q.name,
    ):
        findings.append(
            f"queue {queue.name!r}: STALE queues row, max_concurrent="
            f"{queue.max_concurrent} but no actor is assigned to it. The cap is "
            "inert now and silently applies to the next actor moved onto this queue."
        )

    queue_caps = {q.name: q.max_concurrent for q in queues}
    for name in sorted(stored_by_actor):
        row = stored_by_actor[name]
        # Neither value is invalid alone: only the combination is
        # unsatisfiable, so only a combination check can catch it.
        if (
            row.max_concurrent is not None
            and row.max_pending is not None
            and row.max_pending < row.max_concurrent
        ):
            findings.append(
                f"{name}: INCOHERENT, max_pending={row.max_pending} is below "
                f"max_concurrent={row.max_concurrent}, so the actor may queue fewer "
                "jobs than it may run at once and its concurrency cap is unreachable."
            )
        queue_cap = queue_caps.get(row.queue)
        if (
            row.max_concurrent is not None
            and queue_cap is not None
            and queue_cap < row.max_concurrent
        ):
            findings.append(
                f"{name}: INCOHERENT, max_concurrent={row.max_concurrent} exceeds "
                f"queue {row.queue!r}'s max_concurrent={queue_cap}, which binds first; "
                "raising the actor cap alone changes nothing."
            )
    for worker_id, tally in sorted(worker_stalls or [], key=lambda w: w[0]):
        for actor_name, kinds in sorted(tally.items(), key=lambda kv: str(kv[0])):
            if not isinstance(kinds, dict) or not kinds:
                continue
            kind_map = cast("dict[str, object]", kinds)
            kind_counts: dict[str, int] = {}
            for kind, count in kind_map.items():
                if isinstance(count, (int, float)) and not isinstance(count, bool):
                    kind_counts[str(kind)] = int(count)
            if not kind_counts:
                continue
            total = sum(kind_counts.values())
            dominant = max(kind_counts, key=lambda k: (kind_counts[k], k))
            kind_desc = ", ".join(
                f"{kind} x{count}"
                for kind, count in sorted(kind_counts.items(), key=lambda kv: (-kv[1], kv[0]))
            )
            findings.append(
                f"worker {worker_id}: actor {actor_name} has stalled the event loop "
                f"{total} time(s) ({kind_desc}; {dominant} dominates), "
                f"{remedy_for_kind(dominant)}. The worker's "
                "`event-loop-stall-attributed` warnings name the file:line."
            )

    # The operational-insight families, read from the same statements the
    # docs guide documents (taskq/insights.py): the live-shape conditions
    # the stored-config families above cannot see, because they are about
    # TRAFFIC, not config.  Every read is a SELECT — the read-only
    # contract holds here exactly as it holds above.
    findings.extend(
        _imbalance_findings(imbalance_rows or [], wait_rows or [], stored_by_actor, insights_window)
    )
    findings.extend(_overprovisioning_findings(overprovisioning_rows or [], insights_window))
    findings.extend(_drain_findings(drain_rows or [], imbalance_rows or [], insights_window))
    findings.extend(_cron_lag_findings(cron_rows or [], insights_window))
    return findings
