"""Cross-cutting constants shared across the TaskQ library.

Centralises values that are referenced from multiple modules so that
producer and consumer always agree — e.g. the ``pg_notify`` channel name
used by the worker LISTEN consumer and the future
PostgresBackend enqueue path.
"""

import hashlib
import re
from datetime import timedelta
from typing import Final
from uuid import UUID

__all__ = [
    "BTREE_MAX_ITEM_BYTES",
    "CANCEL_ORIGIN_ABANDONED",
    "CANCEL_ORIGIN_COOPERATIVE",
    "CANCEL_ORIGIN_FORCED",
    "CANCEL_ORIGIN_PENDING",
    "DEFAULT_CHUNK_SIZE",
    "DEFAULT_EVENT_RETENTION_BATCH_SIZE",
    "DEFAULT_EVENT_RETENTION_PERIOD",
    "DEFAULT_EVENT_WRITER_BATCH_SIZE",
    "DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS",
    "DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE",
    "DEFAULT_KEYED_ROW_RECLAIM_PERIOD",
    "DEFAULT_MAX_KEYED_RESERVATIONS",
    "DEFAULT_MAX_RETRY_BACKOFF",
    "DEFAULT_PRUNE_BATCH_SIZE",
    "DEFAULT_PRUNE_RETENTION",
    "DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS",
    "DEFAULT_RECLAIM_POLL_LIMIT",
    "DEFAULT_RESERVATION_BACKOFF",
    "EVENTS_CHANNEL_FMT",
    "IDEMPOTENCY_KEY_BYTES_CEILING",
    "MAX_ATTEMPTS_SMALLINT_CEILING",
    "MAX_ENQUEUABLE_MAX_ATTEMPTS",
    "MAX_IDEMPOTENCY_KEY_BYTES",
    "MAX_RESULT_BYTES",
    "MIN_DEFERRAL_INTERVAL",
    "PROGRESS_CHANNEL_FMT",
    "PROGRESS_GLOBAL_CHANNEL_FMT",
    "QUEUE_CONCURRENCY_PREFIX",
    "RECLAIM_EVENT_VISIBILITY_DELAY",
    "RECLAIM_OUTBOX_RETENTION_MULTIPLIER",
    "RESERVATION_RETRY_HINT_MARGIN",
    "SMALLINT_MAX",
    "SMALLINT_MIN",
    "WAKE_CHANNEL_FMT",
    "WORKER_CHANNEL_FMT",
    "base_name_collides_with_reserved_prefix",
    "check_max_attempts_domain",
    "check_priority_domain",
    "events_channel",
    "progress_channel",
    "progress_global_channel",
    "quote_ident",
    "schema_lock_name",
    "wake_channel",
    "worker_channel",
]

RECLAIM_EVENT_VISIBILITY_DELAY: Final[timedelta] = timedelta(seconds=2)
"""Trailing-watermark safety margin for ``poll_reclaim_events``.

``job_events.id`` (bigserial ``nextval``) and ``occurred_at``
(``clock_timestamp()``) are both stamped at INSERT time within the same
statement, so they are co-monotonic: whichever row was inserted first has
both the lower ``id`` and the earlier ``occurred_at``. Strictly, this
co-monotonicity is itself an assumption, not a guarantee: the two values
come from separate volatile calls which Postgres does not evaluate
atomically with respect to concurrent transactions, so in principle one
transaction can evaluate both of its own between another transaction's
``nextval`` and ``clock_timestamp()``, stamping a lower ``id`` with a
later ``occurred_at`` — the exact inversion the watermark exists to
prevent. The window is nanosecond-scale and no occurrence is known, but
"by construction" is too strong a claim. A plain ``SELECT`` can never see
an uncommitted sibling row at all (it is invisible under MVCC, not just
filtered out), so no snapshot- or transaction-id-based predicate computed
only over *visible* rows can detect one. Instead, ``poll_reclaim_events``
only returns rows whose ``occurred_at`` is older than this margin: by the
time a row clears it, any transaction that could have inserted a
still-lower ``id`` has had at least as long to commit —
so it must have either committed already (and is returned, correctly
ordered, in this or an earlier poll) or aborted (permanently gone, safe
to skip).

**This guarantee is conditional, not unconditional.** It assumes (a) the
non-interleaving of ``nextval``/``clock_timestamp()`` described above,
and (b) that no ``job_events`` writer takes longer than this margin
between its INSERT and its COMMIT. Sweep and terminal-write transactions
are a handful of single-round-trip statements with no external I/O, so
this is a generous
bound under normal operation — but it is an assumption enforced by
nothing in the SQL itself, not a property the query guarantees on its
own. Known ways it can be violated: lock contention delaying commit
after the ``FOR UPDATE SKIP LOCKED`` scan, a slow or overloaded database
extending that scan itself, a stalled/GC-paused worker holding the
transaction open, or an abnormally large batch inserted in one
transaction. If a writer transaction does exceed the margin, the
consequence is a **silently missed event**: a lower-``id`` row can commit
after the cursor has already advanced past its position, with no error
raised anywhere — the same failure mode this feature exists to prevent,
just pushed to a rarer trigger.

``PostgresBackend.check_reclaim_visibility_delay_risk`` turns this from a
silent failure into an operator-visible one: it reports any transaction
that has held ``job_events`` open longer than the margin (see
:class:`~taskq.backend._protocol.LongRunningJobEventsWriter`) — a proxy
warning, not proof of an actual miss.  Every ``TaskQ.watch_reclaims``
consumer runs it automatically on a slow cadence (60s, see
``taskq.client._taskq._VISIBILITY_RISK_CHECK_INTERVAL``) and logs
``watch-reclaims-visibility-delay-at-risk`` when it fires, so detection
is default-on rather than opt-in; it can also be called directly from a
dedicated monitoring/alerting loop (see ``docs/architecture.md``'s
crash-reclaim section for what it does and does not detect).
Configurable via
``WorkerSettings.reclaim_event_visibility_delay`` /
``TASKQ_RECLAIM_EVENT_VISIBILITY_DELAY`` and per-call via
``poll_reclaim_events(..., visibility_delay=...)`` — raise it if sweeps
run under heavy contention or against large batches; lower it if lower
latency matters more and writes are known to be fast.
"""

DEFAULT_RESERVATION_BACKOFF: Final[timedelta] = timedelta(seconds=5)
"""Default backoff when ``RateLimitDecision.retry_after`` is ``None``.

Callers MUST coalesce via an identity check (``is None``), NOT truthiness,
because ``timedelta(0)`` is falsy and represents an allowed decision that
must be passed through unchanged.
"""

RESERVATION_RETRY_HINT_MARGIN: Final[timedelta] = timedelta(seconds=0.5)
"""Safety margin added to a slot denial's capacity-derived retry hint.

A denial for a full bucket reports the earliest held lease's expiry as
its ``retry_after`` (computed against the same server clock that stamps
the leases), so the denied job re-attempts when capacity can actually
free rather than on a fixed cadence. The margin covers the distance
between the hint's read and the re-attempt's arrival — the denial write,
the scheduled-to-pending promotion and the next dispatch round — which
is milliseconds of scheduling latency, not holder processing: a live
holder's heartbeat extends its lease before expiry, so waiting past the
expiry instant never guarantees the slot is free anyway. Sub-second
hints are additionally floored by ``MIN_DEFERRAL_INTERVAL`` downstream,
so the margin's real work is on multi-second lease horizons where it is
noise by design.
"""

CANCEL_ORIGIN_COOPERATIVE: Final[str] = "CancelledCooperatively"
"""``error_class`` a phase-1 cancel's terminal write stamps.

The actor was running, observed the cancel request while it was still
being asked (``cancel_phase = 1``) and stopped — the worker's terminal
write (``mark_cancelled``) is the one that moved the row.
"""

CANCEL_ORIGIN_FORCED: Final[str] = "CancelledForced"
"""``error_class`` a phase-2 cancel's terminal write stamps.

The actor did not yield to the request, the heartbeat loop escalated
(``cancel_phase = 2``) and ``task.cancel()`` interrupted it — the actor
had to be stopped, which is operationally distinct from a cooperative
yield: this actor ignored a cancellation request and needs looking at.
"""

CANCEL_ORIGIN_ABANDONED: Final[str] = "CancelAbandoned"
"""``error_class`` the abandon write (``mark_abandoned``) stamps.

The actor did not yield within the cancellation graces, so the ladder
took the row away from it. Operationally distinct from a cooperative
cancel: this actor needs looking at.
"""

CANCEL_ORIGIN_PENDING: Final[str] = "CancelledBeforeStart"
"""``error_class`` a cancel of a not-yet-running job stamps.

The job never reached a worker, so no attempt exists to explain and no
actor-level hook can have run for it. Covers the single-job request
(``write_cancel_request``) and the bulk filter (``cancel_where``) alike —
the same outcome must read the same way whichever path produced it.

Why ``error_class`` rather than a new column or a ``job_status`` value:
the three origins are one dimension of one terminal state, every terminal
failure path already self-describes through this column
(``DeadlineExceeded``, ``WorkerCrashed``, ``ActorDeregistered``), and the
admin UI, ``taskq doctor`` and the archive all read it already. A status
enum change would break every consumer of the eight-value union for a
distinction that is not a different state. Vendor precedent records the
cancel durably on the row the same way — River stamps
``cancel_attempted_at`` (a row whose cancel timestamp is set is
cancelled, never re-available), Oban stamps ``cancelled_at``.
"""

MIN_DEFERRAL_INTERVAL: Final[timedelta] = timedelta(seconds=1)
"""Minimum effective delay a NON-consuming deferral reschedules out.

A ``Snooze``, a ``RetryAfter(consume_budget=False)``, and an admission
denial's ``retry_after`` all hand their delay to ``mark_snoozed``'s
snooze arm, which maps it onto ``scheduled_at``.  Without a floor, a
zero delay parks the job ``pending`` at ``clock_timestamp()`` — first
in every dispatch round (``ORDER BY scheduled_at``) and instantly
re-claimable, so one job monopolises a worker slot in a claim/refund
round trip per cycle.  Both non-consuming arms therefore apply
``GREATEST(delay, this interval)`` to guard the same edge: a non-future
delay is rejected outright because it feeds back into the head of the
dispatch queue, monopolising a slot.

A consuming ``RetryAfter`` is exempt: an immediate retry is a real
execution, bounded by the budget it spends, not a deferral competing
for the head of the dispatch order.
"""

DEFAULT_MAX_RETRY_BACKOFF: Final[timedelta] = timedelta(hours=24)
"""Default ceiling on a single retry's backoff.

Why 24 h: it is one standard on-call rotation, so a job whose backoff has
reached the ceiling is retried at least once per shift and never waits
longer than the window in which someone is watching. The effective ceiling is
``WorkerSettings.max_retry_backoff``; this is the value that setting
defaults to, and the fallback every retry-computation signature carries
so a caller that constructs one directly (tests, the in-memory backend)
gets the same cap as a worker loaded from settings. Named here because
six call sites had it as an independent literal, where a change to one
would have silently disagreed with the rest.
"""

DEFAULT_MAX_KEYED_RESERVATIONS: Final[int] = 10_000
"""Default ceiling on tracked keyed-reservation entries and their pending reclaims.

The effective value is ``WorkerSettings.max_keyed_reservations``; this
constant is that setting's default, and the fallback every
keyed-reservation bound carries when no settings object is in scope.
Two structures carry the ceiling: the registry's in-process tracking
dict (the entry cap, enforced on the acquisition path) and the
pending-reclaim set of evicted keyed buckets awaiting their
``reservation_slots`` row deletion (its record cap, passed by both
eviction call sites — the per-worker sweep and the opportunistic
eviction on the acquisition path). The pending set is NOT bounded by
the tracked-entry count: entries are evicted and re-materialised in
waves, so pending accumulates across waves up to its own cap, at which
point eviction is vetoed until the drain empties it. The heal-stamp
dicts sit at or below the tracked-entry count (they ride the
registration lifecycle). Named here so the settings default and the
registry fallback cannot drift apart.
"""

DEFAULT_PRUNE_BATCH_SIZE: Final[int] = 10000
"""Default rows deleted per batch by the prune and archive-expiry sweeps.

The effective value is ``WorkerSettings.prune_batch_size``; the sweep
functions carry it as a signature default for direct callers. Batching at
all is what keeps a prune off a long-held lock; the size itself is the
lock-duration / round-trip trade-off.
"""

DEFAULT_EVENT_WRITER_BATCH_SIZE: Final[int] = 100
"""Default rows per committed batch for every writer of ``job_events`` rows.

Why a bound at all: :data:`RECLAIM_EVENT_VISIBILITY_DELAY` (2 s) conditions
``poll_reclaim_events``' trailing-watermark guarantee on no ``job_events``
writer holding its transaction open longer than the margin between INSERT
and COMMIT, and explicitly names "an abnormally large batch inserted in one
transaction" as a violation. A batch cap plus a server-side
``statement_timeout`` (:data:`DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS`)
turn that invariant from a hope into an enforced property: the timeout is
the enforcement, the batch size merely keeps a healthy database off it.

Why 100: the measured cost of a two-statement reclaim batch is ~0.85 ms per
row, so 100 rows is ~85 ms on loopback and ~300 ms at a 3 ms managed-Postgres
round trip — a 6x margin under the 2 s watermark at the RTT the derivation
targets. Loopback extrapolation, not a managed-instance measurement; the
effective value is operator-tunable via ``WorkerSettings.event_writer_batch_size``
and the ``statement_timeout`` remains the guard if the constant is wrong for
a given deployment.

Deliberately NOT :data:`DEFAULT_PRUNE_BATCH_SIZE` (10,000): prune writes no
``job_events`` rows and iterates aggregate rows, so it is a different risk
class. Anything that writes one ``job_events`` row per input row — the
expired-lock, deadline and scheduled-to-pending sweeps, bulk cancel, actor
deregistration — uses this constant (or the setting that defaults to it).

The reduced degradation tier is a quarter of the effective size
(``max(1, size // 4)``), computed where the tier is selected: it exists so a
database that keeps cancelling sweep batches gets smaller bites, not so it
gets another operator knob.
"""

DEFAULT_EVENT_WRITER_STATEMENT_TIMEOUT_MS: Final[int] = 1750
"""Server-side ``statement_timeout`` for one event-writer batch transaction.

7/8 of the 2 s :data:`RECLAIM_EVENT_VISIBILITY_DELAY` default: the batch must
fit inside the visibility margin, and the server aborts it if it does not —
arriving as ``QueryCanceledError`` (SQLSTATE 57014), which the sweep loops
treat as transient. Applied with ``SET LOCAL`` inside the batch transaction
only: a session-level ``SET`` would outlive the pooled connection's checkout
and silently cap unrelated borrowers (dispatch, archive) at a timeout they
never asked for.
"""

DEFAULT_PRUNE_STATEMENT_TIMEOUT_MS: Final[int] = 4000
"""Server-side ``statement_timeout`` for one prune/archive-expiry batch.

80% of the default ``dispatcher_command_timeout`` (5.0 s): the prune
family runs its batches on dispatcher-pool connections, and the pool's
client-side ``command_timeout`` fires as an opaque ``TimeoutError`` — so
the server-side bound is deliberately the *smaller* of the two. An
overloaded database then aborts the batch server-side
(``QueryCanceledError``, SQLSTATE 57014 — the transient family the
:class:`~taskq.backend._sweeps.SweepBatchSizer` breaker counts), and the
breaker latches a reduced batch size for the next attempt, instead of the
client cancelling with no degradation signal. A maintenance loop pairs a
per-query timeout with a reduced-batch circuit breaker so an overloaded
database can drain under controlled batch sizes; the dispatcher pool's
shared command timeout is the tighter ceiling this family must live under,
so the reduced tier — not a longer timeout — is what makes a loaded
database drainable. The effective value is derived from the configured
``dispatcher_command_timeout`` by the prune loops
(:mod:`taskq.worker._leader_sweeps`); this constant is the signature
default for direct callers and matches the default deployment shape.
"""

DEFAULT_PRUNE_RETENTION: Final[timedelta] = timedelta(days=30)
"""Fallback retention for a terminal status with no configured period.

The effective value is ``WorkerSettings.prune_retention_period`` (and the
per-status fields that override it); the sweep uses this constant when
``retention_per_status`` has no entry for a status, so a status added to
``TERMINAL_STATUSES`` without a matching setting is retained rather than
pruned immediately.
"""

DEFAULT_EVENT_RETENTION_PERIOD: Final[timedelta] = timedelta(days=7)
"""Default age at which ``job_events`` rows become deletable, regardless of
parent-job status.

The effective value is ``WorkerSettings.event_retention_period``
(``timedelta(0)`` there disables the sweep entirely); this constant is the
setting's default. The crash-reclaim outbox slice
(``kind='state_change' AND detail->>'reason'='lock_expired'``) is kept
``RECLAIM_OUTBOX_RETENTION_MULTIPLIER`` times this window before the same
sweep deletes it (see that constant for the derivation).

Why 7 days: events are narration — the durable forensic record for a job
is jobs/jobs_archive plus job_attempts/job_attempts_archive, kept for the
30-90 day prune windows and the 365-day archive window — so the event
window only has to bound event volume, not match job retention. At the
measured ~2 events/job and 100 jobs/s, 7 days is ~26 GB steady state
against ~110 GB at 30 days, while staying at or under the shortest
job-retention window (30 d) so events never outlive the shortest
observation an operator could reasonably run.
"""

DEFAULT_EVENT_RETENTION_BATCH_SIZE: Final[int] = 10000
"""Default ``job_events`` rows deleted per committed batch by the retention
sweep.

The effective value is ``WorkerSettings.event_retention_batch_size``; the
sweep function carries it as a signature default for direct callers. The
bound keeps one sweep call's DELETE a constant-size statement against any
backlog size. 10_000 matches the prune family's batch rather than the
100-row event-writer bound: the retention sweep writes no ``job_events``
rows, so the ``RECLAIM_EVENT_VISIBILITY_DELAY`` INSERT-to-COMMIT margin
that caps event *writers* does not bind it — the general
bounded-per-transaction rule does.
"""

DEFAULT_KEYED_ROW_RECLAIM_PERIOD: Final[timedelta] = timedelta(hours=1)
"""Default idle age at which fleet-reclaimable keyed rows (keyed
``reservation_slots`` rows; PG-state-backed keyed ``rate_limit_buckets``
rows) become deletable by the maintenance leader's fleet sweep.

The effective value is ``WorkerSettings.keyed_row_reclaim_period``
(``timedelta(0)`` there disables the sweep entirely); this constant is the
setting's default. Why 1 hour: it is the SAME threshold the in-process
registry eviction uses (``taskq.ratelimit.registry._KEYED_IDLE_THRESHOLD``)
— a keyed entry the registry would already have evicted for idleness is
exactly the entry whose rows the fleet sweep may reclaim, so the two
reclamation tiers converge instead of the fleet sweep racing ahead of the
registry's own idleness definition and churning rows under still-tracked
buckets (the acquire-path heal covers the overlap, but the churn is
pointless when one threshold serves both tiers).
"""

DEFAULT_KEYED_ROW_RECLAIM_BATCH_SIZE: Final[int] = 256
"""Default bound on the fleet reclaim sweep's committed batch per tick.

The effective value is ``WorkerSettings.keyed_row_reclaim_batch_size``. The
unit is BUCKETS for ``reservation_slots`` (each bucket's full slot row set
deletes together — a partial delete would shrink configured capacity) and
ROWS for ``rate_limit_buckets`` (one row per bucket). Why 256: it matches
the in-process pending-reclaim drain's per-statement slice
(``_DEFAULT_RECLAIM_BATCH_NAMES`` in ``taskq.ratelimit.registry``), so both
reclamation tiers move keyed rows at the same constant-size rate — at the
default 30 s sweep interval that is ~512 buckets/min against a backlog
bounded by the per-worker keyed caps, and one tick's write set stays
independent of that backlog.
"""

RECLAIM_OUTBOX_RETENTION_MULTIPLIER: Final[int] = 100
"""How many times the ordinary retention window the crash-reclaim outbox
slice (``kind='state_change' AND detail->>'reason'='lock_expired'``) is
kept before the retention sweep presumes its consumer gone and deletes it.

The outbox cannot be exempt at every age: a fleet with NO
``TaskQ.watch_reclaims`` consumer would then retain every ``lock_expired``
event forever (unbounded growth), and an event committed below a watermark
cursor that already passed it is unreachable to ``poll_reclaim_events``
(``id > $1`` cannot go back) — without an age cap such a row is BOTH
undeliverable and undeletable, permanently lost signal AND permanent
storage. But it also cannot be deleted at the ordinary retention age: the
carve-out exists so a consumer whose cursor has not reached a row yet
still sees it. The multiplier composes the two: an unconsumed outbox row
outlives ordinary events by this factor of the configured retention, then
is deleted — bounded, but far beyond any healthy consumer's lag.

Why 100 exactly: it must clear BOTH pinned ages with headroom on either
side. Upward — a 400-day-old outbox row must survive a sweep call at
30-day retention (``test_lock_expired_reclaim_outbox_is_exempt_from_
retention``, the guard rail the original carve-out pinned): 100 x 30 d ≈
3000 d, 7.5x headroom. Downward — a 1-hour-old unconsumed row must be
deleted by a 1-second-retention drain (``test_rt_orphans_outbox_immortal_
events``, the no-consumer bound): 100 x 1 s = 100 s, 36x headroom. Any
value in (~13.4, 3600) satisfies both pins; 100 sits logarithmically
midway and reads as "two orders of magnitude more patience than the
narration slice gets."
"""

DEFAULT_CHUNK_SIZE: Final[int] = 1000
"""Default rows per round trip for the chunked bulk-job APIs.

Why 1000: it keeps a single chunk's parameter array well inside asyncpg's
comfortable range while amortising the round trip. Every caller can
override it per call via ``chunk_size=``; this is only the default, shared
so the client, backend and SQL layers cannot drift apart.
"""

DEFAULT_RECLAIM_POLL_LIMIT: Final[int] = 100
"""Default rows per ``poll_reclaim_events`` call.

Why shared: this default is declared on the Backend protocol and repeated
by every implementation. If they drift, a caller relying on the protocol
default silently gets a different batch size per backend — the same class
of divergence that made the in-memory and Postgres backends disagree
before. The value itself is a poll batch, not a tuning knob: callers that
care pass ``limit=`` explicitly.
"""

MAX_RESULT_BYTES: Final[int] = 65536
"""Default maximum serialised byte length of a job's terminal result dict.

The effective cap is ``WorkerSettings.result_max_bytes``
(``TASKQ_RESULT_MAX_BYTES``), which defaults to this value; this constant
is what the enforcement points fall back to when no settings object is in
scope (a direct ``Backend`` call, the in-memory backend).

Enforced on both the consumer success path (before ``mark_succeeded``) and
the backend terminal-write path (inside ``_mark_succeeded_on_conn``) so a
result that slips past the consumer check is still rejected at the storage
boundary. Nothing else depends on the value: there is no CHECK constraint
on ``jobs.result_size_bytes`` and the result is never carried in a Redis
event payload.
"""

SMALLINT_MIN: Final[int] = -32768
"""Lower bound of the Postgres ``smallint`` domain."""

SMALLINT_MAX: Final[int] = 32767
"""Upper bound of the Postgres ``smallint`` domain.

``jobs.priority``, ``jobs.max_attempts`` and ``jobs.attempt`` are all
``smallint`` (``migrations/01.00.00_01_pre_initial.sql``). Every layer
that accepts one of those values from a caller refuses out-of-domain
input against these two constants, so the refusal cannot drift between
the client, the actor declaration and the backend boundary.
"""

MAX_ATTEMPTS_SMALLINT_CEILING: Final[int] = SMALLINT_MAX
"""The ``jobs.max_attempts`` column's domain ceiling."""

MAX_ENQUEUABLE_MAX_ATTEMPTS: Final[int] = MAX_ATTEMPTS_SMALLINT_CEILING - 1
"""Largest ``max_attempts`` a fresh enqueue or policy may carry.

One below the column ceiling, retained as a defensive margin: a row
parked at exactly the ceiling has no headroom for any statement that
needs to add one to a max_attempts-derived value. Rows can still legally
REACH the ceiling — a snooze arm's saturating increment parks a snoozed
job there — which is why the retry layer clamps row-stored values back
into this bound before reconstructing a policy.
"""


def _check_is_int(value: object, what: str) -> None:
    """Refuse a non-integer before any range comparison runs.

    The range guards below are reached from ``EnqueueArgs``, a plain
    dataclass with no runtime type enforcement — unlike ``RetryPolicy``,
    where pydantic coerces first. Without this, ``None`` raises a bare
    ``TypeError`` from the ``<`` comparison naming neither the field nor
    the expected type, and a float passes every range check and is stored
    for the driver to reject later. Both are the untyped-refusal shape the
    typed errors here exist to prevent.

    ``bool`` is excluded deliberately: it is an ``int`` subclass, so
    ``True`` would otherwise satisfy a ``>= 1`` bound and silently mean
    one attempt.
    """
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{what} must be an int, got {type(value).__name__}")


def check_priority_domain(value: int, *, what: str = "priority") -> None:
    """Refuse a ``priority`` outside the ``smallint`` column's domain.

    Shared by every layer that accepts a priority so the arithmetic is
    stated once: the client override, the actor declaration, and the
    enqueue-boundary struct all raise the same refusal for the same
    value. ``what`` names the caller's parameter, so the message points
    at the argument to fix rather than at a column in a driver
    traceback.
    """
    _check_is_int(value, what)
    if value < SMALLINT_MIN or value > SMALLINT_MAX:
        raise ValueError(
            f"{what} must fit smallint range ({SMALLINT_MIN}..{SMALLINT_MAX}), got {value}"
        )


def check_max_attempts_domain(value: int, *, what: str = "max_attempts") -> None:
    """Refuse a ``max_attempts`` outside the enqueuable range.

    Below one the job is dispatchable but can never complete — the first
    failure finds no budget left — so the value is refused rather than
    stored. Above :data:`MAX_ENQUEUABLE_MAX_ATTEMPTS` the column has no
    headroom left to raise the ceiling on a reclaim.
    """
    _check_is_int(value, what)
    if value < 1:
        raise ValueError(f"{what} must be >= 1, got {value}")
    if value > MAX_ENQUEUABLE_MAX_ATTEMPTS:
        raise ValueError(
            f"{what} must fit the smallint jobs.max_attempts column with one of "
            f"defensive headroom (<= {MAX_ENQUEUABLE_MAX_ATTEMPTS}), got {value}"
        )


BTREE_MAX_ITEM_BYTES: Final[int] = 2704
"""Postgres btree v4 maximum index-entry size, in bytes.

``nbtree``'s ``BTMaxItemSize`` — roughly a third of an 8 KiB page. An INSERT
whose index entry exceeds it fails with ``index row size N exceeds btree
version 4 maximum 2704 for index ...``. This is the only real bound on
``idempotency_key``/``idempotency_scope``: the column is plain ``text`` with
no length CHECK, but the pair is covered by the composite unique index
``jobs_idempotency_scope_key_uniq (idempotency_scope, idempotency_key)``.
"""

MAX_IDEMPOTENCY_KEY_BYTES: Final[int] = 1024
"""Default cap on the UTF-8 byte length of ``idempotency_key``/``idempotency_scope``.

Bytes, not characters: :data:`BTREE_MAX_ITEM_BYTES` counts encoded bytes, so
a character cap either under-protects (a 4-bytes-per-character key) or
needlessly punishes the ASCII keys callers actually derive from URLs,
composite business keys and vendor continuation cursors.

Arithmetic, measured against a real Postgres (see
``tests/test_idempotency_key_bounds.py``): the index tuple carries 16 bytes
of its own — 1352 + 1352 incompressible bytes report ``index row size
2720`` — so the largest safe symmetric pair is (2704 - 16) / 2 = 1344
bytes. The 1024 default leaves scope + key at 2064 bytes, well clear.

The check is on the *uncompressed* length deliberately: index tuples are
PGLZ-compressed, so a repetitive 8 KiB key does insert, but nothing an
application derives keys from is reliably compressible.

Operator-controlled via ``TaskQSettings.idempotency_key_max_bytes``
(``TASKQ_IDEMPOTENCY_KEY_MAX_BYTES``), whose ceiling is
:data:`IDEMPOTENCY_KEY_BYTES_CEILING`. The prior 256-*character* literal
(duplicated across ``client/_args.py`` and ``client/_jobs.py``) mapped to
nothing and pushed consumers into hashing their keys to fit.
"""

IDEMPOTENCY_KEY_BYTES_CEILING: Final[int] = 1300
"""Largest value ``idempotency_key_max_bytes`` accepts.

2 x 1300 = 2600 bytes of data plus the measured 16-byte index-tuple
overhead is 2616, under :data:`BTREE_MAX_ITEM_BYTES` with 88 bytes to spare
(the measured breaking point is 1344 per value). So no setting of this knob
can turn a valid enqueue into a raw ``index row size ... exceeds btree
version 4 maximum`` error from Postgres.
"""


def schema_lock_name(purpose: str, schema: str) -> str:
    """Schema-qualified advisory-lock name: ``taskq:{purpose}:{schema}``.

    Advisory locks live in a per-database namespace, so a bare
    ``taskq:{purpose}`` is shared by every schema in the database — two
    schemas in one database then serialize, or worse: the loser of a
    leader election never runs its sweeps while dispatch (not leader-gated)
    keeps flowing, so the fleet reports healthy while scheduled work stops
    moving. Qualifying with the schema gives each schema its own lock. This
    is the lock-side twin of the per-schema NOTIFY channel naming and
    follows the same purpose-then-schema ordering as the unique-for lock
    keys built in the enqueue path.

    Upgrade discipline: the qualified names replace the unqualified ones
    outright — a mixed old/new fleet holds different names and can both act
    as leader of the same schema (sweeps stay row-safe under
    ``FOR UPDATE SKIP LOCKED``; cron gains a double-fire window because its
    lock is what serialises ticks). Adopt by restarting the fleet onto the
    new release rather than rolling it; the window is the deploy, not the
    steady state.
    """
    return f"taskq:{purpose}:{schema}"


PG_MAX_IDENTIFIER_BYTES: Final[int] = 63
"""NAMEDATALEN - 1: the longest identifier Postgres keeps intact.

``LISTEN`` takes its channel as an identifier and silently truncates a
longer one (a NOTICE, not an error), while ``pg_notify`` takes text and
raises ``22023 channel name too long`` — so an over-long channel name
splits the two halves of one conversation: the listener subscribes to a
truncated name and the notifier either errors or addresses the full one.
"""

SCHEMA_CHANNEL_TAG_HEX_LEN: Final[int] = 10
"""Hex digits of ``sha224(schema)`` that identify the schema inside every
NOTIFY channel name (:func:`schema_channel_tag`). Ten digits (40 bits) keep
the widest channel — the per-worker one, which also carries a 36-char uuid
— under :data:`PG_MAX_IDENTIFIER_BYTES` with room to spare, while a chance
collision between two schemas of one database needs on the order of a
million schemas. The SQL twin in the wake trigger (migration
``01.00.14_01``) takes the same prefix of the same digest, and the two are
pinned equal end to end by ``tests/test_notify_channel_length.py``."""

WAKE_CHANNEL_FMT: Final[str] = "taskq_wake_{schema_tag}"
"""Format template for the wake-channel name.

``{schema_tag}`` is :func:`schema_channel_tag`, never the schema name
itself: a channel that interpolated the schema overflowed the identifier
limit for long schemas (see :data:`PG_MAX_IDENTIFIER_BYTES`), and the
per-worker channel did so from a 14-character schema on.
"""

EVENTS_CHANNEL_FMT: Final[str] = "taskq_events_{schema_tag}"
"""Format template for the fleet-wide worker-events channel.

All workers in a schema subscribe to this channel.  Each NOTIFY payload
is a JSON object with a ``"type"`` discriminator field so receivers can
route to the appropriate handler without dedicated per-event channels.
"""

WORKER_CHANNEL_FMT: Final[str] = "taskq_worker_{schema_tag}_{worker_id}"
"""Format template for the per-worker events channel.

Only the target worker subscribes, so no payload filtering is needed.
Uses the same JSON payload format as EVENTS_CHANNEL_FMT.
"""

PROGRESS_CHANNEL_FMT: Final[str] = "taskq:{schema_tag}:progress:{job_id}"
"""Format template for the per-job progress channel."""

PROGRESS_GLOBAL_CHANNEL_FMT: Final[str] = "taskq:{schema_tag}:progress"
"""Format template for the schema-wide progress fanout channel."""

CRON_COMMIT_GATE_CHANNEL_FMT: Final[str] = "taskq_cron_commit_{schema_tag}"
"""Format template for the cron tick's self-addressed commit-gate channel
(see ``taskq.worker.cron_loop``). Schema-tagged like every other channel:
channels share one database-wide namespace, so two schemas' cron sessions
in one database would otherwise hear each other's commit signals.
"""

_IDENT_RE = re.compile(r"\A[A-Za-z_][A-Za-z0-9_]*\Z")
r"""SQL identifier validator (schema names, table/column names).

``\A``/``\Z``, not ``^``/``$``: Python's ``$`` also matches immediately BEFORE
a trailing newline, so ``"taskq\n"`` satisfied ``^...$`` and passed validation.
Only a single trailing newline slipped through -- ``"taskq\nDROP TABLE x"`` was
always rejected, because the rest of the string still has to match the charset,
which admits no whitespace.

Not an injection path, and it was not one before: every interpolation site
double-quotes the validated value, and a newline is legal inside a quoted
identifier, so ``"taskq\n"`` would address a schema literally named
``taskq<newline>`` rather than escaping the quotes. The value of the fix is that
this is the canonical identifier validator reused across a dozen modules (and
re-implemented by at least one downstream consumer), so it should mean exactly
what it appears to mean.
"""

_KEYED_KEY_RE = re.compile(r"\A[A-Za-z0-9_\-:.]+\Z")
"""Character set for keyed-ref name components (``base_name`` and ``key``).

Both segments of the concrete name ``f"{base_name}:{key}"`` flow into Redis
key names and PG text columns — this regex prevents control characters,
spaces, slashes, and other metacharacters from reaching storage.  Mirrors
the ``_IDENT_RE`` approach for schema identifiers, including the
``\\A``/``\\Z`` anchoring: ``$`` also matches immediately before a trailing
newline, so ``"key\\n"`` satisfied ``^...$``.
"""

_MAX_KEYED_KEY_LEN = 255
"""Maximum length (chars) for keyed-ref name components (``base_name`` and ``key``).

Bounds storage growth from attacker-controlled keys and base names — the
same rationale as the character regex above.
"""

QUEUE_CONCURRENCY_PREFIX: Final[str] = "taskq:global:queue:"
"""Reserved namespace for fleet-wide per-queue concurrency cap reservations.

Primitives whose name starts with this prefix are internal to TaskQ's
queue-cap bootstrap path and must be registered via
``RateLimitRegistry.register_queue_cap_reservation``, not via the public
``RateLimitRegistry.register`` (which rejects prefixed names to prevent
accidental or malicious shadowing of an internal queue cap). Keyed refs
must never derive concrete names into this namespace either — see the
``base_name`` validators in ``taskq.ratelimit.refs``.

Lives here (rather than in ``ratelimit.registry``) because
``ratelimit.refs`` must reference it in its validators, and ``refs`` is
imported BY ``registry`` — defining it in ``registry`` would be circular.
"""


def base_name_collides_with_reserved_prefix(base_name: str) -> bool:
    """True if a keyed ref's ``f"{base_name}:{key}"`` names ALWAYS land in
    the reserved queue-cap namespace.

    The concrete name is ``f"{base_name}:{key}"``; every such name starts
    with ``QUEUE_CONCURRENCY_PREFIX`` exactly when ``base_name`` itself
    starts with the prefix (e.g. ``"taskq:global:queue:x"``) or equals it
    minus the trailing colon (``"taskq:global:queue"`` — the ``":"``
    separator then completes the prefix for ANY key). Both are rejected at
    ref-construction time so the collision surfaces at startup instead of
    as a per-job ``ValueError`` from the ``register()`` prefix guard.

    A shallower ``base_name`` sharing segments with the prefix (e.g.
    ``"taskq:global"``) only collides when the *key* completes the
    remaining segments (key ``"queue:x"`` → concrete
    ``"taskq:global:queue:x"``); keys are per-job dynamic values, so they
    cannot be rejected at construction — that case remains covered, loudly,
    by the ``register()`` prefix guard at materialization time.
    """
    return base_name.startswith(QUEUE_CONCURRENCY_PREFIX) or (
        f"{base_name}:" == QUEUE_CONCURRENCY_PREFIX
    )


def quote_ident(identifier: str) -> str:
    """Return *identifier* safely wrapped in double-quotes for SQL interpolation.

    Validates against canonical identifier regex before quoting.
    """
    if not _IDENT_RE.match(identifier):
        raise ValueError(f"invalid SQL identifier: {identifier!r}")
    return f'"{identifier}"'


def schema_channel_tag(schema: str) -> str:
    """The fixed-width token that stands for *schema* in every NOTIFY channel.

    The first :data:`SCHEMA_CHANNEL_TAG_HEX_LEN` hex digits of
    ``sha224(schema)``. A hash rather than the name because channels are
    identifiers bounded by :data:`PG_MAX_IDENTIFIER_BYTES` while the
    schema name alone may be that long; a fixed-width tag makes every
    channel's length independent of the schema, so there is no schema
    length at which one channel silently stops matching its listener.
    The tag is computed over the exact text of the name — quoted schema
    identifiers are case-sensitive, so ``Taskq`` and ``taskq`` are distinct
    schemas with distinct tags. The wake trigger derives the same tag in
    SQL from ``TG_TABLE_SCHEMA`` (``left(encode(sha224(...), 'hex'), 10)``).

    Validates *schema* against the same identifier regex used by the
    migration runner. Raises :class:`ValueError` on invalid input.
    """
    if not _IDENT_RE.match(schema):
        raise ValueError(f"invalid schema identifier: {schema!r}")
    # _IDENT_RE admits ASCII only, so the encoding cannot change the digest.
    digest = hashlib.sha224(schema.encode("ascii")).hexdigest()
    return digest[:SCHEMA_CHANNEL_TAG_HEX_LEN]


def _bounded_channel(name: str, *, schema: str) -> str:
    """*name*, or a :class:`ValueError` when it would not survive ``LISTEN``.

    Every derived channel passes through here, so a template edit or an
    over-long interpolated id fails where the name is built rather than
    as a listener that silently hears nothing.
    """
    if len(name.encode()) > PG_MAX_IDENTIFIER_BYTES:
        raise ValueError(
            f"NOTIFY channel {name!r} for schema {schema!r} is "
            f"{len(name.encode())} bytes; Postgres keeps at most "
            f"{PG_MAX_IDENTIFIER_BYTES} (LISTEN would truncate it and "
            "pg_notify would reject it)"
        )
    return name


def wake_channel(schema: str) -> str:
    """Return the wake-channel name for *schema*.

    Raises :class:`ValueError` on an invalid schema identifier.
    """
    return _bounded_channel(
        WAKE_CHANNEL_FMT.format(schema_tag=schema_channel_tag(schema)), schema=schema
    )


def events_channel(schema: str) -> str:
    """Return the fleet-wide events channel name for *schema*.

    All workers in the schema subscribe to this channel.  The NOTIFY
    payload is a JSON object with a ``"type"`` discriminator so workers
    can route without dedicated per-event channels.  Workers filter on
    the ``"worker_id"`` field where relevant.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    return _bounded_channel(
        EVENTS_CHANNEL_FMT.format(schema_tag=schema_channel_tag(schema)), schema=schema
    )


def worker_channel(schema: str, worker_id: str) -> str:
    """Return the per-worker events channel name for *schema* and *worker_id*.

    Only the worker with *worker_id* subscribes to this channel, so no
    payload filtering is needed.  Uses the same JSON payload format as the
    fleet-wide events channel.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    return _bounded_channel(
        WORKER_CHANNEL_FMT.format(schema_tag=schema_channel_tag(schema), worker_id=worker_id),
        schema=schema,
    )


def progress_channel(schema: str, job_id: UUID | str) -> str:
    """Return the per-job progress channel name for *schema* and *job_id*.

    Workers publish :class:`~taskq.progress.ProgressEvent` JSON to this
    channel; SSE consumers subscribe per job_id.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    return _bounded_channel(
        PROGRESS_CHANNEL_FMT.format(schema_tag=schema_channel_tag(schema), job_id=job_id),
        schema=schema,
    )


def progress_global_channel(schema: str) -> str:
    """Return the schema-wide progress fanout channel name for *schema*.

    All progress events for the schema are also published here so that a
    single subscriber can receive updates for every job.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    return _bounded_channel(
        PROGRESS_GLOBAL_CHANNEL_FMT.format(schema_tag=schema_channel_tag(schema)), schema=schema
    )


def cron_commit_gate_channel(schema: str) -> str:
    """Return the cron tick's self-addressed commit-gate channel for *schema*.

    Raises :class:`ValueError` on invalid schema identifier.
    """
    return _bounded_channel(
        CRON_COMMIT_GATE_CHANNEL_FMT.format(schema_tag=schema_channel_tag(schema)), schema=schema
    )


#: The widest ``str(uuid.UUID)`` (36 chars) — the probe id
#: :func:`check_channels_fit` interpolates where a channel carries one.
_WIDEST_UUID_TEXT: Final[str] = "ffffffff-ffff-ffff-ffff-ffffffffffff"


def check_channels_fit(schema: str) -> None:
    """Derive every channel for *schema* so an over-long one fails at load.

    Called by the settings validator: the channels are fixed-width by
    construction, so this is the guard that a template edit cannot quietly
    reintroduce a schema length at which listeners and notifiers disagree.
    Raises :class:`ValueError` naming the offending channel.
    """
    wake_channel(schema)
    events_channel(schema)
    worker_channel(schema, _WIDEST_UUID_TEXT)
    progress_channel(schema, _WIDEST_UUID_TEXT)
    progress_global_channel(schema)
    cron_commit_gate_channel(schema)
