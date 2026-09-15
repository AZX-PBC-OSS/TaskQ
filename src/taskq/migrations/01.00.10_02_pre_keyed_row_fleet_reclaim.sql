-- Fleet-reclaim marking for keyed rate-limit / reservation rows (the
-- #139 residual). Forward-only; there is no down migration. To revert,
-- restore from backup. The literal "{schema}" token is substituted at
-- apply time by the migration runner.
--
-- THE RESIDUAL: keyed reservation_slots rows (materialised by
-- KeyedReservationRef via ensure_slots) and keyed rate_limit_buckets
-- rows (published / preseeded by KeyedRateLimitRef materialisation)
-- orphan when the worker that created them DIES — the in-process
-- reclamation machinery (registry idle eviction + the pending-reclaim
-- drain) lives entirely inside the dead process, so no survivor can
-- name the rows, and they leak unless a live worker happens to
-- re-resolve the same concrete key. The key space is caller-controlled,
-- so steady-state cardinality after enough worker deaths is one bucket
-- per key ever materialised by a process that later died — unbounded.
--
-- THE SHAPE: per-key rows carry their own staleness stamp, refreshed
-- on each acquire/release/upsert operation that already touches the row.
-- The maintenance leader then runs a bounded sweep to delete expired rows
-- — refresh-on-use plus a bounded expiry sweep, instead of registry
-- bookkeeping that dies with the process. This migration adds the two
-- row-borne halves of that shape:
--
--   keyed         marks the rows the fleet sweep may delete. True ONLY
--                 on rows created by a keyed materialisation: a
--                 statically declared bucket's rows are born false and
--                 must NEVER be flipped true (a static reservation has
--                 no acquire-path heal, so deleted rows would deny
--                 forever). For rate_limit_buckets the mark is
--                 additionally restricted to PG-state-backed buckets
--                 (backend="postgres"): a redis-backend keyed bucket's
--                 healthy acquire path never touches PG, so its PG
--                 row's staleness cannot speak for Redis-side use, and
--                 sweeping it would reset the outage-fallback state of
--                 a fixed-quota bucket mid-outage. Redis-backend keyed
--                 rows therefore stay false — never swept.
--
--   last_used_at  the staleness stamp, refreshed by the acquire /
--                 release / upsert statements that already touch the
--                 row (no dedicated stamping round trip exists or is
--                 needed): reservation acquire's UPDATE arm and
--                 release, ensure_slots' INSERT, the token bucket's
--                 preseed / upsert / refund UPDATE, and the keyed
--                 materialisation publish. The maintenance leader's
--                 sweep_idle_keyed_rows deletes keyed rows whose stamp
--                 is older than the operator horizon
--                 (WorkerSettings.keyed_row_reclaim_period, default 1
--                 hour — the same idle threshold the in-process
--                 registry eviction uses), one bounded, committed batch
--                 per tick per table.
--
-- OPS NOTE (locks): every ALTER here is ADD COLUMN with a non-volatile
-- default — metadata-only on PG >= 11 (no table rewrite; the default is
-- stored in pg_attribute, and pre-existing rows read it as a fixed
-- value stamped at ALTER time) — and plain CREATE INDEX (the deliberate
-- non-CONCURRENTLY choice of 01.00.06: the CONCURRENTLY form deadlocks
-- with the migration advisory lock; see that migration's header).
-- reservation_slots and rate_limit_buckets are bounded tables (static
-- declarations plus the per-worker keyed caps), so both lock windows
-- are short. Pre-existing rows are irrelevant to the sweep either way:
-- they read keyed=false (the constant default) and are therefore never
-- deleted, whatever their last_used_at shows.
--
-- ROLLING DEPLOY: pre-phase is safe for both code generations. The
-- previous release's statements reference only columns that still
-- exist; this release's acquire/release/upsert statements list the new
-- columns, which is why they ship in the pre phase, before the code
-- rollout. A pre-migration database makes the new leader-sweep block
-- raise UndefinedColumnError, which that block tolerates per tick (the
-- stale-batches block's pre-migration pattern) until this migration
-- lands.
ALTER TABLE "{schema}".reservation_slots
    ADD COLUMN IF NOT EXISTS keyed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_used_at timestamptz NOT NULL DEFAULT now();

ALTER TABLE "{schema}".rate_limit_buckets
    ADD COLUMN IF NOT EXISTS keyed boolean NOT NULL DEFAULT false,
    ADD COLUMN IF NOT EXISTS last_used_at timestamptz NOT NULL DEFAULT now();

-- The fleet sweep's window scans. Partial on keyed: the sweep's
-- candidate predicate is exactly (keyed AND last_used_at < horizon),
-- and the STABLE statement_timestamp() bound (the module docstring's
-- two-clock doctrine in taskq.backend._sweeps — write stamps are
-- VOLATILE clock_timestamp(), selection bounds are STABLE) lets the
-- planner serve the bound as an Index Cond instead of a post-scan
-- filter over every keyed row.
CREATE INDEX IF NOT EXISTS reservation_slots_keyed_last_used_idx
    ON "{schema}".reservation_slots (last_used_at)
    WHERE keyed;
CREATE INDEX IF NOT EXISTS rate_limit_buckets_keyed_last_used_idx
    ON "{schema}".rate_limit_buckets (last_used_at)
    WHERE keyed;

COMMENT ON COLUMN "{schema}".reservation_slots.keyed IS
    'Fleet-reclaimable mark: the row was created by a keyed materialisation '
    '(KeyedReservationRef) whose owning registry entry may not outlive the '
    'process. The maintenance leader''s sweep_idle_keyed_rows may delete '
    'keyed rows unused past keyed_row_reclaim_period; statically declared '
    'buckets are born false and are never deleted by it.';
COMMENT ON COLUMN "{schema}".reservation_slots.last_used_at IS
    'Staleness stamp for the fleet reclaim sweep, refreshed by the acquire / '
    'release / ensure_slots statements that already touch the row.';
COMMENT ON COLUMN "{schema}".rate_limit_buckets.keyed IS
    'Fleet-reclaimable mark: keyed-materialised AND PG-state-backed '
    '(backend="postgres" — only then does the acquire path touch this row, '
    'keeping last_used_at truthful). Static buckets and redis-backend keyed '
    'buckets are born false and are never deleted by the fleet sweep.';
COMMENT ON COLUMN "{schema}".rate_limit_buckets.last_used_at IS
    'Staleness stamp for the fleet reclaim sweep, refreshed by the preseed / '
    'upsert / refund statements that already touch the row.';
