-- Row-borne lease for maintenance leadership. Forward-only; there is no
-- down migration. To revert, restore from backup. The literal "{schema}"
-- token is substituted at apply time by the migration runner.
--
-- Why the row and not the session: the maintenance role was held by a
-- session-scoped advisory lock, which the server releases only when it
-- believes the session has ended. A holder that stops working while its
-- socket stays open -- a frozen host, a stopped process, a partition that
-- black-holes traffic without resetting it -- keeps the lock until the
-- server's own keepalive reaping closes the session, a horizon measured in
-- hours at stock settings and one no deployment setting bounds. For that
-- whole window nothing sweeps, nothing promotes scheduled jobs, no cron
-- fires and nothing prunes, while dispatch keeps flowing and every pod
-- reports itself healthy. The only recovery was terminating the holder's
-- backend, a privilege managed Postgres commonly reserves -- so where it
-- was unavailable there was no recovery at all.
--
-- A horizon the holder itself writes fixes both halves: a surviving pod
-- takes the role once this instant has passed, needing no privilege beyond
-- the UPDATE on this row it already has, and the wait is bounded by a
-- setting the deployment chooses rather than by the server's connection
-- bookkeeping.
--
-- Why stored rather than derived from last_seen_at: a follower deriving the
-- horizon from its OWN heartbeat_interval would compute a different one
-- than the holder renews on, so a fleet whose new pods run a longer
-- interval than the holder's cadence would take the role from a live
-- leader. Storing the instant the holder chose leaves followers comparing
-- against the clock alone.
--
-- OPS NOTE (locks): ALTER TABLE ... ADD COLUMN with no default is
-- metadata-only, so this does not rewrite the table.
--
-- ROLLING DEPLOY: pre-phase, safe for both code generations. The previous
-- release never reads or writes this column (its upsert names singleton,
-- worker_id, elected_at and last_seen_at, and its ping touches last_seen_at
-- only), so the file applies while such pods are running. NULL therefore
-- means "written by a pod that does not lease", and the election predicate
-- treats such a row as live while its last_seen_at ping is fresh. Rolling
-- back leaves an unused column.
ALTER TABLE "{schema}".maintenance_leader
    ADD COLUMN IF NOT EXISTS expires_at timestamptz;

COMMENT ON COLUMN "{schema}".maintenance_leader.expires_at IS
    'Server-clock instant after which any pod may take maintenance leadership. '
    'Written and renewed by the holder from its own leader_lease setting. NULL '
    'on a row last written by a release that predates the lease, whose liveness '
    'is read from last_seen_at instead.';
