-- Row-borne lease for maintenance leadership. Leadership is held by the pod
-- whose worker_id is on this row while expires_at is in the future, renewed
-- by that pod on its heartbeat cadence and taken over by any pod once it has
-- lapsed -- on a horizon the leader itself wrote, never on the server's
-- connection-reaping schedule, and needing no privilege beyond UPDATE on this
-- row. Forward-only; there is no down migration. The literal "{schema}" token
-- is substituted at apply time by the migration runner.
--
-- The expiry is stored rather than derived from last_seen_at because each
-- follower would otherwise compute the horizon from its OWN heartbeat
-- interval: a fleet whose new pods run a longer interval than the leader's
-- renewal cadence would take over from a live leader. Storing the instant the
-- holder chose leaves followers only a clock comparison.
--
-- Additive and rolling-safe: pre-lease code never reads or writes this column
-- (its upsert names singleton, worker_id, elected_at and last_seen_at, and its
-- heartbeat ping touches last_seen_at only), so it can be applied while such
-- pods are running. That also means a pre-lease pod taking the row leaves
-- whatever expiry it found in place rather than clearing it, so the column
-- cannot by itself say who wrote the row: NULL only until the first leasing
-- pod elects, and stale-but-non-NULL afterwards. The election predicate
-- therefore never judges a holder on this column alone -- it requires the
-- last_seen_at ping to have stopped as well, which is the one signal every
-- release writes.
ALTER TABLE "{schema}".maintenance_leader
    ADD COLUMN IF NOT EXISTS expires_at timestamptz;

COMMENT ON COLUMN "{schema}".maintenance_leader.expires_at IS
    'Server-clock instant after which any pod may take leadership. Written '
    'and renewed by the holder from its own leader_lease setting.';
