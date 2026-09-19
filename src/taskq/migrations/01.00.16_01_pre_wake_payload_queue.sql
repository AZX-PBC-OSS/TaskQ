-- The insert-wake trigger carries the row's queue as its NOTIFY payload so
-- workers stop waking on inserts to queues they would never claim.
-- Forward-only; there is no down migration. To revert, re-apply the
-- previous function body (empty payload) from 01.00.14_01.
-- The literal "{schema}" token is substituted at apply time by the
-- migration runner.
--
-- ── Why this exists ─────────────────────────────────────────────────
-- The wake channel is schema-wide (one channel per schema, by design:
-- the worker subscribes once and claims the queues it serves). Every
-- insert of a pending row therefore woke EVERY worker in the fleet, and
-- each wake costs a full multi-CTE claim round against the dispatcher
-- pool. At fifty workers the fleet answered each enqueue with up to
-- fifty claim statements, nearly all returning zero rows for workers
-- whose queues the insert never touched. The claim cooldown bounds the
-- waste per worker (about twenty rounds per second) but the fleet-wide
-- product still grows linearly with fleet size.
--
-- The fix keeps the single-channel design and moves the filtering into
-- the notification itself: the payload names the inserted row's queue,
-- and each worker's listener sets its wake event only when the payload
-- is a queue it serves (or the payload is empty, see the compatibility
-- contract below). A worker serving the queue claims ALL its queues in
-- the round, exactly as before; a worker serving nothing in the payload
-- skips the round and its fallback poll stays authoritative.
--
-- ── Compatibility contract (rolling deploys) ────────────────────────
-- The wake payload is a queue name, and the listener side treats an
-- EMPTY payload as "wake every subscriber": that keeps three shapes
-- working. Old listener + new trigger: the old listener ignores the
-- payload entirely and wakes as before. New listener + this trigger:
-- filtered. New listener + empty payload (the COPY fixup's bulk wake in
-- backend/_sql_templates.py, which cannot know one queue for a batch of
-- rows spanning queues, and any older trigger still live): everything
-- wakes, the historical contract. Workers refuse to start on pending
-- pre-phase migrations, so a new listener never runs against the old
-- trigger in the same schema.
--
-- Postgres coalesces identical (channel, payload) notifications within
-- one transaction, so a batch of inserts to ONE queue still costs one
-- delivery; a batch spanning queues now costs one delivery per distinct
-- queue (the pre-change cost was one delivery total, but every delivery
-- woke every worker; the filtered trade is strictly fewer spurious
-- claim rounds).

CREATE OR REPLACE FUNCTION "{schema}".notify_job_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify(
            'taskq_wake_' || left(encode(sha224(convert_to(TG_TABLE_SCHEMA::text, 'UTF8')), 'hex'), 10),
            NEW.queue
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
