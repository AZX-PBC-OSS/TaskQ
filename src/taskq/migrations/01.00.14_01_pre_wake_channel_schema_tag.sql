-- The wake trigger's channel is derived from a schema TAG, not the schema
-- name. Forward-only; there is no down migration. To revert, restore from
-- backup. The literal "{schema}" token is substituted at apply time by the
-- migration runner.
--
-- ── Why the channel is hashed ──────────────────────────────────────────
-- NOTIFY channels are identifiers bounded by NAMEDATALEN-1 (63 bytes):
-- LISTEN silently truncates a longer one, pg_notify() rejects it. A
-- channel that interpolated the schema name ('taskq_wake_' || schema,
-- and the app's 'taskq_worker_' || schema || '_' || uuid) therefore had
-- a schema length past which the listener and the notifier no longer
-- named the same channel — from 14 characters for the per-worker cancel
-- channel, 53 for the wake channel — while the settings admit 63.
-- Every channel now embeds left(encode(sha224(schema), 'hex'), 10)
-- instead: fixed width, so no schema length breaks it. The Python twin
-- is taskq.constants.schema_channel_tag; the two are pinned equal end to
-- end by tests/test_notify_channel_length.py, and the prefix length is
-- SCHEMA_CHANNEL_TAG_HEX_LEN there. TG_TABLE_SCHEMA is the schema's
-- exact (case-preserved) name, the same text the application hashes.
--
-- ── The trigger is the sole wake source for inserts ────────────────────
-- Every enqueue path (single INSERT, batch INSERT, COPY) relies on this
-- trigger to wake dispatchers; the application issues no pg_notify of its
-- own after an INSERT. The WHEN clause is what keeps a future-dated row
-- from waking the fleet: every insert path decides status server-side
-- (scheduled_at in the future => 'scheduled'), so 'pending' means
-- dispatchable now. Postgres coalesces identical (channel, payload)
-- notifications within one transaction, so a batch costs one delivery.
--
-- ROLLING DEPLOY: the previous release's workers LISTEN on the old
-- 'taskq_wake_' || schema name and stop receiving wakes once this applies;
-- they keep claiming on their poll interval until restarted. Adopt by
-- restarting the fleet onto the new release (docs/guides/upgrading.md).

CREATE OR REPLACE FUNCTION "{schema}".notify_job_insert()
RETURNS trigger AS $$
BEGIN
    IF NEW.status = 'pending' THEN
        PERFORM pg_notify(
            'taskq_wake_' || left(encode(sha224(convert_to(TG_TABLE_SCHEMA::text, 'UTF8')), 'hex'), 10),
            ''
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
