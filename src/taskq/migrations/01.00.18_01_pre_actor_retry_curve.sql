-- The actor's declared retry curve reaches the server-side enqueue paths.
-- Forward-only; there is no down migration. To revert, DROP the four
-- columns (readers treat NULL as the enqueue default).
-- The literal "{schema}" token is substituted at apply time by the
-- migration runner.
--
-- ── Why these columns exist ─────────────────────────────────────────
-- cron fires and the admin run-now build their EnqueueArgs from the
-- stored actor_config row. The row carried max_attempts and retry_kind
-- but NOT the curve scalars (retry_base, retry_cap, retry_backoff,
-- retry_jitter), so those took the EnqueueArgs dataclass defaults on
-- every server-side fire: an actor declaring base=600s/fixed/backoff
-- and jitter=0.0 was fired by cron with base=5s/exponential/jitter=0.2,
-- and a crash-reclaim sweep re-pended the two arms on different curves.
-- Measured: cron re-pend delay 5.2s against the declared 600.0s.
--
-- NULL means "the actor's declaration is unknown here": the enqueue
-- default stands, exactly as before this migration, so existing rows
-- (and rows for actors whose registry is absent at sync time) degrade
-- to the old behavior, not to an error.
--
-- The sync seeds these columns on FIRST CREATE only (the same seeding
-- semantics as max_attempts and retry_kind: the ON CONFLICT arm does
-- not touch them), so an operator correcting a stored curve is never
-- silently overwritten by a boot; a changed @actor literal surfaces
-- through the existing drift machinery.

ALTER TABLE "{schema}".actor_config
    ADD COLUMN retry_base    interval,
    ADD COLUMN retry_cap     interval,
    ADD COLUMN retry_backoff text,
    ADD COLUMN retry_jitter  float8;
