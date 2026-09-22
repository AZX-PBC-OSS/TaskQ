-- Shared store for the SAML SSO replay and answered-AuthnRequest gates.
--
-- The SAML admin auth (taskq/web/admin/auth/saml.py) previously kept its
-- consumed-assertion records and answered-AuthnRequest records in
-- process-local dicts. In any deployment with more than one process (several
-- admin replicas behind a load balancer, or ``uvicorn --workers N``) a
-- captured, correctly-signed SAML response could be re-POSTed to a SIBLING
-- process and mint a second session: the replaying process saw none of the
-- first process's records, so both the assertion-replay gate and the
-- answered-request gate failed open across replicas. This table is the store
-- every replica shares; the consume is an atomic first-wins INSERT with
-- ``ON CONFLICT DO NOTHING`` semantics (see the upsert in saml.py), so
-- exactly one presentation of an assertion ID, and one answer per
-- AuthnRequest ID, wins fleet-wide.
--
-- Two kinds of record live here, keyed by ``kind``:
--
-- * ``assertion_replay``: a consumed assertion ID; expires at the
--   assertion's NotOnOrAfter (with a fixed fallback when the assertion
--   carries none). A row past its expiry neither blocks a later claim nor
--   matters: the assertion's own timestamp validation already refuses it.
-- * ``answered_request``: an AuthnRequest ID an accepted assertion has
--   already answered; expires with the correlation cookie's 300 s window,
--   past which the cookie can no longer authenticate a presentation.
--
-- TTL ENFORCEMENT: every read and every claim carries an
-- ``expires_at > <now>`` predicate, and each claim additionally sweeps
-- expired rows (``DELETE ... WHERE expires_at <= <now>``). ``now`` is the
-- APPLICATION clock passed as a parameter, not the database clock: the
-- expiry instants are application-clock values (NotOnOrAfter read from the
-- assertion, request TTLs counted from ``time.time()``), so comparing them
-- against a second clock would turn an application/database skew into
-- longer-or-shorter-than-requested TTLs.
--
-- GROWTH: only an ACCEPTED login writes here (every write is preceded by
-- full signature and timestamp validation), rows live at most one
-- NotOnOrAfter window (minutes; the answered kind exactly 300 s), and each
-- claim sweeps expired rows. That bounds the table to roughly the accepted
-- logins of one expiry window, so no index on ``expires_at`` is built: the
-- periodic sweep's sequential scan of a table that small is cheaper than
-- maintaining a second index on a hot insert path.
--
-- ROLLING DEPLOY: additive and inert to old code. Pre-fix processes never
-- read or write this table (their records stay process-local), so it can be
-- applied while they run; a mixed fleet's cross-replica replay protection is
-- as strong as its newest replica, which is strictly better than the
-- all-process-local behavior this migration ships the fix for. Forward-only;
-- there is no down migration.
CREATE TABLE IF NOT EXISTS "{schema}".saml_replay_store (
    kind text NOT NULL,
    id text NOT NULL,
    expires_at timestamptz NOT NULL,
    PRIMARY KEY (kind, id)
);

COMMENT ON TABLE "{schema}".saml_replay_store IS
    'Shared cross-replica store for the SAML admin auth ID gates. Rows are '
    '(kind, id) records with an expiry: kind ''assertion_replay'' consumes a '
    'SAML assertion ID at its NotOnOrAfter (single-use assertions), kind '
    '''answered_request'' records an AuthnRequest ID an accepted assertion '
    'has already answered (single-use AuthnRequest IDs on the cookie path). '
    'Claims are atomic first-wins inserts; every read and claim predicates '
    'on expires_at against the application clock, and each claim sweeps '
    'expired rows. Only accepted logins write here.';
