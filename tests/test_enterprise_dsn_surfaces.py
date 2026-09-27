"""The enterprise DSN/connection surfaces, pinned against real containers.

The audit this file pins (see docs/guides/deployment.md, "Enterprise DSN
surfaces") was measured against postgres:18.6 containers in three shapes:
one plain TCP (the shared ``pg_dsn`` fixture), one with ``ssl=on`` serving
the image's snakeoil pair, and one serving a Unix socket on a bind-mounted
volume. Everything here pins BEHAVIOUR on the current code, including the
honest failures — a DSN form TaskQ mangles or a limitation that is
documented rather than fixed is pinned AS the limitation, so a future
behavior change shows up as a red test that names the surface.

Surfaces, in the order the deployment guide walks them:

1. **Multi-host DSN** (``postgresql://u:p@host1:5432,host2:5432/taskq``,
   libpq multi-host with ``target_session_attrs=read-write``): asyncpg
   parses these natively; the pins cover the whole TaskQ pipeline —
   pydantic's ``PostgresDsn`` roundtrip through ``WorkerSettings.load``,
   the ``post_load`` direct/pooled fallback, and the deps-shaped
   ``create_pool`` call — against one real container named twice (the
   PARSING is under test, not failover).
2. **SSL/TLS**: ``sslmode=require`` through the DSN reaches asyncpg's own
   SSL negotiation. Against a plain server the upgrade refusal is the
   honest failure (pinned, message included); against an ssl=on server
   the pooled connection's ``pg_stat_ssl.ssl`` is ``on``. The
   ``verify-full`` path is pinned both ways: with a CA bundle it
   connects, without one it fails at parameter PARSE time
   (``ClientConfigurationError``) before any socket is opened — the
   documented contract of ``taskq.auth.ensure_sslmode_require``. Also
   pinned: the plain DSN path injects NO sslmode (asyncpg's own
   ``prefer`` default governs), which is why the credential-injecting
   auth factories add it for you.
3. **IPv6 and Unix sockets**: the same roundtrip + rewriter parity the
   plain host form gets, and a real socket connection through a pool.
   The rewriters percent-encode the ``host=`` query value (``%2Ftmp%2F…``);
   asyncpg DECODES query parameters, so the rewritten DSN still connects —
   pinned against a live socket server, not assumed.
4. **The pooled boundary's per-statement budget discipline**, DSN side:
   the pooled DSN string passes through settings byte-identical, the
   worker pool deliberately carries NO client-side ``command_timeout``
   (server-side budgets rule there), and a DSN-carried
   ``statement_timeout`` server setting lands on the pooled connection —
   the knob a PgBouncer deployment uses when the pooler drops startup
   parameters (the pooler interplay itself is pinned by its own suite).
5. **Connection-ceiling honesty**: ``compute_connection_budget``'s
   outputs for the three worked profiles the docs publish
   (34/112/342) — the numbers operators size ``max_connections``
   against are pinned arithmetic, not prose.

Plus one surface that WAS a documented limitation: ``taskq.aws.RdsIamProvider``
(the AWS IAM credential source) could not parse a multi-host DSN —
``urlparse``'s ``port`` cannot cast ``5432,host2:5432`` and construction
raised a raw ``ValueError``. Fixed to the same last-``@`` partition
discipline the DSN rewriters use; the pins cover the construction, the
token signing (first hostspec — the one libpq tries first), the token
landing in every hostspec's password slot through ``enrich_pg_dsn``, the
unchanged single-host parse, and a live connect through the rewritten
DSN failing with the SERVER's auth error (the DSN reaching Postgres
well-formed is what that proves).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

import asyncpg
import pytest

from taskq._dsn import dsn_host
from taskq.auth import enrich_pg_dsn, ensure_sslmode_require
from taskq.connections import statement_cache_kwargs
from taskq.settings import WorkerSettings
from taskq.testing._shared_containers import skip_test_without_docker

# ── DSN builders ─────────────────────────────────────────────────────────


def _single_host_dsn(pg_dsn: str) -> str:
    """The shared fixture's DSN, normalised to a plain postgres:// form."""
    return pg_dsn.replace("postgresql+psycopg2://", "postgresql://")


# ── A1: multi-host DSN ───────────────────────────────────────────────────


def _multi_host_settings(dsn: str, **overrides: str) -> WorkerSettings:
    """WorkerSettings from a multi-host DSN, the way an operator sets one."""
    data: dict[str, str] = {"TASKQ_PG_DSN": dsn}
    data.update(overrides)
    return WorkerSettings.load_from_dict(data)


def test_multi_host_dsn_round_trips_through_worker_settings() -> None:
    """pydantic's PostgresDsn preserves the multi-host hostspec through
    load, post_load's direct/pooled fallback, and str()."""
    dsn = "postgresql://taskq:taskq@host1.invalid:5432,host2.invalid:5432/taskq"
    s = _multi_host_settings(dsn)
    assert str(s.pg_dsn_direct) == dsn
    assert str(s.pg_dsn_pooled) == dsn


def test_multi_host_dsn_target_session_attrs_survive_settings() -> None:
    """Query parameters ride along: target_session_attrs=read-write stays
    on both the direct and the pooled fallback DSN."""
    dsn = (
        "postgresql://taskq:taskq@host1.invalid:5432,host2.invalid:5432"
        "/taskq?target_session_attrs=read-write"
    )
    s = _multi_host_settings(dsn)
    assert str(s.pg_dsn_direct) == dsn
    assert str(s.pg_dsn_pooled) == dsn


def test_multi_host_dsn_split_dsns_are_independent() -> None:
    """Explicit split DSNs (direct = the primary, pooled = a pooler host)
    stay distinct — the multi-host form is per-DSN, not global."""
    direct = "postgresql://taskq:taskq@pg1.invalid:5432,pg2.invalid:5432/taskq"
    pooled = "postgresql://taskq:taskq@pgbouncer.invalid:6432/taskq"
    s = _multi_host_settings(direct, TASKQ_PG_DSN_POOLED=pooled)
    assert str(s.pg_dsn_direct) == direct
    assert str(s.pg_dsn_pooled) == pooled


@pytest.mark.integration
async def test_multi_host_dsn_pool_factory_connects_rw(pg_dsn: str) -> None:
    """The deps.py-shaped pool factory accepts a two-host DSN (both hosts
    the SAME container — the parsing is under test, not failover) and the
    session it lands on is read-write, what target_session_attrs demands.

    The create_pool kwargs mirror worker/deps.py's dispatcher factory
    (command_timeout, statement-cache pair, guarded connection class) so
    the pin covers the factory shape operators actually run.
    """
    from taskq._forkguard import guarded_connection_class

    base = _single_host_dsn(pg_dsn)
    scheme_creds = base.split("@", 1)[0]  # postgresql://user:pass
    db = base.rpartition("/")[2]
    host_only = base.split("@", 1)[1].split("/", 1)[0]  # host:port
    dsn = f"{scheme_creds}@{host_only},{host_only}/{db}?target_session_attrs=read-write"

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=2,
        command_timeout=5.0,
        statement_cache_size=512,
        max_cached_statement_lifetime=3600,
        connection_class=guarded_connection_class(),
    )
    try:
        async with pool.acquire() as conn:
            read_only = await conn.fetchval("show transaction_read_only")
            assert read_only == "off", "target_session_attrs=read-write matched a read-only server"
            assert await conn.fetchval("select 1") == 1
    finally:
        await pool.close()


def test_multi_host_dsn_rewriters_preserve_the_hostspec() -> None:
    """The credential-path DSN rewriters (enrich, sslmode) must not mangle
    the hostspec: both hosts survive, in order, with the query intact."""
    dsn = "postgresql://taskq:taskq@host1.invalid:5432,host2.invalid:5432/taskq"

    ssl_added = ensure_sslmode_require(dsn)
    assert "host1.invalid:5432,host2.invalid:5432" in ssl_added
    assert ssl_added.endswith("?sslmode=require")

    class _Cred:
        username = "rotated"
        password = "token"

    enriched = enrich_pg_dsn(dsn, _Cred())
    assert enriched.startswith("postgresql://rotated:token@")
    assert "host1.invalid:5432,host2.invalid:5432" in enriched
    assert "sslmode=require" in enriched


@pytest.mark.integration
async def test_multi_host_dsn_honest_ssl_failure_names_both_hosts(pg_dsn: str) -> None:
    """A rewritten multi-host DSN (sslmode added) against a plain server
    fails with the HONEST upgrade refusal — proof asyncpg parsed BOTH
    hosts and engaged the TLS client. A mangled hostspec would produce a
    DNS or parse error instead of the server's own refusal."""
    base = _single_host_dsn(pg_dsn)
    scheme_creds = base.split("@", 1)[0]
    db = base.rpartition("/")[2]
    host_only = base.split("@", 1)[1].split("/", 1)[0]
    dsn = ensure_sslmode_require(f"{scheme_creds}@{host_only},{host_only}/{db}")
    assert "sslmode=require" in dsn

    with pytest.raises(ConnectionError, match="rejected SSL upgrade"):
        await asyncpg.connect(dsn)


# ── A2: SSL/TLS ──────────────────────────────────────────────────────────


@pytest.mark.integration
async def test_sslmode_require_fails_honestly_on_a_plain_server(pg_dsn: str) -> None:
    """sslmode=require on a non-SSL server is a loud connection failure
    naming the refusal, never a silent plaintext fallback."""
    dsn = _single_host_dsn(pg_dsn) + "?sslmode=require"
    with pytest.raises(ConnectionError, match="rejected SSL upgrade"):
        await asyncpg.connect(dsn)


@pytest.mark.integration
async def test_sslmode_require_pool_connects_and_ssl_is_on(
    ssl_pg_dsn: str,
) -> None:
    """The deps-shaped pool over an sslmode=require DSN lands on TLS
    connections: pg_stat_ssl reports ssl=on for the session's backend."""
    dsn = _single_host_dsn(ssl_pg_dsn) + "?sslmode=require"
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            ssl_on = await conn.fetchval("select ssl from pg_stat_ssl where pid = pg_backend_pid()")
            assert ssl_on is True, "sslmode=require DSN produced a non-TLS session"
    finally:
        await pool.close()


@pytest.mark.integration
async def test_verify_full_with_ca_bundle_connects_tls(
    ssl_pg_dsn: str,
    ssl_ca_cert: Path,
) -> None:
    """verify-full + sslrootcert: the verifying mode's plumbing reaches
    asyncpg intact and the session is TLS."""
    dsn = _single_host_dsn(ssl_pg_dsn) + f"?sslmode=verify-full&sslrootcert={ssl_ca_cert}"
    conn = await asyncpg.connect(dsn)
    try:
        ssl_on = await conn.fetchval("select ssl from pg_stat_ssl where pid = pg_backend_pid()")
        assert ssl_on is True
    finally:
        await conn.close()


@pytest.mark.integration
async def test_verify_full_without_rootcert_fails_at_parse_time(
    ssl_pg_dsn: str,
) -> None:
    """A verifying sslmode without a CA bundle fails BEFORE any socket is
    opened — the ClientConfigurationError contract documented on
    taskq.auth.ensure_sslmode_require (asyncpg looks for
    ~/.postgresql/root.crt and refuses to guess a trust root)."""
    dsn = _single_host_dsn(ssl_pg_dsn) + "?sslmode=verify-full"
    with pytest.raises(asyncpg.ClientConfigurationError):
        await asyncpg.connect(dsn)


@pytest.mark.integration
async def test_plain_dsn_injects_no_sslmode(pg_dsn: str) -> None:
    """The plain DSN path adds NO sslmode — a DSN without one connects to
    a plain server successfully (asyncpg's own ``prefer`` governs). This
    is why the credential-injecting factories add sslmode=require for
    you; a DSN path that forced require would break every non-TLS dev
    container."""
    conn = await asyncpg.connect(_single_host_dsn(pg_dsn))
    try:
        ssl_on = await conn.fetchval("select ssl from pg_stat_ssl where pid = pg_backend_pid()")
        assert ssl_on is False, "unexpected TLS on a plain-server connection"
    finally:
        await conn.close()


# ── A3: IPv6 + Unix sockets ──────────────────────────────────────────────


def test_ipv6_dsn_round_trips_through_settings_and_rewriters() -> None:
    """The bracketed-IPv6 hostspec survives settings and both rewriters
    byte-identically."""
    dsn = "postgresql://taskq:taskq@[::1]:5432/taskq"
    s = _multi_host_settings(dsn)
    assert str(s.pg_dsn_direct) == dsn
    assert ensure_sslmode_require(dsn) == dsn + "?sslmode=require"

    class _Cred:
        username = "u2"
        password = "p2"

    enriched = enrich_pg_dsn(dsn, _Cred())
    assert enriched == "postgresql://u2:p2@[::1]:5432/taskq?sslmode=require"


@pytest.mark.integration
async def test_ipv6_dsn_pool_connects(pg_dsn: str) -> None:
    """The bracketed [::1] form connects through a pool (the same host the
    plain form resolves; only the spelling differs)."""
    base = _single_host_dsn(pg_dsn)
    port = base.split("@", 1)[1].split("/", 1)[0].split(":")[1]
    dsn = f"postgresql://taskq:taskq@[::1]:{port}/taskq"
    pool = await asyncpg.create_pool(dsn=dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            assert await conn.fetchval("select 1") == 1
    finally:
        await pool.close()


def test_unix_socket_dsn_round_trips_through_settings() -> None:
    """The libpq socket form (empty userinfo host, host= query parameter)
    round-trips through settings and the rewriters."""
    dsn = "postgresql://taskq:taskq@/taskq?host=/var/run/postgresql"
    s = _multi_host_settings(dsn)
    assert str(s.pg_dsn_direct) == dsn

    # The rewriters percent-encode the path; the VALUE must survive the
    # roundtrip semantically (asyncpg decodes query parameters).
    rewritten = ensure_sslmode_require(dsn)
    assert "host=%2Fvar%2Frun%2Fpostgresql" in rewritten
    assert "sslmode=require" in rewritten


@pytest.mark.integration
async def test_unix_socket_dsn_connects_through_a_pool_and_a_rewriter(
    socket_pg_dsn: str,
) -> None:
    """A live socket server: the plain socket DSN connects through a pool,
    AND the rewriter's percent-encoded form still connects — asyncpg
    decodes the query value, so the rewriting is not a mangling."""
    # The rewriter adds sslmode; the socket server is plain, so drop the
    # sslmode from the rewritten form before connecting (the encoding of
    # host= is what is under test here, not the TLS negotiation).
    rewritten = ensure_sslmode_require(socket_pg_dsn).replace("&sslmode=require", "")

    pool = await asyncpg.create_pool(dsn=rewritten, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            assert await conn.fetchval("select 1") == 1
    finally:
        await pool.close()


def test_dsn_host_logging_on_enterprise_forms() -> None:
    """The logging helper's behaviour on the enterprise forms: first host
    of a multi-host spec, the bare IPv6 literal, and 'unknown' for the
    query-carried socket form (log-only — no routing decisions ride it)."""
    assert dsn_host("postgresql://u:p@h1.invalid:5432,h2.invalid:5432/taskq") == "h1.invalid"
    assert dsn_host("postgresql://u:p@[::1]:5432/taskq") == "::1"
    assert dsn_host("postgresql://u:p@/taskq?host=/var/run/postgresql") == "unknown"


# ── A4: the pooled boundary's per-statement budget discipline ────────────


def test_pooled_dsn_passes_through_settings_byte_identical() -> None:
    """The split-DSN roundtrip: whatever the operator puts in
    TASKQ_PG_DSN_POOLED is exactly the string the pool factory receives."""
    direct = "postgresql://taskq:taskq@pg1.invalid:5432/taskq"
    pooled = "postgresql://taskq:taskq@pgbouncer.invalid:6432/taskq"
    s = _multi_host_settings(direct, TASKQ_PG_DSN_POOLED=pooled)
    assert str(s.pg_dsn_direct) == direct
    assert str(s.pg_dsn_pooled) == pooled


def test_pooled_declaration_disables_the_statement_cache() -> None:
    """pg_is_pooled=True forces the 0/0 statement-cache pair regardless of
    the operator's tuning — the pooler-remap hygiene that must hold at the
    pooled boundary no matter what a multi-host or TLS DSN carries."""
    direct = "postgresql://taskq:taskq@pg1.invalid:5432/taskq"
    pooled = "postgresql://taskq:taskq@pgbouncer.invalid:6432/taskq"
    tuned = _multi_host_settings(
        direct,
        TASKQ_PG_DSN_POOLED=pooled,
        TASKQ_PG_IS_POOLED="true",
        TASKQ_STATEMENT_CACHE_SIZE="512",
        TASKQ_MAX_CACHED_STATEMENT_LIFETIME="3600",
    )
    assert statement_cache_kwargs(tuned) == {
        "statement_cache_size": 0,
        "max_cached_statement_lifetime": 0,
    }
    untuned = _multi_host_settings(direct, TASKQ_PG_DSN_POOLED=pooled)
    assert statement_cache_kwargs(untuned)["statement_cache_size"] == 512


@pytest.mark.integration
async def test_dsn_carried_statement_timeout_lands_on_pooled_sessions(
    pg_dsn: str,
) -> None:
    """A statement_timeout carried IN the pooled DSN (asyncpg maps
    unknown query parameters to server_settings) reaches every session
    the pool hands out — the DSN-side per-statement budget a PgBouncer
    deployment uses when the pooler will not forward startup parameters.
    The pool is the deps.py worker-pool shape: deliberately NO
    client-side command_timeout, so the server-side budget is the ruler."""
    base = _single_host_dsn(pg_dsn)
    pooled_dsn = base + "?statement_timeout=2500ms"
    pool = await asyncpg.create_pool(dsn=pooled_dsn, min_size=1, max_size=2)
    try:
        async with pool.acquire() as conn:
            got = await conn.fetchval("show statement_timeout")
            assert got == "2500ms"
        async with pool.acquire() as conn:  # a second session, same budget
            got = await conn.fetchval("show statement_timeout")
            assert got == "2500ms"
    finally:
        await pool.close()


# ── A5: connection-ceiling honesty ───────────────────────────────────────

_DOC_PROFILES = [
    # (name, workers, max_concurrency, total_direct, total_pooled, total_pg,
    #  pgbouncer_recommended) — the numbers the deployment/configuration
    # and scaling guides publish for the steady-state fleet.
    ("Small", 2, 4, 22, 12, 34.0, False),
    ("Medium", 5, 8, 52, 60, 112.0, True),
    ("Large", 10, 16, 102, 240, 342.0, True),
]


@pytest.mark.parametrize(
    "name,pods,conc,total_direct,total_pooled,total_pg,recommended",
    _DOC_PROFILES,
    ids=[p[0] for p in _DOC_PROFILES],
)
def test_documented_budget_profiles_match_the_computation(
    name: str,
    pods: int,
    conc: int,
    total_direct: int,
    total_pooled: int,
    total_pg: float,
    recommended: bool,
) -> None:
    """The docs' worked profiles (34/112/342) are pinned arithmetic from
    compute_connection_budget, at the documented pool-size defaults, one
    leader, no web pods, no PgBouncer compression."""
    from taskq.worker.budget import compute_connection_budget

    s = _multi_host_settings(
        "postgresql://taskq:taskq@pg1.invalid:5432/taskq",
        TASKQ_MAX_CONCURRENCY=str(conc),
    )
    budget = compute_connection_budget(s, num_worker_pods=pods, num_leader_pods=1)
    assert budget.total_direct == total_direct, name
    assert budget.total_pooled == total_pooled, name
    assert budget.total_pg == total_pg, name
    assert budget.pgbouncer_recommended is recommended, name


# ── A6: AWS IAM auth on the multi-host form ─────────────────────────────


# A fake boto3 client: records the (hostname, port, username) each token
# signing was addressed to and returns a token that encodes them, so the
# pins can assert WHO a token was signed for without any AWS machinery.
# ``Region`` is accepted and ignored - the real client's signature is
# scoped to it, but nothing here reads the token's meaning.
class _FakeRdsClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int, str]] = []

    def generate_db_auth_token(
        # Why noqa N803 on every parameter: boto3's real signature is
        # camelCase; the fake must match it argument-for-argument so the
        # recorded calls read like the real ones.
        self,
        DBHostname: str,  # noqa: N803
        Port: int,  # noqa: N803
        DBUsername: str,  # noqa: N803
        Region: str | None,  # noqa: N803
    ) -> str:
        self.calls.append((DBHostname, Port, DBUsername))
        return f"token-for-{DBHostname}:{Port}"


def test_rds_iam_provider_multi_host_dsn_signs_the_first_hostspec() -> None:
    """The multi-host DSN constructs, and the token is signed for the FIRST
    hostspec - the one libpq tries first, and therefore the one the
    token fetch should address. Previously urlparse's ``.port`` cast on
    the joined ``5432,pg2.invalid:5432`` raised a raw ValueError here,
    before any token could be requested."""
    from taskq.aws import RdsIamProvider

    client = _FakeRdsClient()
    provider = RdsIamProvider(
        "postgresql://iamuser@pg1.invalid:5432,pg2.invalid:5432/taskq?sslmode=require",
        client=client,
        region="us-east-1",
    )
    credential = asyncio.run(provider.get_pg_credential())
    assert client.calls == [("pg1.invalid", 5432, "iamuser")]
    assert credential.password == "token-for-pg1.invalid:5432"
    # The DSN's user is preserved (the IAM-mapped DB user); a token
    # credential carries no username override.
    assert credential.username is None


def test_rds_iam_provider_multi_host_parse_is_bracket_aware() -> None:
    """An IPv6 literal inside a multi-hostspec: the bracketed host's colons
    must not feed the port split, per hostspec."""
    from taskq.aws import (
        _parse_dsn,  # pyright: ignore[reportPrivateUsage]  # Why: the parse contract IS the surface under test; the provider's construction rides it.
    )

    assert _parse_dsn("postgresql://u@[::1]:5432,[::2]:5433/db") == ("::1", 5432, "u")


def test_rds_iam_provider_single_host_parse_is_unchanged() -> None:
    """The single-host contract is byte-identical to the pre-multi-host
    behavior: urlparse's lowercased host, its 5432 default, the
    percent-decoded username, and the socket form's localhost fallback.
    A multi-host fix that moved these would move every existing
    deployment's token signing."""
    from taskq.aws import (
        _parse_dsn,  # pyright: ignore[reportPrivateUsage]  # Why: same as above - the parse contract is the surface under test.
    )

    assert _parse_dsn("postgresql://u:p@h1:5432/db") == ("h1", 5432, "u")
    assert _parse_dsn("postgresql://iamuser@host:5432/db") == ("host", 5432, "iamuser")
    assert _parse_dsn("postgresql://host:5432/db") == ("host", 5432, "")
    assert _parse_dsn("postgresql://host/db") == ("host", 5432, "")
    assert _parse_dsn("postgresql://u%40domain@host:5432/db") == ("host", 5432, "u@domain")
    assert _parse_dsn("postgresql://u:p@[::1]:5433/db") == ("::1", 5433, "u")
    assert _parse_dsn("postgresql://u:p@[::1]/db") == ("::1", 5432, "u")
    assert _parse_dsn("postgresql://u:p@/taskq?host=/var/run/postgresql") == (
        "localhost",
        5432,
        "u",
    )


def test_rds_iam_provider_token_lands_in_every_hostspec_password_slot() -> None:
    """The full rewrite, provider + ``enrich_pg_dsn`` on the multi-host
    DSN: the token goes into the DSN's single shared userinfo password
    slot - i.e. EVERY hostspec's password slot, the only credential slot
    a multi-host DSN has - while the hosts, their ports and the
    ``sslmode`` all survive. Each component is asserted off the parsed
    rewritten DSN, not off substring luck."""
    from taskq.aws import RdsIamProvider

    dsn = "postgresql://iamuser@pg1.invalid:5432,pg2.invalid:5432/taskq?sslmode=require"
    provider = RdsIamProvider(dsn, client=_FakeRdsClient(), region="us-east-1")
    credential = asyncio.run(provider.get_pg_credential())

    rewritten = enrich_pg_dsn(dsn, credential)
    parsed = urlparse(rewritten)
    assert unquote(parsed.password or "") == "token-for-pg1.invalid:5432"
    assert parsed.username == "iamuser"
    # Every hostspec survives, in order, each with its own port:
    assert parsed.netloc.rpartition("@")[2] == "pg1.invalid:5432,pg2.invalid:5432"
    assert parse_qs(parsed.query)["sslmode"] == ["require"]


@pytest.mark.integration
async def test_rds_iam_multi_host_dsn_reaches_the_server_well_formed(pg_dsn: str) -> None:
    """A live connect through the rewritten multi-host DSN with a DUMMY
    token: the failure is the SERVER's auth error (28P01,
    ``InvalidPasswordError``), not a parse or DNS error - the proof that
    the rewritten DSN reached Postgres well-formed for every hostspec.
    Both hostspecs name the same container (the parsing is under test,
    not failover), and ``sslmode=disable`` keeps the failure an auth
    failure: a ``require`` DSN against this plain server would fail with
    the SSL-upgrade refusal instead, a different pin."""
    from taskq.aws import RdsIamProvider

    base = _single_host_dsn(pg_dsn)
    user = base.split("@", 1)[0].split("//", 1)[1].split(":", 1)[0]
    db = base.rpartition("/")[2]
    host_only = base.split("@", 1)[1].split("/", 1)[0]  # host:port
    dsn = f"postgresql://{user}@{host_only},{host_only}/{db}?sslmode=disable"

    provider = RdsIamProvider(dsn, client=_FakeRdsClient(), region="us-east-1")
    credential = await provider.get_pg_credential()
    rewritten = enrich_pg_dsn(dsn, credential)
    assert "token-for-" in rewritten  # the dummy token is what rides the DSN

    with pytest.raises(asyncpg.exceptions.InvalidPasswordError):
        await asyncpg.connect(rewritten)


# ── Container fixtures (module-scoped, one per shape) ────────────────────


@pytest.fixture(scope="module")
def ssl_pg_container() -> Any:  # PostgresContainer; Any keeps the testcontainers import lazy
    """A dedicated Postgres with ssl=on, serving the image's snakeoil pair.

    The official image ships /etc/ssl/certs/ssl-cert-snakeoil.pem and the
    matching key, so ssl=on needs no volume mounts — the container starts
    like any other and every DSN against it negotiates real TLS.
    """
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    pg = PostgresContainer(
        "postgres:18.6",
        username="taskq",
        password="taskq",
        dbname="taskq",
        command=(
            "-c ssl=on"
            " -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem"
            " -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key"
        ),
    )
    pg.start()
    try:
        yield pg
    finally:
        with contextlib.suppress(Exception):
            pg.get_wrapped_container().remove(force=True)


@pytest.fixture(scope="module")
def ssl_pg_dsn(ssl_pg_container: Any) -> str:
    """The ssl=on container's client DSN (no sslmode in it — tests add
    the mode they are pinning)."""
    url: str = ssl_pg_container.get_connection_url().replace(
        "postgresql+psycopg2://", "postgresql://"
    )
    return url


@pytest.fixture(scope="module")
def ssl_ca_cert(ssl_pg_container: Any, tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The ssl=on container's snakeoil cert, copied out so a verify-full
    DSN can name it as sslrootcert."""
    container_id = str(ssl_pg_container.get_wrapped_container().id)
    import shutil
    import subprocess

    docker = shutil.which("docker")
    assert docker is not None, (
        "docker CLI not found; the ssl=on container needs it to copy the CA cert out"
    )
    proc = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the container id is the fixture's own started container.
        [docker, "exec", container_id, "cat", "/etc/ssl/certs/ssl-cert-snakeoil.pem"],
        capture_output=True,
        timeout=30,
    )
    if proc.returncode != 0:
        pytest.fail(
            f"could not copy the snakeoil cert out of the ssl=on container: {proc.stderr!r}"
        )
    out = tmp_path_factory.mktemp("ssl") / "snakeoil.pem"
    out.write_bytes(proc.stdout)
    return out


@pytest.fixture(scope="module")
def socket_pg_dsn(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """A dedicated Postgres serving a Unix socket on a bind-mounted volume
    (TCP left on so testcontainers' own readiness probe still works)."""
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    sock_dir = tmp_path_factory.mktemp("pgsock")
    sock_dir.chmod(0o777)  # the container's postgres user (uid 999) creates the socket
    pg = PostgresContainer(
        "postgres:18.6",
        username="taskq",
        password="taskq",
        dbname="taskq",
        # Dual socket dir: the entrypoint's own init steps connect through
        # the image-default /var/run/postgresql, so removing it entirely
        # dies on first init; the mapped /sock is what the DSN under test
        # points at.
        command="-c unix_socket_directories=/sock,/var/run/postgresql",
    ).with_volume_mapping(str(sock_dir), "/sock", mode="rw")
    pg.start()
    try:
        url: str = pg.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")

        async def _wait() -> None:
            for _ in range(120):
                with contextlib.suppress(Exception):
                    conn = await asyncpg.connect(url)
                    await conn.close()
                    return
                await asyncio.sleep(0.5)
            raise TimeoutError("socket-shape Postgres never became ready")

        asyncio.run(_wait())
        # The socket file carries the SERVER's port (5432 inside the
        # container), not the host-mapped one.
        yield f"postgresql://taskq:taskq@/taskq?host={sock_dir}/.s.PGSQL.5432"
    finally:
        with contextlib.suppress(Exception):
            pg.get_wrapped_container().remove(force=True)
