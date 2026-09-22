"""Cross-replica shared replay store for the SAML admin auth (defect #264).

The SAML auth's assertion-replay gate and answered-AuthnRequest gate were
process-local dicts: in any deployment with more than one process, bundle A's
acceptance was invisible to bundle B, so a captured, correctly-signed response
re-POSTed to a SIBLING replica minted a second session (cross-replica replay),
and a flood of accepted logins could evict a consumed record that was then
re-accepted.

These tests drive TWO auth bundles over the SAME Postgres schema -- the
deployment shape of two admin replicas (separate pools, one shared store) --
and pin the shared semantics: the sibling refuses a consumed assertion, refuses
a fresh assertion answering an already-answered request, and cannot evict a
record by flood. The expired-row lifecycle is pinned at the store level.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
from typing import Any

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("onelogin.saml2.auth")

import asyncpg
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from taskq.migrate import apply_pending
from taskq.web.admin.auth.saml import SAMLAuthConfig, create_saml_auth
from tests._sso_saml_crypto import (
    ACS_URL,
    IDP_CERT_PEM,
    IDP_ENTITY_ID,
    SP_ENTITY_ID,
    build_saml_response,
)
from tests.test_sso_saml import (
    _TEST_BASE_URL,  # pyright: ignore[reportPrivateUsage]  # Why: shared fixture constant, the ACS base URL the fixture crypto is built for.
    _assertion_id,  # pyright: ignore[reportPrivateUsage]  # Why: the fixture-XML assertion-ID extractor, shared so both suites read it identically.
    _correlation_cookie,  # pyright: ignore[reportPrivateUsage]  # Why: shared helper, same capture semantics as the single-bundle suites.
    _do_login,  # pyright: ignore[reportPrivateUsage]  # Why: shared helper (spies OneLogin's login to return the issued request ID).
)

pytestmark = [pytest.mark.saml]

_SSO_URL = "https://idp.test.invalid/sso"
_SESSION_SECRET = "s" * 32


@pytest.fixture(scope="module")
def saml_schema() -> str:
    # This module gets its OWN database on the shared container (the
    # module-scoped pg_dsn fixture), so one fixed schema name cannot collide
    # with anything.
    return "taskq_saml_replay"


def _config(**overrides: Any) -> SAMLAuthConfig:
    return SAMLAuthConfig(
        entity_id=SP_ENTITY_ID,
        acs_url=ACS_URL,
        idp_entity_id=IDP_ENTITY_ID,
        idp_sso_url=_SSO_URL,
        idp_x509_cert=IDP_CERT_PEM,
        session_secret=_SESSION_SECRET,
        secure_cookie=False,
        **overrides,
    )


@pytest.fixture(scope="module")
def migrated_dsn(pg_dsn: str, saml_schema: str) -> Iterator[str]:
    """The module's PG database with the bundled migrations applied.

    The real migration path is exercised, not a hand-written CREATE TABLE: if
    the bundled migration drifts from what saml.py's statements expect, every
    test here fails.
    """

    async def _apply() -> None:
        conn = await asyncpg.connect(pg_dsn)
        try:
            await apply_pending(conn, schema=saml_schema)
        finally:
            await conn.close()

    asyncio.run(_apply())
    yield pg_dsn


def _replica(config: SAMLAuthConfig, dsn: str, schema: str) -> FastAPI:
    """One replica: its own pool over the SAME database and schema.

    The lifespan wires exactly the keys the admin app's ``setup_admin_state``
    sets (``pg_pool`` + ``schema``), which is the wiring the store resolution
    in saml.py reads. TestClient runs each app in its own event loop, so each
    replica owns its pool the way a replica owns its process.
    """
    bundle = create_saml_auth(config, base_path="/admin")

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
        pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
        app.state.pg_pool = pool
        app.state.schema = schema
        try:
            yield
        finally:
            await pool.close()

    app = FastAPI(lifespan=lifespan)
    app.include_router(bundle.router, prefix="/admin")
    return app


def _request_with_state(**state: Any) -> Request:
    """A Request whose app.state carries *state* (for store-selection pins)."""
    app = FastAPI()
    for key, value in state.items():
        setattr(app.state, key, value)
    return Request(scope={"type": "http", "app": app})


def _post_response(client: TestClient, response_b64: str) -> Any:
    return client.post(
        "/admin/callback",
        data={"SAMLResponse": response_b64},
        follow_redirects=False,
    )


# ── The two-bundle repro: cross-replica replay must be refused ────────────


def test_a_second_replica_cannot_accept_an_assertion_the_first_consumed(
    migrated_dsn: str,
    saml_schema: str,
) -> None:
    """The confirmed defect, as a test: bundle A consumes an assertion,
    bundle B must NOT succeed on the same assertion.

    Two bundles over one shared store, both replicas live at once. Replica A
    performs an honest login; the captured POST is then presented to replica
    B with the correlation cookie re-supplied (the browser's copy was
    cleared; a replaying party holds a capture). Pre-fix, B's records were
    process-local, saw nothing of A's acceptance, and minted a second
    session. Post-fix, B reads the row A wrote and refuses.
    """
    config = _config()
    with (
        TestClient(_replica(config, migrated_dsn, saml_schema), base_url=_TEST_BASE_URL) as client_a,
        TestClient(_replica(config, migrated_dsn, saml_schema), base_url=_TEST_BASE_URL) as client_b,
    ):
        request_id = _do_login(client_a)
        correlation_cookie = _correlation_cookie(client_a)
        saml_response = build_saml_response(nameid="user-saml-1", in_response_to=request_id)

        first = _post_response(client_a, saml_response)
        assert first.headers.get("location") == "/admin", (
            "replica A must accept the honest login; the repro is void otherwise"
        )
        assert "taskq_session=" in first.headers.get("set-cookie", "")

        client_b.cookies.set("taskq_saml_request", correlation_cookie)
        second = _post_response(client_b, saml_response)

    assert "error=authentication+failed" in second.headers.get("location", ""), (
        "a sibling replica must refuse an assertion another replica already "
        f"consumed; got redirect to {second.headers.get('location')!r}"
    )
    assert "taskq_session=" not in second.headers.get("set-cookie", ""), (
        "the cross-replica replay must not mint a second session"
    )


def test_a_fresh_assertion_answering_an_answered_request_is_refused_on_the_sibling(
    migrated_dsn: str,
    saml_schema: str,
) -> None:
    """The answered-request gate is shared too, separately from the replay gate.

    A second DISTINCT assertion (fresh ID, so the replay gate alone cannot
    refuse it) answering the same AuthnRequest ID, presented to the sibling
    with the captured correlation cookie: refused, because the answered
    record replica A wrote is visible to replica B. This is the gate the
    replay cache cannot cover.
    """
    config = _config()
    with (
        TestClient(_replica(config, migrated_dsn, saml_schema), base_url=_TEST_BASE_URL) as client_a,
        TestClient(_replica(config, migrated_dsn, saml_schema), base_url=_TEST_BASE_URL) as client_b,
    ):
        request_id = _do_login(client_a)
        correlation_cookie = _correlation_cookie(client_a)
        first = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
        accepted = _post_response(client_a, first)
        assert accepted.headers.get("location") == "/admin", (
            "replica A must accept the honest login; the repro is void otherwise"
        )

        second = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
        assert _assertion_id(second) != _assertion_id(first), (
            "the replay must carry a fresh assertion ID so this test can only "
            "pass through the answered-request gate, not the replay gate"
        )

        client_b.cookies.set("taskq_saml_request", correlation_cookie)
        replayed = _post_response(client_b, second)

    assert "error=authentication+failed" in replayed.headers.get("location", ""), (
        "a fresh assertion answering an already-answered AuthnRequest must be "
        f"refused on the sibling; got redirect to {replayed.headers.get('location')!r}"
    )
    assert "taskq_session=" not in replayed.headers.get("set-cookie", "")


# ── Flood eviction is structurally impossible on the shared store ─────────


def test_a_flood_of_accepted_logins_cannot_evict_a_consumed_assertion(
    migrated_dsn: str,
    saml_schema: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 10k-entry flood that evicted a consumed record cannot recur.

    The in-process fallback's cap is shrunk to 2 so the flood below would
    evict the first login's record IF the records were still process-local;
    with the DB-keyed store (no cap: rows expire by TTL, never by pressure)
    the same flood leaves the consumed record in place and the replay is
    refused. Under the in-memory mutation this test goes red on its own.
    """
    import taskq.web.admin.auth.saml as saml_module

    # Shrink BOTH in-process caps so the flood below would evict the first
    # login's records IF the records were still process-local (the triage
    # repro flooded the replay cache's 10k cap; shrinking both caps here
    # makes the same eviction reachable at test scale).
    monkeypatch.setattr(saml_module, "_REPLAY_CACHE_MAX_ENTRIES", 2)
    monkeypatch.setattr(saml_module, "_ANSWERED_REQUEST_MAX_ENTRIES", 2)
    with TestClient(_replica(_config(), migrated_dsn, saml_schema), base_url=_TEST_BASE_URL) as client:
        request_id = _do_login(client)
        first_cookie = _correlation_cookie(client)
        first = build_saml_response(nameid="user-saml-1", in_response_to=request_id)
        accepted = _post_response(client, first)
        assert accepted.headers.get("location") == "/admin", (
            "the honest login must be accepted; the repro is void otherwise"
        )

        # The flood: only ACCEPTED logins write replay records, so each round
        # is a full /login + accepted assertion. Four more accepted logins
        # push five records through a cap-2 in-process set.
        for _ in range(4):
            flood_request_id = _do_login(client)
            flood = build_saml_response(nameid="user-saml-1", in_response_to=flood_request_id)
            flood_resp = _post_response(client, flood)
            assert flood_resp.headers.get("location") == "/admin", (
                "each flood round must be accepted so it actually writes a record"
            )

        # The captured first POST, replayed after the flood.
        replayer = TestClient(client.app, base_url=_TEST_BASE_URL)  # pyright: ignore[reportArgumentType]  # Why: TestClient accepts the ASGI app; client.app is the FastAPI instance.
        replayer.cookies.set("taskq_saml_request", first_cookie)
        replayed = _post_response(replayer, first)

    assert "error=authentication+failed" in replayed.headers.get("location", ""), (
        "a flood of accepted logins must not evict a consumed assertion from "
        f"the shared store; got redirect to {replayed.headers.get('location')!r}"
    )
    assert "taskq_session=" not in replayed.headers.get("set-cookie", "")


# ── Expiry: a past-NotOnOrAfter row neither blocks nor lingers ────────────


def test_an_expired_replay_row_neither_blocks_nor_lingers(
    migrated_dsn: str, saml_schema: str
) -> None:
    """Expiry semantics of the shared store, driven against the real table.

    A replay record whose NotOnOrAfter has passed must not block a later
    claim of the same ID (the assertion's own timestamps already refuse the
    assertion; the record has nothing left to refuse), must not be returned
    by the answered gate past its TTL, and must be swept rather than linger.
    """

    async def _run() -> None:
        pool = await asyncpg.create_pool(migrated_dsn, min_size=1, max_size=1)
        try:
            from taskq.web.admin.auth.saml import (  # pyright: ignore[reportPrivateUsage]  # Why: the store's expiry semantics are the subject; the class is not public API.
                _PostgresSamlReplayStore,
            )

            store = _PostgresSamlReplayStore(pool, saml_schema)
            # The count assertion below is only meaningful on a table this
            # test owns: the module's other tests write rows (fresh IDs each,
            # no cross-test reads), so start from a clean slate whatever the
            # random order.
            async with pool.acquire() as conn:
                # Schema is this module's own fixed name on its own database.
                await conn.execute(f'TRUNCATE "{saml_schema}".saml_replay_store')
            now = time.time()
            # Two rows that are already expired the moment they are written:
            # one assertion record past its NotOnOrAfter, one answered
            # record past its TTL.
            await store.consume("assertion-stale", now - 1000, now=now)
            await store.record_answered("request-stale", ttl=300, now=now - 400)

            # The expired answered record answers nothing.
            assert not await store.already_answered("request-stale", now=now)
            assert not await store.already_answered("request-stale", now=now + 1)

            # A LIVE claim of the same assertion ID succeeds: the expired row
            # does not block it (and is reclaimed in place by the claim).
            await store.consume("assertion-stale", now + 600, now=now + 1)
            # The live record refuses a second presentation.
            with pytest.raises(ValueError, match="replayed"):
                await store.consume("assertion-stale", now + 600, now=now + 2)

            # The sweep left exactly the live row: the expired debris (the
            # stale answered record, any superseded assertion record) is gone.
            async with pool.acquire() as conn:
                count = await conn.fetchval(
                    f'SELECT count(*) FROM "{saml_schema}".saml_replay_store'  # noqa: S608  # Why: schema is this module's own fixed name on its own database; no user input.
                )
            assert count == 1, f"expected exactly the one live row, found {count}"
        finally:
            await pool.close()

    asyncio.run(_run())


# ── Store selection: shared Postgres when the admin pool is present ───────


def test_store_selection_wires_postgres_only_when_the_admin_pool_is_present(
    saml_schema: str,
) -> None:
    """Pins the wiring decision.

    The admin app always populates ``app.state.pg_pool`` and
    ``app.state.schema`` (``setup_admin_state``) before its first request, so
    every SAML callback served through it shares the Postgres store. Bare
    apps -- embedders mounting the router on their own app, and the bare test
    fixtures -- carry neither key and keep the bundle-local in-process store.
    A pool without a schema does not half-share: it stays in-process rather
    than guessing a schema.
    """
    from taskq.web.admin.auth.saml import (  # pyright: ignore[reportPrivateUsage]  # Why: the selection under test IS the wiring; these are not public API.
        _InProcessSamlReplayStore,
        _PostgresSamlReplayStore,
        _replay_store_for,
    )

    fallback = _InProcessSamlReplayStore()

    bare = _request_with_state()
    assert isinstance(_replay_store_for(bare, fallback), _InProcessSamlReplayStore), (
        "a bare app (no admin state) must keep the in-process store"
    )

    wired = _request_with_state(pg_pool=object(), schema=saml_schema)
    store = _replay_store_for(wired, fallback)
    assert isinstance(store, _PostgresSamlReplayStore), (
        "the admin app's state shape must select the shared Postgres store"
    )

    half_wired = _request_with_state(pg_pool=object())
    assert isinstance(_replay_store_for(half_wired, fallback), _InProcessSamlReplayStore), (
        "a pool without a schema must not half-share; the store needs both keys"
    )
