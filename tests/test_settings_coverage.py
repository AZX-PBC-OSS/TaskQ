"""The settings fields no test read: wiring pins for the coverage audit's gap 8.

These fields passed every load/validation test and zero behavioral ones: a
refactor that stopped threading them to their consumer would have gone red
nowhere. One pin per field, each asserting the field's value AT its
consumer, not merely that the field parses:

* ``WorkerSettings.dispatch_scope_by_home_queue`` -- finding first: this
  flag is a DEPRECATED NO-OP. The per-actor-capacity dispatch scoping it
  used to apply no longer exists (dispatch is assignment-routed; see the
  field's own description and the bootstrap's startup-warning block), so
  the "resolved-scope difference" an older audit describes is gone by
  design. Its only remaining consumer is the bootstrap's
  ``deprecated-setting-ignored`` warning, which is what the pin below
  holds: flag on warns (naming the env var to delete), flag off is silent.
* ``watchdog_loop_lag_startup_grace`` / ``watchdog_dump_after_fraction``
  -- the values must reach the watchdog CONSTRUCTORS inside the real
  ``_main`` boot, not just parse.
* ``TaskQSettings.progress_max_sse_connections`` -- ``create_router``
  with the ``max_sse_connections`` parameter LEFT UNSET must read the
  setting, and the SSE endpoint's 429 boundary must move with it.
* ``redis_credential_provider`` -- the settings value (no CLI flag) must
  reach the credential-provider loader on both commands that resolve it
  (``taskq worker`` and ``taskq ui serve``).
* ``SAMLSettings.sp_x509_cert`` -- the settings value must ride
  ``_build_sso_bundle`` into the runtime auth config, and from there be
  emitted into the SP settings dict python3-saml consumes.

Integration tier only where a real boot is the seam (the watchdog pin);
everything else is in-process.
"""

from __future__ import annotations

import asyncio
import contextlib
import functools
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import asyncpg
import pytest
import structlog.testing
from pydantic import BaseModel as _BaseModel
from typer.testing import CliRunner

from taskq._ids import new_base62, new_uuid
from taskq.actor import ActorRef
from taskq.actor import actor as _actor
from taskq.cli import (
    _build_sso_bundle,  # pyright: ignore[reportPrivateUsage]  # Why: the SSO bundle builder is the seam the settings value rides.
    _load_redis_credential_provider,  # pyright: ignore[reportPrivateUsage]
)
from taskq.cli import (
    app as cli_app,
)
from taskq.settings import TaskQSettings, WorkerSettings

runner = CliRunner()


# A populated registry: the worker CLI refuses an empty one. Module-level
# so the CLI can resolve it by ``module:attr``.
class _Payload(_BaseModel):
    x: int


@_actor(name="settings_coverage_actor", queue="default")
async def _settings_coverage_actor(payload: _Payload) -> None: ...


_REGISTRY: dict[str, ActorRef[Any, Any]] = {"settings_coverage_actor": _settings_coverage_actor}  # type: ignore[dict-item]
_REGISTRY_PATH = "tests.test_settings_coverage:_REGISTRY"


# A provider instance the CLI resolves by ref (the settings-fallback pin).
class _ProviderState:
    calls = 0


class _RedisProvider:
    async def get_redis_credential(self) -> Any:
        from taskq.auth import RedisCredential

        _ProviderState.calls += 1
        return RedisCredential(
            username="mi-object-id", password=f"redis-token-{_ProviderState.calls}"
        )


_PROVIDER = _RedisProvider()


# ── 1a. dispatch_scope_by_home_queue: the deprecated-no-op warning ──────


class TestDispatchScopeByHomeQueue:
    """The flag's ONLY consumer is the bootstrap's startup warning.

    The audit's premise ("the flag changes the bootstrap's scope
    resolution") describes the pre-assignment-routing code. Today
    resolution never reads it; what a deployment that still sets it gets
    is the ``deprecated-setting-ignored`` warning naming the env var to
    remove. That warning IS the wiring now, so that is what is pinned.
    """

    def test_flag_on_warns_deprecated_noop(self) -> None:
        from taskq.worker._bootstrap import (
            _emit_startup_warnings,  # pyright: ignore[reportPrivateUsage]
        )

        settings = WorkerSettings.load_from_dict(
            {"TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE": "true"}, validate=False
        )
        assert settings.dispatch_scope_by_home_queue is True

        with structlog.testing.capture_logs() as logs:
            _emit_startup_warnings(settings)

        entry = next(e for e in logs if e["event"] == "deprecated-setting-ignored")
        assert entry["log_level"] == "warning"
        assert entry["setting"] == "TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE"
        assert "assignment-routed" in entry["reason"], (
            "the warning must say WHY the flag does nothing"
        )
        assert "TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE" in entry["remedy"], (
            "the warning must name the variable to remove"
        )

    def test_flag_off_is_silent(self) -> None:
        from taskq.worker._bootstrap import (
            _emit_startup_warnings,  # pyright: ignore[reportPrivateUsage]
        )

        for env in ({}, {"TASKQ_DISPATCH_SCOPE_BY_HOME_QUEUE": "false"}):
            settings = WorkerSettings.load_from_dict(env, validate=False)
            with structlog.testing.capture_logs() as logs:
                _emit_startup_warnings(settings)

            assert [e for e in logs if e["event"] == "deprecated-setting-ignored"] == [], (
                f"no deprecated-setting warning may fire for {env or 'the defaults'}: "
                f"{[e['event'] for e in logs]}"
            )


# ── 1b. the watchdog settings reach the watchdog construction ──────────


@pytest.mark.integration
async def test_watchdog_construction_receives_the_settings(
    pg_dsn: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both watchdog knobs ride the real ``_main`` boot into the real
    constructors.

    Mutating either keyword off the construction site (falling back to
    the dataclass default) reds this: the operator's
    ``TASKQ_WATCHDOG_LOOP_LAG_STARTUP_GRACE`` would silently stop
    covering import-heavy startup, and
    ``TASKQ_WATCHDOG_DUMP_AFTER_FRACTION`` would silently move the
    straggler-dump window.
    """
    from taskq.migrate import apply_pending
    from taskq.testing.health import unique_health_sock_path
    from taskq.worker import _bootstrap as bootstrap_mod
    from taskq.worker.run import _main

    schema = f"tsc_{new_base62()}".lower()
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
    finally:
        await conn.close()

    captured: dict[str, dict[str, Any]] = {}
    real_lag = bootstrap_mod.LoopLagWatchdog
    real_shutdown = bootstrap_mod.ShutdownWatchdog

    class _LagSpy(real_lag):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["lag"] = kwargs
            super().__init__(*args, **kwargs)

    class _ShutdownSpy(real_shutdown):  # type: ignore[misc, valid-type]
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            captured["shutdown"] = kwargs
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(bootstrap_mod, "LoopLagWatchdog", _LagSpy)
    monkeypatch.setattr(bootstrap_mod, "ShutdownWatchdog", _ShutdownSpy)

    settings = WorkerSettings.load_from_dict(
        {
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_WATCHDOG_LOOP_LAG_STARTUP_GRACE": "7.5",
            "TASKQ_WATCHDOG_DUMP_AFTER_FRACTION": "0.9",
            "TASKQ_HEALTH_SOCKET_PATH": unique_health_sock_path("settings_coverage"),
        }
    )

    async def _runner() -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await _main(settings, actor_registry={})

    task = asyncio.create_task(_runner())
    try:
        for _ in range(150):  # up to 15s: the watchdogs are built mid-boot
            if "lag" in captured and "shutdown" in captured:
                break
            await asyncio.sleep(0.1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    conn = await asyncpg.connect(pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()

    assert "lag" in captured, "the boot never built LoopLagWatchdog"
    assert "shutdown" in captured, "the boot never built ShutdownWatchdog"
    assert captured["lag"]["startup_grace"] == settings.watchdog_loop_lag_startup_grace
    assert captured["lag"]["startup_grace"] == 7.5
    assert captured["shutdown"]["dump_after_fraction"] == (settings.watchdog_dump_after_fraction)
    assert captured["shutdown"]["dump_after_fraction"] == 0.9


# ── 1c. progress_max_sse_connections caps the progress SSE stream ──────


class _FakeConn:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    async def fetchrow(self, _query: str, *_args: object) -> dict[str, Any] | None:
        return self._row


class _StubPool:
    def __init__(self, row: dict[str, Any] | None) -> None:
        self._row = row

    def acquire(self, *, timeout: float | None = None) -> Any:
        conn = _FakeConn(self._row)

        class _Ctx:
            async def __aenter__(self) -> _FakeConn:
                return conn

            async def __aexit__(self, *exc_info: object) -> None:
                return None

        return _Ctx()


class _StubPubSub:
    async def subscribe(self, channel: str) -> None:
        return None

    async def get_message(self, **_kw: Any) -> None:
        return None

    async def unsubscribe(self, channel: str) -> None:
        return None

    async def aclose(self) -> None:
        return None


class _StubRedis:
    def __init__(self) -> None:
        self._pubsub = _StubPubSub()

    def pubsub(self) -> _StubPubSub:
        return self._pubsub


_JOB_ID = new_uuid()
_PG_ROW = {
    "status": "running",
    "progress_seq": 5,
    "progress_state": {"step": 1},
}


class TestProgressSseCapFromSettings:
    """``create_router`` with ``max_sse_connections`` left UNSET reads
    ``TASKQ_PROGRESS_MAX_SSE_CONNECTIONS`` (web/progress.py's read site),
    and the endpoint's 429 boundary moves with it."""

    @pytest.mark.parametrize(
        ("cap", "second_admitted"),
        [("1", False), ("2", True)],
        ids=["cap-1-rejects-second", "cap-2-admits-second"],
    )
    async def test_setting_caps_the_stream_429_boundary(
        self, monkeypatch: pytest.MonkeyPatch, cap: str, second_admitted: bool
    ) -> None:
        pytest.importorskip("fastapi")
        pytest.importorskip("sse_starlette")

        from fastapi import HTTPException
        from fastapi.routing import APIRoute

        from taskq.web import _sse_limit
        from taskq.web import progress as progress_mod

        monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
        monkeypatch.setenv("TASKQ_PROGRESS_MAX_SSE_CONNECTIONS", cap)
        # The registry is process-global and keyed (family, limit); clear
        # on entry too, a sibling suite's leak must not pre-fill slots.
        _sse_limit._SEMAPHORES.clear()  # pyright: ignore[reportPrivateUsage]

        pool = _StubPool(dict(_PG_ROW))
        redis = _StubRedis()
        router = progress_mod.create_router(
            pool,  # pyright: ignore[reportArgumentType]  # Why: duck-typed stub at the erased Any boundary.
            redis,
            schema="taskq",
            # max_sse_connections left UNSET: the settings read is the seam under test.
        )
        endpoint = next(
            route.endpoint
            for route in router.routes
            if isinstance(route, APIRoute) and route.path.endswith("/progress/stream")
        )
        call = functools.partial(endpoint, pg_pool=pool, redis_client=redis)

        request = MagicMock()
        request.headers.get.return_value = None  # no Last-Event-ID header

        first = await call(_JOB_ID, request)
        try:
            assert first.status_code == 200, "the first stream must be admitted"
            if second_admitted:
                second = await call(_JOB_ID, request)
                assert second.status_code == 200, f"cap {cap} admits a second concurrent stream"
            else:
                with pytest.raises(HTTPException) as exc_info:
                    await call(_JOB_ID, request)
                assert exc_info.value.status_code == 429, (
                    f"cap {cap} must reject the second concurrent stream with 429"
                )
        finally:
            _sse_limit._SEMAPHORES.clear()  # pyright: ignore[reportPrivateUsage]

    async def test_default_cap_is_fifty_connections(self) -> None:
        """The documented default (50) and the field the factory reads agree."""
        settings = TaskQSettings.load_from_dict({})
        assert settings.progress_max_sse_connections == 50


# ── 1d. redis_credential_provider: the settings value reaches the CLI ──


def _fake_asyncpg(monkeypatch: pytest.MonkeyPatch) -> None:
    """Recording fakes for asyncpg.create_pool / connect (CLI never dials)."""

    class _FakePool:
        async def close(self) -> None:
            return None

    async def _create_pool(**_kwargs: Any) -> _FakePool:
        return _FakePool()

    class _FakeConn:
        async def close(self) -> None:
            return None

    async def _connect(*_args: Any, **_kwargs: Any) -> Any:
        return _FakeConn()

    monkeypatch.setattr(asyncpg, "create_pool", _create_pool)
    monkeypatch.setattr(asyncpg, "connect", _connect)


def _worker_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TASKQ_PG_DSN", "postgresql://app@db.example:5432/taskq")
    monkeypatch.setenv("TASKQ_PG_DSN_DIRECT", "postgresql://app@db.example:5432/taskq")
    monkeypatch.setenv("TASKQ_PG_DSN_POOLED", "postgresql://app@pgbouncer.example:6432/taskq")
    monkeypatch.setenv("TASKQ_REDIS_URL", "rediss://cache.example:6380/0")
    monkeypatch.setenv("TASKQ_NOTIFY_ENABLED", "false")
    monkeypatch.setenv("TASKQ_HEALTH_ENABLED", "false")


class TestRedisCredentialProviderSettingsFallback:
    """``TASKQ_REDIS_CREDENTIAL_PROVIDER`` (no CLI flag) must reach
    ``_load_redis_credential_provider`` on both resolving commands: the
    ``_resolved_ref`` fallback is the seam, flag-beats-settings is
    already pinned by test_cli_credential_provider for the PG ref."""

    def test_worker_command_resolves_the_settings_ref(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _worker_env(monkeypatch)
        _fake_asyncpg(monkeypatch)
        monkeypatch.setenv(
            "TASKQ_REDIS_CREDENTIAL_PROVIDER", "tests.test_settings_coverage:_PROVIDER"
        )

        loaded: list[tuple[str, str]] = []

        def _spy(ref: str, *, option: str) -> object:
            loaded.append((ref, option))
            return _load_redis_credential_provider(ref, option=option)

        monkeypatch.setattr("taskq.cli._load_redis_credential_provider", _spy)
        captured: dict[str, Any] = {}

        def fake_worker_main(settings: Any, *, connections: Any = None, **_kw: Any) -> int:
            captured["connections"] = connections
            return 0

        monkeypatch.setattr("taskq.cli._worker_main", fake_worker_main)

        result = runner.invoke(cli_app, ["worker", "--actors", _REGISTRY_PATH])

        assert result.exit_code == 0, f"stderr: {result.stderr}"
        assert loaded == [
            ("tests.test_settings_coverage:_PROVIDER", "--redis-credential-provider")
        ], f"the settings ref must reach the loader even with no CLI flag: {loaded}"
        assert captured.get("connections") is not None, (
            "the resolved provider must have built the worker's connections"
        )

    def test_ui_serve_resolves_the_settings_ref(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _worker_env(monkeypatch)
        _fake_asyncpg(monkeypatch)
        monkeypatch.setenv(
            "TASKQ_REDIS_CREDENTIAL_PROVIDER", "tests.test_settings_coverage:_PROVIDER"
        )

        loaded: list[tuple[str, str]] = []

        def _spy(ref: str, *, option: str) -> object:
            loaded.append((ref, option))
            return _load_redis_credential_provider(ref, option=option)

        monkeypatch.setattr("taskq.cli._load_redis_credential_provider", _spy)
        monkeypatch.setattr("taskq.cli._ui_serve", lambda *_a, **_kw: None)

        result = runner.invoke(cli_app, ["ui", "serve"])

        assert result.exit_code == 0, f"stderr: {result.stderr}"
        assert loaded == [
            ("tests.test_settings_coverage:_PROVIDER", "--redis-credential-provider")
        ], f"the ui serve arm must resolve the settings ref too: {loaded}"

    def test_settings_ref_unset_loader_never_called(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """The control: no settings value and no flag -> the loader is
        never consulted and the DSN path stands."""
        _worker_env(monkeypatch)
        monkeypatch.delenv("TASKQ_REDIS_CREDENTIAL_PROVIDER", raising=False)

        loaded: list[tuple[str, str]] = []
        monkeypatch.setattr(
            "taskq.cli._load_redis_credential_provider",
            lambda ref, *, option: loaded.append((ref, option)),  # type: ignore[func-returns-value]
        )
        monkeypatch.setattr("taskq.cli._ui_serve", lambda *_a, **_kw: None)

        result = runner.invoke(cli_app, ["ui", "serve"])

        assert result.exit_code == 0, f"stderr: {result.stderr}"
        assert loaded == [], f"no ref, no resolution: {loaded}"


# ── 1e. SAMLSettings.sp_x509_cert reaches the SP config ────────────────


class TestSamlSpX509Cert:
    """Two seams: settings -> the runtime auth config (``_build_sso_bundle``
    construction of ``SAMLAuthConfig``), and the config -> the SP settings
    dict python3-saml consumes (``_build_settings``' ``sp["x509cert"]``
    emission)."""

    def test_settings_value_reaches_the_saml_auth_config(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("TASKQ_SSO_BACKEND", "saml")
        monkeypatch.setenv("TASKQ_SAML_ENTITY_ID", "https://sp.example/meta")
        monkeypatch.setenv("TASKQ_SAML_ACS_URL", "https://sp.example/admin/callback")
        monkeypatch.setenv("TASKQ_SAML_IDP_ENTITY_ID", "https://idp.example/meta")
        monkeypatch.setenv("TASKQ_SAML_IDP_SSO_URL", "https://idp.example/sso")
        monkeypatch.setenv(
            "TASKQ_SAML_IDP_X509_CERT",
            "-----BEGIN CERTIFICATE-----\nIDP\n-----END CERTIFICATE-----",
        )
        monkeypatch.setenv(
            "TASKQ_SAML_SP_X509_CERT", "-----BEGIN CERTIFICATE-----\nSP\n-----END CERTIFICATE-----"
        )
        monkeypatch.setenv("TASKQ_SAML_SESSION_SECRET", "test-session-secret")

        settings = TaskQSettings.load()

        captured: dict[str, Any] = {}

        def _spy(config: Any, *, base_path: str = "") -> Any:
            captured["config"] = config
            return SimpleNamespace(router=None, dependency=lambda: None)

        monkeypatch.setattr("taskq.web.admin.auth.create_saml_auth", _spy)

        bundle = _build_sso_bundle(settings, "/admin")

        assert bundle is not None, "sso_backend=saml must build a bundle"
        assert captured["config"].sp_x509_cert == (
            "-----BEGIN CERTIFICATE-----\nSP\n-----END CERTIFICATE-----"
        ), "the settings value must ride _build_sso_bundle into SAMLAuthConfig"

    def test_sp_cert_is_emitted_into_the_sp_settings_dict(self) -> None:
        from taskq.web.admin.auth.saml import SAMLAuthConfig, _build_settings

        cert = "-----BEGIN CERTIFICATE-----\nSP\n-----END CERTIFICATE-----"
        config = SAMLAuthConfig(
            entity_id="https://sp.example/meta",
            acs_url="https://sp.example/admin/callback",
            idp_entity_id="https://idp.example/meta",
            idp_sso_url="https://idp.example/sso",
            idp_x509_cert="IDP-CERT",
            sp_x509_cert=cert,
            session_secret="s",
        )

        sp = _build_settings(config)["sp"]

        assert sp["x509cert"] == cert, (
            "the SP cert must be emitted under the python3-saml key the "
            "toolkit reads for signed requests / encrypted assertions"
        )

    def test_sp_cert_absent_keeps_the_key_out_of_the_sp_dict(self) -> None:
        """The default (None) must not emit an empty x509cert: python3-saml
        treats a present-but-empty cert as a misconfiguration."""
        from taskq.web.admin.auth.saml import SAMLAuthConfig, _build_settings

        config = SAMLAuthConfig(
            entity_id="https://sp.example/meta",
            acs_url="https://sp.example/admin/callback",
            idp_entity_id="https://idp.example/meta",
            idp_sso_url="https://idp.example/sso",
            idp_x509_cert="IDP-CERT",
            session_secret="s",
        )

        sp = _build_settings(config)["sp"]

        assert "x509cert" not in sp
