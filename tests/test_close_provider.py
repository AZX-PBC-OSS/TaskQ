"""close_provider_bounded: releasing a credential provider at teardown."""

from __future__ import annotations

import asyncio

import structlog.testing

from taskq._close import close_provider_bounded


class _Provider:
    def __init__(self, *, hang: bool = False, fail: bool = False) -> None:
        self.hang = hang
        self.fail = fail
        self.closed = 0

    async def aclose(self) -> None:
        if self.hang:
            await asyncio.Event().wait()
        if self.fail:
            raise RuntimeError("session already closed")
        self.closed += 1


async def test_a_provider_with_aclose_is_closed() -> None:
    provider = _Provider()
    await close_provider_bounded(provider, "test", 1.0)
    assert provider.closed == 1


async def test_a_provider_without_aclose_is_left_alone() -> None:
    """A token signer or a caller-owned Vault client has nothing to release."""
    await close_provider_bounded(object(), "test", 1.0)


async def test_a_hung_close_is_bounded_and_reported() -> None:
    with structlog.testing.capture_logs() as logs:
        await asyncio.wait_for(close_provider_bounded(_Provider(hang=True), "ui-admin", 0.05), 2.0)
    entry = next(e for e in logs if e["event"] == "provider-teardown-close-timeout")
    assert entry["label"] == "ui-admin"
    assert entry["close_timeout"] == 0.05


async def test_a_failing_close_is_reported_and_never_raises() -> None:
    with structlog.testing.capture_logs() as logs:
        await close_provider_bounded(_Provider(fail=True), "ui-admin", 1.0)
    entry = next(e for e in logs if e["event"] == "provider-teardown-close-error")
    assert "RuntimeError" in entry["error"]
