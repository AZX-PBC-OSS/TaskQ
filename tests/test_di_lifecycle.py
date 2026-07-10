"""Unit tests for lifecycle detection functions."""

import warnings
from collections.abc import AsyncIterator, Iterator

from taskq._di.lifecycle import detect_factory_lifecycle, detect_lifecycle
from taskq._di.types import ProviderLifecycle


class _PlainClass:
    pass


class _InitRaises:
    def __init__(self) -> None:
        raise RuntimeError("must not be called")


class _ACMClass:
    async def __aenter__(self) -> "_ACMClass":
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        pass


class _AsyncCloseableClass:
    async def aclose(self) -> None:
        pass


class _SyncCloseableClass:
    def close(self) -> None:
        pass


class _HybridACMAndAclose:
    async def __aenter__(self) -> "_HybridACMAndAclose":
        return self

    async def __aexit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        pass

    async def aclose(self) -> None:
        pass


class _BothCloseClass:
    async def aclose(self) -> None:
        pass

    def close(self) -> None:
        pass


class _SyncACMClass:
    def __enter__(self) -> "_SyncACMClass":
        return self

    def __exit__(
        self,
        exc_type: object,
        exc_val: object,
        exc_tb: object,
    ) -> None:
        pass


class _CloseAsyncNameClass:
    async def close_async(self) -> None:
        pass


# ── Plain class detection ───────────────────────────────────────────


def test_plain_class_detection() -> None:
    """detect_lifecycle returns Plain for a class with only __init__."""
    assert detect_lifecycle(_PlainClass) == ProviderLifecycle.Plain


# ── AsyncContextManager detection ────────────────────────────────────


def test_async_context_manager_detection() -> None:
    """detect_lifecycle returns AsyncContextManager for __aenter__/__aexit__."""
    assert detect_lifecycle(_ACMClass) == ProviderLifecycle.AsyncContextManager


# ── AsyncCloseable detection ─────────────────────────────────────────


def test_async_closeable_detection() -> None:
    """detect_lifecycle returns AsyncCloseable for aclose()."""
    assert detect_lifecycle(_AsyncCloseableClass) == ProviderLifecycle.AsyncCloseable


# ── SyncCloseable detection ─────────────────────────────────────────


def test_sync_closeable_detection() -> None:
    """detect_lifecycle returns SyncCloseable for close()."""
    assert detect_lifecycle(_SyncCloseableClass) == ProviderLifecycle.SyncCloseable


# ── Priority — ACM beats AsyncCloseable ──────────────────────────────


def test_acm_beats_async_closeable() -> None:
    """ACM wins when both __aenter__/__aexit__ and aclose are present."""
    assert detect_lifecycle(_HybridACMAndAclose) == ProviderLifecycle.AsyncContextManager


# ── Priority — AsyncCloseable beats SyncCloseable ────────────────────


def test_async_closeable_beats_sync_closeable() -> None:
    """AsyncCloseable wins when both aclose and close are present."""
    assert detect_lifecycle(_BothCloseClass) == ProviderLifecycle.AsyncCloseable


# ── Sync ACM returns Plain, no warning, no log ──────────────


def test_sync_acm_returns_plain_no_emission() -> None:
    """sync-only context manager → Plain; no warning/log emitted."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = detect_lifecycle(_SyncACMClass)
    assert result == ProviderLifecycle.Plain


# ── Non-standard close name → Plain ──────────────────────────────────


def test_close_async_not_detected() -> None:
    """close_async (non-standard) → Plain; only aclose is detected."""
    assert detect_lifecycle(_CloseAsyncNameClass) == ProviderLifecycle.Plain


# ── Purity: __init__ never called ───────────────────────────────────────────


def test_no_instantiation() -> None:
    """detect_lifecycle never calls __init__ — verified with a class that raises."""
    assert detect_lifecycle(_InitRaises) == ProviderLifecycle.Plain


# ── Import path ─────────────────────────────────────────────────────────────


def test_import_from_di_package() -> None:
    """detect_lifecycle is reachable via taskq._di.lifecycle import."""
    from taskq._di.lifecycle import detect_lifecycle as dl

    assert dl is detect_lifecycle


# ── Factory fixtures ─────────────────────────────────────────────────────────


async def _async_gen_factory() -> AsyncIterator[int]:
    yield 42


def _sync_gen_factory() -> Iterator[int]:
    yield 42


async def _async_fn_factory() -> int:
    return 42


def _sync_fn_factory() -> int:
    return 42


# ── Async generator factory detection ──────────────────────────────────


def test_async_gen_factory_detection() -> None:
    """detect_factory_lifecycle returns AsyncGenerator for async gen."""
    assert detect_factory_lifecycle(_async_gen_factory) == ProviderLifecycle.AsyncGenerator


# ── Sync generator factory detection, no emission ──────────────────────


def test_sync_gen_factory_detection_no_emission() -> None:
    """detect_factory_lifecycle returns SyncGenerator; no warning/log emitted."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = detect_factory_lifecycle(_sync_gen_factory)
    assert result == ProviderLifecycle.SyncGenerator


# ── Async function and sync callable factories → PlainFactory ─────────


def test_plain_factory_detection() -> None:
    """async fn (no yield) and sync callable both return PlainFactory."""
    assert detect_factory_lifecycle(_async_fn_factory) == ProviderLifecycle.PlainFactory
    assert detect_factory_lifecycle(_sync_fn_factory) == ProviderLifecycle.PlainFactory


# ── Factory import path ───────────────────────────────────────────────────────


def test_factory_import_from_lifecycle_module() -> None:
    """detect_factory_lifecycle is reachable via taskq._di.lifecycle import."""
    from taskq._di.lifecycle import detect_factory_lifecycle as dfl

    assert dfl is detect_factory_lifecycle
