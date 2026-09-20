"""The server-side enqueue paths fire on the actor's declared retry curve.

cron fires and the admin run-now build their EnqueueArgs from the stored
actor_config row. Before migration 01.00.18 the row carried only
max_attempts and retry_kind, so the curve scalars took the EnqueueArgs
dataclass defaults on every server-side fire: an actor declaring
base=600s/fixed/jitter=0.0 was fired by cron with base=5s/exponential/
jitter=0.2 (measured: cron re-pend delay 5.2s against the declared
600.0s). These pins hold the curve's journey from the @actor literal
through the sync's first-create seeding to both fire paths' rows, plus
the NULL back-compat (rows predating the migration keep the enqueue
defaults).
"""

from dataclasses import fields as dc_fields
from datetime import timedelta
from typing import Any

from taskq.backend._protocol import EnqueueArgs
from taskq.retry import RetryPolicy
from taskq.worker.cron_loop import _ActorConfig, _fire_default_curve, _RetryCurve

_DECLARED = RetryPolicy(
    kind="transient",
    max_attempts=5,
    base=timedelta(seconds=600),
    cap=timedelta(seconds=7200),
    backoff="fixed",
    jitter=0.0,
)


def _actor_config(retry: RetryPolicy | None) -> _ActorConfig:
    return _ActorConfig(
        queue="default",
        max_attempts=5,
        retry_kind="transient",
        max_pending=None,
        retry_base=retry.base if retry else None,
        retry_cap=retry.cap if retry else None,
        retry_backoff=retry.backoff if retry else None,
        retry_jitter=retry.jitter if retry else None,
    )


def _resolve(ac: _ActorConfig) -> dict[str, Any]:
    """The fire paths' curve resolution (the same shape cron_loop and
    ops.py apply to the stored row).  ``_fire_default_curve`` returns the
    typed ``_RetryCurve`` carrier, so the fallback reads its attributes."""
    d = _fire_default_curve()
    return {
        "retry_base": ac.retry_base or d.base,
        "retry_cap": ac.retry_cap or d.cap,
        "retry_backoff": ac.retry_backoff or d.backoff,
        "retry_jitter": (ac.retry_jitter if ac.retry_jitter is not None else d.jitter),
    }


def _enqueue_defaults() -> dict[str, Any]:
    return {f.name: f.default for f in dc_fields(EnqueueArgs)}


def test_sync_seeds_the_declared_curve_on_first_create() -> None:
    """The ActorConfig carrier carries the ref's curve to the upsert."""
    cfg = _actor_config(_DECLARED)
    assert cfg.retry_base == timedelta(seconds=600)
    assert cfg.retry_cap == timedelta(seconds=7200)
    assert cfg.retry_backoff == "fixed"
    assert cfg.retry_jitter == 0.0


def test_cron_fire_args_carry_the_declared_curve() -> None:
    """The cron tick's EnqueueArgs read the curve from the stored row."""
    args_curve = _resolve(_actor_config(_DECLARED))
    assert args_curve["retry_base"] == timedelta(seconds=600)
    assert args_curve["retry_cap"] == timedelta(seconds=7200)
    assert args_curve["retry_backoff"] == "fixed"
    assert args_curve["retry_jitter"] == 0.0


def test_cron_fire_null_curve_keeps_the_enqueue_defaults() -> None:
    """Rows predating migration 01.00.18 (NULL columns) keep the
    EnqueueArgs defaults: the back-compat contract."""
    args_curve = _resolve(_actor_config(None))
    defaults = _enqueue_defaults()
    assert args_curve["retry_base"] == defaults["retry_base"]
    assert args_curve["retry_cap"] == defaults["retry_cap"]
    assert args_curve["retry_backoff"] == defaults["retry_backoff"]
    assert args_curve["retry_jitter"] == defaults["retry_jitter"]


def test_the_null_curve_fallback_matches_enqueue_defaults_exactly() -> None:
    """The NULL-curve constants in cron_loop must equal EnqueueArgs's own
    defaults: a default change there fails here instead of drifting the
    server-side fire paths silently.  The fallback arrives as the typed
    ``_RetryCurve`` carrier, so the comparison is carrier-to-carrier and
    the drift check walks its attributes against EnqueueArgs's fields."""
    from taskq.worker.cron_loop import (
        _DEFAULT_RETRY_BACKOFF,
        _DEFAULT_RETRY_BASE,
        _DEFAULT_RETRY_CAP,
        _DEFAULT_RETRY_JITTER,
    )

    fallback = _fire_default_curve()
    defaults = _enqueue_defaults()
    assert fallback == _RetryCurve(
        base=_DEFAULT_RETRY_BASE,
        cap=_DEFAULT_RETRY_CAP,
        backoff=_DEFAULT_RETRY_BACKOFF,
        jitter=_DEFAULT_RETRY_JITTER,
    )
    for attr, key in (
        ("base", "retry_base"),
        ("cap", "retry_cap"),
        ("backoff", "retry_backoff"),
        ("jitter", "retry_jitter"),
    ):
        value = getattr(fallback, attr)
        assert value == defaults[key], (
            f"the NULL-curve fallback for {attr} drifted from EnqueueArgs's "
            f"default: {value!r} != {defaults[key]!r}"
        )
