"""Input-boundary attack pins for the settings env cascade.

Two defect classes at the ``WorkerSettings``/``TaskQSettings`` load
boundary, both fed by a malformed environment variable (the one input
every deployment partly controls, k8s ``value: ""`` included):

* An empty value for a non-``Optional`` non-``str`` field coerces to
  ``None`` and silently skips every constraint (``ge``/``le``) and
  validator hook -- dotenvmodel validates coerced values, and ``None``
  is the "no value" sentinel.  The ``None`` then crashes
  ``post_load``'s cross-field arithmetic with a raw ``TypeError``
  (``heartbeat_interval``), or, for fields ``post_load`` never touches
  (``result_max_bytes``), flows to runtime with the documented range
  defeated.  Both are boundary failures: junk at the door must raise
  the typed ``DotEnvModelError`` family, never a raw ``TypeError``,
  and never a silently-``None`` bounded field.

* A non-finite float (``nan``, ``inf``) defeats every comparison-built
  constraint: ``value < ge``, ``value <= gt`` and ``value > le`` are
  all ``False`` for ``nan``, so a field whose whole contract is a
  bounded duration loads ``nan`` -- and downstream every
  ``value < bound`` guard on it is equally transparent, the same hole
  one level down.  ``inf`` happens to trip ``post_load``'s lock_lease
  cascade check, but the error names ``lock_lease``, not the field the
  operator actually mistyped.  Every bounded float must be finite.

These pins sit beside ``tests/test_settings_hardening.py`` (the
worker-identity boundary) and ``tests/test_nul_scan_scaling.py`` (the
payload-content boundary): the same doctrine, junk in, typed error out,
executed proof only.
"""

import math
from types import UnionType
from typing import Union, get_args, get_origin

import pytest
from dotenvmodel import DotEnvModelError

from taskq.settings import TaskQSettings, WorkerSettings

_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"


def _load(overrides: dict[str, str]) -> WorkerSettings:
    """Load WorkerSettings from a dict with the DSN defaulted.

    ``load_from_dict`` expects keys *with* the ``TASKQ_`` prefix and runs
    the same coercion, constraint, validator, and ``post_load`` pipeline
    as ``load()``.
    """
    base: dict[str, str] = {"TASKQ_PG_DSN": _DSN}
    base.update(overrides)
    return WorkerSettings.load_from_dict(base)


def _env_var(config_cls: type, name: str) -> str:
    """The environment variable a field resolves from (prefix + upper)."""
    _, finfo = config_cls._fields[name]
    alias = getattr(finfo, "alias", None)
    return alias if alias else f"{config_cls.env_prefix}{name.upper()}"


def _declared_non_optional(t: object) -> bool:
    """True when *t* does not admit ``None`` (``T``, not ``T | None``)."""
    origin = get_origin(t)
    if origin is Union or origin is UnionType:
        return type(None) not in get_args(t)
    return True


# ── Red: an empty env value must not bypass validation ─────────────────


@pytest.mark.parametrize(
    ("config_cls", "field_name"),
    [
        # Crashes post_load's cascade arithmetic with a raw TypeError today.
        (WorkerSettings, "heartbeat_interval"),
        (WorkerSettings, "heartbeat_command_timeout"),
        (WorkerSettings, "max_heartbeat_failures"),
        # Loads silently to None today: the documented 1 KiB-1 MiB range is
        # defeated and _encode_result's `len(data) > max_bytes` raises
        # TypeError on every dict result at the terminal write.
        (WorkerSettings, "result_max_bytes"),
        (WorkerSettings, "progress_data_max_bytes"),
        # Loads silently to None today: `batch_size // divisor` at the
        # sweep breaker's reduced tier.
        (WorkerSettings, "event_writer_reduced_batch_divisor"),
        # A timedelta field: post_load never touches it, the sweeps do.
        (WorkerSettings, "prune_retention_period"),
        (WorkerSettings, "event_retention_period"),
    ],
)
def test_empty_env_value_for_a_bounded_field_is_a_typed_rejection(
    config_cls: type, field_name: str
) -> None:
    """``TASKQ_X=""`` on a non-Optional bounded field fails the load.

    The failure must be the typed DotEnvModelError family (a returned or
    raised ValidationError), not the raw TypeError post_load's arithmetic
    raises on the None that the empty value coerced to, and not a silent
    load with the field None.
    """
    with pytest.raises(DotEnvModelError):
        _load({_env_var(config_cls, field_name): ""})


def test_defaults_load_with_every_non_optional_field_populated() -> None:
    """The default load leaves no non-Optional field None.

    The generic form of the boundary: whatever an individual crash's
    stack trace, the invariant is "a loaded settings object honours its
    own declarations" -- a non-Optional field is a value, not the None
    sentinel validation uses for 'was not supplied'.
    """
    settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN})
    offenders = [
        name
        for name, (ftype, _) in WorkerSettings._fields.items()
        if _declared_non_optional(ftype) and getattr(settings, name) is None
    ]
    assert offenders == []


# ── Red: nan/inf must not defeat the comparison-built bounds ───────────


@pytest.mark.parametrize(
    ("field_name", "junk"),
    [
        # nan passes ge=0.5 (nan < 0.5 is False) and loads today; the
        # heartbeat loop then schedules on nan.
        ("heartbeat_interval", "nan"),
        # nan passes ge=1024 and loads today; len(data) > nan is always
        # False, so the documented result-size cap can never fire.
        ("result_max_bytes", "nan"),
        # post_load's cascade arithmetic accepts nan (every comparison is
        # False) -- the lease invariant check is equally nan-transparent.
        ("heartbeat_interval", "-nan"),
        ("result_max_bytes", "inf"),
        ("heartbeat_interval", "inf"),
        # The unconstrained lock budgets: no ge/gt/le at all, so nan and
        # -5000 both load; the GUC convention makes the latter legal, but
        # nan is a crash at the set_config bind.
        ("max_pending_lock_timeout_ms", "nan"),
        ("unique_for_lock_timeout_ms", "nan"),
        ("idempotency_lock_timeout_ms", "nan"),
        ("token_bucket_lock_timeout_ms", "nan"),
        ("sliding_window_lock_timeout_ms", "nan"),
    ],
)
def test_non_finite_float_setting_is_a_typed_rejection(field_name: str, junk: str) -> None:
    """A non-finite float in a bounded-duration field fails the load."""
    with pytest.raises(DotEnvModelError):
        _load({_env_var(WorkerSettings, field_name): junk})


def test_non_finite_rejection_names_the_field() -> None:
    """The rejection names the mistyped field, not a bystander.

    Before the finite validators, ``inf`` for heartbeat_interval was
    rejected only by post_load's lock_lease cascade check -- a typed
    error that blamed ``lock_lease`` for an operator typo in
    ``TASKQ_HEARTBEAT_INTERVAL``.
    """
    with pytest.raises(DotEnvModelError) as exc_info:
        _load({_env_var(WorkerSettings, "heartbeat_interval"): "nan"})
    assert "heartbeat_interval" in str(exc_info.value)


def test_finite_validator_holds_under_validate_false() -> None:
    """The finite check is a validator hook, so it survives validate=False.

    The house precedent (_log_format_validator) chose the hook form
    precisely because built-in constraints are skipped under
    ``validate=False`` -- the nan hole would reopen for every caller
    that loads unvalidated (TaskQSettings.post_load's own note).
    """
    with pytest.raises(DotEnvModelError):
        WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": _DSN, "TASKQ_HEARTBEAT_INTERVAL": "nan"}, validate=False
        )


def test_empty_env_guard_holds_under_validate_false() -> None:
    """The empty-env guard lives in post_load, which runs even when
    ``validate=False`` skips the built-in constraints -- the None must be
    refused on the unvalidated path too, the path a load-and-inspect
    caller takes.
    """
    with pytest.raises(DotEnvModelError):
        WorkerSettings.load_from_dict(
            {"TASKQ_PG_DSN": _DSN, "TASKQ_RESULT_MAX_BYTES": ""}, validate=False
        )


@pytest.mark.parametrize("junk", ["nan", "inf"])
def test_taskq_settings_floats_are_finite_too(junk: str) -> None:
    """The base TaskQSettings carries the same boundary, not just the
    worker subclass."""
    with pytest.raises(DotEnvModelError):
        TaskQSettings.load_from_dict(
            {"TASKQ_PG_DSN": _DSN, "TASKQ_ADMIN_UI_POLLING_INTERVAL_SECONDS": junk}
        )


def test_zero_stays_legal_where_the_docs_say_zero() -> None:
    """The finite fix must not narrow a documented-zero field.

    ``cancellation_grace_period`` and friends document ``ge=0``: zero is
    a legal value (the fast-test fixtures rely on it), so only
    non-finite junk and negatives are refused there.
    """
    settings = _load(
        {
            _env_var(WorkerSettings, "cancellation_grace_period"): "0",
            _env_var(WorkerSettings, "cleanup_grace_period"): "0",
            _env_var(WorkerSettings, "reclaim_event_visibility_delay"): "0",
        }
    )
    assert settings.cancellation_grace_period == 0.0
    assert settings.cleanup_grace_period == 0.0
    assert settings.reclaim_event_visibility_delay == 0.0


def test_negative_stays_legal_for_the_lock_budget_guc_convention() -> None:
    """The lock budgets document '0 (or less) waits indefinitely' -- the
    finite fix must not take the documented negative form away."""
    settings = _load({_env_var(WorkerSettings, "max_pending_lock_timeout_ms"): "-1"})
    assert settings.max_pending_lock_timeout_ms == -1.0


def test_the_finite_pin_covers_every_bounded_float_field() -> None:
    """Every float field the settings declare carries the finite check.

    The per-field parametrized pins above can drift behind a newly added
    field; this one enumerates the registry and fails if any float field
    loads a non-finite value, so the class stays closed as the file
    grows.
    """
    for name, (ftype, _finfo) in WorkerSettings._fields.items():
        if ftype is not float:
            continue
        env_var = _env_var(WorkerSettings, name)
        try:
            settings = WorkerSettings.load_from_dict({"TASKQ_PG_DSN": _DSN, env_var: "nan"})
        except DotEnvModelError:
            continue
        value = getattr(settings, name)
        assert isinstance(value, float) and math.isfinite(value), (
            f"{env_var}=nan loaded as {value!r}: the field has no finite check"
        )
