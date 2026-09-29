"""The sweep-statement render cache's byte-equality and validation pins.

The hot-path audit's one behavior-preserving fix: the sweep statements'
SQL renders are memoised per (template, schema) instead of re-running
``str.format`` per call (``sweep_scheduled_to_pending`` re-rendered a
~20 KB template on every leader scheduled-wake tick, byte-identical every
time). These pins hold the two contracts the change owes:

* BYTE-EQUALITY: every cached render is identical to the direct
  ``template.format(schema=...)`` it replaced, per schema.
* VALIDATION: the identifier check still runs before any interpolation -
  an invalid schema raises the same ValueError on every call (the cache
  never serves an unvalidated render), so no caller-supplied text can
  reach ``str.format`` unvalidated.
"""

import pytest

from taskq.backend._sql import INSERT_EVENTS_DETAIL_BATCH_SQL
from taskq.backend._sweeps import (
    _SWEEP_1_ATTEMPTS_BATCH_SQL,
    _SWEEP_1_SQL,
    _SWEEP_2_ATTEMPTS_BATCH_SQL,
    _SWEEP_2_SQL,
    _SWEEP_3_SQL,
    _SWEEP_4_SQL,
    _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    _SWEEP_IDLE_KEYED_SLOTS_SQL,
    _SWEEP_RESULT_TTL_SQL,
    _render_sweep_sql,
)

#: Every template whose per-call render the fix converted, exactly as the
#: call sites render them.
CONVERTED_TEMPLATES = [
    _SWEEP_1_SQL,
    _SWEEP_1_ATTEMPTS_BATCH_SQL,
    _SWEEP_2_SQL,
    _SWEEP_2_ATTEMPTS_BATCH_SQL,
    _SWEEP_3_SQL,
    _SWEEP_4_SQL,
    _SWEEP_RESULT_TTL_SQL,
    _SWEEP_IDLE_KEYED_BUCKETS_SQL,
    _SWEEP_IDLE_KEYED_SLOTS_SQL,
    INSERT_EVENTS_DETAIL_BATCH_SQL,
]

SCHEMAS = ["taskq", "tq_bench", "s", "worker_schema_12"]


@pytest.mark.parametrize("template", CONVERTED_TEMPLATES)
@pytest.mark.parametrize("schema", SCHEMAS)
def test_cached_render_is_byte_identical(template: str, schema: str) -> None:
    assert _render_sweep_sql(template, schema) == template.format(schema=schema)


def test_cached_render_is_memoised_same_object() -> None:
    """Second call returns the SAME str object: the render ran once."""
    first = _render_sweep_sql(_SWEEP_1_SQL, "tq_memoise_pin")
    second = _render_sweep_sql(_SWEEP_1_SQL, "tq_memoise_pin")
    assert first is second


@pytest.mark.parametrize(
    "bad",
    ['bad"; DROP SCHEMA taskq', "", "has space", "up/per", "quote'schema"],
)
def test_invalid_schema_still_raises_per_call(bad: str) -> None:
    """The cache never masks validation: every call with an invalid
    identifier raises the exact pre-fix ValueError, uninterpolated."""
    with pytest.raises(ValueError, match=r"invalid schema identifier"):
        _render_sweep_sql(_SWEEP_1_SQL, bad)
    with pytest.raises(ValueError, match=r"invalid schema identifier"):
        _render_sweep_sql(_SWEEP_1_SQL, bad)
