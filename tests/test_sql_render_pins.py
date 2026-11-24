"""The shared SQL render's pins (``taskq._sql_render``).

Every statement family renders its named constant templates through one
substitution pass now — the workflows bundle, the loop and runner
families, the rootless sweep, the hitl holds, the cron tick and the
progress flush. The pins hold the contracts the unification owes:

* BYTE-EQUALITY: each family's delegate renders its template to the
  same SQL the direct substitution produced (the schema identifier
  validated, then the family's tokens, then — where the family does it
  — the doubled braces un-doubled).
* EQUAL INPUTS, EQUAL SQL: the same (template, schema) renders to the
  same SQL on every call — the memoised render's behavior contract
  (the render is keyed on the template string's value, so a module
  constant re-renders to itself and never to a stale sibling).
* VALIDATION: an invalid schema identifier raises the same ValueError
  on every call — the cache never serves an unvalidated render.
"""

import re

import pytest

from taskq._sql_render import render
from taskq.backend._dispatch_sql import (
    DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL,
    DISPATCH_CLAIMABLE_PROBE_SQL,
    DISPATCH_ROUND_ROBIN_SQL,
    DISPATCH_STRICT_FIFO_CURSOR_SQL,
    DISPATCH_STRICT_FIFO_SQL,
)
from taskq.progress._flush import _FLUSH_UPDATE_SQL, _flush_update_sql
from taskq.worker.cron_loop import (
    CRON_DUE_SQL_TEMPLATE,
    CRON_LOCK_SQL_TEMPLATE,
    cron_due_sql,
    cron_lock_sql,
)
from taskq.workflows._sql import TERMINAL_SQL_SET, WorkflowSql
from taskq.workflows._sql_ledger import LEDGER_TERMINAL_BY_ID_SQL
from taskq.workflows._sql_sweep import (
    EDGE_ROOTLESS_RETENTION_SQL,
    render_rootless_retention_sql,
)
from taskq.workflows.api._hitl import _HOLD_AWAITED_LEDGER_SQL
from taskq.workflows.api._hitl import _render as render_hold_sql
from taskq.workflows.api._sql_loop import LOOP_ADVANCE_SQL, render_loop_sql
from taskq.workflows.api._sql_runner import (
    CLAIMABLE_NODES_SQL_TEMPLATE,
    NODE_CLAIM_SQL_TEMPLATE,
)
from taskq.workflows.api._sql_runner import (
    render_sql as render_runner_sql,
)

SCHEMAS = ["taskq", "tq_render_pin", "s"]

#: The runner exclusion's rendered marker core, pinned as a WRITTEN
#: literal — never read back from the shared renderer. This is the exact
#: text ``_SQL_TOKEN_SUBS`` substitutes for ``{wf_emit_park_core}``: the
#: renderer's marker-core projection ``C("")`` (the complete predicate
#: minus its first status leg, unqualified), single-brace canonical text.
#: A leg, order, alias, or alias-removal mutation in
#: ``backend/_sql_fragments.py`` reds this pin — the delegation claim
#: (every runner spelling renders the shared leaf) is only as strong as
#: an expected value that cannot mutate with the thing it pins.
_WF_EMIT_PARK_CORE_RENDERED = (
    'metadata @> \'{"blocking_reason":"backpressure"}\'::jsonb\n'
    '  AND metadata @> \'{"wf_deferral":'
    '{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}\'::jsonb\n'
    '  AND metadata @> \'{"wf_emit_backpressure":{"version":1}}\'::jsonb\n'
    "  AND step_key IS NOT NULL\n"
    "  AND step_key <> '__flow__'\n"
    "  AND metadata ? 'flow_id'"
)


def test_loop_render_is_the_direct_substitution() -> None:
    for schema in SCHEMAS:
        expected = (
            LOOP_ADVANCE_SQL.replace("{schema}", schema)
            .replace("{terminal}", TERMINAL_SQL_SET)
            .replace("{{", "{")
            .replace("}}", "}")
        )
        assert render_loop_sql(LOOP_ADVANCE_SQL, schema) == expected


def test_runner_render_is_the_direct_substitution() -> None:
    """The runner family's direct substitution, BOTH templates: the
    validated schema, then the exclusion's marker core — the family's
    two substitutions, in the render's own order. The expected value is
    built from the written literal above (never the renderer), so the
    byte-equality claim pins the delegation to a text that cannot drift
    with the code it pins."""
    for template in (NODE_CLAIM_SQL_TEMPLATE, CLAIMABLE_NODES_SQL_TEMPLATE):
        for schema in SCHEMAS:
            expected = template.replace("{schema}", schema).replace(
                "{wf_emit_park_core}", _WF_EMIT_PARK_CORE_RENDERED
            )
            assert render_runner_sql(template, schema) == expected


def test_rootless_render_is_the_direct_substitution() -> None:
    for schema in SCHEMAS:
        expected = (
            EDGE_ROOTLESS_RETENTION_SQL.replace("{schema}", schema)
            .replace("{{", "{")
            .replace("}}", "}")
        )
        assert render_rootless_retention_sql(EDGE_ROOTLESS_RETENTION_SQL, schema) == expected


def test_hold_render_is_the_direct_substitution() -> None:
    for schema in SCHEMAS:
        expected = _HOLD_AWAITED_LEDGER_SQL.replace("{schema}", schema)
        assert render_hold_sql(_HOLD_AWAITED_LEDGER_SQL, schema) == expected


def test_cron_renders_are_the_direct_substitution() -> None:
    for schema in SCHEMAS:
        assert cron_lock_sql(schema) == CRON_LOCK_SQL_TEMPLATE.replace("{schema}", schema)
        assert cron_due_sql(schema) == CRON_DUE_SQL_TEMPLATE.replace("{schema}", schema)


def test_flush_render_is_the_direct_substitution() -> None:
    for schema in SCHEMAS:
        expected = _FLUSH_UPDATE_SQL.replace("{schema}", schema)
        assert _flush_update_sql(schema) == expected


def test_bundle_render_is_the_direct_substitution() -> None:
    for schema in SCHEMAS:
        expected = (
            LEDGER_TERMINAL_BY_ID_SQL.replace("{schema}", schema)
            .replace("{{", "{")
            .replace("}}", "}")
        )
        assert WorkflowSql.build(schema).ledger_terminal_by_id == expected


@pytest.mark.parametrize(
    "sql",
    [DISPATCH_STRICT_FIFO_SQL, DISPATCH_STRICT_FIFO_CURSOR_SQL, DISPATCH_ROUND_ROBIN_SQL],
    ids=["strict_fifo", "strict_fifo_cursor", "round_robin"],
)
def test_every_dispatch_fence_render_refuses_a_stamped_row(sql: str) -> None:
    """Issue #748: every rendered dispatch fence (the candidates and
    re-pended laterals, the lock steps, the terminal UPDATE's race guard)
    carries the hold-stamp leg, the in-process claim CAS's twin. The
    capability InitPlan line is rendered once per fence, so it counts them."""
    fences = sql.count("(SELECT wf_exec.capable FROM wf_exec_capable wf_exec)")
    legs = re.findall(r"AND NOT (\w+)\.metadata \? 'hold'", sql)
    assert fences > 0 and len(legs) == fences, (fences, legs)
    assert set(legs) == {"j", "j2"}  # the race guard (j) and the lateral/lock sites (j2)


@pytest.mark.parametrize(
    "sql",
    [DISPATCH_CLAIMABLE_PROBE_SQL, DISPATCH_CLAIMABLE_PROBE_CURSOR_SQL],
    ids=["probe", "probe_cursor"],
)
def test_every_claimable_probe_render_refuses_a_stamped_row(sql: str) -> None:
    """The probe fence agrees with the claim fence on held rows: a stamped
    row is never counted as routable work."""
    fences = sql.count("THE DISPATCH FENCE, P3 leg only")
    legs = re.findall(r"OR (\w+)\.metadata \? 'hold'", sql)
    assert fences > 0 and len(legs) == fences, (fences, legs)


def test_same_inputs_render_the_same_sql() -> None:
    """Equal (template, schema) → equal SQL, every call — the shared
    render's behavior contract (the memoised render never drifts from
    its own first answer for the same inputs). The comparison rides the
    PRODUCT renderer's exact substitutions (``{terminal}`` is the
    terminal-status set the loop statements spell) — the canonical
    renderer is ``render_loop_sql``; the shared render is its one
    mechanism, called here with the same substitution map."""
    subs: tuple[tuple[str, str], ...] = (("{terminal}", TERMINAL_SQL_SET),)
    first = render(LOOP_ADVANCE_SQL, "tq_render_pin", subs=subs, collapse_braces=True)
    second = render(LOOP_ADVANCE_SQL, "tq_render_pin", subs=subs, collapse_braces=True)
    assert first == second
    assert render_loop_sql(LOOP_ADVANCE_SQL, "tq_render_pin") == first


def test_different_schemas_render_their_own_sql() -> None:
    """A schema-carrying template renders each schema's own statement
    (the schema is the key's second half, never a global)."""
    left = render(LOOP_ADVANCE_SQL, "tq_render_a", collapse_braces=True)
    right = render(LOOP_ADVANCE_SQL, "tq_render_b", collapse_braces=True)
    assert left != right
    assert "tq_render_a" in left and "tq_render_b" not in left
    assert "tq_render_b" in right and "tq_render_a" not in right


def test_bundle_builds_equal_for_the_same_schema() -> None:
    """The hot path (the leader's sweep arms re-build per arm) renders
    the same bundle twice to the same statements."""
    assert WorkflowSql.build("tq_render_pin") == WorkflowSql.build("tq_render_pin")


@pytest.mark.parametrize(
    "bad",
    ['bad"; DROP SCHEMA taskq', "", "has space", "up/per", "quote'schema"],
)
def test_invalid_schema_still_raises_per_call(bad: str) -> None:
    """The cache never masks validation: every call with an invalid
    identifier raises the exact ValueError, uninterpolated."""
    with pytest.raises(ValueError, match=r"invalid schema identifier"):
        render(LOOP_ADVANCE_SQL, bad, collapse_braces=True)
    with pytest.raises(ValueError, match=r"invalid schema identifier"):
        render(LOOP_ADVANCE_SQL, bad, collapse_braces=True)
