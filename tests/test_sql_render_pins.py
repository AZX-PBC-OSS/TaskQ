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

import pytest

from taskq._sql_render import render
from taskq.progress._flush import _FLUSH_UPDATE_SQL, _flush_update_sql
from taskq.worker.cron_loop import (
    CRON_DUE_SQL_TEMPLATE,
    CRON_LOCK_SQL_TEMPLATE,
    cron_due_sql,
    cron_lock_sql,
)
from taskq.workflows._sql import TERMINAL_SQL_SET, WorkflowSql
from taskq.workflows._sql_ledger import LEDGER_TERMINAL_SQL
from taskq.workflows._sql_sweep import (
    EDGE_ROOTLESS_RETENTION_SQL,
    render_rootless_retention_sql,
)
from taskq.workflows.api._hitl import _HOLD_AWAITED_LEDGER_SQL
from taskq.workflows.api._hitl import _render as render_hold_sql
from taskq.workflows.api._sql_loop import LOOP_ADVANCE_SQL, render_loop_sql
from taskq.workflows.api._sql_runner import (
    NODE_CLAIM_SQL_TEMPLATE,
)
from taskq.workflows.api._sql_runner import (
    render_sql as render_runner_sql,
)

SCHEMAS = ["taskq", "tq_render_pin", "s"]


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
    for schema in SCHEMAS:
        expected = NODE_CLAIM_SQL_TEMPLATE.replace("{schema}", schema)
        assert render_runner_sql(NODE_CLAIM_SQL_TEMPLATE, schema) == expected


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
            LEDGER_TERMINAL_SQL.replace("{schema}", schema)
            .replace("{terminal}", TERMINAL_SQL_SET)
            .replace("{{", "{")
            .replace("}}", "}")
        )
        assert WorkflowSql.build(schema).ledger_terminal == expected


def test_same_inputs_render_the_same_sql() -> None:
    """Equal (template, schema) → equal SQL, every call — the shared
    render's behavior contract (the memoised render never drifts from
    its own first answer for the same inputs)."""
    first = render(LOOP_ADVANCE_SQL, "tq_render_pin", collapse_braces=True)
    second = render(LOOP_ADVANCE_SQL, "tq_render_pin", collapse_braces=True)
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
