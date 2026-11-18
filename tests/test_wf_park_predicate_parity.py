# ruff: noqa: S608  # Why: every query's schema identifier comes from the fixture-validated schema name and every value is $-bound; only the f-string interpolation of the schema is flagged.
"""The exact-park predicate's literal parity controls (RULING_718_DIFF's
M-6 as superseded by correction layer 1's C4).

The truthful boundary: Python surfaces render the shared leaf (one ordered
seven-leg list in ``backend/_sql_fragments.py``, exactly two projections —
the complete ``P(a)`` and the marker core ``C(a)``); the released catalog
SQL (the frozen ``01.00.42_01`` migration, not an edit target) necessarily
inlines its own forms; THIS control owns that boundary with written-literal
expected spans, canonicalised for whitespace and the doubled-brace
encoding only — never read back from the migration and never generated
from the same span on both sides. THE ONE-DESIGN NOTE (the correction
layer's observation 2, carried here): the written-literal anchors bind the
renderer's projections AND the migration/Python spans to the SAME
literals — one design, not two — so the migration comparisons run through
the renderer (a shared-leg mutation reds every migration comparison) while
the renderer itself is anchored to the written literals by the first test.

Bindings (C4 items 2-4):
- complete ``P`` → the migration ``fence_not_draining_parks`` body's
  ``parks`` predicate (the written flow-filter literal + ``P("j")``) and
  the ``jobs_wf_emit_park_idx`` partial index's complete unqualified form;
- marker core → the ``wf_emit_park_guard`` trigger body's ``IF … THEN``
  (legs 2-7), both ``_sql_runner.py`` exclusions (unqualified,
  single-brace), and ``_sweeps.py``'s ``sweep_scheduled_to_pending``
  (unqualified, doubled-brace);
- the trigger's full written ``CREATE TRIGGER … WHEN (OLD.status =
  'scheduled' AND NEW.status = 'pending')`` clause is pinned, and the
  renderer's first leg equals the WHEN's first conjunct under alias
  removal;
- the complete output's status leg is FORBIDDEN in the three Python
  ``NOT (…)`` exclusions: the real-PostgreSQL negative control places
  every exact marker leg on a `pending` node and proves it unclaimable
  through both doors (the claimable read and the claim CAS), with the
  opposite-direction complete-``P`` mutant proving the control's teeth —
  the wrong projection makes the marker-carrying `pending` row claimable.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import asyncpg
import pytest

from taskq.testing.fixtures import ModulePgSchema

pytestmark = pytest.mark.integration

_MIGRATION = Path("src/taskq/migrations") / "01.00.42_01_pre_wf_step_ledger_claim_epoch.sql"

# ── The WRITTEN expected spans (canonicalised whitespace + doubled braces
#    only). These are the control's own literals, never read back from the
#    migration and never generated from the renderer on both sides. ──────

_EXPECTED_COMPLETE_J = (
    "j.status = 'scheduled' "
    'AND j.metadata @> \'{"blocking_reason":"backpressure"}\'::jsonb '
    "AND j.metadata @> "
    '\'{"wf_deferral":{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}\'::jsonb '
    'AND j.metadata @> \'{"wf_emit_backpressure":{"version":1}}\'::jsonb '
    "AND j.step_key IS NOT NULL "
    "AND j.step_key <> '__flow__' "
    "AND j.metadata ? 'flow_id'"
)

_EXPECTED_COMPLETE_UNQUALIFIED = (
    "status = 'scheduled' "
    'AND metadata @> \'{"blocking_reason":"backpressure"}\'::jsonb '
    "AND metadata @> "
    '\'{"wf_deferral":{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}\'::jsonb '
    'AND metadata @> \'{"wf_emit_backpressure":{"version":1}}\'::jsonb '
    "AND step_key IS NOT NULL "
    "AND step_key <> '__flow__' "
    "AND metadata ? 'flow_id'"
)

_EXPECTED_CORE = (
    'metadata @> \'{"blocking_reason":"backpressure"}\'::jsonb '
    "AND metadata @> "
    '\'{"wf_deferral":{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}\'::jsonb '
    'AND metadata @> \'{"wf_emit_backpressure":{"version":1}}\'::jsonb '
    "AND step_key IS NOT NULL "
    "AND step_key <> '__flow__' "
    "AND metadata ? 'flow_id'"
)

_EXPECTED_CORE_OLD = (
    'OLD.metadata @> \'{"blocking_reason":"backpressure"}\'::jsonb '
    "AND OLD.metadata @> "
    '\'{"wf_deferral":{"signal":"EmitBackpressure","consume_budget":false,"attempt":0}}\'::jsonb '
    'AND OLD.metadata @> \'{"wf_emit_backpressure":{"version":1}}\'::jsonb '
    "AND OLD.step_key IS NOT NULL "
    "AND OLD.step_key <> '__flow__' "
    "AND OLD.metadata ? 'flow_id'"
)

#: The fence body's own written flow-filter literal (C4 item 2's binding:
#: the parks predicate is THIS literal followed by the complete P("j")).
_EXPECTED_FLOW_FILTER = "(j.metadata->>'flow_id')::uuid = p_root"

#: The trigger object's full WHEN clause (C4 item 4's written literal).
_EXPECTED_TRIGGER_WHEN = "WHEN (OLD.status = 'scheduled' AND NEW.status = 'pending')"


def _undouble(text: str) -> str:
    """The doubled-brace encoding's exact inverse (a sequential pairwise
    scan: the doubled encoding of `{` is `{{`, so in a doubled text every
    true brace IS doubled — including the nested jsonb closes whose true
    text itself shows `}}`)."""
    out: list[str] = []
    i = 0
    while i < len(text):
        if text[i : i + 2] == "{{":
            out.append("{")
            i += 2
        elif text[i : i + 2] == "}}":
            out.append("}")
            i += 2
        else:
            out.append(text[i])
            i += 1
    return "".join(out)


def _canon(text: str) -> str:
    """Whitespace canonicalisation ONLY (for single-brace true text: the
    written expected literals and the rendered Python statements)."""
    return " ".join(text.split())


def _canon_doubled(text: str) -> str:
    """Whitespace + doubled-brace canonicalisation (for the migration's
    doubled-brace catalog inlines): the encoding is un-doubled first, then
    whitespace collapses. A drift in legs, order, aliases, or terms still
    reds."""
    return _canon(_undouble(text))


def _migration_text() -> str:
    return _MIGRATION.read_text(encoding="utf-8")


def _one_span(text: str, pattern: str, what: str) -> str:
    matches = re.findall(pattern, text, re.S)
    assert len(matches) == 1, f"expected exactly one {what} span, got {len(matches)}"
    return matches[0]


def _extract_fence_parks_predicate(mig: str) -> str:
    """The ``fence_not_draining_parks`` body's parks WHERE legs."""
    return _one_span(
        mig,
        r"WITH parks AS \(\s*SELECT j\.id\s*FROM \"\{schema\}\"\.jobs AS j\s*"
        r"WHERE (.*?)\), fenced AS \(",
        "fence_not_draining_parks parks predicate",
    )


def _extract_partial_index_predicate(mig: str) -> str:
    return _one_span(
        mig,
        r"CREATE INDEX IF NOT EXISTS jobs_wf_emit_park_idx\s*"
        r'ON "\{schema\}"\.jobs \(\(\(metadata->>\'flow_id\'\)::uuid\), created_at, id\)\s*'
        r"WHERE (.*?);",
        "jobs_wf_emit_park_idx partial-index predicate",
    )


def _extract_trigger_if_predicate(mig: str) -> str:
    return _one_span(
        mig,
        r"AS \$fn\$\s*BEGIN\s*(?:\s*--[^\n]*\n)*\s*IF (.*?) THEN",
        "wf_emit_park_guard trigger IF predicate",
    )


def _extract_trigger_when_clause(mig: str) -> str:
    return _one_span(
        mig,
        r"(WHEN \(OLD\.status = 'scheduled' AND NEW\.status = 'pending'\))\s*"
        r"EXECUTE FUNCTION",
        "wf_emit_park_guard trigger WHEN clause",
    )


def _renderer_leg_one(alias: str) -> str:
    from taskq.backend._sql_fragments import _PARK_LEGS

    q = f"{alias}." if alias else ""
    return _PARK_LEGS[0].replace("{q}", q)


class TestParkPredicateParity:
    def test_renderer_matches_the_written_literals(self) -> None:
        """The shared renderer's two projections are exactly the control's
        written literals (a mutation of any shared leg reds every
        comparison in this module — the renderer is the Python truth)."""
        from taskq.backend._sql_fragments import (
            emit_park_marker_core,
            emit_park_predicate,
        )

        assert _canon(emit_park_predicate("j")) == _canon(_EXPECTED_COMPLETE_J)
        assert _canon(emit_park_predicate("")) == _canon(_EXPECTED_COMPLETE_UNQUALIFIED)
        assert _canon(emit_park_marker_core("")) == _canon(_EXPECTED_CORE)
        assert _canon(emit_park_marker_core("OLD")) == _canon(_EXPECTED_CORE_OLD)
        # The core is the complete output minus its first leg — a
        # projection of the same list, never a second hand-written list.
        assert emit_park_marker_core("j") == emit_park_predicate("j").split("\n  AND ", 1)[1]

    def test_migration_spells_are_the_renderer_forms(self) -> None:
        """The frozen migration's three catalog inlines are exactly the
        bound renderer forms (C4 items 2-3), compared as exact spans
        through the shared renderer — so a mutation of any shared leg
        reds every migration comparison (the written literals anchor the
        renderer itself in the first test; the migration is never the
        expected side's source)."""
        from taskq.backend._sql_fragments import (
            emit_park_marker_core,
            emit_park_predicate,
        )

        mig = _migration_text()
        parks = _extract_fence_parks_predicate(mig)
        assert _canon_doubled(parks) == _canon(
            f"{_EXPECTED_FLOW_FILTER} AND {emit_park_predicate('j')}"
        ), parks
        index_where = _extract_partial_index_predicate(mig)
        assert _canon_doubled(index_where) == _canon(emit_park_predicate("")), index_where
        trigger_if = _extract_trigger_if_predicate(mig)
        assert _canon_doubled(trigger_if) == _canon(emit_park_marker_core("OLD")), trigger_if

    def test_trigger_when_clause_and_status_leg_binding(self) -> None:
        """The full written ``CREATE TRIGGER … WHEN`` clause is pinned and
        the renderer's first leg equals the WHEN's first conjunct under
        alias removal (a status-leg mutation reds the trigger parity
        too); ``NEW.status = 'pending'`` remains part of the binding."""
        mig = _migration_text()
        when = _extract_trigger_when_clause(mig)
        assert _canon_doubled(when) == _canon(_EXPECTED_TRIGGER_WHEN), when
        assert _canon(_renderer_leg_one("OLD")) == _canon("OLD.status = 'scheduled'")
        assert _canon(_renderer_leg_one("j")) == _canon("j.status = 'scheduled'")
        assert "NEW.status = 'pending'" in when, when

    def test_python_exclusions_are_the_marker_core_projection(self) -> None:
        """The three Python NOT-(…) exclusions contain the renderer's own
        marker-core output under their declared brace conventions — and
        NOT the complete predicate's status leg (C4 item 5's forbid)."""
        from taskq.workflows.api._sql_runner import (
            CLAIMABLE_NODES_SQL_TEMPLATE,
            NODE_CLAIM_SQL_TEMPLATE,
            render_sql,
        )

        schema = "parity_schema_probe"
        for name, template in (
            ("NODE_CLAIM_SQL_TEMPLATE", NODE_CLAIM_SQL_TEMPLATE),
            ("CLAIMABLE_NODES_SQL_TEMPLATE", CLAIMABLE_NODES_SQL_TEMPLATE),
        ):
            rendered = render_sql(template, schema)
            span = _one_span(rendered, r"AND NOT \((.*?)\)\s*AND deps_pending", name)
            assert _canon(span) == _canon(_EXPECTED_CORE), (name, span)
            assert "status = 'scheduled'" not in span, (
                name,
                "the complete predicate's status leg is forbidden in a NOT(...) exclusion",
            )
        from taskq.backend import _sweeps

        rendered = _sweeps._render_sweep_sql(_sweeps._SWEEP_3_SQL, schema)
        span = _one_span(
            rendered, r"AND NOT \((.*?)\)\s*ORDER BY scheduled_at", "sweep_scheduled_to_pending"
        )
        assert _canon(span) == _canon(_EXPECTED_CORE), span
        assert "status = 'scheduled'" not in span, (
            "the complete predicate's status leg is forbidden in the sweep's exclusion"
        )


class TestMarkerCarryingPendingRowUnclaimable:
    """The negative real-PostgreSQL control (C4 item 5): a `pending` node
    carrying EVERY exact marker leg is unclaimable through BOTH direct
    doors, and the complete-`P` mutant (the forbidden injection) makes it
    claimable — the opposite-direction proof the exclusion's teeth are the
    marker core, not the trigger."""

    async def _seed_marker_pending_node(
        self, conn: asyncpg.Connection, schema: str
    ) -> tuple[str, str]:
        from taskq._ids import new_uuid

        flow = str(new_uuid())
        node = str(new_uuid())
        # A live root (the terminal-flow fence must pass) and the node.
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, max_attempts, '
            "retry_kind, status, step_key, metadata, fairness_key, idempotency_scope, "
            "idempotency_key) VALUES ($1, 'r17', 'default', '{}', 3, 'transient', "
            "'running', '__flow__', $2::jsonb, $3, 'workflow-run', $4)",
            flow,
            _json_dumps({"flow_id": flow}),
            flow,
            f"flow:{flow}",
        )
        marker_meta = {
            "flow_id": flow,
            "blocking_reason": "backpressure",
            "wf_deferral": {
                "signal": "EmitBackpressure",
                "consume_budget": False,
                "attempt": 0,
            },
            "wf_emit_backpressure": {
                "version": 1,
                "page_width": 2,
                "bound": 10,
                "outstanding": 10,
                "deferrals": 1,
                "wakes": 0,
                "deferred_at": "2026-11-18T09:30:00.123456+00:00",
            },
            "wf_source_logical_attempt": 1,
        }
        await conn.execute(
            f'INSERT INTO "{schema}".jobs (id, actor, queue, payload, status, attempt, '
            "max_attempts, retry_kind, step_key, map_index, metadata, scheduled_at, "
            "created_at, fairness_key, assignment_routed, idempotency_scope, "
            "idempotency_key) VALUES ($1, 'r17', 'default', '{}', 'pending', 0, 3, "
            f"'transient', 'source', NULL, $2::jsonb, clock_timestamp(), "
            "clock_timestamp(), $3, true, 'workflow-run', $4)",
            node,
            _json_dumps(marker_meta),
            flow,
            f"wf:{flow}:source:{node}",
        )
        return flow, node

    async def test_pending_marker_row_is_unclaimable_through_both_doors(
        self,
        module_pg_schema: ModulePgSchema,
        module_pg_pool: asyncpg.Pool,
    ) -> None:
        """Both doors refuse the marker-carrying pending row; the
        complete-P mutant (the forbidden projection) reds this by making
        the row claimable — the negative control's own teeth."""
        from taskq.backend._sql_fragments import emit_park_predicate
        from taskq.workflows.api._sql_runner import (
            CLAIMABLE_NODES_SQL_TEMPLATE,
            NODE_CLAIM_SQL_TEMPLATE,
            render_sql,
        )

        schema = module_pg_schema.schema_name
        from taskq._ids import new_uuid

        worker_id = str(new_uuid())
        async with module_pg_pool.acquire() as conn:
            flow, node = await self._seed_marker_pending_node(conn, schema)
            claimable = render_sql(CLAIMABLE_NODES_SQL_TEMPLATE, schema)
            cas = render_sql(NODE_CLAIM_SQL_TEMPLATE, schema)
            # DOOR 1: the claimable read returns nothing for this flow.
            rows = await conn.fetch(claimable, flow)
            assert list(rows) == [], [dict(r) for r in rows]
            # DOOR 2: the claim CAS moves nothing.
            cas_with_flow = cas.replace(
                "UPDATE {schema}".replace("{schema}", f'"{schema}"'), "UPDATE " + f'"{schema}"'
            )
            result = await conn.execute(cas, node, worker_id)
            assert result == "UPDATE 0", result
            del cas_with_flow
            # THE OPPOSITE DIRECTION (the complete-P mutant): injecting the
            # status leg into the exclusions makes the SAME row claimable
            # through both doors — the forbidden injection the parity
            # mechanism exists to prevent (and the migration's trigger
            # cannot rescue it: its WHEN covers only scheduled -> pending).
            complete_unqualified = emit_park_predicate("")
            mutant_claimable = claimable.replace(_core_span(claimable), complete_unqualified, 1)
            rows = await conn.fetch(mutant_claimable, flow)
            assert len(rows) == 1 and str(rows[0]["id"]) == node, [dict(r) for r in rows]
            mutant_cas = cas.replace(_core_span(cas), complete_unqualified, 1)
            result = await conn.execute(mutant_cas, node, worker_id)
            assert result == "UPDATE 1", result
            moved = await conn.fetchval(
                f'SELECT status::text FROM "{schema}".jobs WHERE id = $1::uuid', node
            )
            assert moved == "running", moved


def _json_dumps(obj: Any) -> str:
    import json

    return json.dumps(obj)


def _core_span(rendered: str) -> str:
    """The rendered exclusion's current core span (the mutant's splice
    point — the exact bytes the renderer produced into the template)."""
    from taskq.backend._sql_fragments import emit_park_marker_core

    core = emit_park_marker_core("")
    assert core in rendered, "the rendered statement no longer contains the shared marker core"
    return core
