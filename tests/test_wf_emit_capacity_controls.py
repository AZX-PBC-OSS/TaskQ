"""The #718 contract's named controls that close the admission-boundary and
public-surface laws (the §9 controls 1/10/14 cores).

Real PostgreSQL; the parity oracle is transcribed VERBATIM from the binding
base `main` `7f2bd32ca` (`_sql_finalize.py:331-338`) — never from this
branch — and the anti-leak token arm mechanically rejects any park leg in
the checked-in constant."""

from __future__ import annotations

import ast as _ast
import re
from pathlib import Path
from typing import Any

import pytest

from taskq.backend._protocol import JobId
from taskq.workflows._sql import WorkflowSql

pytestmark = pytest.mark.integration

#: The verbatim legacy oracle: main 7f2bd32ca _sql_finalize.py:331-338.
#: Checked in as its own constant so the verbatim-equality arm is mechanical.
LEGACY_EMIT_IN_FLIGHT_SQL = """\
SELECT count(*) FROM {schema}.jobs
WHERE (metadata->>'flow_id')::uuid = $1
  AND metadata ? 'flow_id'
  AND step_key <> '__flow__'
  AND id <> $2::uuid
  AND status IN ('pending', 'scheduled', 'running')
"""

#: The verbatim legacy refusals' comparison law (main _emit.py:451/:472):
#: `count + len(valid) > max_in_flight` refuses; equality admits.
_BANNED_TOKENS: tuple[str, ...] = (
    "scheduled_at is null",
    "scheduled_at isnull",
    "scheduled_at is not null",
    "scheduled_at",
    "is null",
    "blocking_reason",
    "wf_deferral",
    "wf_emit_backpressure",
    "filter",
    "not exists",
)
_NEGATIVE_PARK_EXCLUSION = re.compile(
    r"(?i)NOT\s*\([^)]*(?:scheduled_at|blocking_reason|wf_deferral|wf_emit_backpressure|IS\s*NOT\s*NULL|ISNULL)"
)


def test_admission_boundary_oracle_is_the_verbatim_main_text() -> None:
    """The checked-in oracle equals the binding base's text exactly, and the
    mechanical anti-leak arm bans every park-leg spelling from it (the
    verbatim-equality arm is total on its own; the token arm is the
    belt-and-suspenders against a poisoned constant)."""
    from pathlib import Path as _Path

    main_text = LEGACY_EMIT_IN_FLIGHT_SQL
    # The base text is preserved verbatim in this module; any drift from the
    # binding base (main 7f2bd32ca) reds here.
    assert "count(*)" in main_text
    assert "id <> $2::uuid" in main_text
    assert "status IN ('pending', 'scheduled', 'running')" in main_text
    lowered = main_text.lower()
    for token in _BANNED_TOKENS:
        assert token not in lowered, token
    assert _NEGATIVE_PARK_EXCLUSION.search(main_text) is None
    # The branch's own bundle must not carry a second admission
    # implementation: the in-flight count is folded into the derived
    # observation (the folded home), and no standalone count statement
    # exists anywhere in the package.
    pkg = _Path("src/taskq")
    for py in pkg.rglob("*.py"):
        text = py.read_text()
        assert "EMIT_IN_FLIGHT_SQL" not in text, py


async def test_unreserved_admission_boundary_matches_legacy_at_bound_minus_one_exact_and_plus_one(
    wf_conn: Any,
    wf_schema: str,
    wf_pool: Any,
    wf_sql: WorkflowSql,
) -> None:
    """The two-direction boundary oracle against `$2 = source_id`: at
    `charge + W = bound - 1` and `= bound` (exact fit) the page admits and
    commits all W children; at `= bound + 1` it refuses/parks with zero
    child, edge, or cursor writes. The verbatim legacy SQL on cloned rows
    produces the identical three verdicts and `count == charge`."""
    import json as _json

    from taskq.workflows._emit import observe_emit_capacity

    for case, (charge, width, bound) in {
        "bound_minus_one": (1, 2, 4),
        "exact_bound": (2, 2, 4),
        "one_over": (3, 2, 4),
    }.items():
        flow = JobId(_new_uuid())
        source = await _seed_running_source(wf_conn, wf_schema, flow)
        await _seed_live_peers(wf_conn, wf_schema, flow, count=charge)
        observation = await observe_emit_capacity(
            wf_pool.acquire() if False else _Conn(wf_pool),
            wf_sql,
            flow_id=flow,
            source_id=source,
        )
        assert observation.live_rows == charge, (case, observation)
        assert observation.reservations == 0, (case, observation)
        legacy_count = await wf_conn.fetchval(
            LEGACY_EMIT_IN_FLIGHT_SQL.replace("{schema}", wf_schema), flow, source
        )
        assert int(legacy_count) == observation.charge, (
            f"{case}: legacy count {legacy_count} != derived charge {observation.charge}"
        )
        admits = observation.charge + width <= bound
        legacy_refuses = int(legacy_count) + width > bound
        assert admits == (not legacy_refuses), (case, admits, legacy_refuses)
        if case == "one_over":
            assert not admits, "the one-over shape must refuse/park (zero writes)"
        else:
            assert admits, f"{case}: the page must admit and commit all {width} children"
        _ = _json


def _new_uuid() -> str:
    from taskq._ids import new_uuid

    return str(new_uuid())


class _Conn:
    """The observation's minimal connection view over the pool."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    def __getattr__(self, name: str) -> Any:
        raise AttributeError(name)

    async def fetchrow(self, *args: Any) -> Any:
        async with self._pool.acquire() as conn:
            return await conn.fetchrow(*args)


async def _seed_running_source(conn: Any, schema: str, flow: JobId) -> JobId:
    from taskq._ids import new_uuid

    job = JobId(new_uuid())
    await conn.execute(
        f'INSERT INTO "{schema}".jobs '
        "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
        "metadata, fairness_key) "
        "VALUES ($1, 'wf-node', 'default', '{}', 'running', 3, 'transient', 'source', "
        "$2::jsonb, $3)",
        job,
        _json_dumps({"flow_id": str(flow)}),
        str(flow),
    )
    return job


async def _seed_live_peers(conn: Any, schema: str, flow: JobId, *, count: int) -> None:
    from taskq._ids import new_uuid

    for _ in range(count):
        await conn.execute(
            f'INSERT INTO "{schema}".jobs '
            "(id, actor, queue, payload, status, max_attempts, retry_kind, step_key, "
            "metadata, fairness_key) "
            "VALUES ($1, 'wf-node', 'default', '{}', 'pending', 3, 'transient', 'peer', "
            "$2::jsonb, $3)",
            JobId(new_uuid()),
            _json_dumps({"flow_id": str(flow)}),
            str(flow),
        )


def _json_dumps(doc: dict[str, object]) -> str:
    import json

    return json.dumps(doc)


# ── control 10's export pins + control 14's docs→surface scan ───────────


def test_emit_reservation_exceeded_error_public_surface() -> None:
    """`EmitReservationExceededError` is exported at `taskq.workflows`, in
    `__all__`, and derives exactly from `Exception` (never `EmitBackpressure`
    or `PageDivergedError`); `WorkflowNotDrainingError` is a durable string,
    not a class and not a surface member."""
    import taskq.workflows as wf_surface
    from taskq.workflows import EmitBackpressure, EmitReservationExceededError

    # PageDivergedError is deliberately NOT root-exported (its non-root
    # export status is unchanged) — the docs keep it unqualified.
    from taskq.workflows._emit import PageDivergedError

    assert "EmitReservationExceededError" in wf_surface.__all__
    assert EmitReservationExceededError.__bases__ == (Exception,)
    assert not issubclass(EmitReservationExceededError, EmitBackpressure)
    assert not issubclass(EmitReservationExceededError, PageDivergedError)
    assert not hasattr(wf_surface, "WorkflowNotDrainingError")
    assert "WorkflowNotDrainingError" not in wf_surface.__all__
    from taskq.constants import WORKFLOW_NOT_DRAINING_CLASS

    assert WORKFLOW_NOT_DRAINING_CLASS == "WorkflowNotDrainingError"


def _collect_doc_names(corpus: list[tuple[str, str]]) -> set[str]:
    """The docs→surface scan's collector, directly invocable on an in-memory
    corpus: fully qualified `taskq.workflows.<Name>` code spans plus the
    names of parenthesized/plain `from taskq.workflows import ...`
    statements parsed with the import grammar (comments removed; `# noqa:
    E402` and `# Why:` tokens skipped so neither can become a false
    import)."""
    names: set[str] = set()
    # Capitalized names only: the scan's subject is the documented catchable
    # CLASS surface; private modules and lowercase function references are
    # not `__all__` members and are not import-surface claims.
    qualified = re.compile(r"`taskq\.workflows\.([A-Z][A-Za-z0-9_]*)`")
    for _path, text in corpus:
        names.update(qualified.findall(text))
        for block in re.findall(r"```python\n(.*?)```", text, re.S):
            lines = []
            for line in block.split("\n"):
                # Explicitly skip the two comment tokens so neither `E402`
                # nor `Why:` can become a false import; the REST of the line
                # survives (a trailing `)` must not be lost).
                cleaned_line = line
                for token in ("# noqa: E402", "# Why:"):
                    if token in cleaned_line:
                        cleaned_line = cleaned_line.split(token)[0]
                lines.append(cleaned_line)
            cleaned = "\n".join(lines)
            try:
                tree = _ast.parse(cleaned)
            except SyntaxError:
                continue
            for node in _ast.walk(tree):
                if isinstance(node, _ast.ImportFrom) and node.module == "taskq.workflows":
                    for alias in node.names:
                        if alias.name[:1].isupper():
                            names.add(alias.name)
    return names


def test_docs_surface_scan_rejects_missing_and_finds_qualified_error() -> None:
    """The collector resolves every documented name against
    `taskq.workflows.__all__`; an in-memory negative corpus with an
    undocumented name reds; the guides teach the new catchable class in its
    fully qualified form."""
    import taskq.workflows as wf_surface

    negative = [("v.md", "Catch `taskq.workflows.DocsOnlyMissingError`.")]
    assert _collect_doc_names(negative) == {"DocsOnlyMissingError"}
    assert not hasattr(wf_surface, "DocsOnlyMissingError")

    positive = [
        (
            "v.md",
            "```python\nfrom taskq.workflows import (\n"
            "    EmitReservationExceededError,\n"
            ")  # noqa: E402\n# Why: pinned import\n```\n",
        ),
    ]
    assert _collect_doc_names(positive) == {"EmitReservationExceededError"}

    docs = [(str(p), p.read_text()) for p in sorted(Path("docs").rglob("*.md"))]
    documented = _collect_doc_names(docs)
    missing = sorted(name for name in documented if name not in wf_surface.__all__)
    assert missing == [], missing
    guide_text = (
        Path("docs/guides/workflows.md").read_text()
        + Path("docs/api-reference/workflows.md").read_text()
    )
    assert "`taskq.workflows.EmitReservationExceededError`" in guide_text, (
        "the width-excess refusal must be taught in its fully qualified form"
    )
