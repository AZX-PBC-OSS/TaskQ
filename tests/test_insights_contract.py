# Why: schema is a fixture-derived test identifier, not user input; the
# rendered statements are $-bound and never interpolate caller data.
"""The scaling-operator SQL contract, pinned and versioned.

The ``fetch_*`` functions in :mod:`taskq.insights` are the IN-DATABASE
path a future dynamic worker-scaling operator builds its queries on, so
their surface is a versioned contract, not an implementation detail:
``INSIGHTS_CONTRACT_VERSION`` names it, and this file pins all three
layers of it.

* **The function set** — ``taskq.insights.__all__``'s fetch surface is
  exactly the seven functions the docs guide documents. A removal or a
  rename reds here.
* **The signatures** — every function's parameter names, kinds and
  defaults, because an operator calls them keyword-only from a pinned
  script. A renamed keyword, a changed default, a parameter demoted to
  positional reds here.
* **The return-row shapes** — every statement's column names AND Postgres
  types, prepared against a real migrated schema (the columns are what an
  operator's ``row["eta_seconds"]``-style read indexes into; the types are
  what its driver deserializes). A removed, renamed, retyped or reordered
  column reds here.

All three are STABLE promises: a change that breaks any of them is a
breaking change and must bump ``INSIGHTS_CONTRACT_VERSION`` — the version
test refuses to let the constant drift silently. Additive changes (a new
function, a new column appended at the END of a row) do not break a
positional ``row[i]``-free consumer, so they ship without a major bump;
the contract test still sees them, because it asserts the shapes EQUAL
this file's tables rather than merely contain them — an added column is
itself a deliberate contract amendment to record here.
"""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Iterator
from typing import Any

import asyncpg
import pytest

import taskq.insights as insights_mod  # pyright: ignore[reportPrivateUsage]  # Why: the contract renders the module's own SQL templates, never a restated copy.
from taskq.insights import (
    _build_cron_ledger_sql,
    _build_wait_sql,
)
from taskq.migrate import apply_pending
from taskq.testing._shared_containers import creator_labels, skip_test_without_docker

#: The contract version this file documents. Bump WITH
#: ``INSIGHTS_CONTRACT_VERSION`` in the same commit as the breaking change.
_CONTRACT_VERSION = 1

_PG_IMAGE = "postgres:18"

#: The function set: ``__all__``'s exact contents, order-insensitive (the
#: ordering is presentation; the set is the contract).
_FUNCTION_SET = {
    "INSIGHTS_CONTRACT_VERSION",
    "INSIGHTS_WINDOWS",
    "fetch_actor_backlog",
    "fetch_cron_ledger",
    "fetch_drain_estimates",
    "fetch_overprovisioning",
    "fetch_queue_imbalance",
    "fetch_wait_distribution",
    "fetch_worker_busy_ratio",
}

#: Per function: the positional parameters in order, then the keyword-only
#: parameters in order with their defaults (``_NO_DEFAULT`` = no default).
#: The ``conn`` parameter is positional-or-keyword in the source; the rest
#: are keyword-only.
_NO_DEFAULT: Any = inspect.Parameter.empty
_FUNCTION_CONTRACTS: dict[str, tuple[tuple[str, ...], dict[str, Any]]] = {
    "fetch_wait_distribution": (
        ("conn",),
        {"schema": _NO_DEFAULT, "window": _NO_DEFAULT, "per_actor": False},
    ),
    "fetch_queue_imbalance": (
        ("conn",),
        {"schema": _NO_DEFAULT, "worker_liveness_seconds": 30},
    ),
    "fetch_actor_backlog": (("conn",), {"schema": _NO_DEFAULT}),
    "fetch_overprovisioning": (
        ("conn",),
        {"schema": _NO_DEFAULT, "window": _NO_DEFAULT, "worker_liveness_seconds": 30},
    ),
    "fetch_worker_busy_ratio": (
        ("conn",),
        {"schema": _NO_DEFAULT, "window": _NO_DEFAULT, "worker_id": None},
    ),
    "fetch_drain_estimates": (("conn",), {"schema": _NO_DEFAULT, "window": _NO_DEFAULT}),
    "fetch_cron_ledger": (("conn",), {"schema": _NO_DEFAULT, "window": _NO_DEFAULT}),
}

#: The return-row shapes: per statement, the column names in order with
#: their Postgres type names as asyncpg reports them
#: (``prepare(...).get_attributes()``). Rendered from the module's own
#: templates against a migrated schema — the production text, never a copy.
_ROW_CONTRACTS: dict[str, list[tuple[str, str]]] = {
    "fetch_wait_distribution(per_queue)": [
        ("queue", "text"),
        ("segment", "text"),
        ("count", "int8"),
        ("p50_wait_s", "float8"),
        ("p95_wait_s", "float8"),
        ("max_wait_s", "float8"),
    ],
    "fetch_wait_distribution(per_actor)": [
        ("queue", "text"),
        ("actor", "text"),
        ("segment", "text"),
        ("count", "int8"),
        ("p50_wait_s", "float8"),
        ("p95_wait_s", "float8"),
        ("max_wait_s", "float8"),
    ],
    "fetch_queue_imbalance": [
        ("queue", "text"),
        ("depth", "int4"),
        ("oldest_due_at", "timestamptz"),
        ("oldest_due_age_s", "float8"),
        ("scheduled_depth", "int4"),
        ("wave_min_scheduled_at", "timestamptz"),
        ("wave_max_scheduled_at", "timestamptz"),
        ("live_workers", "int4"),
        ("actor_capacity", "int4"),
        ("effective_capacity", "int4"),
        ("utilization", "float8"),
    ],
    "fetch_actor_backlog": [
        ("actor", "text"),
        ("queue", "text"),
        ("backlog", "int4"),
        ("running", "int4"),
        ("max_concurrent", "int4"),
        ("saturation", "float8"),
        ("unservable_backlog", "int4"),
    ],
    "fetch_overprovisioning": [
        ("queue", "text"),
        ("live_workers", "int4"),
        ("depth", "int4"),
        ("terminalisations", "int4"),
        ("overprovisioned", "bool"),
    ],
    "fetch_worker_busy_ratio": [
        ("worker_id", "uuid"),
        ("hostname", "text"),
        ("pid", "int4"),
        ("queues", "text[]"),
        ("started_at", "timestamptz"),
        ("last_seen_at", "timestamptz"),
        ("busy_ms", "int8"),
        ("observed_ms", "float8"),
        ("busy_ratio", "float8"),
    ],
    "fetch_drain_estimates": [
        ("queue", "text"),
        ("depth", "int4"),
        ("terminalisations", "int4"),
        ("completions_per_second", "float8"),
        ("has_traffic", "bool"),
        ("eta_seconds", "float8"),
        ("scheduled_depth", "int4"),
        ("wave_min_scheduled_at", "timestamptz"),
        ("wave_max_scheduled_at", "timestamptz"),
    ],
    "fetch_cron_ledger": [
        ("schedule_id", "uuid"),
        ("actor", "text"),
        ("cron_expr", "text"),
        ("timezone", "text"),
        ("dst_strategy", "text"),
        ("enabled", "bool"),
        ("fires_window", "int4"),
        ("cleared_window", "int4"),
        ("fires_prior", "int4"),
        ("cleared_prior", "int4"),
        ("outstanding", "int4"),
        ("runaway_trending", "bool"),
    ],
}


def _render_statements(schema: str) -> dict[str, str]:
    """Render every contracted statement from the module's own templates."""
    m = insights_mod
    return {
        "fetch_wait_distribution(per_queue)": _build_wait_sql(schema, per_actor=False),
        "fetch_wait_distribution(per_actor)": _build_wait_sql(schema, per_actor=True),
        "fetch_queue_imbalance": m._QUEUE_IMBALANCE_SQL.format(  # pyright: ignore[reportPrivateUsage]
            schema=schema,
            _DUE_NOW=m._DUE_NOW,  # pyright: ignore[reportPrivateUsage]
        ),
        "fetch_actor_backlog": m._ACTOR_BACKLOG_SQL.format(schema=schema),  # pyright: ignore[reportPrivateUsage]
        "fetch_overprovisioning": m._QUEUE_OVERPROVISIONING_SQL.format(  # pyright: ignore[reportPrivateUsage]
            schema=schema,
            _TERMINAL_IN=m._TERMINAL_IN,  # pyright: ignore[reportPrivateUsage]
            _FINISHED_BOUND=m._FINISHED_BOUND,  # pyright: ignore[reportPrivateUsage]
            _DUE_NOW=m._DUE_NOW,  # pyright: ignore[reportPrivateUsage]
        ),
        "fetch_worker_busy_ratio": m._WORKER_BUSY_SQL.format(schema=schema),  # pyright: ignore[reportPrivateUsage]
        "fetch_drain_estimates": m._QUEUE_DRAIN_SQL.format(  # pyright: ignore[reportPrivateUsage]
            schema=schema,
            _TERMINAL_IN=m._TERMINAL_IN,  # pyright: ignore[reportPrivateUsage]
            _FINISHED_BOUND=m._FINISHED_BOUND,  # pyright: ignore[reportPrivateUsage]
            _DUE_NOW=m._DUE_NOW,  # pyright: ignore[reportPrivateUsage]
        ),
        "fetch_cron_ledger": _build_cron_ledger_sql(schema),
    }


# ── The static pins (no container needed) ───────────────────────────────


def test_contract_version_is_pinned() -> None:
    """The version constant and this file's documented version move
    together. An unsynced bump is exactly the silent drift the version
    exists to prevent."""
    assert insights_mod.INSIGHTS_CONTRACT_VERSION == _CONTRACT_VERSION, (
        "INSIGHTS_CONTRACT_VERSION changed: the SQL contract broke backwards "
        "compatibility for operator queries. Update "
        "tests/test_insights_contract.py's pins in the same commit and "
        "document the break in the docs guide's contract section."
    )


def test_the_function_set_is_pinned() -> None:
    assert set(insights_mod.__all__) == _FUNCTION_SET, (
        "taskq.insights's exported surface changed: the fetch_* set is the "
        "scaling operator's in-database contract. Additions are amendments "
        "(update _FUNCTION_SET and the docs guide); removals or renames are "
        "breaking (bump INSIGHTS_CONTRACT_VERSION)."
    )


def test_the_window_selector_set_is_pinned() -> None:
    assert set(insights_mod.INSIGHTS_WINDOWS) == {"1h", "6h", "24h", "7d"}


@pytest.mark.parametrize("function_name", sorted(_FUNCTION_CONTRACTS))
def test_signatures_are_pinned(function_name: str) -> None:
    positional, kwonly = _FUNCTION_CONTRACTS[function_name]
    sig = inspect.signature(getattr(insights_mod, function_name))
    params = list(sig.parameters.values())
    assert [p.name for p in params] == [*positional, *kwonly], (
        f"{function_name}'s parameter list changed — an operator calls these "
        "by name from pinned scripts"
    )
    for param in params[: len(positional)]:
        assert param.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD, function_name
    for param in params[len(positional) :]:
        assert param.kind is inspect.Parameter.KEYWORD_ONLY, function_name
    for name, default in kwonly.items():
        param = sig.parameters[name]
        assert param.default == default, (
            f"{function_name}'s {name} default changed (contract: {default!r})"
        )


# ── The return-row pins (a real migrated schema) ────────────────────────


@pytest.fixture(scope="module")
def plain_dsn() -> Iterator[str]:
    skip_test_without_docker()
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(
        image=_PG_IMAGE, username="taskq", password="taskq", dbname="taskq"
    ).with_kwargs(labels=creator_labels()) as container:
        yield container.get_connection_url().replace("postgresql+psycopg2://", "postgresql://")


@pytest.fixture(scope="module")
async def plain_env(plain_dsn: str) -> AsyncIterator[tuple[asyncpg.Connection, str]]:
    """A migrated schema, EMPTY: the pins prepare each statement, which
    resolves every column's name and type without any row data — the
    shapes are schema facts, not population facts."""
    conn = await asyncpg.connect(plain_dsn)
    schema = "insights_contract"
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        await apply_pending(conn, schema=schema)
        yield conn, schema
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


@pytest.mark.integration
async def test_return_row_columns_and_types_are_pinned(
    plain_env: tuple[asyncpg.Connection, str],
) -> None:
    conn, schema = plain_env
    rendered = _render_statements(schema)
    assert set(rendered) == set(_ROW_CONTRACTS), "a statement lost its shape pin"
    for name, expected in _ROW_CONTRACTS.items():
        statement = await conn.prepare(rendered[name])
        actual = [(attribute.name, attribute.type.name) for attribute in statement.get_attributes()]
        assert actual == expected, (
            f"{name}'s return-row shape changed — operator queries read "
            f"these columns by name and type"
        )
