"""DocShelf's own durable state — the app's tables beside the TaskQ rows.

Everything the measurements read lives HERE, in the app's schema: the
document register, the mock API's call ledger (the double-effect and
retry-count witnesses), the parent↔child run links, the re-index
cursor, the index and the digests. All writes are idempotent — the
duplicate-feed drill's zero-double-effects invariant reads these tables.
"""

from __future__ import annotations

import json
from typing import Any

import asyncpg

from examples.docshelf.models import DocPayload

_DDL = """
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_documents (
    doc_id    TEXT PRIMARY KEY,
    content   TEXT NOT NULL,
    seq       BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    registered_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_api_calls (
    id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    chunk_key TEXT NOT NULL,
    attempt   INT  NOT NULL,
    outcome   TEXT NOT NULL,
    called_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS docshelf_api_calls_chunk
    ON {{schema}}.docshelf_api_calls (chunk_key);
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_child_runs (
    child_run_id  TEXT PRIMARY KEY,
    parent_run_id TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    linked_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_index (
    doc_id       TEXT PRIMARY KEY,
    n_chunks     INT  NOT NULL,
    tokens       INT  NOT NULL,
    reindexed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_sub_results (
    child_run_id  TEXT PRIMARY KEY,
    parent_run_id TEXT NOT NULL,
    doc_id        TEXT NOT NULL,
    payload       JSONB NOT NULL,
    recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS docshelf_sub_results_parent
    ON {{schema}}.docshelf_sub_results (parent_run_id);
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_reindex_cursor (
    id       INT PRIMARY KEY CHECK (id = 1),
    last_seq BIGINT NOT NULL DEFAULT 0
);
INSERT INTO {{schema}}.docshelf_reindex_cursor (id, last_seq) VALUES (1, 0)
    ON CONFLICT (id) DO NOTHING;
CREATE TABLE IF NOT EXISTS {{schema}}.docshelf_digests (
    delivery_id  TEXT PRIMARY KEY,
    digest       JSONB NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def ensure_tables(pool: asyncpg.Pool, schema: str) -> None:
    """Create the app's tables (idempotent). Called at test setup and at
    worker boot — the app owns its own DDL, never the library's."""
    await pool.execute(
        _DDL.replace(
            "{{schema}}", f'"{schema}"'
        )  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.


async def register_docs(
    pool: asyncpg.Pool, schema: str, docs: list[DocPayload]
) -> tuple[list[DocPayload], list[str]]:
    """Idempotent ingestion: INSERT ON CONFLICT DO NOTHING per document.

    Returns (the NEWLY-registered documents — a re-delivered document is
    NOT re-processed, whoever delivers it — and the delivered ids that
    were ALREADY registered: the duplicate-feed witness).
    """
    new: list[DocPayload] = []
    seen: set[str] = set()
    duplicates: list[str] = []
    for doc in docs:
        if doc.doc_id in seen:
            # A duplicate WITHIN one delivery: deduped on arrival.
            if doc.doc_id not in duplicates:
                duplicates.append(doc.doc_id)
            continue
        seen.add(doc.doc_id)
        row = await pool.fetchrow(
            f'INSERT INTO "{schema}".docshelf_documents (doc_id, content) '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "VALUES ($1, $2) "
            "ON CONFLICT (doc_id) DO NOTHING "
            "RETURNING doc_id, content",
            doc.doc_id,
            doc.content,
        )
        if row is None:
            duplicates.append(doc.doc_id)
        else:
            new.append(DocPayload(doc_id=row["doc_id"], content=row["content"]))
    return new, sorted(duplicates)


async def record_api_call(
    pool: asyncpg.Pool, schema: str, chunk_key: str, attempt: int, outcome: str
) -> None:
    """The mock extraction API's durable call ledger — one row per call."""
    await pool.execute(
        f'INSERT INTO "{schema}".docshelf_api_calls (chunk_key, attempt, outcome) '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "VALUES ($1, $2, $3)",
        chunk_key,
        attempt,
        outcome,
    )


async def api_call_summary(pool: asyncpg.Pool, schema: str) -> dict[str, dict[str, int]]:
    """Per-chunk outcome counts + the global tally — the drills' witness."""
    rows = await pool.fetch(
        f'SELECT chunk_key, outcome, count(*) AS n FROM "{schema}".docshelf_api_calls '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "GROUP BY chunk_key, outcome"
    )
    summary: dict[str, dict[str, int]] = {}
    for r in rows:
        summary.setdefault(r["chunk_key"], {})[r["outcome"]] = r["n"]
    return summary


async def record_child_link(
    pool: asyncpg.Pool, schema: str, child_run_id: str, parent_run_id: str, doc_id: str
) -> None:
    """The parent↔child link, durable and idempotent — the operator's
    join between a run and its sub-analysis children."""
    await pool.execute(
        f'INSERT INTO "{schema}".docshelf_child_runs (child_run_id, parent_run_id, doc_id) '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "VALUES ($1, $2, $3) ON CONFLICT (child_run_id) DO NOTHING",
        child_run_id,
        parent_run_id,
        doc_id,
    )


async def child_links(pool: asyncpg.Pool, schema: str) -> list[dict[str, str]]:
    rows = await pool.fetch(
        f'SELECT child_run_id, parent_run_id, doc_id FROM "{schema}".docshelf_child_runs'  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )
    return [
        {
            "child_run_id": r["child_run_id"],
            "parent_run_id": r["parent_run_id"],
            "doc_id": r["doc_id"],
        }
        for r in rows
    ]


async def record_sub_result(
    pool: asyncpg.Pool,
    schema: str,
    child_run_id: str,
    parent_run_id: str,
    doc_id: str,
    payload: dict[str, Any],
) -> int:
    """One child's typed outcome, durable. Idempotent by child run (the
    tail's re-run — a reclaim — records the same outcome once). Returns
    the batch's recorded count AFTER this insert."""
    raw = json.dumps(payload)
    await pool.execute(
        f'INSERT INTO "{schema}".docshelf_sub_results '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "(child_run_id, parent_run_id, doc_id, payload) "
        "VALUES ($1, $2, $3, $4::jsonb) "
        "ON CONFLICT (child_run_id) DO NOTHING",
        child_run_id,
        parent_run_id,
        doc_id,
        raw,
    )
    return int(
        await pool.fetchval(
            f'SELECT count(*) FROM "{schema}".docshelf_sub_results WHERE parent_run_id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            parent_run_id,
        )
    )


async def assemble_sub_results(
    pool: asyncpg.Pool, schema: str, parent_run_id: str
) -> list[dict[str, Any]]:
    """The batch's recorded outcomes (the delivering tail's assembly)."""
    rows = await pool.fetch(
        f'SELECT payload FROM "{schema}".docshelf_sub_results '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE parent_run_id = $1 ORDER BY doc_id",
        parent_run_id,
    )
    return [
        json.loads(r["payload"]) if isinstance(r["payload"], str) else r["payload"] for r in rows
    ]


async def join_failures(
    pool: asyncpg.Pool, schema: str, flow_id: str, join_key: str
) -> list[dict[str, Any]]:
    """The collect join's absorbed failures (the row's bounded summary)."""
    raw = await pool.fetchval(
        f"SELECT metadata->'failures' FROM \"{schema}\".jobs "  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE metadata->>'flow_id' = $1 AND step_key = $2",
        flow_id,
        join_key,
    )
    if raw is None:
        return []
    return json.loads(raw) if isinstance(raw, str) else raw


async def upsert_index(
    pool: asyncpg.Pool, schema: str, doc_id: str, n_chunks: int, tokens: int
) -> None:
    """The shelf index — idempotent by doc (the re-index's double-effect guard)."""
    await pool.execute(
        f'INSERT INTO "{schema}".docshelf_index (doc_id, n_chunks, tokens) '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "VALUES ($1, $2, $3) "
        "ON CONFLICT (doc_id) DO UPDATE SET "
        "n_chunks = EXCLUDED.n_chunks, tokens = EXCLUDED.tokens, "
        "reindexed_at = now()",
        doc_id,
        n_chunks,
        tokens,
    )


async def read_cursor(pool: asyncpg.Pool, schema: str) -> int:
    """The durable re-index cursor."""
    return int(
        await pool.fetchval(f'SELECT last_seq FROM "{schema}".docshelf_reindex_cursor WHERE id = 1')  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
    )


async def advance_cursor(pool: asyncpg.Pool, schema: str, seq: int) -> int:
    """Advance the cursor to *seq* — GREATEST: the cursor NEVER rewinds,
    not on a crash, not on a stale writer."""
    return int(
        await pool.fetchval(
            f'UPDATE "{schema}".docshelf_reindex_cursor '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
            "SET last_seq = GREATEST(last_seq, $1) WHERE id = 1 RETURNING last_seq",
            seq,
        )
    )


async def new_documents(pool: asyncpg.Pool, schema: str, from_seq: int) -> list[tuple[str, int]]:
    """The increment: documents registered after *from_seq* (the cursor's read)."""
    rows = await pool.fetch(
        f'SELECT doc_id, seq FROM "{schema}".docshelf_documents '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "WHERE seq > $1 ORDER BY seq",
        from_seq,
    )
    return [(r["doc_id"], int(r["seq"])) for r in rows]


async def document_content(pool: asyncpg.Pool, schema: str, doc_id: str) -> str | None:
    return await pool.fetchval(
        f'SELECT content FROM "{schema}".docshelf_documents WHERE doc_id = $1',  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        doc_id,
    )


async def publish_digest_row(
    pool: asyncpg.Pool, schema: str, delivery_id: str, digest: dict[str, Any]
) -> bool:
    """Idempotent digest publication — True when THIS call published it."""
    raw = json.dumps(digest)
    row = await pool.fetchrow(
        f'INSERT INTO "{schema}".docshelf_digests (delivery_id, digest) '  # noqa: S608  # Why: the interpolated value is the app's settings-validated schema identifier; every caller-controlled value is $N-bound.
        "VALUES ($1, $2::jsonb) "
        "ON CONFLICT (delivery_id) DO NOTHING RETURNING delivery_id",
        delivery_id,
        raw,
    )
    return row is not None
