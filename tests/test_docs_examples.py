"""Executable documentation: run every ```python fence in docs/** against the real package.

Review area 18's finding: none of the ~118 docs examples were ever executed —
docs rot silently and nothing catches API drift. This module is the fix.

The extraction lives in :mod:`tests._docs_examples` (also used by the
one-off inventory script, ``scripts/docs_example_inventory.py``). Each
```python fence is executed in its own subprocess — fresh interpreter, fresh
event loop, process-global registries never leak between examples — with:

- a **private Postgres schema** per example (``DROP SCHEMA IF EXISTS`` before
  and after), against the shared testcontainers-backed PG 18;
- the shared Redis container (examples run sequentially: the module's
  ``integration`` marker pins it to one xdist worker);
- an empty ``DOTENV_DIR``, so a developer's ``.env`` cannot silently satisfy
  or poison an example (same hermeticity rule as the session fixture in the
  root conftest);
- a hard :data:`EXAMPLE_TIMEOUT_S` wall-clock kill. An example that cannot
  finish in 45 s is broken documentation, not a slow machine.

Fence info-string convention (see :mod:`tests._docs_examples`): a bare
```python fence is executed; ``python no-exec <reason>`` marks fragments —
continuations of an earlier fence's scope, excerpts, placeholder connection
strings, and interactive/long-running snippets — deliberately, visibly in
the docs themselves. Any other tag is a collection error.

The module is opt-in because every test needs Docker: run it with
``TASKQ_DOCS_EXAMPLES=1`` (``make test-docs-examples``). The default suite
skips the whole module, so there is no collision with the fast tier.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import sys
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

import pytest

from tests._docs_examples import REPO_ROOT, DocsExample, iter_examples, render_script

#: Env var that opts into the docs-example tier. Unset → the module skips at
#: collection (one visible skip, never silent) and the fast tier is untouched.
DOCS_EXAMPLES_ENV = "TASKQ_DOCS_EXAMPLES"

#: Hard wall-clock kill per example. Generous: an example that legitimately
#: needs longer is long-running by design and must be tagged ``no-exec``.
EXAMPLE_TIMEOUT_S = 45

if not os.environ.get(DOCS_EXAMPLES_ENV):
    pytest.skip(
        f"docs examples execute only with {DOCS_EXAMPLES_ENV}=1 "
        "(needs Docker for the PG/Redis containers)",
        allow_module_level=True,
    )

pytestmark = [pytest.mark.integration, pytest.mark.docs_examples]


def _example_schema(example: DocsExample) -> str:
    """Private schema name for one example, stable across runs of one source."""
    digest = hashlib.sha256(example.example_id.encode()).hexdigest()[:10]
    stem = Path(example.example_id.split(":")[0]).stem.replace("-", "_")
    return f"docs_{stem[:40]}_{digest}"


@pytest.fixture(scope="module")
def docs_pg_dsn(pg_dsn: str) -> str:
    """The module's per-test database DSN; every example gets its own schema in it.

    Consumed through ``pg_dsn`` (the per-module database, force-dropped at
    module teardown), never through the raw ``pg_container`` DSN - the
    suite-hygiene guard forbids that, and rightly: a bare container DSN
    would leave example schemas in the shared default database.
    """
    return pg_dsn


@pytest.fixture
async def example_schema(docs_pg_dsn: str, example: DocsExample) -> AsyncIterator[str]:
    """Drop the example's private schema before and after its run.

    Before: a rerun never sees the previous run's jobs. After: the shared
    test database does not accumulate one schema per example per run.
    """
    import asyncpg

    schema = _example_schema(example)
    conn = await asyncpg.connect(docs_pg_dsn)
    try:
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        yield schema
        await conn.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        await conn.close()


async def _run_example(example: DocsExample, schema: str, pg_dsn: str, redis_url: str) -> None:
    """Execute one fence in a subprocess; fail with its stderr on any exit."""
    try:
        script = render_script(example.code)
    except SyntaxError as exc:
        pytest.fail(
            f"{example.example_id}: example does not compile: {exc.msg} "
            f"(line {exc.lineno}); fix it or tag the fence 'python no-exec'"
        )
    with tempfile.TemporaryDirectory(prefix="docs-example-") as tmp:
        source = Path(tmp) / "example.py"
        source.write_text(script, encoding="utf-8")
        env = {
            **os.environ,
            "TASKQ_PG_DSN": pg_dsn,
            "TASKQ_SCHEMA_NAME": schema,
            "TASKQ_REDIS_URL": redis_url,
            # Hermetic: the developer's .env must not satisfy an example.
            "DOTENV_DIR": tmp,
        }
        proc = await asyncio.create_subprocess_exec(
            sys.executable,
            str(source),
            cwd=tmp,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(proc.communicate(), timeout=EXAMPLE_TIMEOUT_S)
        except TimeoutError:
            proc.kill()
            await proc.wait()
            pytest.fail(
                f"{example.example_id}: no exit within {EXAMPLE_TIMEOUT_S}s — an "
                "example must terminate; tag it 'python no-exec' if long-running"
            )
        if proc.returncode != 0:
            fence = f"{REPO_ROOT / example.path}:{example.open_line}"
            pytest.fail(
                f"{example.example_id} (fence at {fence}) failed with exit code "
                f"{proc.returncode}:\n{stderr.decode(errors='replace')[-3000:]}"
            )


#: Collected once at import; the two guards below pin the population sizes.
_EXECUTED_EXAMPLES: list[DocsExample] = [e for e in iter_examples() if e.executed]
_NO_EXEC_EXAMPLES: list[DocsExample] = [e for e in iter_examples() if not e.executed]


@pytest.mark.parametrize("example", _EXECUTED_EXAMPLES, ids=lambda e: e.example_id)
async def test_docs_example(
    example: DocsExample,
    docs_pg_dsn: str,
    redis_url: str,
    example_schema: str,
) -> None:
    """One ```python fence from the docs runs green against the real package."""
    await _run_example(example, example_schema, docs_pg_dsn, redis_url)


def test_no_exec_tag_is_deliberate() -> None:
    """The no-exec population is explicit and bounded, never a silent skip.

    Guards the convention itself: if a future refactor marks every fence
    ``no-exec`` (or the extractor stops seeing fences), this fails instead of
    the suite quietly rotting back to the 0/N the review found.
    """
    assert _NO_EXEC_EXAMPLES, "no no-exec fences: the convention is not in use"
    assert _EXECUTED_EXAMPLES, "no executed fences: the harness would prove nothing"
    total = len(_EXECUTED_EXAMPLES) + len(_NO_EXEC_EXAMPLES)
    executed_ratio = len(_EXECUTED_EXAMPLES) / total
    # 75/309 at introduction (the rest are tagged fragments — continuations,
    # excerpts, placeholder DSNs). The floor only has to catch rot toward
    # zero, not police a target ratio.
    assert executed_ratio >= 0.2, (
        f"only {len(_EXECUTED_EXAMPLES)}/{total} docs examples are executed; "
        "growing the no-exec population without running anything is rot"
    )


def test_every_python_fence_has_a_known_tag() -> None:
    """Unknown info-string tags fail loudly (see ``_docs_examples._tag``).

    Pins ``_tag``'s contract directly rather than re-calling it on the
    inventory: a regression that made ``_tag`` silently accept an unknown
    tag (e.g. mapping it to ``no-exec``) would defeat the extraction's own
    fail-fast too, and this probe is what reds that instead of waving the
    fence through as a skip.
    """
    from tests._docs_examples import _tag

    assert _tag("python") == "exec"
    assert _tag("python no-exec — some reason") == "no-exec"
    with pytest.raises(ValueError, match="unknown docs-example tag"):
        _tag("python maybe-skippable")  # type: ignore[arg-type]
