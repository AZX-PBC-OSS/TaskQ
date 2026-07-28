"""Unit tier for ``set_actor_config_capacity`` input validation.

The integration tier (test_actor_config_ops.py) covers the SQL path on
real Postgres; these tests pin the guards that must reject a bad value
*before anything is written*:

* ``bool`` — ``False`` is an ``int`` and would be written as 0 (asyncpg
  pre-types the parameter and coerces it), flooring the dispatch
  residual ``GREATEST(cap - in_flight, 0)`` and silently pausing the
  actor.
* NaN / ±inf ``result_ttl`` — ``nan < 0`` is False so a negative guard
  cannot see it, but ``now() + NaN * interval '1 second'`` raises
  ``interval out of range`` in the terminal-write UPDATE, failing every
  completion for the actor.

Behavioral assertion: the invalid call raises ``ValueError`` naming the
problem, and the connection never sees a statement.
"""

import pytest

from taskq.worker.actor_config_ops import UNSET, set_actor_config_capacity


class _RecordingConn:
    """Stub connection: any statement execution is a test failure."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    async def fetchrow(self, query: str, *args: object) -> None:
        self.statements.append(query)
        return None


async def test_rejects_bool_max_concurrent_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match=r"max_concurrent.*bool"):
        await set_actor_config_capacity(conn, "a", max_concurrent=False)  # type: ignore[arg-type]  # Why: the guard exists precisely because bool slips past isinstance(x, int).
    assert conn.statements == []


async def test_rejects_bool_max_pending_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match=r"max_pending.*bool"):
        await set_actor_config_capacity(conn, "a", max_pending=True)  # type: ignore[arg-type]
    assert conn.statements == []


async def test_rejects_bool_result_ttl_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match=r"result_ttl.*bool"):
        await set_actor_config_capacity(conn, "a", result_ttl=False)  # type: ignore[arg-type]
    assert conn.statements == []


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
async def test_rejects_non_finite_result_ttl_without_writing(bad: float) -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="result_ttl must be finite"):
        await set_actor_config_capacity(conn, "a", result_ttl=bad)
    assert conn.statements == []


async def test_rejects_negative_values_without_writing() -> None:
    conn = _RecordingConn()
    with pytest.raises(ValueError, match="max_concurrent"):
        await set_actor_config_capacity(conn, "a", max_concurrent=-1)
    with pytest.raises(ValueError, match="max_pending"):
        await set_actor_config_capacity(conn, "a", max_pending=-1)
    with pytest.raises(ValueError, match="result_ttl"):
        await set_actor_config_capacity(conn, "a", result_ttl=-0.5)
    assert conn.statements == []


async def test_unset_and_none_and_zero_pass_validation() -> None:
    """Sentinels and boundary values are not validation errors (their
    write semantics are covered by the integration tier)."""
    conn = _RecordingConn()
    await set_actor_config_capacity(
        conn, "a", max_concurrent=UNSET, max_pending=UNSET, result_ttl=UNSET
    )
    await set_actor_config_capacity(conn, "a", max_concurrent=None, result_ttl=None)
    await set_actor_config_capacity(conn, "a", max_concurrent=0, max_pending=0, result_ttl=0)
    # Validation passed — statements were attempted (and the stub saw them).
    assert len(conn.statements) == 3
