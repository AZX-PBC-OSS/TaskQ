# Why: schema is a fixed test identifier, not user input; every value is $-bound.
"""Red-team: round-trip fidelity of the serialization boundary for
exotic-but-legal and adversarial-but-representable values.

Pins the DESIRED observable at the ``taskq._json`` boundary (orjson):
legal exotics (UUID/date objects, astral surrogate pairs, unicode, huge
strings, huge dict cardinality, deep-but-small nesting) must round-trip
without data loss; adversarial-but-representable values must fail with a
TYPED, bounded error at the boundary (never an interpreter crash, never a
silent non-standard token that ``jsonb`` would reject).

Known and pinned elsewhere (not re-hunted here): the #134 OPT_NON_STR_KEYS
compat break (tests/test_json_non_str_keys.py), the NUL guard
(tests/test_progress_flush.py, tests/test_rt_cancel_nul_guards.py).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest
from pydantic import BaseModel

from taskq._json import dumps, loads
from taskq.testing.clock import FakeClock
from taskq.testing.in_memory import InMemoryBackend
from taskq.testing.jobs import make_enqueue_args
from taskq.worker._consumer import _encode_result


class _Inner(BaseModel):
    label: str


class _NestedResult(BaseModel):
    inner: _Inner
    ref: UUID
    at: datetime


def _deep(depth: int) -> dict[str, object]:
    node: object = None
    for _ in range(depth):
        node = {"a": node}
    return {"deep": node}  # pyright: ignore[reportUnknownVariableType,reportReturnType]  # Why: the loop deliberately builds object-typed nesting; the dict wrapper is dict[str, object] by construction.


# ── Exotic-but-legal values round-trip ──────────────────────────────────


def test_uuid_datetime_and_astral_pairs_round_trip() -> None:
    """UUID objects, UTC/naive datetimes, astral surrogate pairs and
    non-ASCII unicode survive dumps -> loads with their JSON-safe forms."""
    ref = UUID("12345678123456781234567812345678")
    utc = datetime(2026, 1, 1, 12, 30, 45, tzinfo=UTC)
    naive = datetime(2026, 6, 1, 8, 0, 0)
    value = {
        "ref": ref,
        "utc": utc,
        "naive": naive,
        "astral": "😀🚀",
        "cjk": "日本語テキスト",
        "combining": "éclair",
    }
    out = loads(dumps(value))
    assert out == {
        "ref": str(ref),
        "utc": "2026-01-01T12:30:45Z",
        "naive": "2026-06-01T08:00:00Z",
        "astral": "😀🚀",
        "cjk": "日本語テキスト",
        "combining": "éclair",
    }, (
        "contract: UUID->str, datetimes->ISO-8601 UTC 'Z' form, astral pairs and unicode round-trip byte-exact"
    )


def test_huge_string_and_huge_cardinality_round_trip() -> None:
    """A 1 MiB string and a 100k-key dict are representable and must
    round-trip exactly (no truncation, no key loss)."""
    big = "x" * (1024 * 1024)
    out = loads(dumps({"blob": big}))
    assert out == {"blob": big}, "contract: a 1 MiB string round-trips untruncated"

    wide = {str(i): i for i in range(100_000)}
    out2 = loads(dumps(wide))
    assert out2 == wide, "contract: 100k dict keys round-trip without key loss"


def test_deep_but_small_nesting_round_trips() -> None:
    """Depth-200 nesting is small on the wire and must round-trip."""
    out = loads(dumps(_deep(200)))
    node: Any = out["deep"]
    depth = 0
    while isinstance(node, dict) and "a" in node:
        node = node["a"]
        depth += 1
    assert depth == 200, f"contract: depth-200 nesting round-trips; walked {depth}"


def test_nested_base_model_result_encodes_to_json_safe_forms() -> None:
    """``_encode_result`` on a BaseModel result emits the model_dump(json)
    forms: UUID/datetime fields as their string forms, nested models as
    plain dicts."""
    result = _NestedResult(
        inner=_Inner(label="l"),
        ref=UUID("12345678123456781234567812345678"),
        at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    data = _encode_result(result)
    assert data is not None
    out = loads(data)
    assert out == {
        "inner": {"label": "l"},
        "ref": "12345678-1234-5678-1234-567812345678",
        "at": "2026-01-01T00:00:00Z",
    }, "contract: BaseModel results encode to plain JSON-safe dicts"


# ── NaN / Infinity: no non-standard JSON token may reach jsonb ──────────


def test_nan_and_infinity_serialize_to_null() -> None:
    """NaN and +-Infinity serialize as JSON null (orjson contract): the
    emitted document is STANDARD JSON that jsonb accepts -- no NaN/Infinity
    token is ever written, so no flush can fail with 22P02/22P03."""
    for hostile in (float("nan"), float("inf"), float("-inf")):
        data = dumps({"ratio": hostile})
        assert data == b'{"ratio":null}', (
            f"contract: {hostile!r} must serialize as null (standard JSON), got {data!r}"
        )
        assert loads(data) == {"ratio": None}, "contract: null round-trips as None"


async def test_nan_result_succeeds_and_stores_null_mirror() -> None:
    """An actor returning NaN in its result dict must land 'succeeded'
    with the NaN stored as null -- a silent but STANDARD coercion, never a
    jsonb rejection or a stranded job (in-memory mirror of the PG pin)."""
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))

    def stub(
        payload: object, ctx: object
    ) -> dict[str, object]:  # Why: runner stub signature is fixed; both params are unused here.
        return {"ratio": float("nan"), "ok": True}

    backend.register_stub("nan_actor", stub)
    args = make_enqueue_args(
        actor="nan_actor", payload={"value": 1}, scheduled_at=backend._clock.now()
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    row = await backend.enqueue(args)
    await backend.run_until_drained()

    stored = await backend.get(row.id)
    assert stored is not None
    assert stored.status == "succeeded", (
        "contract: a NaN-bearing result is storable (as null), not a failure"
    )
    assert stored.result == {"ratio": None, "ok": True}, (
        f"contract: NaN is stored as JSON null; got {stored.result!r}"
    )


# ── Over-deep nesting: typed boundary failure, not a crash ──────────────


def test_overdeep_nesting_raises_typed_type_error() -> None:
    """Nesting beyond orjson's recursion limit must raise a TYPED
    TypeError at the boundary -- not a RecursionError interpreter crash
    and not a stack overflow mid-flush."""
    with pytest.raises(TypeError, match="Recursion limit reached"):
        dumps(_deep(5000))


def test_overdeep_document_load_raises_typed_value_error() -> None:
    """Decoding an over-deep document must raise orjson's typed
    JSONDecodeError (a ValueError), never a crash."""
    doc = ('{"a":' * 2000) + "null" + ("}" * 2000)
    with pytest.raises(ValueError, match="depth limit exceeded"):
        loads(doc)


async def test_overdeep_payload_is_refused_at_enqueue_boundary_mirror() -> None:
    """The enqueue boundary refuses over-deep payloads with the same
    typed TypeError before any row exists (in-memory mirror of the PG
    pin)."""
    backend = InMemoryBackend(clock=FakeClock(datetime(2026, 1, 1, tzinfo=UTC)))
    backend.register_stub(
        "deep_actor", lambda p, c: None
    )  # Why: runner stub signature is fixed; never reached.
    args = make_enqueue_args(
        actor="deep_actor", payload=_deep(5000), scheduled_at=backend._clock.now()
    )  # pyright: ignore[reportPrivateUsage]  # Why: test-only access to the FakeClock-backed InMemoryBackend, the established runner pattern.
    job_id = args.id
    with pytest.raises(TypeError, match="Recursion limit reached"):
        await backend.enqueue(args)
    assert await backend.get(job_id) is None, "contract: a refused enqueue leaves no row behind"
