"""A/B: orjson ``OPT_NON_STR_KEYS`` flag on ``taskq._json.dumps``.

``dumps()`` currently passes ``OPT_NAIVE_UTC | OPT_UTC_Z | OPT_NON_STR_KEYS``
on every call.  The orjson docs warn that ``OPT_NON_STR_KEYS`` slows
serialization even when all keys are strings.  This bench measures that cost
on TaskQ-shaped payloads that all use str keys:

  A = current flags (with OPT_NON_STR_KEYS)
  B = same flags minus OPT_NON_STR_KEYS

Methodology follows the ``bench_hotspots.py`` house rules: interleaved A/B
batches (the harness's ``ab_bench``), correctness assertion that B's output
is byte-identical to A's before any timing is trusted, honest variants (both
sides pass the same ``default`` fallback).

Run: .venv/bin/python benchmarks/ab_orjson_flags.py
"""

from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import (
    uuid4,  # noqa: TID251  # Why: throwaway payload ids for a serialization A/B; PK locality is not the variable under test.
)

import orjson

sys.path.insert(0, str(Path(__file__).parent))

import stress_dispatch
from bench_hotspots import ABResult, ab_bench, report

_A_FLAGS = orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z | orjson.OPT_NON_STR_KEYS
_B_FLAGS = orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z


# Mirror dumps() exactly: same default fallback, only the flags differ.
from taskq._json import _orjson_fallback  # noqa: E402


def dumps_a(value: object) -> bytes:
    return orjson.dumps(value, default=_orjson_fallback, option=_A_FLAGS)


def dumps_b(value: object) -> bytes:
    return orjson.dumps(value, default=_orjson_fallback, option=_B_FLAGS)


# ── TaskQ-shaped payloads (all str keys) ──────────────────────────────


def job_payload_4kb() -> dict[str, object]:
    return stress_dispatch._make_payload_4kb()


def metadata_1kb() -> dict[str, str]:
    return {
        "request_id": str(uuid4()),
        "tenant": "acme-corp",
        "region": "us-east-1",
        "pipeline": "orders.ingest.v3",
        "submitted_by": "service:orders-api@prod",
        "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        "labels": ",".join(f"label_{i:02d}=value_{i:02d}" for i in range(24)),
        "notes": "m" * 512,
    }


def log_event_dict() -> dict[str, object]:
    return {
        "event": "job_dispatched",
        "level": "info",
        "job_id": str(uuid4()),
        "actor": "orders.enqueue_payload",
        "queue": "default",
        "attempt": 1,
        "worker": "worker-7f3a",
        "ts": datetime.now(UTC).isoformat(),
        "duration_ms": 12.34,
        "tags": ["bench", "stress", "dispatch"],
        "extra": {"shard": 3, "priority": 0, "fair": True},
    }


def progress_state() -> dict[str, object]:
    return {
        "step": "uploading_chunks",
        "percent": 67.5,
        "seq": 42,
        "detail": f"chunk 270/400 at {datetime.now(UTC).isoformat()}",
        "data": {"chunks_done": 270, "chunks_total": 400, "bytes": 17_825_792},
    }


PAYLOADS: dict[str, callable[[], object]] = {  # type: ignore[assignment]
    "job_payload_4kb": job_payload_4kb,
    "metadata_1kb": metadata_1kb,
    "log_event": log_event_dict,
    "progress_state": progress_state,
}


def main() -> None:
    print("== correctness: B output byte-identical to A (str-keyed inputs) ==")
    for name, factory in PAYLOADS.items():
        payload = factory()  # one instance — A and B must see identical input
        a_out = dumps_a(payload)
        b_out = dumps_b(payload)
        assert a_out == b_out, f"{name}: outputs differ"
        assert isinstance(a_out, bytes)
        print(f"  {name:<18} identical ({len(a_out):,} B)")

    # Edge cases that distinguish the flags (not timed):
    #   int/bool/None keys: A coerces silently, B raises TypeError.
    #   bytes keys: BOTH raise TypeError — the flag does not license bytes keys.
    print("== edge cases (behaviour, not timing) ==")
    try:
        dumps_a({1: "x"})
        print("  int key      A (with flag): serialized ->", dumps_a({1: "x"}))
    except TypeError as exc:
        print(f"  int key      A (with flag): TypeError: {exc}")
    try:
        dumps_b({1: "x"})
        print("  int key      B (without)   : serialized ->", dumps_b({1: "x"}))
    except TypeError as exc:
        print(f"  int key      B (without)   : TypeError: {exc}")
    for label, fn in (("A (with flag)", dumps_a), ("B (without)", dumps_b)):
        try:
            fn({b"key": "x"})
            print(f"  bytes key    {label}: serialized (!)")
        except TypeError as exc:
            print(f"  bytes key    {label}: TypeError: {exc}")

    results: list[ABResult] = []
    for name, factory in PAYLOADS.items():
        payload = factory()  # one instance; both sides serialize the same object
        assert dumps_a(payload) == dumps_b(payload), name
        results.append(
            ab_bench(
                f"orjson_flags[{name}] A=+NON_STR_KEYS B=-NON_STR_KEYS",
                lambda p=payload: dumps_a(p),
                lambda p=payload: dumps_b(p),
                batch=500,
                batches=7,
                correct=True,
                note="B drops OPT_NON_STR_KEYS only; default=_orjson_fallback on both sides",
            )
        )

    # Re-verify correctness on the exact objects timed (post-warmup state).
    for name, factory in PAYLOADS.items():
        payload = factory()
        assert dumps_a(payload) == dumps_b(payload), name

    print()
    report(results)
    print(
        "\nnote: orjson docs — OPT_NON_STR_KEYS 'slows serialization';\n"
        "speedup > 1.0 means dropping the flag is faster (B wins)."
    )


if __name__ == "__main__":
    main()
