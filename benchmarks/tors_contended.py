"""Contended A/B: GIL-free tors primitives vs pure-Python under N threads.

The adoption map's concurrency evidence.  tors's big-input primitives run
their scan with the GIL released (one ``py.detach`` per call), so threads
hammering the same primitive scale with cores; TaskQ's current expressions
(``str.encode``, ``hashlib`` + ``hexdigest``) hold the GIL for their
whole wall.  Each cell runs T threads x ops calls against the SAME
callable and reports the wall per op, so a GIL-free primitive overlaps on
cores while a GIL-bound one serializes — exactly the adoption question.

Shapes are the two mapped candidates:

- ``utf8_byte_len`` vs ``len(s.encode())`` at 1 KiB / 64 KiB / 1 MiB —
  the terminal-write byte-cap shape and the idempotency-validation shape.
- ``sha256_hex`` vs ``hashlib.sha256().hexdigest()`` at 4 KiB — the
  ``redact_payload`` shape.

Run: .venv/bin/python benchmarks/tors_contended.py
"""

from __future__ import annotations

import hashlib
import json
import sys
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

import tors

sys.path.insert(0, str(Path(__file__).parent))

_THREAD_COUNTS = [1, 2, 4, 8]
_OPS_PER_THREAD = 400
META = {"tors": tors.__version__, "threads": _THREAD_COUNTS, "ops_per_thread": _OPS_PER_THREAD}

# ── Payload shapes (deterministic, TaskQ-realistic sizes) ──────────────

PAYLOAD_1K = "é" * 512  # 1024 UTF-8 bytes, non-ASCII (the cold-cache borrow lane)
PAYLOAD_64K = "é" * 32768  # 65536 bytes, exactly the MAX_RESULT_BYTES cap
PAYLOAD_1M = "é" * (512 * 1024)  # 1 MiB
HASH_BLOB = ("job-payload:" + "x" * 4090).encode()


def _contended_wall_per_op(fn: Callable[[], object], threads: int, ops: int) -> float:
    """Total wall for threads x ops calls of ``fn``, divided by the op count.

    A barrier starts all threads together; the wall covers the whole
    hammering window.
    """
    barrier = threading.Barrier(threads + 1)

    def worker() -> None:
        barrier.wait()
        for _ in range(ops):
            fn()

    hs = [threading.Thread(target=worker) for _ in range(threads)]
    for h in hs:
        h.start()
    barrier.wait()
    t0 = time.perf_counter()
    for h in hs:
        h.join()
    return (time.perf_counter() - t0) / (threads * ops) * 1e9  # ns/op


def _make_len_encode(s: str) -> Callable[[], object]:
    def call() -> object:
        return len(s.encode())

    return call


def _make_utf8_byte_len(s: str) -> Callable[[], object]:
    def call() -> object:
        return tors.utf8_byte_len(s)

    return call


def _make_hashlib_hex() -> Callable[[], object]:
    def call() -> object:
        return hashlib.sha256(HASH_BLOB).hexdigest()

    return call


def _make_tors_sha256_hex() -> Callable[[], object]:
    def call() -> object:
        return tors.sha256_hex(HASH_BLOB)

    return call


def contended_table(
    name: str,
    sizes: dict[str, tuple[Callable[[], object], Callable[[], object]]],
    thread_counts: list[int],
    ops: int,
) -> dict[str, object]:
    """{size: {threads: {a_ns, b_ns, speedup}}} for one A/B pair.

    Each size entry is the (A, B) zero-arg callable pair, built outside
    the timed region.
    """
    out: dict[str, object] = {}
    for size_label, (a_fn, b_fn) in sizes.items():
        cells: dict[int, dict[str, float]] = {}
        for t in thread_counts:
            a_ns = _contended_wall_per_op(a_fn, t, ops)
            b_ns = _contended_wall_per_op(b_fn, t, ops)
            cells[t] = {"a_ns": a_ns, "b_ns": b_ns, "speedup": a_ns / b_ns if b_ns else 0.0}
        out[size_label] = cells
        print(f"\n{name} — {size_label}")
        print(f"  {'threads':>8} {'A (current)':>16} {'B (tors)':>16} {'speedup':>9}")
        for t in thread_counts:
            c = cells[t]
            print(f"  {t:>8} {c['a_ns']:>13,.0f} ns {c['b_ns']:>13,.0f} ns {c['speedup']:>8.2f}x")
    return out


def main() -> None:
    # Parity pins, up front (no timing below is trusted past these):
    assert tors.utf8_byte_len(PAYLOAD_1K) == len(PAYLOAD_1K.encode())
    assert tors.utf8_byte_len(PAYLOAD_64K) == len(PAYLOAD_64K.encode())
    assert tors.utf8_byte_len(PAYLOAD_1M) == len(PAYLOAD_1M.encode())
    assert tors.sha256_hex(HASH_BLOB) == hashlib.sha256(HASH_BLOB).hexdigest()

    print(f"tors {tors.__version__} — contended, {_OPS_PER_THREAD} ops/thread, wall per op\n")
    byte_len = contended_table(
        "len(s.encode()) vs tors.utf8_byte_len",
        {
            "1KiB": (_make_len_encode(PAYLOAD_1K), _make_utf8_byte_len(PAYLOAD_1K)),
            "64KiB": (_make_len_encode(PAYLOAD_64K), _make_utf8_byte_len(PAYLOAD_64K)),
            "1MiB": (_make_len_encode(PAYLOAD_1M), _make_utf8_byte_len(PAYLOAD_1M)),
        },
        _THREAD_COUNTS,
        _OPS_PER_THREAD,
    )
    sha = contended_table(
        "hashlib.sha256 vs tors.sha256_hex",
        {"4KiB": (_make_hashlib_hex(), _make_tors_sha256_hex())},
        _THREAD_COUNTS,
        _OPS_PER_THREAD,
    )

    out = {
        "recorded_at": datetime.now(UTC).isoformat(),
        "meta": META,
        "utf8_byte_len": byte_len,
        "sha256_hex": sha,
        "python": sys.version.split()[0],
    }
    path = Path(__file__).parent / "results" / "tors-contended.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2) + "\n")
    print(f"\nresults → {path}")


if __name__ == "__main__":
    main()
