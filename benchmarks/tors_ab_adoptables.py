"""A/B: tors primitives against TaskQ's per-job text/ID/hash paths.

The companion to ``docs/design/tors-adoption-map.md``: every mapped
candidate measured on its real TaskQ call shape, interleaved A/B with the
correctness assertion the ``bench_hotspots.py`` house rules require.  A =
TaskQ's current expression; B = the tors spelling.  p50/p99 are over 61
interleaved rounds.

Mapped call sites (all measured here):

- ``client/_args.py`` ``validate_idempotency`` — per-enqueue UTF-8 byte
  caps on idempotency key/scope (A: ``len(s.encode())``; B:
  ``tors.utf8_byte_len``).  Shapes: the typical ASCII key, a
  1024-codepoint CJK scope at the cap edge, and the 64 KiB
  ``MAX_RESULT_BYTES``-capped result string the terminal write re-encodes
  (``backend/_terminal.py``'s ``len(serialized_result.encode("utf-8"))``).
- ``_ids.py`` ``new_base62`` random mode (A: ``os.urandom`` + modulo +
  per-char divmod loop; B: ``tors.random_b62``) and ``new_uuid`` (A:
  ``uuid_utils.uuid7()`` wrapped in ``UUID(bytes=...)``; B: the same
  wrapper over ``tors.uuid7_bytes()``, plus the bare-bytes spellings).
- ``obs/_structlog.py`` ``redact_payload`` (A: ``hashlib.sha256`` +
  ``hexdigest()[:16]``; B: ``tors.sha256_digest`` + hex slicing, and the
  direct ``tors.sha256_hex`` spelling).
- ``cli.py`` ``_format_event_detail``'s whitespace collapse (A:
  ``" ".join(s.split())``; B: ``tors.strip_controls`` — NOT semantically
  identical, measured only to size the SKIP) and the admin's
  ``_truncate_traceback`` hard cut (A: slice + suffix; B:
  ``tors.truncate_ellipsis`` — different marker contract, same SKIP).

Run: .venv/bin/python benchmarks/tors_ab_adoptables.py
"""

from __future__ import annotations

import hashlib
import sys
import time
from functools import partial
from pathlib import Path
from uuid import UUID

import tors

sys.path.insert(0, str(Path(__file__).parent))

from tors_harness import (  # pyright: ignore[reportMissingImports]  # Why: the sys.path bootstrap above is the benchmarks/ convention; pyright's include is src/tests/examples.
    ABRow,
    ab,
    dump_results,
    print_rows,
)

from taskq import (
    _ids,  # pyright: ignore[reportPrivateUsage]  # Why: the private helper IS the path new_base62 serves; the bench measures it in place.
)

META = {
    "tors": tors.__version__,
    "shapes": "idkey ascii/cjk-1k-cp/result-64KiB; b62-8; uuid7; sha256-4KiB",
}

# ── Payload shapes (TaskQ-realistic, deterministic) ────────────────────

ID_KEY_ASCII = "0123456789abcdefghijklmnopqrstuvwxyz"  # 36 chars, the common idempotency key
ID_SCOPE_CJK = "订单" * 512  # 1024 codepoints, 3 bytes each: the cap-edge non-ASCII shape
RESULT_64K = "é" * 32768  # 32768 codepoints = 65536 UTF-8 bytes, exactly the MAX_RESULT_BYTES cap
HASH_BLOB = ("job-payload:" + "x" * 4096).encode()
TRACEBACK_TEXT = ("Traceback ...\n  " + "x" * 4000) + " tail"
DETAIL_LINE = "lock\texpired\nfor  job  x\x00\x1f trailing"

_DISPLAY_LIMIT = 4000  # the admin jobs view's _TRACEBACK_DISPLAY_LIMIT analogue


def _admin_cut(tb: str) -> str:
    """``web/admin/jobs.py`` ``_truncate_traceback``'s exact expression."""
    if len(tb) <= _DISPLAY_LIMIT:
        return tb
    remaining = len(tb) - _DISPLAY_LIMIT
    suffix = f"\n... ({remaining} more characters)"
    return tb[: _DISPLAY_LIMIT - len(suffix)] + suffix


# ── The A expressions (TaskQ's current spellings) ──────────────────────


def a_len_encode(s: str) -> int:
    return len(s.encode())


def a_random_base62() -> str:
    return _ids._random_base62(8)  # pyright: ignore[reportPrivateUsage]


def a_new_uuid() -> UUID:
    return _ids.new_uuid()


def a_uuid7_bytes() -> bytes:
    return _ids.new_uuid().bytes


def a_sha256_hex() -> str:
    return hashlib.sha256(HASH_BLOB).hexdigest()


def a_sha256_hex16() -> str:
    return hashlib.sha256(HASH_BLOB).hexdigest()[:16]


def a_ws_collapse() -> str:
    return " ".join(DETAIL_LINE.split())


def a_admin_cut() -> str:
    return _admin_cut(TRACEBACK_TEXT)


# ── The B expressions (the tors spellings) ─────────────────────────────


def b_utf8_byte_len(s: str) -> int:
    return tors.utf8_byte_len(s)


def b_random_b62() -> str:
    return tors.random_b62(8)


def b_uuid7() -> UUID:
    return UUID(bytes=tors.uuid7_bytes())


def b_uuid7_bare() -> bytes:
    return tors.uuid7_bytes()


def b_sha256_hex() -> str:
    return tors.sha256_hex(HASH_BLOB)


def b_sha256_hex16() -> str:
    return tors.sha256_digest(HASH_BLOB).hex()[:16]


def b_strip_controls() -> str:
    return tors.strip_controls(DETAIL_LINE)


def b_truncate_ellipsis() -> str:
    return tors.truncate_ellipsis(TRACEBACK_TEXT, _DISPLAY_LIMIT)


# ── Correctness pins (checked before any timing is read) ───────────────


def check_byte_len_ascii() -> bool:
    return tors.utf8_byte_len(ID_KEY_ASCII) == len(ID_KEY_ASCII.encode())


def check_byte_len_cjk() -> bool:
    return tors.utf8_byte_len(ID_SCOPE_CJK) == len(ID_SCOPE_CJK.encode())


def check_byte_len_64k() -> bool:
    return tors.utf8_byte_len(RESULT_64K) == len(RESULT_64K.encode())


def check_b62() -> bool:
    # Draw-independent CSPRNG streams: only alphabet membership + length
    # can pin; the uniformity contracts are each side's own tests.
    a = _ids._random_base62(8)  # pyright: ignore[reportPrivateUsage]
    b = tors.random_b62(8)
    return (
        len(a) == len(b) == 8
        and set(a) <= set(tors.CHARSET_B62)
        and set(b) <= set(tors.CHARSET_B62)
    )


def check_sha() -> bool:
    return tors.sha256_hex(HASH_BLOB) == hashlib.sha256(HASH_BLOB).hexdigest() and (
        tors.sha256_digest(HASH_BLOB) == hashlib.sha256(HASH_BLOB).digest()
    )


def check_controls() -> bool:
    # Only the shared contract (no C0/DEL survives) is comparable; the two
    # spellings disagree on non-control whitespace by design — the note
    # field carries the semantics divergence, this check is tors-only.
    return "\x00" not in tors.strip_controls(DETAIL_LINE) and "\x1f" not in tors.strip_controls(
        DETAIL_LINE
    )


def check_truncate() -> bool:
    a = _admin_cut(TRACEBACK_TEXT)
    b = tors.truncate_ellipsis(TRACEBACK_TEXT, _DISPLAY_LIMIT)
    return len(a) == _DISPLAY_LIMIT and 0 < len(b) <= _DISPLAY_LIMIT


def check_uuid7() -> bool:
    return _ids.new_uuid().version == 7 and tors.uuid_version(tors.uuid7()) == 7


def main() -> None:
    rows: list[ABRow] = [
        # ── utf8_byte_len: the byte-cap validations ────────────────────
        ab(
            "idkey-36B-ascii  len(encode) vs utf8_byte_len",
            partial(a_len_encode, ID_KEY_ASCII),
            partial(b_utf8_byte_len, ID_KEY_ASCII),
            batch=2000,
            check=check_byte_len_ascii,
        ),
        ab(
            "idscope-1k-cp-cjk  len(encode) vs utf8_byte_len",
            partial(a_len_encode, ID_SCOPE_CJK),
            partial(b_utf8_byte_len, ID_SCOPE_CJK),
            batch=2000,
            check=check_byte_len_cjk,
        ),
        ab(
            "result-64KiB  len(encode) vs utf8_byte_len",
            partial(a_len_encode, RESULT_64K),
            partial(b_utf8_byte_len, RESULT_64K),
            batch=200,
            check=check_byte_len_64k,
            note="terminal-write site (reserved file; map-only)",
        ),
        # ── ID generation: per-enqueue ─────────────────────────────────
        ab(
            "base62-8  _random_base62 vs tors.random_b62",
            a_random_base62,
            b_random_b62,
            batch=500,
            check=check_b62,
            note="alphabet+len pinned; draws are independent CSPRNG streams",
        ),
        ab(
            "uuid7-UUID  uuid_utils vs tors.uuid7_bytes",
            a_new_uuid,
            b_uuid7,
            batch=500,
            check=check_uuid7,
        ),
        ab(
            "uuid7-bare  uuid_utils vs tors.uuid7 (str)",
            a_uuid7_bytes,
            b_uuid7_bare,
            batch=500,
            check=check_uuid7,
        ),
        # ── hashing: redact_payload / migrate checksums ────────────────
        ab(
            "sha256-4KiB-hex  hashlib vs tors.sha256_hex",
            a_sha256_hex,
            b_sha256_hex,
            batch=500,
            check=check_sha,
        ),
        ab(
            "sha256-4KiB-[:16]  hashlib vs tors.sha256_digest",
            a_sha256_hex16,
            b_sha256_hex16,
            batch=500,
            check=check_sha,
        ),
        # ── SKIP-sized text helpers, measured for the record ───────────
        ab(
            "detail-ws-collapse  split-join vs strip_controls",
            a_ws_collapse,
            b_strip_controls,
            batch=2000,
            check=check_controls,
            note="NOT semantics-identical (all-WS vs C0/DEL): SKIP evidence",
        ),
        ab(
            "traceback-cut-4k  slice+suffix vs truncate_ellipsis",
            a_admin_cut,
            b_truncate_ellipsis,
            batch=2000,
            check=check_truncate,
            note="different marker contract: SKIP evidence",
        ),
    ]
    print(f"tors {tors.__version__} — A/B, 61 interleaved rounds, p50/p99 ns/op\n")
    print_rows(rows)
    dump_results(rows, "tors-ab-adoptables.json", META)


if __name__ == "__main__":
    start = time.perf_counter()
    main()
    print(f"(wall {time.perf_counter() - start:.1f}s)")
