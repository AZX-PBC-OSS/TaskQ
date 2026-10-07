"""The workflow failure IO-capture writer (T04, G1 — §10.3's policy).

The capture WRITE belongs at failure-finalize (tx1 — a row already being
updated; the success path writes NOTHING extra). The failed node's input +
error are captured per the workflow's ``none | errors-only | all`` policy
(**default ``errors-only``**), truncated at the configurable byte cap with
the explicit ``"__truncated__": N`` marker via tors ``truncate_to_bounds``
(grapheme-safe; its verified ``tors.aio`` twin exists if the payload path
ever goes async — a GIL-holding call here would be the ``kind=gil_held``
stall the watchdog attributes), and passed through the redact chain BEFORE
persistence.

THE COMPOSITION VERDICT (TORS-REV-0.16 §G1): the DEFAULT redactor is the
vetted chain — the tors ``scrub_secrets`` pre-pass (behind the
``_SECRET_HEAD_TRIGGERS`` prefilter) → TaskQ's fail-closed regex masks —
exactly :func:`taskq.obs.mask_credentials`, the public wrapper of the
exception-rendering paths' chain (its composition with the tors pre-pass is
already pinned by ``tests/test_tors_scrub_secrets_composition.py``). The
workflow's user ``redact=fn`` hook POST-COMPOSES on the chain's output and
NEVER REPLACES it — the pipeline is CHAIN → HOOK, unconditionally:

* the asymmetry doctrine — over-redaction is the safe failure;
* a hook that no-ops still leaves the chain's masks on the row;
* a hook that REPLACES the chain is the hole where a canary reaches the
  capture row — and it is structurally impossible here, because the hook
  receives already-scrubbed text whose masks are irreversible. A
  replacement-seeking hook (one returning raw fixture text) still lands a
  MASKED row: the pin asserts the chain's masks are present on the persisted
  row REGARDLESS of the hook's return (red-first pin 11's negative arm).

The capture runs on the worker's event loop inside tx1, so every call in the
pipeline must be GIL-free or short-input by construction (tors is GIL-free;
the chain's regex passes are prefilters + short-substitution scans, the
measured house shape).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Final

from tors import truncate_to_bounds, utf8_byte_len

# Why: private import -- ``mask_credentials`` (the vetted redact chain, tors
# pre-pass included) is the public wrapper of the exception-rendering paths'
# chain but is not re-exported through ``taskq.obs``; the capture pipeline
# composes the SAME chain, not a copy (a copy could drift from the masks the
# composition pin holds). Same package, behavior pinned by
# tests/test_tors_scrub_secrets_composition.py.
from taskq.obs._redact_exc import mask_credentials  # pyright: ignore[reportPrivateUsage]

__all__ = ["build_capture", "redact_capture"]

CAPTURE_POLICY_NONE: Final[str] = "none"
CAPTURE_POLICY_ERRORS_ONLY: Final[str] = "errors-only"
CAPTURE_POLICY_ALL: Final[str] = "all"
_CAPTURE_POLICIES: Final[frozenset[str]] = frozenset(
    {CAPTURE_POLICY_NONE, CAPTURE_POLICY_ERRORS_ONLY, CAPTURE_POLICY_ALL}
)

#: The truncation marker tors ``truncate_to_bounds`` cannot add: the capture
#: records HOW MUCH was cut, explicitly, so a renderer can say so.
TRUNCATED_MARKER: Final[str] = "__truncated__"

#: The default capture byte cap (the configurable cap, T09's settings row).
DEFAULT_CAPTURE_MAX_BYTES: Final[int] = 8 * 1024


def _truncate_to_byte_cap(text: str, max_bytes: int) -> tuple[str, int | None]:
    """Truncate *text* to fit *max_bytes* (grapheme-safe via tors), returning
    ``(text, truncated_chars)`` — the explicit ``__truncated__`` count, or
    ``None`` when nothing was cut.

    ``truncate_to_bounds`` bounds CHARS; the cap is BYTES. One char is at
    most 4 UTF-8 bytes, so a chars bound of ``max_bytes // 4`` never exceeds
    the byte cap, and the byte length is verified after (utf8_byte_len, the
    measured house call) — a pathological multi-byte-heavy payload truncates
    a little further via the halving loop, never exceeding the cap.
    """
    if utf8_byte_len(text) <= max_bytes:
        return text, None
    # Start from the 4-bytes-per-char floor and halve until it fits: at
    # most log2(max_chars) iterations, and the payload is already
    # over-cap (the cold path), so the loop's cost is bounded by the cap.
    chars = max(1, max_bytes // 4)
    while True:
        candidate = truncate_to_bounds(text, chars)
        if utf8_byte_len(candidate) <= max_bytes or chars == 1:
            return candidate, len(text) - len(candidate)
        chars //= 2


def redact_capture(
    text: str,
    *,
    redact: Callable[[str], str] | None = None,
) -> str:
    """The CHAIN → HOOK pipeline, unconditionally.

    The chain (the vetted mask chain, tors pre-pass included) runs FIRST and
    ALWAYS; the user hook receives the chain's already-scrubbed output. A
    hook that tries to widen visibility is not refused — intent is
    undetectable — it is structurally POWERLESS: the masks on its input are
    irreversible, so its replacement attempt still lands masked text.
    """
    chained = mask_credentials(text)
    if redact is None:
        return chained
    return redact(chained)


def build_capture(
    *,
    policy: str,
    node_input: str | None,
    error: str | None,
    max_bytes: int = DEFAULT_CAPTURE_MAX_BYTES,
    redact: Callable[[str], str] | None = None,
) -> dict[str, Any] | None:
    """Build the capture payload for a failure-finalize, or ``None``.

    *policy* ``none`` captures nothing (even on failure); ``errors-only``
    (the default) and ``all`` capture input + error on the failure path.
    Every captured field is REDACTED FIRST (chain → hook), THEN truncated —
    masks are irreversible, so truncating scrubbed text cannot reveal a
    suffix the mask hid, and the persisted row carries no canary.
    """
    if policy not in _CAPTURE_POLICIES:
        raise ValueError(
            f"capture policy must be one of {sorted(_CAPTURE_POLICIES)}, got {policy!r}"
        )
    if policy == CAPTURE_POLICY_NONE:
        return None
    if error is None and node_input is None:
        return None

    capture: dict[str, Any] = {}
    if node_input is not None:
        capture["input"] = redact_capture(node_input, redact=redact)
    if error is not None:
        capture["error"] = redact_capture(error, redact=redact)

    total = sum(utf8_byte_len(v) for v in capture.values())
    if total > max_bytes:
        # Truncate the LARGEST field until the whole payload fits — the
        # error (the reason the capture exists) survives a bloated input.
        for key in sorted(capture, key=lambda k: utf8_byte_len(capture[k]), reverse=True):
            overflow = total - max_bytes
            field_max = max(1, utf8_byte_len(capture[key]) - overflow)
            capture[key], cut = _truncate_to_byte_cap(capture[key], field_max)
            if cut is not None:
                capture[TRUNCATED_MARKER] = capture.get(TRUNCATED_MARKER, 0) + cut
            total = sum(utf8_byte_len(v) for v in capture.values())
            if total <= max_bytes:
                break
    return capture
