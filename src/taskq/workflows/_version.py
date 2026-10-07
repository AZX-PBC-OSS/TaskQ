"""The per-attempt code-version record's canonical hash (T04, G8).

``jobs.code_version`` is §22.1's "code-version hash per attempt": the deploy
computes the module hash, or the decorator pins it — written at claim by the
workflow claim path. It is a RECORD, not a cache (K3's ``cache_key`` /
content-addressed machinery stays dead).

THE HASH COMPUTES VIA THE TORS CONTENT HASH — the golden-tested canonical
serializer. ``docs/design/tors-adoption-map.md`` recorded ``content_hash`` as
SKIP because "no canonical-hash call site exists"; T03's ``code_version``
column CREATES that call site, voiding the skip verdict's premise (the
adoption map's row updates at T14's maintenance tail; the rev's disposition
is ADOPT on T03's landing).

Why tors and not a hand-rolled ``hashlib`` walk: the cores are GIL-free —
the claim path runs on the worker's event loop, and a GIL-holding hash over
module source is exactly the ``kind=gil_held``/``blocking_call`` stall the
watchdog exists to attribute. Honest API note (verified on 0.16.0):
``content_hash`` has NO ``tors.aio`` twin — it is sync-but-GIL-free, so the
call stays short-input and loop-safe by construction.

§12.7-5's hash-instability lesson applies even to a record: a
dict-ordering/float-repr mutation must NOT change the hash (a false
invalidation is the silent failure — pin 12, red-first). ``content_hash`` is
verified canonical under dict ordering (the pin holds the door).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Final

from tors import content_hash

if TYPE_CHECKING:
    # Type-only export: JSONValue lives in tors' stubs, not its runtime
    # module -- future annotations make the string-form uses safe.
    from tors import JSONValue


__all__ = ["code_version_payload", "compute_code_version"]

#: The payload's schema tag, so a future payload-shape change invalidates
#: cleanly (a tag bump IS a code-version change, by construction).
_PAYLOAD_VERSION: Final[str] = "wf-code-version/v1"


def code_version_payload(
    module: str,
    qualname: str,
    source: str | None,
    pinned: str | None = None,
) -> JSONValue:
    """The canonical payload the version hash covers.

    Every field is NAMED (a dict, hashed canonically by tors — key order
    cannot matter), never a positional concat: adding a field changes every
    hash (a clean invalidation), reordering nothing.
    """
    # Annotated as JSONValue (not dict[str, str]): the stub's dict shape is
    # invariant in its value type, and the literal contextualizes cleanly.
    payload: JSONValue = {
        "schema": _PAYLOAD_VERSION,
        "module": module,
        "qualname": qualname,
        "source": source if source is not None else "",
        "pinned": pinned if pinned is not None else "",
    }
    return payload


def compute_code_version(
    module: str,
    qualname: str,
    source: str | None = None,
    *,
    pinned: str | None = None,
) -> str:
    """The code-version record for one step body.

    *pinned* (the decorator's explicit pin) participates in the hash when
    given — a re-pin IS a new code version. The hash is the tors canonical
    content hash over the named payload: dict-ordering-insensitive by
    construction, pinned so by test.
    """
    return content_hash(code_version_payload(module, qualname, source, pinned))
