"""orjson-backed JSON helpers.

The library never imports stdlib ``json`` directly. Use ``dumps`` / ``loads``
from this module so behaviour is consistent and the serialization hot path
stays fast.

``loads`` does NOT revive UUID-like strings into :class:`uuid.UUID` objects.
Type coercion is the responsibility of the consuming Pydantic model
(``model_validate`` coerces strings to ``UUID`` when the field is typed
``UUID``, and keeps them as ``str`` when the field is typed ``str``).
This respects the principle of least surprise: the developer's declared
field type is the source of truth, not the deserializer's guess.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Final

import orjson

from taskq.exceptions import UnencodableValue

__all__ = [
    "NUL_JSONB_ERROR",
    "check_no_nul_str",
    "decode_result_bytes",
    "dumps",
    "dumps_jsonb_str",
    "dumps_str",
    "embed_encoded",
    "loads",
    "sanitize_nul_str",
    "sanitize_surrogates",
    "structlog_serializer",
]

# orjson renders a NUL codepoint as exactly these six bytes.
_NUL_ESCAPE_BYTES = b"\\u0000"

NUL_JSONB_ERROR: Final[str] = (
    "value contains a NUL character (U+0000), which PostgreSQL cannot "
    "store in a jsonb column; strip control characters before storing"
)
"""The exact message :func:`dumps_jsonb_str` raises for a NUL payload.

Declared once (not per call site) so every path that rejects a NUL, the
dict form here, and the byte-level scan over already-serialized result
bytes in the terminal writes, raises byte-for-byte the same ValueError
and no site can drift behind the wording tests pin.
"""


def _orjson_fallback(obj: Any) -> Any:
    """Convert types orjson can't serialize natively to a JSON-safe form.

    Only reached when *obj* is not a type orjson handles (UUID, datetime,
    str, int, float, bool, None, list, dict).  Kept fast-path: the vast
    majority of values never hit this function.
    """
    cls: type[Any] = type(obj)  # pyright: ignore[reportUnknownVariableType]  # Why: obj is Any from orjson's default function; type() always returns a valid type object.
    mod = cls.__module__
    name = cls.__qualname__

    # asyncpg protocol-level UUID, raw record access can leak these into
    # structlog event dicts; convert to standard UUID string form.
    if mod.startswith("asyncpg") and "UUID" in name:
        return str(obj)

    # bytes in a log event dict, decode with replacement.
    if isinstance(obj, (bytes, bytearray)):
        return obj.decode("utf-8", errors="replace")

    raise TypeError(f"Type is not JSON serializable: {mod}.{name}")


def dumps(value: Any, /) -> bytes:
    """Serialize to bytes. Uses orjson defaults (UTC datetimes, UUID, etc.).

    Requires ``str`` dict keys: ``OPT_NON_STR_KEYS`` is deliberately not
    set, so a dict with ``int`` (or other non-``str``) keys raises
    ``TypeError`` instead of being silently coerced to strings. Dropping
    the flag is 1.29-1.73x faster on str-keyed input (its only effect
    there).

    This is a contract change for raw (unvalidated) caller dicts ,
    ``jobs.enqueue(metadata={1: ...})``, an actor returning ``{1: ...}``,
    and ``ctx.progress(data={1: ...})`` (whose size check and PG flush
    dump the dict directly) previously serialized via silent ``{"1": ...}``
    coercion and now raise ``TypeError``. Pydantic-validated payloads are
    unaffected: ``dict[str, ...]`` model fields REJECT non-str keys at
    validation (they do not coerce), so a payload that reaches
    :class:`~taskq.backend._protocol.EnqueueArgs` through
    ``jobs.enqueue`` already has string keys. Non-str keys were always
    lossy on the wire, JSON objects and PG ``jsonb`` can only carry
    string keys, so failing fast surfaces at the boundary what used to
    surface as a silently rewritten key on read-back. NUL handling
    (``dumps_jsonb_str``) and all other behaviour are unchanged.

    Every ``TypeError`` orjson raises here, a lone surrogate, a
    non-``str`` dict key, an object the fallback cannot convert, is
    re-raised as :class:`~taskq.exceptions.UnencodableValue` (a
    ``TypeError`` subclass, so ``except TypeError`` callers and wording
    pins are unaffected). The distinct class is what lets the retry
    classifier fail a deterministic encoding defect non-retryably, and
    what lets the durable-write boundary recognize exactly the family it
    escapes rather than strands.
    """
    try:
        return orjson.dumps(
            value,
            default=_orjson_fallback,
            option=orjson.OPT_NAIVE_UTC | orjson.OPT_UTC_Z,
        )
    except TypeError as exc:
        raise UnencodableValue(str(exc)) from exc


def embed_encoded(data: bytes, /) -> orjson.Fragment:
    """Wrap :func:`dumps` output so a containing document embeds it verbatim.

    orjson emits a nested value exactly as it emits that value at top
    level, so a document built around the embedded bytes is byte-identical
    to one that encodes the original value in place, for the price of a
    copy of the bytes rather than a second walk of the value. The bytes
    must be :func:`dumps` output: orjson embeds a fragment unvalidated.
    """
    return orjson.Fragment(data)


def dumps_str(value: Any, /) -> str:
    """Serialize to ``str``. Use only when the consumer demands text (e.g.,
    asyncpg jsonb codec). Prefer :func:`dumps` for everything else."""
    return dumps(value).decode("utf-8")


# Optional accelerator, probed once at import (the [text-accel] extra).
# tors.contains_unescaped is the escape-parity byte scan the pure fallback
# below spells out, in Rust over two zero-copy PyBytes borrows with the GIL
# released -- the same algorithm, not a near one, so the dispatch cannot
# move a verdict (tests/test_tors_nul_parity.py pins the differential on
# the adversarial shapes and a randomized sweep). The probe is
# attribute-level on purpose: a tors build that predates
# contains_unescaped degrades to the pure path instead of breaking the
# import, and the package's absence (the default install) is a supported
# configuration, not an error. No other tors surface is adopted: the
# redaction scrub and payload_hash are regex/sha256 contracts whose
# observables a tors primitive would change, and the normalization/
# chunking APIs have no TaskQ call site at this boundary.
if TYPE_CHECKING:
    from collections.abc import Callable

    #: The escape-parity scan's signature, spelled once so the probe's
    #: DECLARED type survives tors's absence: a contributor without the
    #: extra cannot resolve the import, and an inferred (Unknown) probe
    #: would spray unknown-type reports over every dispatch site.
    _EscapeParityScan = Callable[[bytes, bytes], bool]

# The declared annotation is load-bearing for the type checker, not the
# runtime: it makes the probe's type independent of whether tors resolved.
_tors_contains_unescaped: _EscapeParityScan | None
# Why: the optional [text-accel] accelerator -- the absence branch below is
# the supported default, so a missing package is probed, never an import
# error, and an unresolvable import must not leave the probe Unknown (the
# declared annotation above types every dispatch site either way).
try:
    from tors import (  # pyright: ignore[reportMissingImports]
        contains_unescaped as _tors_contains_unescaped,  # pyright: ignore[reportUnknownVariableType]  # Why: an unresolvable import must not leave the probe Unknown -- the declared annotation above types every dispatch site either way.
    )
except ImportError:  # pragma: no cover - exercised with tors absent (the default install)
    _tors_contains_unescaped = None


def _pure_encoded_has_nul(data: bytes, /) -> bool:
    """The pure-Python escape-parity scan; the reference :func:`_encoded_has_nul` delegates to when tors is absent.

    orjson renders the *literal text* ``\\u0000`` as an escaped backslash
    followed by the same six bytes, so a raw byte match is ambiguous. Each
    match is confirmed by counting the backslashes immediately before it:
    an even run means the escape is live (a real NUL); an odd run means the
    match's leading backslash closes a ``\\\\`` pair and the sequence is the
    literal six characters, which ``jsonb`` accepts.
    """
    pos = data.find(_NUL_ESCAPE_BYTES)
    while pos != -1:
        backslashes = 0
        cursor = pos - 1
        while cursor >= 0 and data[cursor : cursor + 1] == b"\\":
            backslashes += 1
            cursor -= 1
        if backslashes % 2 == 0:
            return True
        pos = data.find(_NUL_ESCAPE_BYTES, pos + 1)
    return False


def _encoded_has_nul(data: bytes, /) -> bool:
    """True when *data* (orjson output) encodes a real NUL codepoint.

    Dispatches on the import-time probe above: tors's
    ``contains_unescaped`` (Rust, zero-copy, GIL-released) when the
    ``[text-accel]`` extra is installed and current enough, the pure
    :func:`_pure_encoded_has_nul` loop otherwise. Both compute the same
    predicate -- an occurrence of :data:`_NUL_ESCAPE_BYTES` whose
    immediately-preceding backslash run has even length -- so the verdicts
    are byte-identical whichever path serves the call; the extra changes
    the cost, never the answer.
    """
    if _tors_contains_unescaped is not None:
        return _tors_contains_unescaped(data, _NUL_ESCAPE_BYTES)
    return _pure_encoded_has_nul(data)


def dumps_jsonb_str(value: Any, /) -> str:
    """Serialize for binding to a PostgreSQL ``jsonb`` parameter.

    Identical to :func:`dumps_str`, except a NUL (U+0000) anywhere in the
    value is rejected here rather than by the database.

    ``\\u0000`` is valid JSON and a ``json`` column stores it happily, but
    ``jsonb`` decodes to ``text``, which cannot hold a NUL, so ``jsonb_in``
    fails with SQLSTATE 22P05.  Caller-supplied payloads, metadata and actor
    results reach ``jsonb`` columns after type-and-size validation that says
    nothing about content, so without this guard a single NUL surfaces as an
    ``asyncpg.UntranslatableCharacterError`` raised from deep inside the
    INSERT: opaque to an enqueue caller, and worse on the terminal-write
    path, where ``_TERMINAL_WRITE_INFRA_EXCEPTIONS`` reads any
    ``PostgresError`` as transient infrastructure failure.  The job is then
    never marked failed, it stays ``running`` until the lease sweep reclaims
    it, re-runs, produces the same NUL, and loops, re-executing the actor's
    already-committed side effects each time.  Raising a ``ValueError`` here
    keeps that classification honest: it is a permanent data defect, so the
    normal actor-failure path handles it.

    Refusal, not escape, is this boundary's contract for values no UTF-8
    encoder accepts (a lone surrogate, NUL's mirror defect), exactly as it
    is for NUL itself: the enqueue paths bind caller-supplied
    payloads/metadata/tags through here, and caller input must fail fast
    at the door with the typed :class:`~taskq.exceptions.UnencodableValue`
    rather than be silently rewritten to an escaped form. The escape for
    values the actor already produced, progress state reaching a durable
    write, lives at the consumption sites
    (:func:`sanitize_surrogates`), mirroring how the NUL family splits
    this function (refuse) from :func:`sanitize_nul_str` (escape derived
    text at its bind sites).

    The NUL scan runs on the encoded bytes: :func:`_encoded_has_nul`
    prefilters with a byte-level ``find`` and confirms a hit by backslash
    parity, so neither a decode nor a parse-and-walk of the payload is
    needed to separate a real NUL from the literal text ``\\u0000``.
    """
    data = dumps(value)
    if _encoded_has_nul(data):
        raise ValueError(NUL_JSONB_ERROR)
    return data.decode("utf-8")


def check_no_nul_str(value: str, /, *, what: str = "value") -> None:
    """Raise ``ValueError`` if *value* contains a NUL (U+0000) codepoint.

    For callers binding plain text directly (a ``text`` or ``text[]``
    parameter) rather than transiting jsonb, so :func:`dumps_jsonb_str`
    doesn't apply. PostgreSQL rejects a NUL in a ``text`` value with
    ``CharacterNotInRepertoireError`` (SQLSTATE 22021) -- a
    ``PostgresError`` subclass, exactly like jsonb's
    ``UntranslatableCharacterError`` that :func:`dumps_jsonb_str` guards
    against, and it trips the same ``_TERMINAL_WRITE_INFRA_EXCEPTIONS``
    misclassification: a permanent data defect read as transient infra
    failure, so the job retries forever instead of failing. Raising a
    ``ValueError`` here, before the value ever reaches the pool, keeps
    that classification honest.
    """
    if "\x00" in value:
        raise ValueError(
            f"{what} contains a NUL character (U+0000), which PostgreSQL cannot "
            "store in a text column; strip control characters before storing"
        )


def sanitize_nul_str(value: str, /) -> str:
    """Replace NUL codepoints in *value* with the visible ``\\x00`` escape.

    For text DERIVED from uncontrolled sources, an actor exception's
    message or formatted traceback, where rejecting (as
    :func:`check_no_nul_str` does for caller-supplied values) would strand
    the very work the text describes: the terminal write fails, the job
    never reaches a terminal state, and the crash-reclaim loop re-dispatches
    it into the same exception forever. Replacing keeps the write valid and
    keeps the defect diagnosable: the stored text shows exactly where the
    NUL was.
    """
    return value.replace("\x00", "\\x00")


def sanitize_surrogates(value: Any, /) -> Any:
    """Rewrite unencodable codepoints into their visible backslash-escaped form.

    The object-walking sibling of :func:`sanitize_nul_str`, for values the
    actor already produced (progress state reaching a durable write): a
    lone surrogate, ``"\\udcff"``, exactly what ``os.fsdecode`` of a
    non-UTF-8 filename byte yields, is a legal Python ``str`` that no
    UTF-8 encoder accepts, so no ``text``/``jsonb`` form of it exists.
    Where rejecting (as :func:`dumps_jsonb_str` does for caller-supplied
    values) would strand the very work the value describes, the terminal
    write fails, the job never reaches a terminal state, and the
    crash-reclaim loop re-dispatches it into the same value forever ,
    the escaped form keeps the write valid and the defect diagnosable:
    the stored state shows exactly where the unencodable codepoint was.

    ``str.encode("utf-8", "backslashreplace")`` is the identity for any
    string a UTF-8 encoder already accepts, so the walk is a no-op on
    clean values and callers pay it only on the cold path a
    :func:`dumps` attempt already rejected. Containers are walked; every
    other type passes through unchanged, so a value refused for a
    non-encoding reason (a non-``str`` dict key, over-deep nesting)
    raises again on the retry.
    """
    if isinstance(value, str):
        return value.encode("utf-8", "backslashreplace").decode("utf-8")
    if isinstance(value, dict):
        return {
            sanitize_surrogates(k): sanitize_surrogates(v)
            for k, v in value.items()  # pyright: ignore[reportUnknownVariableType]  # Why: the parameter is Any by contract (the walk repairs caller-agnostic JSON values), so the comprehension's k/v inherit Unknown; every branch returns the walked shape.
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_surrogates(v) for v in value]  # pyright: ignore[reportUnknownVariableType]  # Why: same Any-contract walk as the dict branch.
    return value


def loads(data: bytes | bytearray | memoryview | str, /) -> Any:
    """Deserialize bytes or text to a Python value.

    Returns plain Python types (str, int, float, bool, None, list, dict).
    UUID-like strings remain ``str``, the consuming Pydantic model's
    ``model_validate`` coerces them to ``UUID`` when the field is typed
    ``UUID``, and keeps them as ``str`` when the field is typed ``str``.
    """
    return orjson.loads(data)


def decode_result_bytes(data: bytes, /) -> Any:
    """Decode pre-serialized ``result_bytes`` for storage, rejecting
     undecodable input with the canonical message.

     Both backends' ``result_bytes`` boundaries call this one helper, so a
     bad encoding raises a byte-identical ``ValueError`` on either backend
    , the standard the NUL and empty guards already hold. Bound as text
     and cast server-side, the same bytes would surface as a
     ``PostgresError`` the terminal-write classification reads as
     transient infrastructure, looping the job through reclaim on a
     permanent data defect; orjson output is always valid JSON, so this
     fires only for direct Backend-protocol callers. The parse also
     proves the bytes are decodable utf-8 (orjson rejects undecodable
     input with the same exception family), so a caller binding
     ``data.decode("utf-8")`` afterwards cannot fail there.
    """
    try:
        return loads(data)
    except ValueError as exc:
        raise ValueError(
            "result_bytes must be valid orjson output (taskq._json.dumps); "
            f"the bytes are not decodable JSON ({exc})"
        ) from exc


def structlog_serializer(value: Any, /, **_kwargs: Any) -> str:
    """Serialize to ``str`` for structlog's ``JSONRenderer(serializer=...)``.

    Accepts and ignores ``**_kwargs`` (e.g. ``default``) that structlog passes
    internally, orjson handles all types we encounter natively and does not
    use the ``default`` fallback that stdlib ``json`` requires.
    """
    return dumps_str(value)
