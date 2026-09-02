"""Credential- and PII-safe exception text for telemetry spans and logs.

Spans and JSON log lines leave the trust boundary for whatever telemetry
backend is configured (Azure Monitor / Application Insights, an OTLP
collector, a vendor SaaS), so exception text must be scrubbed before it goes
there -- the same discipline :func:`taskq._dsn.dsn_host` already applies to
logs, which the span path bypassed entirely.

Two concrete leaks, both verified by execution rather than assumed:

* **Postgres error DETAIL carries row values.** ``str()`` of an asyncpg
  ``PostgresError`` appends the server's ``DETAIL:`` and ``HINT:`` lines, and
  for a constraint violation the DETAIL quotes the offending column values --
  ``Key (idempotency_key)=(customer-4417-...) already exists.`` TaskQ's
  ``idempotency_key``, ``identity_key`` and ``fairness_key`` are all
  caller-supplied and routinely carry tenant or subject identifiers.
* **Credentials in URI-shaped text.** Any ``scheme://user:password@host``
  appearing in a message is masked, so a DSN that reaches an exception by any
  route cannot be forwarded verbatim.

Scope, deliberately narrow: only ``DETAIL`` is dropped. ``HINT`` is Postgres's
suggested fix and ``CONTEXT`` is the PL/pgSQL call stack -- both structural,
neither quotes a row value, and both were previously deleted for no privacy
benefit. Losing them left an operator with a constraint name and nothing else.

The DETAIL drop is switchable off by :func:`set_exception_redaction_enabled`
(``TASKQ_EXCEPTION_REDACTION_ENABLED=false``) for advanced debugging, and the
worker warns loudly at startup when it is. The URI credential mask is NOT
switchable: no debugging case justifies shipping a password to a telemetry
vendor.

Note ``opentelemetry``'s ``Span.record_exception`` always derives
``exception.message`` from ``str(exception)`` with no hook to override it,
which is why this module emits its own ``exception`` event instead of calling
it.
"""

import re
import sys
import traceback
from types import TracebackType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.trace import Span

__all__ = [
    "EXCEPTION_MESSAGE_FIELDS",
    "EXCEPTION_TRACEBACK_FIELDS",
    "record_exception_safe",
    "safe_exception_message",
    "safe_exception_parts",
    "scrub_exception_field",
    "set_exception_message_max_chars",
    "set_exception_redaction_enabled",
]

#: Postgres DETAIL lines. Row values live there; the primary message above
#: them is a static template, and HINT/CONTEXT below them are structural --
#: this pattern deliberately does not match either.
#:
#: Matched LINE-WISE, not to end-of-string. A chained traceback renders several
#: exception messages, so a greedy DOTALL match starting at the first DETAIL
#: would delete every outer frame after it -- destroying the diagnostic while
#: appearing to work on a single-exception test.
_PG_DETAIL_RE = re.compile(r"^[ \t]*DETAIL:.*$", re.MULTILINE)

#: Companion to :data:`_PG_DETAIL_RE` for ``repr()``-flattened text.
#: ``repr(exc)`` renders the newline before DETAIL as the two
#: literal characters ``\n``, which the line-anchored pattern above cannot
#: see — and ``error=repr(exc)`` is a majority log idiom. Consumes from the
#: escaped newline up to (not including) the next escaped newline or the
#: end of the line; the closing-quote alternative (``['\"]\)?\s*$``) can
#: only succeed at end-of-line, so it keeps a repr's trailing ``')`` when
#: present without ever stopping the scrub early and leaving row values
#: behind. ``MULTILINE`` makes ``$`` match per real line, so a repr line
#: embedded in a rendered traceback (real newlines around it) is scrubbed
#: too. Optional escaped ``\r`` covers the CRLF boundary shape.
_PG_DETAIL_ESCAPED_RE = re.compile(
    r"(?:\\r)?\\n[ \t]*DETAIL:.*?(?=(?:\\r)?\\n|['\"]\)?\s*$)",
    re.MULTILINE,
)

#: userinfo in a URI. Group 1 is the scheme+user, group 2 the password.
_URI_CRED_RE = re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]+):([^\s@]+)@")

#: Default bound on scrubbed message text. 2000 to match
#: ``web/admin/jobs.py``'s ``_TRACEBACK_DISPLAY_LIMIT`` — one number for "how
#: much error text do we keep", not two. Overridable via
#: ``TASKQ_EXCEPTION_MESSAGE_MAX_CHARS`` because no single number suits both a
#: terse constraint violation and an actor that formats a large object into its
#: message. Truncation reports the remainder rather than ending mid-sentence, so
#: an operator can see text was dropped and raise the bound.
_DEFAULT_MAX_MESSAGE_CHARS = 2000

_max_message_chars: int = _DEFAULT_MAX_MESSAGE_CHARS

#: Whether DETAIL lines are dropped. Default True: the safe behaviour is what
#: an operator gets by doing nothing. Set False by worker startup from
#: ``TASKQ_EXCEPTION_REDACTION_ENABLED``.
_redaction_enabled: bool = True


def set_exception_redaction_enabled(enabled: bool) -> None:
    """Set the module-level exception-redaction flag.

    Mirrors :func:`taskq.obs.set_otel_enabled`: worker startup calls this once
    after loading ``WorkerSettings`` so every scrub site reads a module global
    instead of importing ``settings`` (which would be a circular import from
    the modules that depend on ``obs``).

    Passing ``False`` disables the DETAIL drop on BOTH the span and the log
    channel -- they share :func:`_scrub_text`, so the toggle cannot be applied
    to one and not the other. It does NOT disable the URI credential mask.
    """
    global _redaction_enabled
    _redaction_enabled = enabled


def _scrub_text(text: str) -> str:
    """Drop Postgres DETAIL lines and mask URI credentials.

    Both newline forms are covered: real newlines (``str(exc)``) by
    :data:`_PG_DETAIL_RE`, and the literal ``\\n`` ``repr()`` flattens them
    into by :data:`_PG_DETAIL_ESCAPED_RE`.

    The credential mask is applied unconditionally, outside the
    ``_redaction_enabled`` guard: the debugging case that wants a row value
    never wants a password, and a DSN reaching a telemetry vendor is a
    credential disclosure regardless of why redaction was relaxed.
    """
    if _redaction_enabled:
        text = _PG_DETAIL_RE.sub("", text)
        text = _PG_DETAIL_ESCAPED_RE.sub("", text)
    return _URI_CRED_RE.sub(r"\1:***@", text)


def set_exception_message_max_chars(limit: int) -> None:
    """Set the module-level bound on scrubbed message text.

    Mirrors :func:`set_exception_redaction_enabled`: a module global set once
    at worker startup, so the obs layer needs no import of settings.
    """
    global _max_message_chars
    _max_message_chars = limit


def _bound_message(text: str) -> str:
    """Strip and length-bound scrubbed message text.

    Reports the dropped character count, matching ``_truncate_traceback`` in
    the admin UI — a bare "...[truncated]" hides how much is missing, so an
    operator cannot tell whether raising the bound would help.
    """
    text = text.strip()
    if len(text) <= _max_message_chars:
        return text
    remaining = len(text) - _max_message_chars
    return text[:_max_message_chars] + f"... ({remaining} more characters)"


def safe_exception_message(exc: BaseException) -> str:
    """Exception text with the Postgres DETAIL dropped and URI creds masked.

    The primary Postgres message is kept: it is a static template naming the
    constraint or relation, which is the part that is actually diagnostic.
    ``HINT`` and ``CONTEXT`` are kept for the same reason -- neither carries
    row values, and both are what an operator reads next.
    """
    return _bound_message(_scrub_text(str(exc)))


def _safe_stacktrace(exc: BaseException) -> str:
    """Formatted traceback, scrubbed the same way.

    A traceback's final line is the exception repr, so the same DETAIL text
    reappears there if it is not stripped. Chained causes are included, so each
    of their messages needs the same treatment -- hence scrubbing the rendered
    string rather than only the head exception.

    Caveat worth knowing: a traceback also quotes the SOURCE LINE of each
    frame. Those come from TaskQ's own source, not from data, so they carry no
    row values -- but a secret written as a literal in application code would
    appear. Do not put credentials in source.
    """
    return _scrub_text("".join(traceback.format_exception(type(exc), exc, exc.__traceback__)))


#: A validated ``(cls, exc, tb)`` triple ready for ``traceback.format_exception``.
_ResolvedExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]

#: What structlog event dicts can carry on the ``exc_info`` key. Untrusted
#: boundary input: the tuple members are ``object`` until validated at runtime,
#: so the ``_resolve_exc_info`` narrowing checks are load-bearing, not redundant.
_ExcInfoInput = bool | BaseException | tuple[object, object, object] | None


def _resolve_exc_info(value: _ExcInfoInput) -> _ResolvedExcInfo | None:
    """Resolve structlog-style ``exc_info`` into a real ``(cls, exc, tb)`` triple.

    Mirrors the documented semantics of structlog's ``format_exc_info``: a
    ``BaseException`` instance, a valid 3-tuple, or any other truthy value
    resolved against ``sys.exc_info()``. Returns ``None`` when *value* does
    not represent an exception or no exception is currently being handled.
    """
    if isinstance(value, BaseException):
        return (value.__class__, value, value.__traceback__)
    if isinstance(value, tuple) and len(value) == 3:
        cls, exc, tb = value
        if (
            isinstance(cls, type)
            and issubclass(cls, BaseException)
            and isinstance(exc, BaseException)
            and (tb is None or isinstance(tb, TracebackType))
        ):
            return (cls, exc, tb)
    if value:
        live = sys.exc_info()
        if live == (None, None, None):
            return None
        cls, exc, tb = live
        if cls is None or exc is None:
            return None
        return (cls, exc, tb)
    return None


def safe_exception_parts(exc_info: _ExcInfoInput) -> dict[str, str] | None:
    """Render structlog-style ``exc_info`` into scrubbed ``exception.*`` parts.

    Returns the same attribute names :func:`record_exception_safe` emits on
    spans (``exception.type`` / ``exception.message`` / ``exception.stacktrace``)
    so both telemetry channels share one scrubbed shape, or ``None`` when
    *exc_info* resolves to nothing (structlog's behavior of leaving the event
    dict without an exception entry).
    """
    resolved = _resolve_exc_info(exc_info)
    if resolved is None:
        return None
    _cls, exc, _tb = resolved
    return {
        "exception.type": type(exc).__qualname__,
        "exception.message": safe_exception_message(exc),
        "exception.stacktrace": _scrub_text("".join(traceback.format_exception(*resolved))),
    }


def record_exception_safe(span: "Span", exc: BaseException) -> None:
    """Record *exc* on *span* without leaking row values or credentials.

    Emits the same ``exception`` event shape the OTel semantic conventions
    define, so backends that special-case it still render an exception.
    """
    span.add_event(
        "exception",
        attributes={
            "exception.type": type(exc).__qualname__,
            "exception.message": safe_exception_message(exc),
            "exception.stacktrace": _safe_stacktrace(exc),
        },
    )


#: Event-dict field names that conventionally carry exception MESSAGE text on
#: the log channel. Derived from the log sites in ``src/taskq`` that render
#: exception text into a field (``error=…``, ``error_message=…``, the
#: terminal-write log's ``job_error_message``/``infra_error_message`` …) —
#: NOT an automatically exhaustive set: when a new log field is introduced
#: whose value is rendered exception text (``str(exc)``/``repr(exc)``/
#: ``traceback.format_exception``), its name must be added here or the JSON
#: channel ships it unredacted. ``test_log_fields_carrying_exception_text_…``
#: in tests/test_obs_exception_redaction.py guards the ``*error_message``/
#: ``*error_traceback`` suffix family against exactly that drift; values
#: that are classification strings ("deadline_exceeded") pass the scrubbers
#: unchanged, so scrubbing only bites text that genuinely carries exception
#: detail.
EXCEPTION_MESSAGE_FIELDS = frozenset(
    {"error", "error_message", "exc", "job_error_message", "infra_error_message"}
)

#: Event-dict field names that conventionally carry rendered TRACEBACK text —
#: scrubbed line-wise like :func:`_safe_stacktrace`, without the message-length
#: bound, so the traceback stays diagnostic. Same derivation and guard
#: contract as :data:`EXCEPTION_MESSAGE_FIELDS`.
EXCEPTION_TRACEBACK_FIELDS = frozenset(
    {"error_traceback", "job_error_traceback", "infra_error_traceback"}
)


def scrub_exception_field(field: str, value: object) -> object:
    """Scrub a known exception-bearing log-field value; everything else passes through.

    Exception objects render as the scrubbed safe message (they previously
    reached the orjson fallback and dropped the whole log line). Strings in
    message-style fields get the message scrub; strings in traceback-style
    fields get the line-wise stacktrace scrub. Non-string, non-exception
    values (ints, bools, None) are returned unchanged.
    """
    if isinstance(value, BaseException):
        return safe_exception_message(value)
    if not isinstance(value, str):
        return value
    if field in EXCEPTION_TRACEBACK_FIELDS:
        return _scrub_text(value)
    return _bound_message(_scrub_text(value))
