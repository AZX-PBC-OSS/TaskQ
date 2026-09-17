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
  (the empty-username ``scheme://:password@host`` form included) and any
  password-family connection parameter -- query string (``?password=…``) or
  libpq keyword/value (``host=db … password=…``, single-quoted values
  included), in any casing -- appearing in a message is masked, so a DSN
  that reaches an exception by any route cannot be forwarded verbatim, in
  whichever spelling it carries the credential.

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
from dataclasses import dataclass
from types import TracebackType
from typing import TYPE_CHECKING

from opentelemetry.trace import StatusCode

from taskq._json import sanitize_nul_str

if TYPE_CHECKING:
    from opentelemetry.trace import Span

__all__ = [
    "EXCEPTION_MESSAGE_FIELDS",
    "EXCEPTION_TRACEBACK_FIELDS",
    "ExceptionText",
    "ScrubbedText",
    "add_exception_event",
    "record_exception_safe",
    "record_exception_text",
    "render_exception",
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
#:
#: The optional ``(?:[ \t]*[|+][ \t]*)*`` prefix absorbs
#: ``traceback.format_exception``'s ``ExceptionGroup`` rendering, which
#: indents every line of a sub-exception with a repeated ``| `` (or, on a
#: group's own header/separator lines, ``+``) marker -- one added layer per
#: level of nesting -- before the exception's own text. Without it, a DETAIL
#: line inside a grouped or ``except*``-caught sub-exception reads
#: ``    | DETAIL:  Key (...)=(...) already exists.`` and the anchor on a
#: bare ``^[ \t]*`` never reaches past the marker, so the row value ships to
#: the span/log unredacted. The prefix is still consumed only when it is
#: immediately followed by ``DETAIL:`` -- a header line such as
#: ``  | ExceptionGroup: ...`` does not itself start with ``DETAIL:`` and so
#: is not touched.
_PG_DETAIL_RE = re.compile(r"^(?:[ \t]*[|+][ \t]*)*[ \t]*DETAIL:.*$", re.MULTILINE)

#: Companion to :data:`_PG_DETAIL_RE` for ``repr()``-flattened text.
#: ``repr(exc)`` renders the newline before DETAIL as the two
#: literal characters ``\n``, which the line-anchored pattern above cannot
#: see — and ``error=repr(exc)`` is a majority log idiom. Consumes from the
#: escaped newline up to (not including) the next escaped newline, or up to
#: the repr tail: a quote followed by the run of ``)``/``]`` closers a
#: ``repr()`` ends with (``')`` for a plain exception, ``')])`` once the
#: exception sits in an ``ExceptionGroup``'s list, one more ``])`` per
#: nesting level) at end of line. The closing alternatives can only succeed
#: at end-of-line, so they keep a repr's trailing closers when present
#: without ever stopping the scrub early and leaving row values behind. The
#: final bare ``$`` leg is fail-closed: a DETAIL whose tail matches NEITHER
#: safe delimiter (an unterminated repr, or one embedded mid-line with more
#: text after it) is scrubbed through end of line rather than shipped — a
#: delimiter miss must delete more text, never less of the secret.
#: ``MULTILINE`` makes ``$`` match per real line, so a repr line embedded in
#: a rendered traceback (real newlines around it) is scrubbed too. Optional
#: escaped ``\r`` covers the CRLF boundary shape.
_PG_DETAIL_ESCAPED_RE = re.compile(
    r"(?:\\r)?\\n[ \t]*DETAIL:.*?(?=(?:\\r)?\\n|['\"][)\]]*\s*$|$)",
    re.MULTILINE,
)

#: userinfo in a URI. Group 1 is the scheme+user, group 2 the password.
#:
#: The username class is ``*``, not ``+``: an EMPTY username is a real shape —
#: ``postgresql://:SECRET@host/db`` is what a DSN renders when only a
#: password is set — and ``+`` skipped it entirely, shipping the password
#: verbatim. With ``*`` group 1 is the bare scheme prefix, so the masked
#: form still reads ``scheme://:***@host``.
_URI_CRED_RE = re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]*):([^\s@]+)@")

#: Connection-parameter names whose value is credential material. Kept tight
#: to the password family: broader names (``secret``, ``token``, …) would
#: redact non-credential parameters, which is its own bug. ``sslpassword`` is
#: the passphrase for the client TLS key — a credential in its own right.
#:
#: Spelled once, in one case, and compiled into the matcher below rather than
#: written out as literals inside a pattern: a hand-maintained list of exact
#: spellings is what let case variants and ``sslpassword`` through, and a
#: derived matcher makes the next spelling a one-word edit here.
_CRED_PARAM_NAMES = ("password", "passphrase", "passwd", "pwd", "sslpassword")

#: password-family credentials in a connection string, in BOTH spellings that
#: carry one. Group 1 is the delimiter plus the parameter name — kept verbatim
#: so the masked form still names which setting carried the credential — and
#: group 2 is the value.
#:
#: Delimiters cover the URI query string (``?password=…`` / ``&password=…``)
#: and the libpq keyword/value conninfo form (``host=db … password=…``), which
#: carries neither ``://`` nor ``?`` and so slipped past a query-only matcher.
#: The keyword form's delimiter is a boundary, expressed as a lookbehind for
#: anything that could be the tail of a LONGER parameter name, so ``cpwd=`` is
#: not mistaken for ``pwd=``.
#:
#: ``IGNORECASE``: libpq parameter names are case-insensitive, and psql, ORMs
#: and operator-typed DSNs echo back whatever casing was written, so a matcher
#: keyed to one exact spelling ships the value verbatim in every other.
#:
#: The value is either a libpq single-quoted string — which may carry spaces
#: and honours the ``\'`` and ``\\`` escapes, so the quote run must be
#: consumed whole or the tail of the secret rides along after the ``***`` —
#: or an unquoted token. The unquoted class stops only at whitespace and
#: ``&`` (the next parameter). It deliberately does NOT stop at ``@``: a
#: password may legally contain an unencoded ``@``, and a matcher that
#: treats it as a boundary leaves the tail of the secret riding along after
#: the ``***``.
_URI_PARAM_CRED_RE = re.compile(
    r"((?:[?&]|(?<![A-Za-z0-9_]))(?:"
    + "|".join(_CRED_PARAM_NAMES)
    + r")=)('(?:[^'\\]|\\.)*'|[^\s&]+)",
    re.IGNORECASE,
)

#: Lowercased trigger substrings for :data:`_URI_PARAM_CRED_RE`'s prefilter.
#: Derived from the same name tuple, so a name added above is guarded here
#: without a second edit — a prefilter that drifts from its pattern silently
#: stops masking.
_CRED_PARAM_TRIGGERS = tuple(f"{name}=" for name in _CRED_PARAM_NAMES)

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

    Both credential shapes are masked: userinfo (``scheme://user:pass@host``,
    empty username included) by :data:`_URI_CRED_RE`, then password-family
    connection parameters — query-string and libpq keyword/value alike, in any
    casing — by :data:`_URI_PARAM_CRED_RE`. The order is what makes a DSN
    carrying both at once safe (``scheme://user:SECRET@host/db?password=OTHER``):
    the userinfo mask runs first and claims the password up to the FIRST
    ``@``, so an RFC 3986-shaped DSN leaves the parameter mask a string whose
    only ``@`` is the one the userinfo mask wrote ``***`` in front of. The
    boundary really is the first ``@``, not the RFC 3986 userinfo end: a
    password carrying an unencoded ``@`` (``scheme://user:SEC@RET@host``) is
    masked only up to it and the tail (``RET``) rides through. That is
    accepted rather than guessed around: an unencoded ``@`` is not valid in
    userinfo (RFC 3986 requires percent-encoding), and in arbitrary non-URI
    text a later ``@`` more often belongs to the next token (an email
    address, a mention) than to the password, so last-``@`` matching would
    over-delete diagnostics to catch a malformed shape. Neither mask's
    ``***`` output contains anything the other regex can re-match — each
    fires exactly once.

    The credential masks are applied unconditionally, outside the
    ``_redaction_enabled`` guard: the debugging case that wants a row value
    never wants a password, and a DSN reaching a telemetry vendor is a
    credential disclosure regardless of why redaction was relaxed.

    Each regex is behind a substring prefilter stating a NECESSARY condition
    for that pattern to match at all, derived from the pattern text:

    * ``_PG_DETAIL_RE`` anchors a line on the literal ``DETAIL:`` and
      ``_PG_DETAIL_ESCAPED_RE`` matches it after an escaped newline — both
      require ``"DETAIL:"`` in the subject.
    * ``_URI_CRED_RE`` requires a ``scheme://`` separator.
    * ``_URI_PARAM_CRED_RE`` requires a password-family parameter name
      followed by ``=``, compared case-insensitively to match the pattern —
      and deliberately NOT ``://``: bare ``host/db?password=…`` and libpq
      ``host=db … password=…`` text must stay masked, so the guard is on the
      parameter names, not a scheme.

    Skipping a substitution when its trigger substring is absent cannot
    change the output (the pattern could not have matched), which collapses
    the four regex passes to three substring scans for the common
    error-bearing log field — the cost that matters at error-storm rates.
    """
    if _redaction_enabled and "DETAIL:" in text:
        text = _PG_DETAIL_RE.sub("", text)
        text = _PG_DETAIL_ESCAPED_RE.sub("", text)
    if "://" in text:
        text = _URI_CRED_RE.sub(r"\1:***@", text)
    lowered = text.lower()
    if any(trigger in lowered for trigger in _CRED_PARAM_TRIGGERS):
        return _URI_PARAM_CRED_RE.sub(r"\1***", text)
    return text


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


class ScrubbedText(str):
    """Exception text that has been through :func:`_scrub_text`.

    A ``str`` in every other respect, so every renderer and serializer treats
    it as plain text. The type is the invariant: the log processor
    (``_scrub_exception_fields``) passes a ``ScrubbedText`` field through
    untouched, which is what lets a handler scrub a traceback once and emit it
    on several log lines. Only this module constructs one from freshly
    rendered text; a caller that needs one holds a value that already is one.
    """

    __slots__ = ()

    def nul_escaped(self) -> "ScrubbedText":
        """The same text with NUL codepoints rewritten to the visible ``\\x00``.

        Scrubbing must run BEFORE the escape, never after: the credential
        mask's keyword boundary is a lookbehind for a non-word character, and
        the escape's trailing ``0`` satisfies it in the wrong direction, so
        ``\\x00password=…`` escaped first ships the password. The escape
        itself introduces only backslash, ``x`` and ``0`` -- it cannot
        reassemble a DETAIL line or a credential the scrub removed, so the
        result keeps its scrubbed standing.
        """
        return ScrubbedText(sanitize_nul_str(self))


@dataclass(frozen=True, slots=True)
class ExceptionText:
    """One rendering of an exception, shared by every sink that reports it.

    A failed attempt is reported on the ``attempt.N`` span, on the
    ``job_exception`` and ``job-failed`` log lines and in the durable
    ``ErrorInfo``. Rendering the traceback and scrubbing it are the dominant
    CPU cost of a failed job (a 27-frame traceback is ~0.8 ms to render and
    ~0.4-0.8 ms to scrub, GIL-held), so :func:`render_exception` does each
    once and the sinks are handed this value rather than the exception.

    ``raw_stacktrace`` is unscrubbed: the durable row lives inside the trust
    boundary and keeps the DETAIL row values the operator needs; only the
    telemetry-bound text is scrubbed.
    """

    type_name: str
    """``__qualname__`` of the exception class, as OTel's ``exception.type``."""

    raw_stacktrace: str
    """``traceback.format_exception`` output, verbatim."""

    message: ScrubbedText
    """``str(exc)`` scrubbed and length-bounded -- see :func:`safe_exception_message`."""

    stacktrace: ScrubbedText
    """``raw_stacktrace`` scrubbed line-wise, never length-bounded, so it stays diagnostic."""


def render_exception(exc: BaseException) -> ExceptionText:
    """Render and scrub *exc* once, for every sink that reports it.

    A traceback's final line is the exception repr, so the same DETAIL text
    reappears there if it is not stripped. Chained causes are included, so each
    of their messages needs the same treatment -- hence scrubbing the rendered
    string rather than only the head exception.

    Caveat worth knowing: a traceback also quotes the SOURCE LINE of each
    frame. Those come from TaskQ's own source, not from data, so they carry no
    row values -- but a secret written as a literal in application code would
    appear. Do not put credentials in source.
    """
    raw_stacktrace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return ExceptionText(
        type_name=type(exc).__qualname__,
        raw_stacktrace=raw_stacktrace,
        message=ScrubbedText(_bound_message(_scrub_text(str(exc)))),
        stacktrace=ScrubbedText(_scrub_text(raw_stacktrace)),
    )


def safe_exception_message(exc: BaseException) -> str:
    """Exception text with the Postgres DETAIL dropped and URI creds masked.

    The primary Postgres message is kept: it is a static template naming the
    constraint or relation, which is the part that is actually diagnostic.
    ``HINT`` and ``CONTEXT`` are kept for the same reason -- neither carries
    row values, and both are what an operator reads next.
    """
    return _bound_message(_scrub_text(str(exc)))


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


def add_exception_event(span: "Span", text: ExceptionText) -> None:
    """Attach *text* to *span* as the OTel semantic-convention ``exception`` event.

    The event shape is the one the conventions define, so backends that
    special-case it still render an exception.
    """
    span.add_event(
        "exception",
        attributes={
            "exception.type": text.type_name,
            "exception.message": text.message,
            "exception.stacktrace": text.stacktrace,
        },
    )


def record_exception_text(span: "Span", text: ExceptionText) -> None:
    """Mark *span* failed by *text*: ERROR status described by the message, plus the event."""
    span.set_status(StatusCode.ERROR, text.message)
    add_exception_event(span, text)


def record_exception_safe(span: "Span", exc: BaseException) -> None:
    """Record *exc* on *span* without leaking row values or credentials.

    The event-only form of :func:`record_exception_text`, for a call site that
    sets the span status itself.
    """
    add_exception_event(span, render_exception(exc))


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
#: scrubbed line-wise like :func:`render_exception`, without the message-length
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
    fields get the line-wise stacktrace scrub. A :class:`ScrubbedText` has
    already had the treatment its type promises and passes through, as do
    non-string, non-exception values (ints, bools, None).
    """
    if isinstance(value, BaseException):
        return safe_exception_message(value)
    if not isinstance(value, str) or isinstance(value, ScrubbedText):
        return value
    if field in EXCEPTION_TRACEBACK_FIELDS:
        return _scrub_text(value)
    return _bound_message(_scrub_text(value))
