"""Credential- and PII-safe exception text for telemetry spans.

Spans are shipped to third-party backends (Azure Monitor / Application
Insights, OTLP collectors, vendor SaaS). Whatever reaches a span attribute
leaves the trust boundary, so exception text must be scrubbed before it goes
there -- the same discipline :func:`taskq._dsn.dsn_host` already applies to
logs, which the span path bypassed entirely.

Two concrete leaks, both verified by execution rather than assumed:

* **Postgres error DETAIL carries row values.** ``str()`` of an asyncpg
  ``PostgresError`` appends the server's ``DETAIL:`` and ``HINT:`` lines, and
  for a constraint violation those quote the offending column values --
  ``Key (idempotency_key)=(customer-4417-...) already exists.`` TaskQ's
  ``idempotency_key``, ``identity_key`` and ``fairness_key`` are all
  caller-supplied and routinely carry tenant or subject identifiers.
* **Credentials in URI-shaped text.** Any ``scheme://user:password@host``
  appearing in a message is masked, so a DSN that reaches an exception by any
  route cannot be forwarded verbatim.

Note ``opentelemetry``'s ``Span.record_exception`` always derives
``exception.message`` from ``str(exception)`` with no hook to override it,
which is why this module emits its own ``exception`` event instead of calling
it.
"""

import re
import traceback
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from opentelemetry.trace import Span

__all__ = ["record_exception_safe", "safe_exception_message"]

#: Postgres DETAIL/HINT/CONTEXT lines. Row values live there; the primary
#: message above them is a static template.
#:
#: Matched LINE-WISE, not to end-of-string. A chained traceback renders several
#: exception messages, so a greedy DOTALL match starting at the first DETAIL
#: would delete every outer frame after it -- destroying the diagnostic while
#: appearing to work on a single-exception test.
_PG_DETAIL_RE = re.compile(r"^[ \t]*(?:DETAIL|HINT|CONTEXT):.*$", re.MULTILINE)

#: userinfo in a URI. Group 1 is the scheme+user, group 2 the password.
_URI_CRED_RE = re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]+):([^\s@]+)@")

_MAX_MESSAGE_CHARS = 512


def safe_exception_message(exc: BaseException) -> str:
    """Exception text with Postgres DETAIL/HINT dropped and URI creds masked.

    The primary Postgres message is kept: it is a static template naming the
    constraint or relation, which is the part that is actually diagnostic.
    """
    text = str(exc)
    text = _PG_DETAIL_RE.sub("", text)
    text = _URI_CRED_RE.sub(r"\1:***@", text)
    text = text.strip()
    if len(text) > _MAX_MESSAGE_CHARS:
        text = text[:_MAX_MESSAGE_CHARS] + "...[truncated]"
    return text


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
    rendered = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    rendered = _PG_DETAIL_RE.sub("", rendered)
    return _URI_CRED_RE.sub(r"\1:***@", rendered)


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
