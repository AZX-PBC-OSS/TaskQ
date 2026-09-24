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
* **Bearer tokens and signatures in HTTP-failure bodies.** A managed-identity
  credential failure (azure-identity's ``HttpResponseError`` shape) appends
  the HTTP body to ``str(exc)``, and the body carries a raw access token --
  as a standalone JWT, after ``Authorization: Bearer`` (quoted renderings of
  the header included), or under its own OAuth token name
  (``access_token``/``refresh_token``/``id_token``, camelCase included),
  which is what catches an OPAQUE token the JWT shape cannot see. Presigned
  AWS query strings carry the same class of credential as
  ``X-Amz-Signature=``/``Signature=``/``sig=`` values. All are masked
  (issue #317, verified by an end-to-end repro through the
  pg-credential-refresh failure log site). Stated limits, so the next
  reader knows they are decisions rather than gaps: a token whose header
  name was corrupted by homoglyphs is not caught (the masks are literal,
  and a corrupted name is not the header the credential was sent under);
  an opaque credential appearing with NEITHER a bearer header nor a token
  name around it is not caught (masking arbitrary long strings would
  over-redact diagnostics); the JWT mask's conservative shape is its own
  trade-off, see :data:`_JWT_RE` -- which over-masks a three-long-label
  hostname, an accepted cost recorded there.

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
    "safe_repr",
    "safe_str",
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
#: The ``[ \t|+]*`` prefix absorbs ``traceback.format_exception``'s
#: ``ExceptionGroup`` rendering, which indents every line of a sub-exception
#: with a repeated ``| `` (or, on a group's own header/separator lines,
#: ``+``) marker -- one added layer per level of nesting -- before the
#: exception's own text. Without it, a DETAIL line inside a grouped or
#: ``except*``-caught sub-exception reads
#: ``    | DETAIL:  Key (...)=(...) already exists.`` and the anchor on a
#: bare ``^[ \t]*`` never reaches past the marker, so the row value ships to
#: the span/log unredacted. The prefix is still consumed only when it is
#: immediately followed by ``DETAIL:`` -- a header line such as
#: ``  | ExceptionGroup: ...`` does not itself start with ``DETAIL:`` and so
#: is not touched.
#:
#: Why ONE character class and not the marker-shaped
#: ``(?:[ \t]*[|+][ \t]*)*`` it replaced: the two accept exactly the same
#: prefixes (every prefix they match is a run of spaces, tabs and ``|``/``+``
#: -- a marker run is N zero-whitespace repetitions, the whitespace around
#: each marker rides a repetition's ``[ \t]*`` arms or the trailing one),
#: but the class matches in a single pass. The nested form put a quantifier
#: (``[ \t]*``) inside another quantifier (the marker group's ``*``), so a
#: long marker run with no ``DETAIL:`` after it could be partitioned across
#: the repetitions in exponentially many ways and the engine tried them all:
#: ~60 ms of scrub at 20 markers, ~4x that per marker pair added, seconds by
#: the mid-20s and effectively unbounded beyond -- on a pass that runs
#: synchronously on the event loop (the failed-attempt scrub, behind the
#: ``"DETAIL:" in text`` prefilter, so one DETAIL line anywhere in the text
#: plus one marker-heavy line is enough). A poison job whose message echoes
#: that shape stalls every heartbeat with it until the watchdog dumps the
#: worker (5 s) and then kills it (30 s), deterministically, on every retry.
#: A character class has no nested quantifier to re-partition, so the match
#: is linear in the line whatever it carries;
#: ``test_detail_scrub_stays_under_a_time_bound_on_marker_runs`` pins the
#: budget and ``test_no_nested_quantifier_regexes_in_taskq_obs`` the shape.
_PG_DETAIL_RE = re.compile(r"^[ \t|+]*DETAIL:.*$", re.MULTILINE)

#: Companion to :data:`_PG_DETAIL_RE` for ``repr()``-flattened text.
#: ``repr(exc)`` renders the newline before DETAIL as the two
#: literal characters ``\n``, which the line-anchored pattern above cannot
#: see, and ``error=repr(exc)`` is a majority log idiom. Consumes from the
#: escaped newline up to (not including) the next escaped newline, or up to
#: the repr tail: a quote followed by the run of ``)``/``]`` closers a
#: ``repr()`` ends with (``')`` for a plain exception, ``')])`` once the
#: exception sits in an ``ExceptionGroup``'s list, one more ``])`` per
#: nesting level) at end of line. The closing alternatives can only succeed
#: at end-of-line, so they keep a repr's trailing closers when present
#: without ever stopping the scrub early and leaving row values behind.
#: The closers leg ends in ``[ \t]*``, same-line trailing whitespace only,
#: deliberately NOT ``\s*``: ``\s`` crosses newlines, so a DETAIL value
#: carrying a quote, closers and a (CR/)LF boundary could satisfy the leg
#: by peering PAST the line end: on CR-bearing text it really does, and
#: the scrub then stopped at the mid-value quote and kept what the
#: no-closers control scrubbed. A terminator that cannot cross a line
#: boundary fails closed there instead: the closers ride the scrub (more
#: deletion, never less), and the repr-tail shape the leg exists for,
#: closers, optional same-line spaces, end of line, still terminates it
#: (``test_repr_channel_pins``'s embedded-traceback case).
#:
#: The ``[ \t|+]*`` after the escaped newline is the same marker class
#: :data:`_PG_DETAIL_RE` carries, for the same reason: the repr channel
#: sees marker-prefixed DETAIL text too: an exception message that embeds
#: a rendered ``ExceptionGroup`` traceback (or any echoed ``| | DETAIL:``
#: text) reprs with the markers inline after the escaped newline, and an
#: anchor without the class shipped the row value verbatim on exactly the
#: poison-message vector the marker class was added for.
#:
#: The final bare ``$`` leg is fail-closed: a DETAIL whose tail matches
#: NEITHER safe delimiter (an unterminated repr, or one embedded mid-line
#: with more text after it) is scrubbed through end of line rather than
#: shipped: a delimiter miss must delete more text, never less of the
#: secret. ``MULTILINE`` makes ``$`` match per real line, so a repr line
#: embedded in a rendered traceback (real newlines around it) is scrubbed
#: too. Optional escaped ``\r`` covers the CRLF boundary shape.
_PG_DETAIL_ESCAPED_RE = re.compile(
    r"(?:\\r)?\\n[ \t|+]*DETAIL:.*?(?=(?:\\r)?\\n|['\"][)\]]*[ \t]*$|$)",
    re.MULTILINE,
)

#: userinfo in a URI. Group 1 is the scheme+user, group 2 the password.
#:
#: The username class is ``*``, not ``+``: an EMPTY username is a real shape ,
#: ``postgresql://:SECRET@host/db`` is what a DSN renders when only a
#: password is set, and ``+`` skipped it entirely, shipping the password
#: verbatim. With ``*`` group 1 is the bare scheme prefix, so the masked
#: form still reads ``scheme://:***@host``.
_URI_CRED_RE = re.compile(r"(\b[a-zA-Z][a-zA-Z0-9+.-]*://[^\s:/@]*):([^\s@]+)@")

#: Connection-parameter names whose value is credential material. Kept tight
#: to the password family: broader names (``secret``, ``token``, …) would
#: redact non-credential parameters, which is its own bug. ``sslpassword`` is
#: the passphrase for the client TLS key, a credential in its own right.
#:
#: Spelled once, in one case, and compiled into the matcher below rather than
#: written out as literals inside a pattern: a hand-maintained list of exact
#: spellings is what let case variants and ``sslpassword`` through, and a
#: derived matcher makes the next spelling a one-word edit here.
_CRED_PARAM_NAMES = ("password", "passphrase", "passwd", "pwd", "sslpassword")

#: password-family credentials in a connection string, in BOTH spellings that
#: carry one. Group 1 is the delimiter plus the parameter name, kept verbatim
#: so the masked form still names which setting carried the credential, and
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
#: The value is either a libpq single-quoted string, which may carry spaces
#: and honours the ``\'`` and ``\\`` escapes, so the quote run must be
#: consumed whole or the tail of the secret rides along after the ``***`` ,
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

#: ``Authorization: Bearer <token>`` in any casing and loose around the colon
#: and spaces. Group 1 keeps the header text verbatim so the masked form still
#: reads ``Bearer ***``; the token class stops at whitespace and the list
#: delimiters a rendered header or a JSON-ish body can carry. ``[ \t]``, not
#: ``\s``: ``\s`` crosses newlines, and a value that can only be terminated by
#: a line boundary must not be able to peer past one.
#:
#: The optional ``["'\\]`` runs around the colon and ``bearer`` admit the
#: QUOTED header spellings a response body or a repr()d dict renders: JSON
#: (``"Authorization": "Bearer ..."``, the ``\"``-escaped form included) and
#: Python repr (``{'Authorization': 'Bearer ...'}``). The classes only match
#: runs of quotes, backslashes and blanks, so they cannot jump over the
#: letters of an intervening value to reach an unrelated ``bearer`` -- the
#: first non-blank, non-quote character after the colon must literally be
#: the scheme word. Without them an opaque token under a quoted header
#: shipped verbatim: the JWT pass cannot see a token with no dot structure,
#: which is exactly what an error body echoing request headers carries.
#: The token class also stops at ``'`` and ``\`` for the same reason: the
#: closing quote of a quoted value must terminate the token, not ride with
#: it.
#:
#: Matched before the JWT mask below: the header form is the more specific
#: shape, so it claims the value first and the JWT mask finds nothing left of
#: it to re-match.
_BEARER_TOKEN_RE = re.compile(
    r"(\bauthorization[\"'\\]*[ \t]*:[ \t]*[\"'\\]*[ \t]*bearer[ \t]+)[^\s,;\"'\\]+",
    re.IGNORECASE,
)

#: OAuth token parameter names whose value is credential material by
#: RFC 6749, whatever the token's shape: an OPAQUE access token (no dot
#: structure) is invisible to the JWT mask below, and a response body or
#: form body quoting one under its own name is the same disclosure as a
#: JWT under ``Authorization: Bearer``. Both the RFC 6749 snake_case
#: spellings and the camelCase ones caches and SDKs render are listed;
#: ``IGNORECASE`` makes the casing itself irrelevant, both spellings exist
#: because the separator differs (``_`` vs nothing).
#:
#: Kept to the token family: names like ``token`` or ``key`` alone are too
#: generic to redact in non-credential text -- the same discipline the
#: password-family list below applies.
_OAUTH_TOKEN_NAMES = (
    "access_token",
    "refresh_token",
    "id_token",
    "accessToken",
    "refreshToken",
    "idToken",
)

#: A quoted-value or assignment delimiter: JSON/repr quotes around the
#: token (``"access_token": "..."``, ``{'access_token': '...'}``, the
#: ``\"``-escaped form a repr()d JSON string renders), a bare-colon
#: key/value dump (``access_token: ...``), or form/query encoding
#: (``access_token=...``). A quote is matched as backslash-then-quote or a
#: bare quote -- written as an ALTERNATION of quantifier-free arms and not
#: ``\\?["']`` because a ``?`` whose body carries another repeat is the
#: nested-quantifier shape
#: ``test_no_nested_quantifier_regexes_in_taskq_obs`` bans; ``[ \t]``, not
#: ``\s``: the delimiter must not be able to peer past a line boundary, for
#: the same reason :data:`_BEARER_TOKEN_RE` documents.
_OAUTH_TOKEN_QUOTE = r"(?:\\[\"']|[\"'])"  # noqa: S105  # Why: a regex quote-class snippet, not a secret; the bandit rule keys on the quote characters.

#: The masked shape for an OAuth token value. Group 1 keeps the name and
#: its delimiter verbatim (as :data:`_URI_PARAM_CRED_RE` does), group 2 is
#: the value: a quoted string consumed whole, or an unquoted token. The
#: unquoted class stops at whitespace, ``&``/``;`` (the next form
#: parameter) and ``,`` -- the delimiters a rendered query string or body
#: can carry -- so masking ``access_token=`` never eats the neighbouring
#: ``expires_in=3600``. ``IGNORECASE``: the casing is whatever the sender
#: emitted.
_OAUTH_TOKEN_RE = re.compile(
    r"((?:[?&]|(?<![A-Za-z0-9_]))(?:"
    + "|".join(_OAUTH_TOKEN_NAMES)
    + r")(?:"
    + _OAUTH_TOKEN_QUOTE
    + r"?[ \t]*:[ \t]*|=))("
    + _OAUTH_TOKEN_QUOTE
    + r"[^\s\"']+"
    + _OAUTH_TOKEN_QUOTE
    + r"|[^\s&;,\"']+)",
    re.IGNORECASE,
)

#: Lowercased trigger substrings for :data:`_OAUTH_TOKEN_RE`'s prefilter,
#: derived from the same name tuple the pattern is built from -- a name
#: added above is guarded here without a second edit, the same derivation
#: :data:`_CRED_PARAM_TRIGGERS` uses.
_OAUTH_TOKEN_TRIGGERS = tuple(name.lower() for name in _OAUTH_TOKEN_NAMES)

#: A JWT-shaped token: three base64url segments separated by two dots. This is
#: deliberately CONSERVATIVE, a stated trade-off rather than a parsed JWT:
#:
#: * each segment must be 16+ chars. Real Entra ID / access-token segments are
#:   far longer (a JOSE header alone is ~36), while the dotted three-segment
#:   strings that appear legitimately -- semvers, dotted ids, filenames -- are
#:   short in at least one segment. The floor is what keeps ``abc.def.ghi``
#:   and ``1.2.3`` intact; the cost is a hypothetical short JWT (an ``alg:
#:   none`` token) shipping verbatim. Un-masking a token that short loses
#:   nothing real; mangling a short id costs a diagnostic.
#: * the charset is base64url only (``A-Za-z0-9_-``), so ``:``, ``/``, ``@``,
#:   ``?``, ``=`` -- the punctuation a URI or a path is built from -- can never
#:   sit inside a match.
#: * exactly two dots, all three segments, word-bounded: a plain base64 blob
#:   with no dots is not token-shaped and is left alone.
#: * KNOWN OVER-MATCH, accepted and recorded: a string with three base64url
#:   segments of 16+ each is masked whatever it means, and a hostname whose
#:   three labels are all that long (``performance-metrics.
#:   analytics-dashboard.corporate-domain``) is such a string -- it goes to
#:   ``***`` in an error message. That is the cost side of the floor choice:
#:   the mask is fail-closed (a shape this JWT-like is deleted, the
#:   diagnostic loses a hostname rather than a vendor gaining a token), the
#:   16+ floor cannot be raised without letting a real short-segment JWT
#:   (an ``alg: HS256`` JOSE header is 20 chars) through, and no regex can
#:   tell a JWT from such a hostname without decoding it.
#:   ``test_three_long_label_hostname_is_masked_and_that_is_documented``
#:   records the decision.
#:
#: Written FLAT -- three separate ``{16,}`` repeats, no quantifier inside a
#: quantifier -- because :func:`test_no_nested_quantifier_regexes_in_taskq_obs`
#: bans the nested shape a ``(?:\.{seg}){2,}`` spelling would compile to, and
#: the flat form matches the same strings.
_JWT_RE = re.compile(r"\b[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\.[A-Za-z0-9_-]{16,}\b")

#: AWS presigned-URL signature parameters: the ``X-Amz-Signature`` query
#: parameter of a SigV4 presigned URL, the ``Signature`` spelling CloudFront
#: signed URLs use, and the shorter ``sig=``. The value -- hex for SigV4,
#: base64url for CloudFront's RSA signature -- is credential material
#: exactly like a password, and the password-family name list below
#: deliberately does not carry ``sig``/``signature`` (too generic a name to
#: redact in non-AWS text), so this needs its own pattern. Group 1 keeps the
#: parameter name verbatim, as :data:`_URI_PARAM_CRED_RE` does, so the
#: masked form still names which parameter carried it.
#:
#: The value class is the URL-safe token alphabet plus ``%`` (lowercase hex
#: per the SigV4 spec, but case-variant in the wild, and a value that was
#: percent-encoded must not leave its ``%XX`` tail riding after the mask --
#: a delimiter miss deletes more, never less). It stops at whitespace,
#: ``&`` and ``,``/``;`` like the other parameter-value classes.
#: ``IGNORECASE``: the parameter casing is whatever the signer emitted.
_AWS_SIG_RE = re.compile(
    r"((?:[?&]|(?<![A-Za-z0-9_]))(?:x-amz-signature|signature|sig)=)[0-9a-z_%-]+",
    re.IGNORECASE,
)

#: Lowercased trigger substrings for :data:`_URI_PARAM_CRED_RE`'s prefilter.
#: Derived from the same name tuple, so a name added above is guarded here
#: without a second edit, a prefilter that drifts from its pattern silently
#: stops masking.
_CRED_PARAM_TRIGGERS = tuple(f"{name}=" for name in _CRED_PARAM_NAMES)

#: Default bound on scrubbed message text. 2000 to match
#: ``web/admin/jobs.py``'s ``_TRACEBACK_DISPLAY_LIMIT``, one number for "how
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
    """Drop Postgres DETAIL lines and mask bearer, JWT and URI credentials.

    Both newline forms are covered: real newlines (``str(exc)``) by
    :data:`_PG_DETAIL_RE`, and the literal ``\\n`` ``repr()`` flattens them
    into by :data:`_PG_DETAIL_ESCAPED_RE`.

    Five credential shapes are masked, in this order:

    1. ``Authorization: Bearer <token>`` headers -- quoted renderings
       (``"Authorization": "Bearer ..."``, ``{'Authorization': '...'}``, the
       escaped-quote form) included -- by :data:`_BEARER_TOKEN_RE`.
    2. OAuth token values under their own names (``access_token``,
       ``refresh_token``, ``id_token``, camelCase spellings included; JSON,
       repr, bare-colon, query and form encodings), by
       :data:`_OAUTH_TOKEN_RE`. This is what catches an OPAQUE access token
       in a response body: it has no dot structure for the JWT mask to see.
       It runs after the bearer pass and before the JWT pass, so the
       name-specific mask claims a value first and the shape-generic mask
       finds nothing left of it to re-match.
    3. Standalone JWT-shaped tokens (three base64url segments, two dots), by
       :data:`_JWT_RE` -- the conservative-shape trade-off is documented there.
       It runs after the bearer pass, so a header-shaped value is claimed by
       the more specific mask first, and before the URI passes, so a token
       riding in URI userinfo cannot be seen half-consumed.
    4. Presigned-URL signature parameters (``X-Amz-Signature=``,
       ``Signature=``, ``sig=``), by :data:`_AWS_SIG_RE` -- the
       password-family name list deliberately excludes ``sig``/
       ``signature``, so this needs its own pattern.
    5. userinfo (``scheme://user:pass@host``, empty username included) by
       :data:`_URI_CRED_RE`, then password-family connection parameters,
       query-string and libpq keyword/value alike, in any casing, by
       :data:`_URI_PARAM_CRED_RE`. The order is what makes a DSN carrying
       both at once safe (``scheme://user:SECRET@host/db?password=OTHER``):
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
       ``***`` output contains anything the other regex can re-match, each
       fires exactly once.

    The credential masks are applied unconditionally, outside the
    ``_redaction_enabled`` guard: the debugging case that wants a row value
    never wants a password, and a DSN or an access token reaching a telemetry
    vendor is a credential disclosure regardless of why redaction was relaxed.

    Each regex is behind a substring prefilter stating a NECESSARY condition
    for that pattern to match at all, derived from the pattern text:

    * ``_PG_DETAIL_RE`` anchors a line on the literal ``DETAIL:`` and
      ``_PG_DETAIL_ESCAPED_RE`` matches it after an escaped newline, both
      require ``"DETAIL:"`` in the subject.
    * ``_BEARER_TOKEN_RE`` requires the literal ``bearer``.
    * ``_OAUTH_TOKEN_RE`` requires one of the token names, lowercased
      (``access_token`` ...).
    * ``_JWT_RE`` requires at least two dots (one ``str.count``). That
      condition is WEAK on rendered tracebacks -- every ``.py`` in a file
      path supplies dots -- so the JWT scan does run on them: measured
      ~15 us for a 27-frame traceback, linear in the text (the poison
      shapes -- long word-runs with near-miss segments, the
      backtracking food of a ``{16,}`` repeat -- are pinned linear by
      ``test_jwt_scan_stays_linear_on_long_word_runs``). A stronger
      str-level necessary condition does not exist: "a dot with 16 word
      chars beside it" already requires a scan that costs what the pass
      costs. ``test_clean_error_line_runs_no_regex`` pins the strict
      claim this docstring makes: on trigger-free text NO regex runs.
    * ``_AWS_SIG_RE`` requires ``sig=`` or ``signature=`` (the latter
      covers ``x-amz-signature=``, which contains it).
    * ``_URI_CRED_RE`` requires a ``scheme://`` separator.
    * ``_URI_PARAM_CRED_RE`` requires a password-family parameter name
      followed by ``=``, compared case-insensitively to match the pattern ,
      and deliberately NOT ``://``: bare ``host/db?password=…`` and libpq
      ``host=db … password=…`` text must stay masked, so the guard is on the
      parameter names, not a scheme.

    Skipping a substitution when its trigger substring is absent cannot
    change the output (the pattern could not have matched), which collapses
    the regex passes to substring scans for the common error-bearing log
    field, the cost that matters at error-storm rates. The lowercased subject
    is recomputed only after a pass that actually substituted, so the clean
    path lowers once, as it always did.
    """
    if _redaction_enabled and "DETAIL:" in text:
        text = _PG_DETAIL_RE.sub("", text)
        text = _PG_DETAIL_ESCAPED_RE.sub("", text)
    lowered = text.lower()
    if "bearer" in lowered:
        text = _BEARER_TOKEN_RE.sub(r"\1***", text)
        lowered = text.lower()
    if any(trigger in lowered for trigger in _OAUTH_TOKEN_TRIGGERS):
        text = _OAUTH_TOKEN_RE.sub(r"\1***", text)
        lowered = text.lower()
    if text.count(".") >= 2:
        text = _JWT_RE.sub("***", text)
        lowered = text.lower()
    if "sig=" in lowered or "signature=" in lowered:
        text = _AWS_SIG_RE.sub(r"\1***", text)
        lowered = text.lower()
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
    the admin UI, a bare "...[truncated]" hides how much is missing, so an
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

    Caveat: a traceback also quotes the SOURCE LINE of each
    frame. Those come from TaskQ's own source, not from data, so they carry no
    row values -- but a secret written as a literal in application code would
    appear. Do not put credentials in source.
    """
    raw_stacktrace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    return ExceptionText(
        type_name=type(exc).__qualname__,
        raw_stacktrace=raw_stacktrace,
        # Why safe_str: the exception is uncontrolled (an actor raises it);
        # a __str__ that raises must degrade to the constant marker, not
        # convert inside the handler that was classifying it.
        message=ScrubbedText(_bound_message(_scrub_text(safe_str(exc)))),
        stacktrace=ScrubbedText(_scrub_text(raw_stacktrace)),
    )


def safe_exception_message(exc: BaseException) -> str:
    """Exception text with the Postgres DETAIL dropped and URI creds masked.

    The primary Postgres message is kept: it is a static template naming the
    constraint or relation, which is the part that is actually diagnostic.
    ``HINT`` and ``CONTEXT`` are kept for the same reason -- neither carries
    row values, and both are what an operator reads next.

    The ``str()`` is guarded: the exception is uncontrolled input (an actor
    raises it), and an exception whose own ``__str__`` raises would otherwise
    convert HERE, inside the handler that was classifying it, into an uncaught
    ``TypeError`` that escapes the classification entirely. The fallback
    mirrors CPython's own traceback rendering for the same condition
    ("<exception str() failed>"), so sinks stay consistent.
    """
    return _bound_message(_scrub_text(safe_str(exc)))


def safe_str(exc: BaseException) -> str:
    """``str(exc)`` that cannot raise, for uncontrolled exceptions.

    The one conversion every sink of raw exception text shares: a hostile
    or broken ``__str__`` (an actor's exception class is the attacker's
    code) must degrade to a constant marker instead of raising a fresh
    exception inside a caller's ``except`` block and escaping the
    classification that was running. Unlike :func:`safe_exception_message`
    this is the RAW text (no scrub, no bound) - call sites that derive
    stored/log fields from it apply their own guards
    (``sanitize_nul_str``), and the render-once invariant
    (``render_exception`` is the single scrub) is not paid twice. The
    fallback marker mirrors CPython's own traceback rendering for the same
    condition (measured on 3.14: ``format_exception`` survives and prints
    ``Boom: <exception str() failed>``), so a reader sees one idiom. The
    catch is a BARE ``except BaseException`` for the same reason CPython's
    ``traceback._safe_string`` uses one: a ``__str__`` may raise any
    ``BaseException`` subclass, and ``except Exception`` would let it
    convert inside the guard. Swallowing a ``CancelledError`` here is safe:
    this is a string-rendering helper, never an await point, so there is no
    suspension it could strand.
    """
    try:
        return str(exc)
    # Why BaseException: CPython's traceback._safe_string idiom - a hostile
    # __str__ may raise any BaseException subclass, and this helper is a
    # pure string render, never an await point, so a swallowed
    # CancelledError cannot strand anything.
    except BaseException:
        return "<exception str() failed>"


def safe_repr(exc: BaseException) -> str:
    """``repr(exc)`` that cannot raise, for uncontrolled exceptions.

    The ``except``-handler logs that render ``repr(exc)`` (hook failures,
    classifier failures) would otherwise have their own swallow converted
    into an escape by a ``__repr__`` that raises - the same defect shape as
    :func:`safe_str`, one level deeper. The fallback keeps the class name
    because it is the one diagnostic that survives (pinned), but the NAME
    ACCESS is guarded too: a metaclass can define ``__name__`` as a
    property that raises, so even the interpolation is uncontrolled input
    and degrades to ``<unknown>``.
    """
    try:
        return repr(exc)
    # Why BaseException: same reasoning as :func:`safe_str` directly above -
    # a hostile __repr__ may raise any BaseException subclass, and this
    # helper is a pure string render, never an await point, so a swallowed
    # CancelledError cannot strand anything.
    except BaseException:
        try:
            name = type(exc).__name__
        except BaseException:
            name = "<unknown>"
        return f"<exception repr() failed: {name}>"


#: A validated ``(cls, exc, tb)`` triple ready for ``traceback.format_exception``.
_ResolvedExcInfo = tuple[type[BaseException], BaseException, TracebackType | None]

#: What structlog event dicts can carry on the ``exc_info`` key. Untrusted
#: boundary input: the tuple members are ``object`` until validated at runtime,
#: so the ``_resolve_exc_info`` narrowing checks are essential, not redundant.
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
#: terminal-write log's ``job_error_message``/``infra_error_message`` …) ,
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

#: Event-dict field names that conventionally carry rendered TRACEBACK text ,
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
