"""Exception text on spans must not carry row values or credentials.

Spans leave the trust boundary for whatever telemetry backend is configured
(Azure Monitor / Application Insights, an OTLP collector, a vendor SaaS), so
this is an exfiltration surface, not a formatting preference. The dispatch span
called `span.record_exception(exc)` and `set_status(..., str(exc))` with no
redaction, while the same codebase already routes DSNs through `dsn_host()` so
credentials never reach logs -- the span path bypassed that discipline.

The real leak is NOT DSN passwords, which is what was originally reported.
Verified by execution: asyncpg connection failures carry no credentials. The
actual vector is that `str()` of an asyncpg `PostgresError` appends the
server's `DETAIL:` line, and for a constraint violation that quotes the
offending column values. TaskQ's `idempotency_key`, `identity_key` and
`fairness_key` are all caller-supplied and routinely carry tenant or subject
identifiers.
"""

from __future__ import annotations

import ast
import re

import asyncpg
import pytest

from taskq.obs import (
    record_exception_safe,
    safe_exception_message,
    set_exception_message_max_chars,
)


def _unique_violation(detail: str) -> asyncpg.exceptions.UniqueViolationError:
    exc = asyncpg.exceptions.UniqueViolationError(
        'duplicate key value violates unique constraint "jobs_idempotency_key_key"'
    )
    exc.detail = detail
    return exc


class _RecordingSpan:
    """Minimal Span stand-in capturing add_event calls."""

    def __init__(self) -> None:
        self.events: list[tuple[str, dict[str, str]]] = []

    def add_event(self, name: str, attributes: dict[str, str]) -> None:
        self.events.append((name, attributes))


def test_postgres_detail_row_values_are_stripped() -> None:
    exc = _unique_violation("Key (idempotency_key)=(customer-4417-ssn-078051120) already exists.")
    # Precondition: the raw text really does leak, or this test proves nothing.
    assert "078051120" in str(exc)

    safe = safe_exception_message(exc)
    assert "078051120" not in safe
    assert "customer-4417" not in safe
    # The diagnostic part -- a static template naming the constraint -- survives.
    assert "jobs_idempotency_key_key" in safe
    assert "duplicate key value violates unique constraint" in safe


def test_hint_and_context_lines_are_stripped_too() -> None:
    exc = asyncpg.exceptions.PostgresError("some failure")
    exc.detail = "Key (identity_key)=(tenant-secret) already exists."
    exc.hint = "hint text"
    assert "tenant-secret" not in safe_exception_message(exc)


@pytest.mark.parametrize(
    "raw",
    [
        "could not connect to postgresql://taskq:hunter2@db.internal:5432/taskq",
        "redis://default:AZaBcD3f@cache.internal:6380 unreachable",
        "postgres://u:p%40ss@h/db failed",
    ],
)
def test_uri_credentials_are_masked(raw: str) -> None:
    safe = safe_exception_message(Exception(raw))
    for secret in ("hunter2", "AZaBcD3f", "p%40ss"):
        assert secret not in safe
    assert ":***@" in safe
    # Host survives: it is the diagnostic part.
    assert "internal" in safe or "h/db" in safe


@pytest.mark.parametrize(
    "raw",
    [
        "could not connect to postgresql://:hunter2@db.internal:5432/taskq",
        "redis://:AZaBcD3f@cache.internal:6380 unreachable",
    ],
)
def test_uri_credentials_with_empty_username_are_masked(raw: str) -> None:
    # Why a case of its own: `[^\s:/@]+` demanded a username of at least one
    # character, so the `scheme://:pass@` form (what a DSN renders when only a
    # password is set) matched nothing and shipped the password verbatim.
    safe = safe_exception_message(Exception(raw))
    for secret in ("hunter2", "AZaBcD3f"):
        assert secret not in safe
    # The scheme prefix is group 1, so the masked empty-username form still
    # reads `scheme://:***@host` rather than a mangled fragment.
    assert ":***@" in safe
    # Host survives: it is the diagnostic part.
    assert "internal" in safe


@pytest.mark.parametrize(
    "raw",
    [
        "could not connect to postgresql://db.internal:5432/taskq?password=hunter2",
        "postgres://h/db?sslmode=require&password=p%40ss failed",
    ],
)
def test_uri_query_param_password_is_masked(raw: str) -> None:
    # Why: libpq takes the password as a query parameter too, and that
    # spelling has no userinfo for the userinfo mask to bite on, so the value
    # reached the telemetry backend verbatim. Both `?password=` (first
    # parameter) and `&password=` (later parameter) positions are covered.
    safe = safe_exception_message(Exception(raw))
    for secret in ("hunter2", "p%40ss"):
        assert secret not in safe
    # The parameter NAME survives -- an operator can see which setting carried
    # the credential -- and every non-credential parameter is untouched.
    assert "password=***" in safe
    assert "sslmode=require" in safe or "db.internal" in safe


@pytest.mark.parametrize(
    "raw",
    [
        "could not connect to postgresql://db/jobs?PASSWORD=hunter2",
        "postgresql://db/jobs?PassWord=hunter2",
        "postgres://h/db?sslmode=require&PWD=hunter2 failed",
    ],
)
def test_query_param_password_is_masked_whatever_its_case(raw: str) -> None:
    # Why case matters: libpq connection parameter names are case-insensitive
    # and psql, ORMs and operator-typed DSNs all echo back whatever casing was
    # written, so a mask keyed to one exact spelling misses the same parameter
    # written any other way and ships the value verbatim.
    safe = safe_exception_message(Exception(raw))
    assert "hunter2" not in safe
    assert "***" in safe


@pytest.mark.parametrize(
    "raw",
    [
        "postgresql://db/jobs?sslmode=verify-full&sslpassword=hunter2",
        "postgresql://db/jobs?sslmode=verify-full&SSLPASSWORD=hunter2",
    ],
)
def test_sslpassword_query_param_is_masked(raw: str) -> None:
    # `sslpassword` is libpq's passphrase for the client SSL key: a credential
    # in its own right, and one that a password-family name set spelled out
    # literally is easy to omit.
    safe = safe_exception_message(Exception(raw))
    assert "hunter2" not in safe
    # sslmode is not a credential and stays intact, so the message keeps
    # saying which TLS posture the failed connection was using.
    assert "sslmode=verify-full" in safe


@pytest.mark.parametrize(
    "raw",
    [
        "host=db port=5432 dbname=jobs user=app password=hunter2",
        "host=db user=app PASSWORD=hunter2",
        "host=db user=app sslpassword=hunter2",
    ],
)
def test_libpq_keyword_value_dsn_password_is_masked(raw: str) -> None:
    # The libpq keyword/value conninfo form carries no `://` and no `?`/`&`,
    # so neither URI mask can bite on it, yet it is a routine shape: it is
    # what a constructed conninfo string and psycopg's own connection errors
    # render into the message text.
    safe = safe_exception_message(Exception(raw))
    assert "hunter2" not in safe
    # Structural, non-secret keywords survive so the message stays diagnostic.
    assert "host=db" in safe
    assert "user=app" in safe


@pytest.mark.parametrize(
    ("raw", "leaked"),
    [
        ("host=db user=app password='hun ter2'", "ter2"),
        # libpq quoting honours \' and \\ escapes inside the quotes.
        (r"host=db user=app password='it\'s secret'", "secret"),
        ("host=db user=app PASSWORD='hun ter2'", "ter2"),
    ],
    ids=["quoted-with-space", "escaped-quote-inside", "quoted-uppercase-name"],
)
def test_libpq_quoted_password_value_is_masked(raw: str, leaked: str) -> None:
    """libpq single-quotes a value that carries spaces (``password='a b'``).

    A value class that stops at whitespace masks ``'hun`` and ships
    ``ter2'`` - most of the credential verbatim. The mask must consume the
    whole quoted value instead, escapes (``\\'``, ``\\\\``) included.
    """
    safe = safe_exception_message(Exception(raw))
    assert leaked not in safe
    assert "password=***" in safe.lower()
    # Structural, non-secret keywords survive so the message stays diagnostic.
    assert "host=db" in safe
    assert "user=app" in safe


def test_query_param_password_containing_at_sign_is_fully_masked() -> None:
    # A password may legally contain an unencoded `@`. If the masked value
    # class treats `@` as a boundary it stops early and the tail of the secret
    # rides along after the `***`, which is a partial credential disclosure
    # and enough to shorten a brute force considerably.
    safe = safe_exception_message(Exception("postgresql://db/jobs?password=hun@ter2"))
    assert "hun@ter2" not in safe
    assert "ter2" not in safe
    assert "password=***" in safe


@pytest.mark.parametrize(
    "raw",
    [
        "postgresql://app:hunter2@db:5432/jobs",
        "postgresql://:hunter2@db:5432/jobs",
        "redis://:hunter2@cache:6379/0",
        "postgresql://db/jobs?password=hunter2",
        "postgresql://db/jobs?PASSWORD=hunter2",
        "postgresql://db/jobs?sslmode=verify-full&sslpassword=hunter2",
        "host=db port=5432 dbname=jobs user=app password=hunter2",
    ],
)
def test_no_connection_string_shape_ships_a_plaintext_password(raw: str) -> None:
    """Every connection-string spelling TaskQ can meet must mask its secret.

    The credential mask runs unconditionally, outside the redaction toggle,
    because this text is what reaches log lines and OTel span attributes --
    it leaves the trust boundary whatever the toggle is set to. A shape the
    mask does not recognise is therefore a silent credential disclosure to
    whatever telemetry backend is configured.
    """
    import taskq.obs._redact_exc as redact_mod

    assert "hunter2" not in safe_exception_message(Exception(raw))

    # And with redaction relaxed: the toggle exists so an operator can get row
    # values back while debugging, and the debugging case that wants a row
    # value never wants a password. Masking that the toggle can switch off is
    # not a credential guarantee at all.
    redact_mod.set_exception_redaction_enabled(False)
    try:
        assert "hunter2" not in safe_exception_message(Exception(raw))
    finally:
        redact_mod.set_exception_redaction_enabled(True)


def test_uri_with_userinfo_and_query_param_password_masks_both() -> None:
    # Why the exact shape: a DSN can carry both spellings at once and the two
    # masks run in sequence, so this pins the ordering -- each fires exactly
    # once and neither corrupts the other's already-masked output.
    raw = "postgresql://taskq:hunter2@db.internal:5432/taskq?password=Zaq12edx"
    safe = safe_exception_message(Exception(raw))
    assert "hunter2" not in safe
    assert "Zaq12edx" not in safe
    assert safe == "postgresql://taskq:***@db.internal:5432/taskq?password=***"


def test_message_is_length_bounded_and_reports_what_it_dropped() -> None:
    """A bounded message says how much was cut, so the bound is actionable.

    Why the remainder count matters: a bare truncation marker leaves an
    operator unable to tell whether raising the bound would reveal anything,
    which is how a diagnostic gets quietly lost. Mirrors the admin UI's
    ``_truncate_traceback``.
    """
    safe = safe_exception_message(Exception("x" * 5000))
    assert safe.endswith("... (3000 more characters)")
    assert len(safe) <= 2000 + len("... (3000 more characters)")


def test_message_bound_is_configurable() -> None:
    """An actor that formats large context into its message can raise the bound."""
    try:
        set_exception_message_max_chars(4000)
        safe = safe_exception_message(Exception("x" * 5000))
        assert safe.endswith("... (1000 more characters)")
    finally:
        set_exception_message_max_chars(2000)


def test_short_messages_are_untouched() -> None:
    assert safe_exception_message(Exception("boom")) == "boom"


def test_record_exception_safe_emits_a_redacted_exception_event() -> None:
    exc = _unique_violation("Key (fairness_key)=(acme-corp-tenant-99) already exists.")
    span = _RecordingSpan()
    record_exception_safe(span, exc)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    assert len(span.events) == 1
    name, attrs = span.events[0]
    # Keeps the semantic-convention shape so backends still render it.
    assert name == "exception"
    assert attrs["exception.type"] == "UniqueViolationError"
    assert set(attrs) == {"exception.type", "exception.message", "exception.stacktrace"}
    for value in attrs.values():
        assert "acme-corp-tenant-99" not in value


def test_stacktrace_is_redacted_including_chained_causes() -> None:
    """A traceback's last line is the exception repr, so DETAIL reappears
    there; chained causes render their own messages too."""
    # Built indirectly so the secrets do not appear in a source line that the
    # traceback itself quotes -- otherwise the test would be asserting against
    # its own source rather than against the exception messages.
    inner_exc = _unique_violation("Key (identity_key)=(" + "subject-31337" + ") already exists.")
    outer_msg = "wrapping postgresql://u:" + "s3cret" + "@h/db"
    try:
        try:
            raise inner_exc
        except asyncpg.exceptions.UniqueViolationError as inner:
            raise RuntimeError(outer_msg) from inner
    except RuntimeError as outer:
        span = _RecordingSpan()
        record_exception_safe(span, outer)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    _, attrs = span.events[0]
    trace = attrs["exception.stacktrace"]
    assert "subject-31337" not in trace
    assert "s3cret" not in trace
    # Still a usable traceback.
    assert "RuntimeError" in trace
    assert "UniqueViolationError" in trace


def test_stacktrace_is_redacted_inside_an_exception_group() -> None:
    """``traceback.format_exception`` prefixes every line of a sub-exception
    inside an ``ExceptionGroup`` with a ``| `` marker, which a line-anchored
    ``^DETAIL:`` pattern does not see through -- the row value must still be
    dropped once that marker is stripped away."""
    inner_exc = _unique_violation(
        "Key (idempotency_key)=(" + "customer-90210" + ") already exists."
    )
    try:
        try:
            raise inner_exc
        except asyncpg.exceptions.UniqueViolationError as inner:
            raise ExceptionGroup("group", [inner]) from None
    except ExceptionGroup as group:
        span = _RecordingSpan()
        record_exception_safe(span, group)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    _, attrs = span.events[0]
    trace = attrs["exception.stacktrace"]
    assert "customer-90210" not in trace
    # Still a usable traceback.
    assert "ExceptionGroup" in trace
    assert "UniqueViolationError" in trace


def test_stacktrace_is_redacted_inside_a_nested_exception_group() -> None:
    """A group inside a group deepens the ``| `` prefix (more leading
    whitespace, repeated markers); the scrub must not be anchored to a
    single prefix depth."""
    inner_exc = _unique_violation("Key (identity_key)=(" + "tenant-55512" + ") already exists.")
    try:
        try:
            try:
                raise inner_exc
            except asyncpg.exceptions.UniqueViolationError as inner:
                raise ExceptionGroup("inner-group", [inner]) from None
        except ExceptionGroup as inner_group:
            raise ExceptionGroup("outer-group", [inner_group]) from None
    except ExceptionGroup as outer_group:
        span = _RecordingSpan()
        record_exception_safe(span, outer_group)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    _, attrs = span.events[0]
    trace = attrs["exception.stacktrace"]
    assert "tenant-55512" not in trace
    assert "ExceptionGroup" in trace
    assert "UniqueViolationError" in trace


def test_stacktrace_is_redacted_under_except_star() -> None:
    """``except*`` is the idiomatic way TaskQ's own ``TaskGroup`` siblings
    catch ExceptionGroup; confirm the scrub holds on the exception it binds,
    not only on a group constructed and caught with plain ``except``."""
    inner_exc = _unique_violation("Key (fairness_key)=(" + "acme-77821" + ") already exists.")
    try:
        try:
            raise inner_exc
        except asyncpg.exceptions.UniqueViolationError as inner:
            raise ExceptionGroup("group", [inner]) from None
    except* asyncpg.exceptions.UniqueViolationError as caught:
        span = _RecordingSpan()
        record_exception_safe(span, caught)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    _, attrs = span.events[0]
    trace = attrs["exception.stacktrace"]
    assert "acme-77821" not in trace
    assert "UniqueViolationError" in trace


# A per-file check that _dispatch_sql.py spells the call
# `record_exception_safe(span, exc)` used to sit here. It is subsumed by
# test_no_raw_record_exception_remains_anywhere below: that guard is repo-wide,
# so a dispatch site reverting to `span.record_exception(exc)` fails there
# whatever file it lives in, and the redaction behaviour itself is pinned by
# the tests above. Two assertions of the same fact, one of them naming a file
# path that moves.


def test_no_raw_record_exception_remains_anywhere() -> None:
    """Repo-wide guard: every span exception path must go through redaction."""
    from pathlib import Path

    src = Path(__file__).resolve().parent.parent / "src" / "taskq"
    offenders: list[str] = []
    for path in sorted(src.rglob("*.py")):
        if path.name == "_redact_exc.py":  # Why: the redacting helper itself.
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            # Parsed, not substring-matched: the old form also fired on the
            # name in a comment or docstring, and would have missed a call
            # reached through an alias assignment.
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "record_exception"
            ):
                offenders.append(f"{path.relative_to(src).as_posix()}:{node.lineno}")
    assert offenders == [], f"unredacted record_exception in: {offenders}"


# ── cron auto-disable span event: same redaction contract ──────────


async def test_cron_auto_disabled_event_omits_row_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``cron.auto_disabled`` must carry the redacted message, not ``str(exc)``.

    The auto-disable branch already calls ``set_status(..., safe_exception_message(exc))``
    - this pins the ``add_event`` attribute to the same contract. Drives the
    real ``tick_cron`` error path to the 3-strike auto-disable with a backend
    whose batched enqueue fails with a DETAIL-carrying asyncpg
    ``UniqueViolationError``: the recurring-caller-key leak vector, shipped
    to the telemetry backend on every tick until disable fires.
    """
    from datetime import UTC, datetime

    from taskq._ids import new_uuid
    from taskq.backend._protocol import EnqueueArgs, JobRow
    from taskq.testing.clock import FakeClock
    from taskq.testing.in_memory import InMemoryBackend
    from taskq.testing.otel import setup_tracer
    from taskq.worker.cron_loop import tick_cron

    from .test_cron_loop import (
        _cron_settings,
        _FakeCronConn,
        _make_actor_config_row,
        _make_schedule_row,
    )

    canary = "tenant-99-subject-31337"
    exc = _unique_violation(f"Key (identity_key)=({canary}) already exists.")
    # Precondition: the raw text really does leak, or this test proves nothing.
    assert canary in str(exc)

    class _EnqueueFailsBackend(InMemoryBackend):
        """Real in-memory backend whose batched enqueue fails like a live
        PG would."""

        async def enqueue_batch(
            self,
            args_list: list[EnqueueArgs],
            *,
            connection: object = None,
            enforce_max_pending: bool = True,
        ) -> list[JobRow]:
            raise exc

    _, exporter = setup_tracer(monkeypatch)
    backend = _EnqueueFailsBackend(clock=FakeClock(datetime(2025, 1, 1, 10, 5, 0, tzinfo=UTC)))

    for i in range(3):
        conn = _FakeCronConn(
            schedule_rows=[
                _make_schedule_row(
                    actor="leaky_actor",
                    consecutive_failures=i,
                    next_fire_at=datetime(2025, 1, 1, 10, 0, 0, tzinfo=UTC),
                )
            ],
            actor_config_rows=[_make_actor_config_row(actor="leaky_actor")],
            disabled_count=1,
        )
        async with conn.transaction():
            await tick_cron(conn, _cron_settings(), backend, "taskq", new_uuid())

    auto_disabled = [
        ev
        for span in exporter.spans_named("cron fire")
        for ev in span.events
        if ev.name == "cron.auto_disabled"
    ]
    assert len(auto_disabled) == 1
    attrs = dict(auto_disabled[0].attributes or {})
    # Subscript, not .get(default): production unconditionally sets this
    # attribute, and a defaulting read would pass vacuously if it ever
    # stopped (the baaec0 doctrine for contracted keys).
    last_error = attrs["last_error"]
    assert isinstance(last_error, str)
    assert canary not in last_error
    # The diagnostic template - the part that is not row data - survives.
    assert "duplicate key value violates unique constraint" in last_error


# ── repo guard: add_event attribute dicts ──────────────────────────


def test_no_raw_exception_text_in_span_event_attributes() -> None:
    """Repo-wide guard: ``add_event`` attribute dicts must not embed raw
    ``str()``/``repr()``/f-string renders of exception objects.

    ``str()`` of an asyncpg ``PostgresError`` appends the server's DETAIL
    line, which quotes row values - so an unredacted render inside a span
    event attribute reopens the exact surface ``record_exception_safe``
    exists to close. AST-based so multi-line ``add_event(...)`` calls are
    covered (the leak this guards against spans 8 lines). Redaction helpers
    are exempt by construction: the guard only flags ``str``/``repr`` calls
    and f-strings applied to exception-shaped variable names.
    """
    import ast
    import re
    from pathlib import Path

    excish = re.compile(r"exc|err|error|exception|failure", re.IGNORECASE)
    descriptor_suffix = re.compile(r"_(class|type|name|code)$", re.IGNORECASE)

    def _is_exception_var(name: str) -> bool:
        if name.lower() == "e":
            return True
        return bool(excish.search(name)) and not descriptor_suffix.search(name)

    def _attribute_containers(call: ast.Call) -> list[ast.AST]:
        containers: list[ast.AST] = []
        if len(call.args) >= 2:
            containers.append(call.args[1])
        containers.extend(kw.value for kw in call.keywords if kw.arg == "attributes")
        return containers

    offenders: list[str] = []
    src = Path(__file__).resolve().parent.parent / "src" / "taskq"
    for path in sorted(src.rglob("*.py")):
        if path.name == "_redact_exc.py":
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "add_event"
            ):
                continue
            for container in _attribute_containers(node):
                for sub in ast.walk(container):
                    if (
                        isinstance(sub, ast.Call)
                        and isinstance(sub.func, ast.Name)
                        and sub.func.id in {"str", "repr"}
                    ):
                        for arg in sub.args:
                            if isinstance(arg, ast.Name) and _is_exception_var(arg.id):
                                offenders.append(
                                    f"{path.relative_to(src).as_posix()}: add_event "
                                    f"attribute renders raw {sub.func.id}({arg.id})"
                                )
                    elif isinstance(sub, ast.JoinedStr):
                        for frag in sub.values:
                            if not isinstance(frag, ast.FormattedValue):
                                continue
                            for inner in ast.walk(frag.value):
                                if isinstance(inner, ast.Name) and _is_exception_var(inner.id):
                                    offenders.append(
                                        f"{path.relative_to(src).as_posix()}: add_event "
                                        f"attribute f-strings exception {inner.id}"
                                    )
    assert offenders == [], f"raw exception text in span event attributes: {offenders}"


# ── repo guard: log-call exception-text field names ─────────────────


def test_log_fields_carrying_exception_text_are_listed_for_scrubbing() -> None:
    """Repo-wide guard: every log-call keyword whose name conventionally
    carries exception text must be listed in the obs scrub sets.

    ``_scrub_exception_fields`` (obs/_structlog.py) scrubs only the names in
    ``EXCEPTION_MESSAGE_FIELDS`` / ``EXCEPTION_TRACEBACK_FIELDS``, so a log
    site introducing a new ``*error_message`` / ``*error_traceback`` field
    ships raw exception text to every telemetry backend the JSON channel
    feeds - exactly how ``job_error_message``/``infra_error_message``/
    ``job_error_traceback``/``infra_error_traceback`` (the terminal-write
    log in worker/_handlers.py) leaked the actor's exception unredacted.

    Suffix-scoped so classification fields (``error_class``, ``error_type``,
    ``job_error_class`` - class names, not exception text) never fire: the
    suffix family is the shape that conventionally carries rendered
    exception text.
    """
    import ast
    from pathlib import Path

    from taskq.obs._redact_exc import EXCEPTION_MESSAGE_FIELDS, EXCEPTION_TRACEBACK_FIELDS

    log_methods = {"debug", "info", "warning", "warn", "error", "critical", "exception", "log"}
    offenders: list[str] = []
    src = Path(__file__).resolve().parent.parent / "src" / "taskq"
    for path in sorted(src.rglob("*.py")):
        if (
            path.name == "_redact_exc.py"
        ):  # Why: the module defining the sets; it makes no log calls.
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr in log_methods
            ):
                continue
            for kw in node.keywords:
                if kw.arg is None:  # Why: **kwargs splat - no field name to check.
                    continue
                if kw.arg.endswith("error_message") and kw.arg not in EXCEPTION_MESSAGE_FIELDS:
                    offenders.append(
                        f"{path.relative_to(src).as_posix()}:{node.lineno}: "
                        f"log field {kw.arg!r} carries exception text but is not in "
                        "EXCEPTION_MESSAGE_FIELDS"
                    )
                elif (
                    kw.arg.endswith("error_traceback") and kw.arg not in EXCEPTION_TRACEBACK_FIELDS
                ):
                    offenders.append(
                        f"{path.relative_to(src).as_posix()}:{node.lineno}: "
                        f"log field {kw.arg!r} carries traceback text but is not in "
                        "EXCEPTION_TRACEBACK_FIELDS"
                    )
    assert offenders == [], f"unlisted exception-text log fields: {offenders}"


# ── repr()-flattened DETAIL lines ───────────────────────────────────


def test_repr_flattened_detail_line_is_scrubbed_but_hint_survives() -> None:
    """``repr()`` flattens the newline before DETAIL into the literal
    two characters ``\\n``, which the line-anchored scrub cannot see - and
    ``error=repr(exc)`` is the majority log idiom (59 sites vs 33 ``str``).

    asyncpg's own ``__repr__`` renders only the primary message, so the
    leak shape is a relayed PG error: a plain exception whose message is
    the rendered PG text.

    HINT is asserted to SURVIVE here: it is Postgres's suggested fix, it
    quotes no row value, and scrubbing it was pure diagnostic loss. Only
    DETAIL is value-bearing.
    """
    from taskq.obs._redact_exc import scrub_exception_field

    exc = asyncpg.exceptions.PostgresError("some failure")
    exc.detail = "Key (identity_key)=(" + "subject-424242" + ") already exists."
    exc.hint = "try another identity_key"
    relayed = RuntimeError(str(exc))
    # Precondition: the flattened form really does leak, or this test proves nothing.
    assert "subject-424242" in repr(relayed)
    assert "try another" in repr(relayed)

    safe = scrub_exception_field("error", repr(relayed))
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.

    assert "subject-424242" not in safe
    # Sensible single-line shape: the class, the primary template and the HINT
    # survive, and the repr's closing quote is kept rather than amputated.
    assert safe == "RuntimeError('some failure\\nHINT:  try another identity_key')"


def test_repr_flattened_detail_inside_an_exception_group_is_scrubbed() -> None:
    """A repr()-flattened ExceptionGroup still loses the DETAIL.

    repr() of a group closes the sub-exception's message with a RUN of
    closers - ``')])``: the exception's own ``')``, then the group's ``]``
    and ``)`` - so a scrub terminator that admits only a lone ``')`` at
    end-of-line never matches, and the row value ships verbatim.
    """
    from taskq.obs._redact_exc import scrub_exception_field

    secret = "subject-31337"
    group = ExceptionGroup(
        "group",
        [
            RuntimeError(
                "some failure\nDETAIL:  Key (identity_key)=(" + secret + ") already exists."
            )
        ],
    )
    flattened = repr(group)
    # Precondition: the flattened form really does leak, or this test proves nothing.
    assert secret in flattened

    safe = scrub_exception_field("error", flattened)
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.

    # Exact shape: the group structure and primary message survive, the closers
    # are kept, and only the DETAIL payload is gone.
    assert safe == "ExceptionGroup('group', [RuntimeError('some failure')])"


def test_repr_flattened_detail_inside_a_nested_exception_group_is_scrubbed() -> None:
    """Each nesting level adds a ``])`` to the repr's closing run; the scrub
    must not be anchored to one fixed run length."""
    from taskq.obs._redact_exc import scrub_exception_field

    secret = "tenant-55512"
    group = ExceptionGroup(
        "outer",
        [
            ExceptionGroup(
                "inner",
                [
                    RuntimeError(
                        "some failure\nDETAIL:  Key (identity_key)=(" + secret + ") exists."
                    )
                ],
            )
        ],
    )
    flattened = repr(group)
    # Precondition: the flattened form really does leak, or this test proves nothing.
    assert secret in flattened

    safe = scrub_exception_field("error", flattened)
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.

    assert (
        safe == "ExceptionGroup('outer', [ExceptionGroup('inner', [RuntimeError('some failure')])])"
    )


def test_repr_flattened_detail_without_a_safe_terminator_is_scrubbed_anyway() -> None:
    """A DETAIL whose tail matches no safe delimiter is scrubbed through end
    of line rather than shipped: a redaction miss must delete more text,
    never less of the secret."""
    from taskq.obs._redact_exc import scrub_exception_field

    secret = "subject-31337"
    # Unterminated repr: no closing quote and no further escaped newline, so
    # neither precise terminator can fire.
    flattened = "RuntimeError('some failure\\nDETAIL:  Key (identity_key)=(" + secret

    safe = scrub_exception_field("error", flattened)
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.

    assert safe == "RuntimeError('some failure"


def test_scrub_preserves_non_detail_escaped_newlines() -> None:
    """Only DETAIL/HINT/CONTEXT-shaped escaped lines are scrubbed - a
    repr whose message merely spans lines keeps every line."""
    from taskq.obs._redact_exc import scrub_exception_field

    safe = scrub_exception_field("error", repr(ValueError("line one\nline two")))
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.

    assert "line one" in safe
    assert "line two" in safe
    assert "DETAIL" not in safe


# ── substring prefilters on _scrub_text (perf: error-storm hot path) ────
#
# Each scrub regex requires a literal trigger substring in the subject:
# _PG_DETAIL_RE and _PG_DETAIL_ESCAPED_RE need "DETAIL:"; _URI_CRED_RE needs
# "://"; _URI_PARAM_CRED_RE needs a password-family parameter name followed
# by "=" (and does NOT need "://" - bare "host/db?password=…" must stay
# masked). The prefilter must therefore be a NECESSARY-condition guard per
# regex: skipping when the substring is absent never changes output, only
# cost. These tests pin both the skip and the byte-identical outputs.


def test_scrub_text_skips_all_regexes_when_no_trigger_substrings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Text without any trigger substring must not run a single regex sub."""
    import taskq.obs._redact_exc as redact_mod

    class _Boom:
        def __init__(self, name: str) -> None:
            self._name = name

        def sub(self, *args: object, **kwargs: object) -> str:
            raise AssertionError(
                f"{self._name}.sub must not run for text without its trigger substring"
            )

    for pattern_name in (
        "_PG_DETAIL_RE",
        "_PG_DETAIL_ESCAPED_RE",
        "_URI_CRED_RE",
        "_URI_PARAM_CRED_RE",
    ):
        monkeypatch.setattr(redact_mod, pattern_name, _Boom(pattern_name))

    clean = "plain failure: connection refused after 3 attempts"
    assert redact_mod._scrub_text(clean) == clean


def test_scrub_text_skips_detail_regexes_when_detail_substring_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """URI-shaped text runs the URI masks but must not run the DETAIL regexes
    (the DETAIL guard is independent of the URI guards)."""
    import taskq.obs._redact_exc as redact_mod

    class _Boom:
        def sub(self, *args: object, **kwargs: object) -> str:
            raise AssertionError("DETAIL regex sub must not run for text without 'DETAIL:'")

    monkeypatch.setattr(redact_mod, "_PG_DETAIL_RE", _Boom())
    monkeypatch.setattr(redact_mod, "_PG_DETAIL_ESCAPED_RE", _Boom())

    assert redact_mod._scrub_text("postgresql://taskq:hunter2@db.internal:5432/taskq") == (
        "postgresql://taskq:***@db.internal:5432/taskq"
    )


@pytest.mark.parametrize(
    ("label", "raw", "expected"),
    [
        (
            "pg-detail",
            'duplicate key value violates unique constraint "jobs_pkey"\n'
            "DETAIL: Key (idempotency_key)=(tenant-4417-ssn) already exists.",
            'duplicate key value violates unique constraint "jobs_pkey"\n',
        ),
        (
            "pg-detail-leading-ws",
            "error:\n  DETAIL: Key (k)=(v) exists.\nHINT: check",
            "error:\n\nHINT: check",
        ),
        (
            "pg-detail-repr-escaped",
            "UniqueViolationError('duplicate key...\\nDETAIL: Key (k)=(secret-row-value) already exists.')",
            "UniqueViolationError('duplicate key...')",
        ),
        (
            "uri-userinfo",
            "could not connect to postgresql://taskq:hunter2@db.internal:5432/taskq",
            "could not connect to postgresql://taskq:***@db.internal:5432/taskq",
        ),
        (
            "uri-empty-user",
            "redis://:AZaBcD3f@cache.internal:6380 unreachable",
            "redis://:***@cache.internal:6380 unreachable",
        ),
        (
            "uri-query-param",
            "could not connect to postgresql://db.internal:5432/taskq?password=hunter2",
            "could not connect to postgresql://db.internal:5432/taskq?password=***",
        ),
        (
            # No "://" anywhere: _URI_PARAM_CRED_RE must still fire.
            "uri-query-param-no-scheme",
            "db.internal:5432/taskq?password=hunter2 failed",
            "db.internal:5432/taskq?password=*** failed",
        ),
        (
            "uri-both-shapes",
            "scheme://user:SECRET@host/db?password=OTHER",
            "scheme://user:***@host/db?password=***",
        ),
        ("pwd-param-partial", "cpwd=x and &pwd=y", "cpwd=x and &pwd=***"),
        ("detail-lowercase-not-matched", "detail: not-a-match", "detail: not-a-match"),
        (
            "clean",
            "plain failure: connection refused after 3 attempts",
            "plain failure: connection refused after 3 attempts",
        ),
        ("unicode", "échec: ünïcödé ✨ message", "échec: ünïcödé ✨ message"),
        ("empty", "", ""),
        ("newline-no-detail", "line one\nline two", "line one\nline two"),
        ("path-colon-slash-no-uri", "path:/not/a/uri and a:b", "path:/not/a/uri and a:b"),
    ],
)
def test_scrub_text_outputs_byte_identical(label: str, raw: str, expected: str) -> None:
    """Exact scrub outputs - the prefilter must not change a single byte."""
    from taskq.obs._redact_exc import _scrub_text

    assert _scrub_text(raw) == expected, label


def test_scrub_text_prefilter_disabled_redaction_still_skips_detail_regexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With redaction off, clean text still runs only the URI masks - the
    toggle semantics and the prefilter compose."""
    import taskq.obs._redact_exc as redact_mod

    class _Boom:
        def sub(self, *args: object, **kwargs: object) -> str:
            raise AssertionError("DETAIL regex sub must not run while redaction is disabled")

    monkeypatch.setattr(redact_mod, "_PG_DETAIL_RE", _Boom())
    monkeypatch.setattr(redact_mod, "_PG_DETAIL_ESCAPED_RE", _Boom())
    redact_mod.set_exception_redaction_enabled(False)
    try:
        text = "DETAIL: would-be-dropped\nbut redaction is off"
        assert redact_mod._scrub_text(text) == text
    finally:
        redact_mod.set_exception_redaction_enabled(True)


def test_libpq_password_inside_an_exception_group_stacktrace_is_masked() -> None:
    """The password-family masks run on the whole rendered stacktrace, same as
    the DETAIL masks -- a libpq ``password=`` shape sitting in a sub-exception's
    message inside an ``ExceptionGroup`` must not survive the ``| ``-prefixed
    rendering ``traceback.format_exception`` gives group members, even though
    (unlike the DETAIL regex) the credential regexes are not line-anchored and
    so do not need a matching prefix carve-out."""
    try:
        try:
            raise RuntimeError("conn failed: host=db user=app password=hunter2")
        except RuntimeError as inner:
            raise ExceptionGroup("group", [inner]) from None
    except ExceptionGroup as group:
        span = _RecordingSpan()
        record_exception_safe(span, group)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    _, attrs = span.events[0]
    trace = attrs["exception.stacktrace"]
    assert "hunter2" not in trace
    assert "password=***" in trace
    assert "ExceptionGroup" in trace


def test_uri_query_param_password_inside_a_nested_exception_group_is_masked() -> None:
    """Same as above, one nesting level deeper (doubled ``| `` prefix), for the
    URI query-param spelling rather than the libpq keyword/value spelling."""
    try:
        try:
            try:
                raise RuntimeError("postgresql://db/jobs?PASSWORD=hunter2")
            except RuntimeError as inner:
                raise ExceptionGroup("inner-group", [inner]) from None
        except ExceptionGroup as inner_group:
            raise ExceptionGroup("outer-group", [inner_group]) from None
    except ExceptionGroup as outer_group:
        span = _RecordingSpan()
        record_exception_safe(span, outer_group)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.

    _, attrs = span.events[0]
    trace = attrs["exception.stacktrace"]
    assert "hunter2" not in trace
    assert "password=***" in trace.lower()


def test_libpq_password_survives_through_chained_cause_and_implicit_context() -> None:
    """A DSN password can appear on either link of a chained exception -- the
    explicit ``raise ... from cause`` form and the implicit ``__context__``
    TaskQ gets for free from a bare ``except``/``raise`` inside it -- and both
    render into the same traceback text that reaches the span."""
    # Explicit __cause__.
    try:
        try:
            raise RuntimeError("host=db user=app password=hunter2")
        except RuntimeError as inner:
            raise RuntimeError("outer failure") from inner
    except RuntimeError as outer:
        span = _RecordingSpan()
        record_exception_safe(span, outer)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.
    _, attrs = span.events[0]
    assert "hunter2" not in attrs["exception.stacktrace"]

    # Implicit __context__ (no `from`).
    try:
        try:
            raise RuntimeError("postgresql://db/jobs?sslpassword=hunter2")
        except RuntimeError:
            raise RuntimeError("outer failure, unrelated")  # noqa: B904  # Why: this case exists to exercise the implicit-__context__ chain; an explicit `from` would change the shape under test.
    except RuntimeError as outer2:
        span2 = _RecordingSpan()
        record_exception_safe(span2, outer2)  # type: ignore[arg-type]  # Why: structural stand-in for opentelemetry Span.
    _, attrs2 = span2.events[0]
    assert "hunter2" not in attrs2["exception.stacktrace"]


def test_repr_flattened_libpq_password_is_masked() -> None:
    """``repr(exc)`` is a majority log idiom (``error=repr(exc)``) and does not
    escape a credential shape the way it escapes a real newline -- the
    password-family regexes are not newline-anchored like the DETAIL pair, so
    they must bite directly on the repr text with no companion escaped-form
    pattern needed. Pinning that here rather than assuming it from the DETAIL
    behaviour, since the two mask families reach the text through different
    mechanisms."""
    from taskq.obs._redact_exc import _scrub_text

    exc = Exception("host=db user=app password=hunter2")
    scrubbed = _scrub_text(repr(exc))
    assert "hunter2" not in scrubbed
    assert "password=***" in scrubbed


@pytest.mark.parametrize(
    "raw",
    [
        "postgresql://db/jobs?password=hun.ter2$",
        "postgresql://db/jobs?password=a(b)c+d*e[f]",
        "host=db user=app password=hun.ter2$",
        "host=db user=app password=a(b)c+d*e[f]",
    ],
    ids=[
        "query-param-regex-metachars",
        "query-param-more-metachars",
        "libpq-regex-metachars",
        "libpq-more-metachars",
    ],
)
def test_password_containing_regex_metacharacters_is_fully_masked(raw: str) -> None:
    """The masked VALUE is matched by a character class, never re-interpreted
    as a regex fragment -- a password that happens to contain characters with
    regex meaning (``.``, ``$``, ``(``, ``)``, ``+``, ``*``, ``[``, ``]``) is
    ordinary data to ``re.sub`` and must be masked whole, not partially matched
    or used to corrupt the substitution."""
    safe = safe_exception_message(Exception(raw))
    assert "***" in safe
    for fragment in ("hun.ter2$", "a(b)c+d*e[f]"):
        assert fragment not in safe


def test_url_encoded_password_value_is_masked_as_written() -> None:
    """A percent-encoded password in a query string is masked as the literal
    encoded token it is -- the redactor must not need to URL-decode first to
    find the boundary, and the encoded form itself is exactly as sensitive as
    the decoded one (it round-trips through any URL decoder downstream)."""
    safe = safe_exception_message(Exception("postgresql://db/jobs?password=hun%40secret%3D"))
    assert "hun%40secret%3D" not in safe
    assert "password=***" in safe


def test_libpq_password_at_end_of_string_with_no_trailing_delimiter_is_masked() -> None:
    """The unquoted libpq value class stops at whitespace or ``&`` -- when the
    secret is the last thing in the string, with no trailing delimiter
    at all, the value must still be consumed to the true end of the string
    rather than left dangling because no delimiter was found to stop at."""
    safe = safe_exception_message(Exception("host=db port=5432 user=app password=hunter2"))
    assert safe == "host=db port=5432 user=app password=***"


def test_sslpassword_libpq_form_at_end_of_string_is_masked() -> None:
    """Same end-of-string boundary, for the ``sslpassword`` keyword rather than
    ``password`` -- the issue's third named spelling, in the shape most likely
    to appear (the TLS key passphrase is typically the last keyword in a
    hand-built conninfo string)."""
    safe = safe_exception_message(Exception("host=db user=app sslpassword=hunter2"))
    assert safe == "host=db user=app sslpassword=***"


# ── the marker prefix must stay LINEAR under adversarial input ──
#
# _PG_DETAIL_RE's ExceptionGroup marker prefix was originally spelled
# ``(?:[ \t]*[|+][ \t]*)*`` -- a quantifier inside a quantifier. Given one
# DETAIL line anywhere in the text (all the ``"DETAIL:" in text`` prefilter
# needs) plus a line of markers with no DETAIL after it, the engine
# re-partitioned the marker run across the repetitions in exponentially
# many ways and tried them all: ~60 ms at 20 markers, ~4x per marker pair
# added, effectively unbounded past the mid-20s. The scrub runs
# synchronously on the event loop (the failed-attempt, dispatch-error and
# cron paths), so a poison job whose message echoes that shape held off
# every heartbeat with it until the watchdog dumped the loop (5 s) and then
# killed the worker (30 s) -- deterministically, on every retry. The pins
# below: a time budget no super-linear matcher can meet, byte-for-byte
# equivalence with the retired pattern (the one-pass fix must not scrub a
# byte more or less), and a structural guard so the nested-quantifier shape
# cannot quietly return in any regex the obs package compiles.

#: The retired, catastrophically-backtracking form of _PG_DETAIL_RE, kept
#: verbatim as the equivalence oracle: the one-pass character class that
#: replaced it must accept exactly the same marker-prefixed DETAIL lines.
_RETIRED_MARKER_PREFIX_DETAIL_RE = re.compile(
    r"^(?:[ \t]*[|+][ \t]*)*[ \t]*DETAIL:.*$", re.MULTILINE
)

#: Wall-clock budget for one scrub of a pathological subject. The one-pass
#: pattern measures in single-digit milliseconds at 100k markers, so this is
#: ~60x headroom for CI noise -- while no super-linear matcher can meet it
#: (quadratic at 100k markers is minutes; the retired exponential form does
#: not finish at all past ~30).
_SCRUB_BUDGET_SECS = 0.25


def test_detail_scrub_stays_under_a_time_bound_on_marker_runs() -> None:
    """The scrub must stay linear on the inputs that made the retired
    pattern exponential: a marker-heavy line (with or without whitespace,
    before or after a DETAIL anchor) in text that carries one DETAIL line
    somewhere, and the repr()-flattened delimiter-miss shape.

    Why synthetic marker runs and not only a deep ExceptionGroup:
    ``traceback`` truncates group rendering at ``max_group_depth`` (10), so
    an organically rendered traceback tops out near nine marker levels --
    harmless for even the retired pattern. The blowup needs ~20+ markers on
    one line, which reaches the scrub as adversarial text echoed into an
    exception MESSAGE (an actor formatting a traceback into its message, a
    PG error echoing operator text) -- the poison-job shape modelled here.
    """
    import time

    from taskq.obs._redact_exc import scrub_exception_field

    cases: list[tuple[str, str]] = [
        # The poison-message shape: a real DETAIL line plus a 2000-marker
        # line with no DETAIL on it (the anchored match FAILS on that line,
        # which is where the retired pattern re-partitioned the run).
        (
            "poison-message-marker-run",
            "DETAIL: Key (idempotency_key)=(v) exists.\n"
            + "| " * 2_000
            + "echoed text, no DETAIL on this line\n",
        ),
        # Scaling legs: 100k markers kill a merely-quadratic regression,
        # not just the exponential one.
        ("marker-run-100k", "DETAIL: legit\n" + "| " * 100_000 + "tail\n"),
        ("dense-marker-run-100k", "DETAIL: legit\n" + "|" * 100_000 + "\n"),
        ("markers-before-detail-100k", "| " * 100_000 + "DETAIL: secret row value\n"),
        # The escaped-newline companion's fail-closed leg: a DETAIL whose
        # tail matches no safe delimiter scrubs through end of line, over a
        # 100k run of closers.
        (
            "repr-escaped-delimiter-miss-100k",
            "RuntimeError('some failure\\nDETAIL: Key (k)=(sec) exists. " + ")" * 100_000 + " x",
        ),
    ]
    for label, text in cases:
        start = time.perf_counter()
        scrubbed = scrub_exception_field("error_traceback", text)
        elapsed = time.perf_counter() - start
        assert elapsed < _SCRUB_BUDGET_SECS, (
            f"{label}: scrubbing took {elapsed * 1000:.1f} ms -- a super-linear "
            "marker-prefix matcher is back, and this scrub runs on the event "
            "loop where seconds per failed job arm the watchdog's kill path"
        )
        assert isinstance(
            scrubbed, str
        )  # Why: narrows the object return for the membership assert below.
        # Non-vacuous: the subject really exercised the DETAIL scrub, so the
        # budget was spent on the work, not skipped by a prefilter miss.
        assert "DETAIL:" not in scrubbed, label


def test_detail_scrub_stays_bounded_on_a_rendered_exception_group() -> None:
    """The organic shape: a nested ExceptionGroup wrapping a PG error,
    rendered by ``traceback.format_exception`` and scrubbed whole.

    Depth 8 renders the inner DETAIL line (inside ``max_group_depth``), so
    the scrub really has marker-prefixed work to do; depth 30 exercises the
    truncated rendering path. Both must stay inside the same budget.
    """
    import time

    from taskq.obs._redact_exc import render_exception

    def _deep_group(levels: int) -> BaseException:
        exc: BaseException = _unique_violation("Key (idempotency_key)=(cust-88) exists.")
        for depth in range(levels):
            try:
                raise exc
            except Exception as caught:  # Why: re-raising the accumulated chain is what gives each level a real traceback; the group is the shape under test, not the handler.
                exc = ExceptionGroup(f"layer-{depth}", [caught])
        return exc

    try:
        raise _deep_group(8)
    except BaseException as group8:
        rendered = render_exception(group8)
        assert "cust-88" in rendered.raw_stacktrace, (
            "precondition: the 8-deep render must carry the DETAIL line the "
            "scrub exists to drop, or the budget below proves nothing"
        )
        start = time.perf_counter()
        scrubbed = render_exception(group8)
        elapsed = time.perf_counter() - start
        assert elapsed < _SCRUB_BUDGET_SECS, (
            f"8-deep group render+scrub took {elapsed * 1000:.1f} ms"
        )
        assert "cust-88" not in scrubbed.stacktrace
        assert "cust-88" not in scrubbed.message

    try:
        raise _deep_group(30)
    except BaseException as group30:
        start = time.perf_counter()
        render_exception(group30)
        elapsed = time.perf_counter() - start
        assert elapsed < _SCRUB_BUDGET_SECS, (
            f"30-deep group render+scrub took {elapsed * 1000:.1f} ms"
        )


def test_detail_pattern_accepts_the_same_lines_as_the_retired_marker_shape() -> None:
    """The one-pass ``[ \\t|+]*`` prefix must scrub byte-for-byte identically
    to the retired ``(?:[ \\t]*[|+][ \\t]*)*`` form on every input --
    realistic PG traces, real rendered ExceptionGroups, adversarial marker
    runs, and a randomized sweep over the prefix alphabet.

    Equivalence is the whole safety argument of the fix: the retired form's
    accepted-prefix language is exactly "any run of spaces, tabs and
    ``|``/``+`` markers", which is what the character class spells directly,
    so swapping it cannot over-scrub (lose a diagnostic line) or under-scrub
    (ship a row value) anywhere the old one was correct.
    """
    import random
    import traceback as traceback_mod

    from taskq.obs._redact_exc import (
        _PG_DETAIL_RE,  # pyright: ignore[reportPrivateUsage]  # Why: the module's own pattern is the object under test; the public behaviour pins live in the byte-identical cases above.
    )

    corpus: list[str] = [
        # Realistic PG error text: primary template, DETAIL, HINT, CONTEXT.
        'duplicate key value violates unique constraint "jobs_pkey"\n'
        "DETAIL:  Key (idempotency_key)=(tenant-4417-ssn) already exists.\n"
        'HINT:  Perhaps "idempotency_key" is unique for a reason.\n'
        "CONTEXT:  PL/pgSQL function taskq.enqueue(text) line 12 at SQL statement",
        "error:\n  DETAIL: Key (k)=(v) exists.\nHINT: check",
        "detail: lowercase is not Postgres's spelling",
        # Marker-prefixed DETAIL lines at every organic nesting depth.
        "| DETAIL:  Key (k)=(v)",
        "| | DETAIL:  Key (k)=(v)",
        "| | | | DETAIL:  Key (k)=(v)",
        "\t| \t+ DETAIL: mixed markers",
        # Group header/separator lines: must SURVIVE both patterns.
        "  | ExceptionGroup: layer-0 (1 sub-exception)",
        "  +-+---------------- 1 ----------------",
        "| HINT:  structural, kept",
        # Adversarial marker runs, with and without a DETAIL anchor. Runs
        # stay at <=16 markers BECAUSE the retired oracle is itself
        # exponential on a failing marker run (the perf pin above holds the
        # 100k-marker legs; 2^16 partitions is already far more re-split
        # ambiguity than any real prefix produces).
        "| " * 16,
        "|" * 16,
        "+|" * 8 + " DETAIL: after dense run",
        "| " * 16 + "DETAIL: after spaced run",
        "| " * 16 + "no detail here",
        "no markers at all",
        "",
    ]

    # Real rendered groups at depths 1-3 (the depths that render inside
    # max_group_depth), each with a DETAIL-bearing PG error at the bottom.
    for levels in (1, 2, 3):
        exc: BaseException = _unique_violation("Key (identity_key)=(subject-31337) exists.")
        for depth in range(levels):
            try:
                raise exc
            except Exception as caught:  # Why: same construction as the perf pin -- a real traceback per level is the organic rendering under test.
                exc = ExceptionGroup(f"layer-{depth}", [caught])
        try:
            raise exc
        except BaseException as group:
            corpus.append(
                "".join(traceback_mod.format_exception(type(group), group, group.__traceback__))
            )

    # Randomized sweep over the alphabet the two prefixes can disagree on:
    # marker/whitespace runs, the DETAIL anchor, and filler text around
    # them, at random line positions.
    rng = random.Random(20260917)  # noqa: S311  # Why: a fixed seed keeps the equivalence sweep deterministic; nothing cryptographic.
    alphabet = [" ", "\t", "|", "+", "DETAIL:", "x", "Key (k)=(v)", "HINT:", "\n"]
    for _ in range(60):
        lines = [
            [rng.choice(alphabet) for _ in range(rng.randint(1, 12))]
            for _ in range(rng.randint(2, 25))
        ]
        # Guarantee the scrub has something to do in most (not all) sweeps:
        # a DETAIL-less text pins the no-match path just as tightly.
        if rng.random() < 0.8:
            lines[rng.randrange(len(lines))].append("DETAIL: seeded")
        corpus.append("\n".join("".join(parts) for parts in lines))

    scrubbed_any = False
    for text in corpus:
        expected = _RETIRED_MARKER_PREFIX_DETAIL_RE.sub("", text)
        actual = _PG_DETAIL_RE.sub("", text)
        assert actual == expected, (
            "the one-pass marker prefix scrubbed differently than the retired "
            f"form on {text!r} -- the fix must be pure performance, never a "
            "semantic change: expected (retired) "
            f"{expected!r}, got (current) {actual!r}"
        )
        scrubbed_any = scrubbed_any or actual != text
    assert scrubbed_any, "the corpus scrubbed nothing -- the equivalence above passed vacuously"


def test_no_nested_quantifier_regexes_in_taskq_obs() -> None:
    """Structural guard: no regex compiled anywhere in ``taskq.obs`` may put
    a quantifier inside a quantifier -- the catastrophic-backtracking shape
    that made the retired _PG_DETAIL_RE exponential.

    Every pattern here runs on text an actor or a database error chose
    (exception messages, tracebacks), on the event loop, at error-storm
    rates -- a hostile message must not be able to spend seconds in a
    scrub. The check walks the parsed pattern tree: a plain ``*``/``+``/
    ``{m,n}``/lazy repeat whose body contains another plain repeat can be
    re-partitioned by backtracking in exponentially many ways.

    Two passes, because each sees a surface the other cannot. The
    attribute walk covers every ``re.Pattern`` object the modules expose
    at import time (compiled module-level constants, and anything
    re-exported into them) -- the package's standing convention is that
    scrub regexes ARE module-level constants, visible to this walk, to
    the reader, and compiled once. The AST pass closes that walk's
    visibility hole: a future function-local ``re.compile`` (zero such
    sites today) never becomes a module attribute, so the walk would
    silently skip it -- the AST pass audits every ``*.compile(...)`` call
    site in the package's source wherever it sits, checking its pattern
    argument when it is a constant and FLAGGING it when it is not (a
    dynamically built pattern is unauditable by any static guard, and
    the flag is the honest answer). A pattern that does not even parse
    as a regex is flagged rather than skipped, the same fail-closed
    posture the follow-up review endorsed for the parser import
    itself: a guard that quietly tolerates what it cannot check is a
    guard that reports green on the next .

    Two deliberate scope limits, both stated so the next author knows the
    guard's edge: an ``ATOMIC_GROUP`` / possessive-repeat boundary is not
    crossed for the containment check (an outer quantifier cannot
    re-partition what an atomic group committed -- though a nested pair
    fully INSIDE one is still flagged by the recursion, since it explodes
    within its own single match attempt); and the overlapping-alternation
    shape (``(?:a|a)*``, equally catastrophic, equally absent here) is
    not analysed -- an alternation inside a repeat must keep DISJOINT
    first characters, the way ``'(?:[^'\\\\]|\\\\.)*'`` does. Unknown
    opcodes fail the test rather than pass silently, so the guard extends
    consciously.
    """
    import ast
    import importlib
    from pathlib import Path
    from types import ModuleType
    from typing import Any

    import taskq.obs as obs_pkg

    # Runtime-resolved and held as Any, deliberately: the pattern-tree
    # walker needs the parser's own opcode vocabulary, which re exposes
    # only as private submodules (re._parser / re._constants -- no public
    # API, no type stubs), and importlib keeps the guard working on every
    # interpreter that can run the suite while pyright stays quiet without
    # a blanket ignore. No try/except around the import on purpose: a
    # future Python that moves the parser again must FAIL this guard
    # loudly, not pass it vacuously.
    sre_parser: Any = importlib.import_module("re._parser")
    sre_constants: Any = importlib.import_module("re._constants")

    _plain_repeats = (sre_constants.MAX_REPEAT, sre_constants.MIN_REPEAT)
    _breakers = {sre_constants.ATOMIC_GROUP, sre_constants.POSSESSIVE_REPEAT}
    _leaves = {
        sre_constants.LITERAL,
        sre_constants.NOT_LITERAL,
        sre_constants.IN,
        sre_constants.ANY,
        sre_constants.AT,
        sre_constants.CATEGORY,
        sre_constants.GROUPREF,
    }

    def _subtrees(op: Any, av: Any) -> list[list[Any]]:
        """The nested node lists *op* carries; leaf opcodes carry none."""
        if op in _leaves:
            return []
        if op in _plain_repeats or op is sre_constants.POSSESSIVE_REPEAT:
            return [av[2]]
        if op is sre_constants.ATOMIC_GROUP:
            return [av]
        if op is sre_constants.SUBPATTERN:
            return [av[3]]
        if op is sre_constants.BRANCH:
            return list(av[1])
        if op in (sre_constants.ASSERT, sre_constants.ASSERT_NOT):
            return [av[1]]
        if op is sre_constants.GROUPREF_EXISTS:
            return [av[1], av[2]]
        raise AssertionError(
            f"pattern-tree walker met an opcode it does not know ({op!r}) -- "
            "extend the container/leaf maps here rather than let a new regex "
            "construct pass this guard unchecked"
        )

    def _contains_plain_repeat(nodes: list[Any], *, cross_breakers: bool) -> bool:
        for op, av in nodes:
            if op in _plain_repeats:
                return True
            if op in _breakers and not cross_breakers:
                continue
            if any(
                _contains_plain_repeat(sub, cross_breakers=cross_breakers)
                for sub in _subtrees(op, av)
            ):
                return True
        return False

    def _offenders(nodes: list[Any]) -> list[str]:
        found: list[str] = []
        for op, av in nodes:
            # The containment check stops at atomic/possessive boundaries:
            # an outer repeat cannot re-partition what they committed. The
            # recursion below still crosses them, since a nested pair inside
            # one explodes within its own single match attempt.
            if op in _plain_repeats and _contains_plain_repeat(av[2], cross_breakers=False):
                found.append(f"repeat body contains another repeat: {op!r} over {av[2]!r}")
            for sub in _subtrees(op, av):
                found.extend(_offenders(sub))
        return found

    def _pattern_offenders(pattern: re.Pattern[str]) -> list[str]:
        return _offenders(sre_parser.parse(pattern.pattern, pattern.flags))

    # Non-vacuity first: the guard must fire on the retired shape (and on
    # the textbook nested-quantifier forms), or it cannot be trusted to
    # catch a regression of .
    for bad in (
        r"^(?:[ \t]*[|+][ \t]*)*[ \t]*DETAIL:.*$",
        r"(a+)+b",
        r"(?:a*b)*c",
        r"(a*)*b",
    ):
        assert _pattern_offenders(re.compile(bad)), (
            f"the nested-quantifier walker failed to flag the known-catastrophic {bad!r} "
            "-- the guard is vacuous and cannot be trusted"
        )
    # And it must NOT flag the mitigated shapes an author might reasonably
    # reach for (atomic/possessive boundaries), or it would cry wolf.
    for ok in (
        r"(?>a*)b",
        r"(?:(?>a*b*))*c",
        r"a*+b",
    ):
        assert _pattern_offenders(re.compile(ok)) == [], (
            f"the walker flagged the mitigated pattern {ok!r} -- atomic/possessive "
            "boundaries must not count as nested quantifiers"
        )

    # Pass one, recorded: remember which module attributes were audited, so
    # pass two can close the loop for dynamically built patterns below.
    audited_attrs: set[tuple[str, str]] = set()  # (module_name, attr_name)

    # The actual audit, pass one: every regex compiled by every module of
    # the obs package (file-driven, so a new module is covered the day it
    # lands), read from the module attributes -- the module-level-constant
    # convention this package keeps its scrub regexes under.
    pkg_dir = Path(obs_pkg.__file__).resolve().parent
    checked = 0
    for path in sorted(pkg_dir.glob("*.py")):
        mod_name = "taskq.obs" if path.stem == "__init__" else f"taskq.obs.{path.stem}"
        module: ModuleType = importlib.import_module(mod_name)
        for attr_name, attr in vars(module).items():
            if not isinstance(attr, re.Pattern) or attr_name.startswith("__"):
                continue
            checked += 1
            audited_attrs.add((mod_name, attr_name))
            offenders = _pattern_offenders(attr)
            assert offenders == [], (
                f"{mod_name}.{attr_name} ({attr.pattern!r}) carries the "
                f"nested-quantifier shape that made exception redaction "
                f"exponential on hostile input: {offenders}"
            )
    assert checked >= 4, (
        "the audit found fewer compiled regexes than the obs package is known "
        "to carry -- the walker is probably reading the wrong modules"
    )

    # Pass two, the visibility hole the attribute walk cannot close: a
    # future function-local re.compile never becomes a module attribute.
    # The AST pass audits every *.compile(...) call site in the package's
    # own source wherever it sits -- a constant pattern is checked on the
    # spot; a NON-constant pattern is acceptable only as the right-hand
    # side of a module-level assignment, whose compiled object lands in a
    # module attribute that pass one audited (asserted below, closing the
    # loop); anywhere else -- a function body, a conditional, an inline
    # expression -- a dynamically built pattern is unauditable by any
    # static guard and is flagged. A pattern that does not even parse as
    # a regex is flagged too (re.compile would reject it at runtime -- the
    # site is broken, not unauditable).
    def _compile_sites(node: ast.AST, in_top_assign_value: bool) -> list[tuple[ast.Call, bool]]:
        """Every ``*.compile(...)`` call under *node*, flagged with whether
        it sits inside a MODULE-LEVEL assignment's value."""
        sites: list[tuple[ast.Call, bool]] = []
        if isinstance(node, ast.Call):
            if isinstance(node.func, ast.Attribute) and node.func.attr == "compile" and node.args:
                sites.append((node, in_top_assign_value))
            for arg in node.args:
                sites.extend(_compile_sites(arg, in_top_assign_value))
            for kwarg in node.keywords:
                sites.extend(_compile_sites(kwarg.value, in_top_assign_value))
            return sites
        for child in ast.iter_child_nodes(node):
            sites.extend(_compile_sites(child, in_top_assign_value))
        return sites

    compile_calls = 0
    for path in sorted(pkg_dir.glob("*.py")):
        mod_name = "taskq.obs" if path.stem == "__init__" else f"taskq.obs.{path.stem}"
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for stmt in tree.body:
            if isinstance(stmt, ast.Assign | ast.AnnAssign) and stmt.value is not None:
                sites = _compile_sites(stmt.value, in_top_assign_value=True)
                targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
                assign_names = [t.id for t in targets if isinstance(t, ast.Name)]
            else:
                sites = _compile_sites(stmt, in_top_assign_value=False)
                assign_names = []
            for call, in_top_assign in sites:
                compile_calls += 1
                pattern_arg = call.args[0]
                if not (
                    isinstance(pattern_arg, ast.Constant) and isinstance(pattern_arg.value, str)
                ):
                    if not in_top_assign:
                        raise AssertionError(
                            f"{mod_name}: re.compile at line {call.lineno} "
                            "builds its pattern dynamically OUTSIDE a "
                            "module-level assignment -- no static guard can "
                            "audit it, and the compiled object never becomes "
                            "an attribute pass one can see. Spell the pattern "
                            "as a literal, or hoist the call to a module-level "
                            "constant so the compiled object is audited"
                        )
                    # Dynamic, but the compiled object lands in module
                    # attributes: pass one must have audited every one of
                    # them.
                    assert assign_names, (
                        f"{mod_name}: re.compile at line {call.lineno} is a "
                        "dynamically built pattern assigned to no plain name "
                        "-- pass one cannot see its compiled object"
                    )
                    for name in assign_names:
                        assert (mod_name, name) in audited_attrs, (
                            f"{mod_name}: the dynamically built pattern "
                            f"assigned to {name!r} did not reach pass one's "
                            "audit -- the attribute walk and the AST walk "
                            "have drifted apart"
                        )
                    continue
                try:
                    offenders = _offenders(sre_parser.parse(pattern_arg.value, 0))
                except Exception as exc:  # Why: any parse failure means re.compile itself would fail at runtime; the site is broken and must be flagged, never skipped.
                    raise AssertionError(
                        f"{mod_name}: re.compile at line {call.lineno} takes "
                        f"a pattern that does not parse as a regex ({exc!r}) "
                        "-- the call site is broken"
                    ) from exc
                assert offenders == [], (
                    f"{mod_name}: re.compile at line {call.lineno} "
                    f"({pattern_arg.value!r}) carries the nested-quantifier "
                    f"shape that made exception redaction exponential on "
                    f"hostile input: {offenders}"
                )
    assert compile_calls >= 4, (
        "the AST pass found fewer re.compile call sites than the obs package "
        "is known to carry -- it is probably scanning the wrong files"
    )


# ── the repr channel: marker parity and a terminator that cannot cross lines ──
#
# The repr channel -- the ``error=repr(exc)`` majority log idiom,
# scrubbed by _PG_DETAIL_ESCAPED_RE -- was half-updated: the line-anchored
# _PG_DETAIL_RE had
# gained the ``[ \t|+]*`` ExceptionGroup marker class, the escaped companion
# had kept ``[ \t]*``, so a marker-prefixed DETAIL line inside an exception
# message shipped verbatim once repr() flattened its newline. That is the
# branch's own named poison vector -- adversarial text echoed into an
# exception message -- so the parity gap was a leak on exactly the threat
# model the perf fix had closed the stall for. The escaped scrub's
# closers terminator also ended in ``\s*``: ``\s`` crosses
# newlines, so on CR-bearing text a DETAIL value carrying quote + closers +
# a CR/LF boundary satisfied the repr-tail leg by peering PAST the line end,
# and the scrub stopped at the mid-value quote, keeping closers the
# no-closers control scrubbed. The pins below cover both, red-for-old.


def test_repr_channel_scrubs_marker_prefixed_detail_lines() -> None:
    """The repr channel must see through the same marker prefixes the line
    channel does: ``repr()`` flattens the newline before DETAIL into the
    literal ``\\n`` two-char sequence, and any markers the message carries
    ride right after it.

    Red for the pre-fix escaped anchor (``[ \\t]*``): every shape below
    shipped its row value verbatim, including the group shape where repr
    renders the member inline -- and the embedded-traceback shape the
    escaped pattern's MULTILINE leg exists for.
    """
    from taskq.obs._redact_exc import scrub_exception_field

    def _marker_detail_message() -> str:
        return "some failure\n| | DETAIL:  Key (identity_key)=(" + "tenant-secret-88" + ") exists."

    # (1) A plain exception whose message carries a marker-prefixed DETAIL
    # line: repr flattens the newline, the markers ride the escaped anchor.
    safe = scrub_exception_field("error", repr(RuntimeError(_marker_detail_message())))
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.
    assert "tenant-secret-88" not in safe
    assert "some failure" in safe  # the diagnostic template survives

    # (2) The same exception inline in an ExceptionGroup's list: repr
    # renders the member inline (no marker lines of its own), so the
    # markers can only come from the message text -- and the closers run
    # the scrub must stop at is the GROUP's, not the value's.
    group = ExceptionGroup("group", [RuntimeError(_marker_detail_message())])
    safe = scrub_exception_field("error", repr(group))
    assert isinstance(safe, str)  # Why: see above.
    assert "tenant-secret-88" not in safe
    # The group structure and its closers survive (the repr-tail leg).
    assert safe.endswith("ExceptionGroup('group', [RuntimeError('some failure')])")

    # (3) Dense markers, no whitespace between them: the class must not
    # assume the ``| `` spaced rendering.
    dense = RuntimeError("some failure\n||DETAIL:  Key (k)=(" + "tenant-secret-88" + ") exists.")
    safe = scrub_exception_field("error", repr(dense))
    assert isinstance(safe, str)  # Why: see above.
    assert "tenant-secret-88" not in safe

    # (4) A repr line embedded in a rendered traceback (real newlines
    # around it) -- the documented MULTILINE case, now with markers: the
    # escaped anchor must see through them there too.
    embedded = (
        "Traceback (most recent call last):\n"
        '  File "app.py", line 3, in run\n'
        "RuntimeError('some failure\\n| | DETAIL:  Key (k)=(" + "tenant-secret-88" + ") exists.')\n"
        "during handling, another exception occurred"
    )
    safe = scrub_exception_field("error_traceback", embedded)
    assert isinstance(safe, str)  # Why: see above.
    assert "tenant-secret-88" not in safe
    assert "Traceback (most recent call last):" in safe
    assert "another exception occurred" in safe


def test_repr_channel_boundary_is_line_shaped_detail_only() -> None:
    """The deliberate boundary both channels share: a DETAIL that is not
    line-initial -- no real newline, no escaped newline, no marker prefix
    BEFORE it on the line -- is not a DETAIL *line*, and neither channel
    scrubs it.

    This is the redactor's standing shape law (true at ``bd30c1b`` and on
    main before the marker work, for ``boom DETAIL:`` exactly as for
    ``boom | | DETAIL:``): Postgres renders DETAIL at the start of its own
    line, and every carve-out since (markers, repr flattening) widens what
    counts as the START of that line -- never where on the line the anchor
    may sit. Covering a mid-line anchor would mean unanchored ``DETAIL``
    matching, which scrubs non-value text (any message quoting the word)
    and is its own over-redaction bug. Pinned so the next reader sees the
    boundary is a decision, not an oversight.
    """
    from taskq.obs._redact_exc import scrub_exception_field

    midline = repr(RuntimeError("boom | | DETAIL:  Key (k)=(" + "subject-1" + ") exists."))
    safe = scrub_exception_field("error", midline)
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.
    assert "subject-1" in safe, (
        "if this ever scrubs, an unanchored DETAIL matcher crept in -- "
        "check what else it now deletes"
    )
    # ...while the same text WITH a newline before the markers is a
    # marker-prefixed DETAIL line and must be scrubbed (the parity case).
    lined = repr(RuntimeError("boom\n| | DETAIL:  Key (k)=(" + "subject-2" + ") exists."))
    safe = scrub_exception_field("error", lined)
    assert isinstance(safe, str)  # Why: see above.
    assert "subject-2" not in safe


def test_repr_escaped_terminator_cannot_cross_a_line_boundary() -> None:
    """The closers leg of the escaped scrub's terminator ends in
    ``[ \\t]*``: same-line trailing whitespace only, never ``\\s*``.

    ``\\s`` crosses newlines, so a DETAIL value carrying a quote, closers
    and a CR/LF boundary satisfied the repr-tail leg by peering PAST the
    line end (on CR-bearing text it fires for real), and the scrub stopped
    at the mid-value quote -- keeping closers the no-closers control
    scrubs, less deletion than the control, against the module's law. The
    shape below fails under a ``\\s*`` scrub: the closers survive as a
    fake repr tail.
    """
    from taskq.obs._redact_exc import scrub_exception_field

    # The CR-rendered blank line shape: the value carries
    # quote + closers + a CR/LF boundary. The scrub must NOT accept the
    # closers as a repr tail across that boundary -- it fails closed and
    # the closers ride the scrub (more deletion, never less).
    crlf = (
        "RuntimeError('some failure\\nDETAIL:  Key (k)=('"
        + "PART1-SECRET-VALUE"
        + "')]\r\n\r\nPART2-SECRET-TAIL more')"
    )
    safe = scrub_exception_field("error", crlf)
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.
    assert "PART1-SECRET-VALUE" not in safe
    assert "')]" not in safe, (
        "the closers run after a mid-value quote must not be kept as a repr "
        "tail by a terminator that peers across the CR/LF boundary -- the "
        "scrub must fail closed there and delete more, never less"
    )
    # The line-wise boundary, stated directly: text on the NEXT physical
    # line is outside this line-bounded scrub's reach (``.`` never crosses
    # a real newline) -- identically for the shape and its no-closers
    # control below. Covering the next line is the line-channel's job on
    # real-newline text, not the repr channel's.
    assert "PART2-SECRET-TAIL" in safe

    # The control -- same structure, no quote+closers in the value -- scrubs
    # its DETAIL line and leaves the next line alone: the shape above must
    # not scrub LESS of its own line than the control does.
    control = "RuntimeError('some failure\\nDETAIL:  Key (k)=(PART1-PLAIN-VALUE)\r\n\r\nPART2-CONTROL more')"
    safe_control = scrub_exception_field("error", control)
    assert isinstance(safe_control, str)  # Why: see above.
    assert "PART1-PLAIN-VALUE" not in safe_control
    assert "PART2-CONTROL" in safe_control


def test_repr_line_embedded_in_a_traceback_keeps_its_closers() -> None:
    """The documented case the closers leg EXISTS for, pinned explicitly:
    a repr line inside a rendered traceback ends with its ``')`` (or
    ``')])``) closers at end of line, and the scrub keeps them while
    dropping the DETAIL payload -- no-newline-crossing must not cost the
    repr tail its terminator.

    This is the fixture the F5 fix had to stay green against: a
    terminator narrowed to end-of-string would eat these closers (more
    deletion, permitted by the law, but pointless diagnostic loss the leg
    exists to avoid); ``[ \\t]*$`` keeps them.
    """
    from taskq.obs._redact_exc import scrub_exception_field

    embedded = (
        "Traceback (most recent call last):\n"
        '  File "app.py", line 3, in run\n'
        "RuntimeError('some failure\\nDETAIL:  Key (k)=("
        + "subject-424242"
        + ") already exists.')\n"
        "during handling, another exception occurred"
    )
    safe = scrub_exception_field("error_traceback", embedded)
    assert isinstance(safe, str)  # Why: narrows the object return for the membership asserts.
    assert "subject-424242" not in safe
    # The closers are kept: the scrub stopped at the repr tail, not at the
    # line end (which would have amputated `')`).
    assert "RuntimeError('some failure')" in safe
    # The real traceback lines around the repr line are untouched.
    assert "Traceback (most recent call last):" in safe
    assert "another exception occurred" in safe


# ── bearer / JWT / AWS-signature credential masks (issue #317) ───────────
#
# A managed-identity access token reached logs through ``error=str(exc)``:
# an azure-identity-shaped ``HttpResponseError`` appends the HTTP body to
# ``str(exc)``, the body carries a raw bearer JWT, and the scrub pipeline's
# masks (DETAIL lines, URI userinfo, password-family query params) matched
# none of it. These tests pin the new masks AND the false-positive boundary:
# a conservative JWT shape must not mangle short ids or non-JWT base64.

_ACCESS_TOKEN = (
    "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9"
    ".eyJhdWQiOiJkYi1jbGllbnQiLCJpc3MiOiJodHRwczovL3N0cy5uZXQifQ"
    ".KmZ0Y2hfNFJlNGxseV9zZWNyZXRfc2lnbmF0dXJlX2J5dGVz"
)


class _HttpResponseShapedError(Exception):
    """Mimics azure.core's ``HttpResponseError``: ``str(exc)`` appends the
    HTTP response body, which is where a managed-identity token lives."""

    def __init__(self, body: str) -> None:
        super().__init__("ManagedIdentityCredential authentication failed")
        self._body = body

    def __str__(self) -> str:
        return f"{self.args[0]}\nContent: {self._body}"


def test_access_token_in_http_response_body_is_masked_on_the_error_field() -> None:
    """The triage's repro shape, end to end through the exact call the log
    pipeline makes: the token must not survive, and the DSN control must
    still mask the way it always has."""
    from taskq.obs._redact_exc import scrub_exception_field

    exc = _HttpResponseShapedError(f'{{"access_token": "{_ACCESS_TOKEN}", "token_type": "Bearer"}}')
    scrubbed = scrub_exception_field("error", str(exc))
    assert isinstance(scrubbed, str)  # Why: narrows the object return for the membership asserts.
    assert _ACCESS_TOKEN not in scrubbed, "access token reached the scrubbed error field"
    assert "***" in scrubbed

    # The scrub must not have cost the diagnostic: the provider failure
    # itself and the "Content:" separator stay for the operator.
    assert "ManagedIdentityCredential" in scrubbed

    # DSN control: unchanged behaviour on the shape the URI mask exists for.
    dsn = "connect failed: postgresql://taskq:hunter2@db.internal:5432/taskq"
    assert scrub_exception_field("error", dsn) == (
        "connect failed: postgresql://taskq:***@db.internal:5432/taskq"
    )


def test_access_token_is_masked_when_the_exception_object_is_passed_directly() -> None:
    """``error=exc`` (object, not string) renders through
    ``safe_exception_message`` and needs the same masks."""
    from taskq.obs._redact_exc import safe_exception_message

    exc = _HttpResponseShapedError(f'{{"access_token": "{_ACCESS_TOKEN}"}}')
    safe = safe_exception_message(exc)
    assert _ACCESS_TOKEN not in safe


@pytest.mark.parametrize(
    "header",
    [
        f"Authorization: Bearer {_ACCESS_TOKEN}",
        f"authorization: bearer {_ACCESS_TOKEN}",
        f"Authorization:Bearer {_ACCESS_TOKEN}",
        f"AUTHORIZATION  :  BEARER {_ACCESS_TOKEN}",
    ],
    ids=["plain", "lowercase", "no-space", "shouty-loose-space"],
)
def test_bearer_authorization_header_token_is_masked(header: str) -> None:
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text(f"request rejected: {header}")
    assert _ACCESS_TOKEN not in out
    # The header text is kept verbatim, only the token is replaced.
    assert out.endswith("BEARER ***") or out.endswith("Bearer ***") or out.endswith("bearer ***")


def test_opaque_bearer_token_is_masked_even_when_not_jwt_shaped() -> None:
    """An opaque bearer token (no dot structure) must be caught by the
    BEARER mask specifically: the JWT mask cannot see it, so removing the
    bearer pattern turns this red on its own (mutation sharpness)."""
    from taskq.obs._redact_exc import _scrub_text

    opaque = "smQ7_wJ8mP2xLk9ZhR4tNvBc3dF6gH1jY0pQ5sV2eT8o"
    assert "." not in opaque
    out = _scrub_text(f"Authorization: Bearer {opaque} :: 401")
    assert opaque not in out
    assert out == "Authorization: Bearer *** :: 401"


def test_jwt_shaped_token_is_masked_without_a_bearer_header() -> None:
    """A bare JWT (no ``Authorization:`` prefix around it) is masked too:
    response bodies quote the token raw."""
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text(f"token {_ACCESS_TOKEN} rejected: expired")
    assert _ACCESS_TOKEN not in out
    assert out == "token *** rejected: expired"


def test_jwt_inside_a_bearer_header_is_masked_once_without_residue() -> None:
    """Bearer and JWT passes compose: the bearer mask claims the header form,
    and nothing the masks write re-triggers a later pass."""
    from taskq.obs._redact_exc import _scrub_text

    raw = f"connect failed: postgresql://u:pw@h/db response: Authorization: Bearer {_ACCESS_TOKEN}"
    out = _scrub_text(raw)
    assert _ACCESS_TOKEN not in out
    assert "pw@" not in out
    assert out == "connect failed: postgresql://u:***@h/db response: Authorization: Bearer ***"


def test_x_amz_signature_query_param_is_masked() -> None:
    """Presigned-S3-style query strings: the signature hex is credential
    material and the password-family param mask does not cover the name."""
    from taskq.obs._redact_exc import _scrub_text

    raw = (
        "GET https://s3.example.test/bucket/obj"
        "?X-Amz-Algorithm=AWS4-HMAC-SHA256"
        "&X-Amz-Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
        "&X-Amz-Expires=60 failed"
    )
    out = _scrub_text(raw)
    assert "0123456789abcdef" not in out
    assert "X-Amz-Signature=***" in out
    assert "X-Amz-Algorithm=AWS4-HMAC-SHA256" in out  # non-credential params stay


def test_short_sig_query_param_is_masked() -> None:
    """``sig=`` in a query string (the other common presign spelling) is
    masked whatever the hex length."""
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text("download failed: https://svc.example.test/obj?sig=deadbeefdeadbeef")
    assert "deadbeef" not in out
    assert "sig=***" in out


@pytest.mark.parametrize(
    "benign",
    [
        "version 1.2.3 deployed",  # dotted, but segments far too short
        "job abc-123.def-456.ghi-789 finished",  # three segments, all short
        "digest c2VjcmV0c2VjcmV0c2VjcmV0c2VjcmV0",  # base64 blob, no dots: not JWT-shaped
        "pair aaaaaaaaaaaaaaaa.bbbbbbbbbbbbbbbb ok",  # two segments, not three
        "epoch ms 1758451200000.125 has a dot",  # numeric, single dot
        "plain failure: connection refused after 3 attempts",
    ],
    ids=["semver", "short-ids", "base64-blob", "two-segments", "numeric-dot", "clean"],
)
def test_benign_dotted_and_base64_like_strings_survive_scrubbing(benign: str) -> None:
    """The JWT shape is deliberately conservative (three base64url segments,
    each 16+ chars): short ids, semvers and non-JWT base64 must come through
    the scrub byte-identical."""
    from taskq.obs._redact_exc import _scrub_text

    assert _scrub_text(benign) == benign


# ── red-team round two: what the #317 masks still missed ─────────────────
#
# Every test below was run against the d1cebcf1 shape of the fix and FAILED
# there before its source change landed here (red/green). The holes:
#
# * a bearer header rendered with QUOTES around it -- the JSON and Python
#   repr shapes (``"Authorization": "Bearer ..."``, ``{'Authorization':
#   'Bearer ...'}``) -- sat between the name and the colon and the bearer
#   pattern only allowed spaces and tabs there. A JWT under such a header
#   escaped via the JWT pass, but an OPAQUE token escaped entirely.
# * an opaque access token under its own name in a response body (the
#   module docstring promised raw body tokens were covered; only the
#   JWT-shaped ones were). OAuth token names are credential carriers by
#   RFC 6749 regardless of the token's shape, so the value is masked by
#   name now.
# * a CloudFront-style ``Signature=`` query parameter: also a presigned
#   AWS credential, and its value is base64url, which the hex-only value
#   class could not even partially claim.
# * a percent-encoded ``sig=`` value: the value class stopped at ``%`` and
#   the tail rode through after the mask.

_OPAQUE = "smQ7_wJ8mP2xLk9ZhR4tNvBc3dF6gH1jY0pQ5sV2eT8o"
_OPAQUE_2 = "Zx9QpW3mK7rT2vY8bN5cD1fG4hJ6lS0aE3uIoPqRtUwMzX2"


@pytest.mark.parametrize(
    "raw",
    [
        f'{{"headers": {{"Authorization": "Bearer {_OPAQUE}"}}}}',
        f"KeyError: {{'Authorization': 'Bearer {_OPAQUE}'}}",
        f'ValueError(\'body {{\\"Authorization\\": \\"Bearer {_OPAQUE}\\"}} rejected\')',
    ],
    ids=["json-double", "repr-single", "escaped-json-in-repr"],
)
def test_quoted_bearer_header_forms_are_masked(raw: str) -> None:
    """A quote between the header name and its colon (JSON payload, Python
    repr, a repr()d JSON string) must not blind the bearer mask: an opaque
    token under such a header is invisible to the JWT pass, so the bearer
    pass itself has to claim it. The masked form keeps the ``Bearer`` text
    and the surrounding quotes readable."""
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text(raw)
    assert _OPAQUE not in out, f"opaque bearer token survived: {out!r}"
    assert "Bearer ***" in out


@pytest.mark.parametrize(
    "raw",
    [
        f'{{"error": "oops", "access_token": "{_OPAQUE}", "token_type": "mac"}}',
        f"KeyError: {{'access_token': '{_OPAQUE}'}}",
        f'{{"accessToken": "{_OPAQUE}"}}',
        f'{{"refresh_token": "{_OPAQUE}"}}',
        f'{{"id_token": "{_OPAQUE}"}}',
        f"GET /oauth2/token?access_token={_OPAQUE}&expires_in=3600 failed",
        f"grant_type=refresh_token&refresh_token={_OPAQUE}",
        f"token store dump: access_token: {_OPAQUE}",
        f'ValueError(\'body {{\\"access_token\\": \\"{_OPAQUE}\\"}} rejected\')',
    ],
    ids=[
        "json-body",
        "repr-single-quotes",
        "camel-case-json",
        "refresh-token",
        "id-token",
        "query-string",
        "form-body",
        "bare-colon",
        "escaped-json-in-repr",
    ],
)
def test_oauth_token_values_are_masked_by_name(raw: str) -> None:
    """An opaque (non-JWT) access token under its own name in a response
    body, a query string, a form body or a repr'd dict is credential
    material (RFC 6749) exactly like a JWT: the JWT pass cannot see it, so
    the token NAMES must be masked themselves. camelCase spellings and the
    repr()d ``\\\"`` form are the same credential."""
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text(raw)
    assert _OPAQUE not in out, f"opaque token value survived: {out!r}"


def test_oauth_mask_keeps_neighbouring_parameters_readable() -> None:
    """The token-value class must stop at the parameter delimiters: masking
    ``access_token=`` may not eat ``expires_in=3600`` (that is the
    over-redaction the password-family list exists to avoid)."""
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text(f"GET /token?access_token={_OPAQUE}&expires_in=3600&scope=db")
    assert _OPAQUE not in out
    assert "expires_in=3600" in out
    assert "scope=db" in out


def test_two_secrets_in_one_message_are_both_masked() -> None:
    """Pass ordering and per-match greediness must not let the first secret
    consume the delimiters the second one needs: an OAuth body value, a
    bearer header and a bare JWT in one message all go to ``***``."""
    from taskq.obs._redact_exc import _scrub_text

    raw = (
        f'body {{"access_token": "{_OPAQUE}"}} then Authorization: Bearer {_OPAQUE_2}'
        f" then bare {_ACCESS_TOKEN}"
    )
    out = _scrub_text(raw)
    assert _OPAQUE not in out
    assert _OPAQUE_2 not in out
    assert _ACCESS_TOKEN not in out
    assert out == 'body {"access_token": ***} then Authorization: Bearer *** then bare ***'


def test_nested_exception_repr_carries_no_token() -> None:
    """``str(outer)`` nesting ``repr(inner)`` (a chained failure rendered by
    hand, or ``__context__`` flattened by ``traceback``) puts quotes and
    backslashes around the inner token: the masks must still claim it."""
    from taskq.obs._redact_exc import _scrub_text

    raw = (
        f'Outer(\'inner: ContextFailed("Authorization: Bearer {_OPAQUE}")'
        f" body=\\'{_ACCESS_TOKEN}\\'')"
    )
    out = _scrub_text(raw)
    assert _OPAQUE not in out
    assert _ACCESS_TOKEN not in out


def test_cloudfront_style_signature_query_param_is_masked() -> None:
    """``Signature=`` is the presigned-CloudFront spelling of the same
    credential, and its value is base64url, not hex -- the hex-only value
    class matched neither the name nor (all of) the value."""
    from taskq.obs._redact_exc import _scrub_text

    raw = (
        "GET /videos/movie.mp4?Expires=1758500000"
        "&Signature=a3f9K2mQ7_pW-1xLk9ZhR4tNvBc3dF6gH1jY0pQ5sV2eT8oZx9QpW3mK7"
        "&Key-Pair-Id=K123 failed"
    )
    out = _scrub_text(raw)
    assert "a3f9K2mQ7" not in out
    assert "Signature=***" in out
    assert "Key-Pair-Id=K123" in out  # non-credential parameter stays


def test_percent_encoded_sig_value_is_masked_whole() -> None:
    """A percent-encoded ``sig=`` value must not leave its ``%XX`` tail
    riding after the mask (fail-closed: a delimiter miss deletes more,
    never less)."""
    from taskq.obs._redact_exc import _scrub_text

    out = _scrub_text("download failed: ?sig=dead%2Fbeef%20cafe")
    assert "dead" not in out
    assert "%2F" not in out
    assert "sig=***" in out


@pytest.mark.parametrize(
    "benign",
    [
        # A base64 IMAGE payload: standard base64 carries / + = and no dots,
        # so no three-segment JWT shape and nothing the masks may claim.
        "img data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0"
        "+QAAAAlwSFlzAAALEwAACxMBAJqcGAAAA/?",
        # A Kubernetes-style FQDN: five labels, several short -- no
        # three-16+-segment run.
        "dial worker-7f9d.taskq-workers.production.svc.cluster.local:9010 refused",
        # A W3C traceparent-style dotted trace id: long segments, but two
        # segments, not three.
        "trace 4bf92f3577b34da6a3ce929d084d41b8.span-0099-live-2026 ok",
    ],
    ids=["base64-image", "k8s-fqdn", "dotted-trace-id"],
)
def test_realistic_log_lines_survive_byte_identical(benign: str) -> None:
    """Realistic long-token-ish log text (image data, k8s DNS names, trace
    ids) must come through the scrub byte-identical: the JWT mask's floor is
    what keeps it conservative and these are the shapes it must not eat."""
    from taskq.obs._redact_exc import _scrub_text

    assert _scrub_text(benign) == benign


def test_three_long_label_hostname_is_masked_and_that_is_documented() -> None:
    """The accepted cost of the 16+ floor: a hostname whose THREE labels are
    all 16+ base64url chars is JWT-shaped to the mask and goes to ``***``.
    The _JWT_RE docstring states this over-match; this pin exists so the
    behaviour is a recorded decision, not an accident someone re-discovers
    in production."""
    from taskq.obs._redact_exc import _scrub_text

    host = "performance-metrics.analytics-dashboard.corporate-domain"
    assert _scrub_text(f"connection refused: {host}") == "connection refused: ***"


def test_clean_error_line_runs_no_regex(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every regex pass in :func:`_scrub_text` sits behind a substring
    prefilter stating a NECESSARY condition for the pattern to match. This
    pin makes that claim executable in the strict direction: on error text
    that carries none of the triggers, NO compiled pattern in the module may
    run ``sub`` at all -- a dropped or typo'd prefilter (the cheap path
    silently turning into a six-regex scan on every record) turns this red,
    even though the masking output would be byte-identical.

    The pattern list is read off the module's attributes, so a pass added
    tomorrow is guarded without editing this test; the module convention
    that scrub regexes are module-level constants is pinned by the
    no-nested-quantifier guard's attribute walk.
    """
    import taskq.obs._redact_exc as redact_mod

    class _ExplodingPattern:
        def sub(self, *_args: object, **_kw: object) -> str:
            raise AssertionError("a scrub regex ran .sub() on a clean line")

    pattern_names = [
        name for name, value in vars(redact_mod).items() if isinstance(value, re.Pattern)
    ]
    assert pattern_names, "no compiled patterns found: the pin guards an empty set"
    for name in pattern_names:
        monkeypatch.setattr(redact_mod, name, _ExplodingPattern())

    clean_lines = [
        "connection refused after 3 attempts",
        "queue drained in 42ms",
        "heartbeat missed on shard 7",
        "worker 8f3a2b1c (pid 4417) stopped",
    ]
    for line in clean_lines:
        assert redact_mod._scrub_text(line) == line


def test_jwt_scan_stays_linear_on_long_word_runs() -> None:
    """``_JWT_RE`` is a ``{16,}`` repeat: on a long word-run whose segments
    all NEAR-MISS (a 4000-char base64 blob plus two dots, a run followed by
    a one-under-the-floor segment), every start position backtracks through
    the run and a quadratic engine would spend seconds. These are the
    shapes a poison message reaches the scrub in (an actor formatting a
    large object, a server echoing a token-like blob), so the scan must be
    linear in the text. Budget is generous -- the 4000-char shapes measure
    ~0.2 ms -- so this pin catches blowups, not noise."""
    import time

    from taskq.obs._redact_exc import _scrub_text

    cases = [
        ("A" * 4_000 + ".a.b"),  # long run, two dots behind it
        ("A" * 4_000 + "." + "B" * 15 + ".x"),  # every start a near-miss
        (".a.b" + "A" * 4_000),  # dots before the run
        ("A." * 4_000),  # many short dotted runs
    ]
    for text in cases:
        start = time.perf_counter()
        _scrub_text(text)
        elapsed = time.perf_counter() - start
        assert elapsed < 0.05, (
            f"scrub of a {len(text)}-char word-run took {elapsed * 1000:.1f} ms -- "
            "the JWT scan's backtracking is no longer linear"
        )


def test_scrubbing_a_realistic_traceback_preserves_the_diagnostics() -> None:
    """The common large field: a rendered 27-frame traceback. Its ``.py``
    file paths supply the dots, so (unlike trigger-free text) the JWT scan
    DOES run on it -- that is stated in the ``_scrub_text`` docstring. The
    behavioral contract: a credential-free traceback comes out the
    identity, every frame and the final exception line intact -- the scrub
    must never corrupt the diagnostics it exists to protect.

    Why this is not a timing gate: an earlier revision asserted the scrub
    stayed under 1 ms here, and failed at 2.2 ms on a loaded CI runner --
    a single scheduler preemption between the two clock reads, not a code
    change. That is the same lottery ``perf-evidence-redaction.md``
    documents for the retired 10 us ``redact_payload`` gate, so the
    per-traceback cost claim (measured ~20-25 us, linear in frames: a
    1080-frame traceback scrubs in under a millisecond) lives there now,
    where runner noise can be stated honestly instead of failing a PR.
    The pathological-regression guard stays in the suite as a structural
    pin: ``test_jwt_scan_stays_linear_on_long_word_runs`` holds the scan's
    linearity on the dotted shapes a traceback reaches the scrub with.
    """
    from taskq.obs._redact_exc import _scrub_text

    text = (
        "Traceback (most recent call last):\n"
        + '  File "taskq/worker.py", line 1, in run\n' * 27
        + "RuntimeError: deadline exceeded"
    )
    assert text.count(".") >= 2  # Why: non-vacuous, the JWT prefilter really fires.
    assert _scrub_text(text) == text, (
        "a credential-free traceback must survive the scrub verbatim - every "
        "frame line and the final exception line are the diagnostics the "
        "scrub exists to deliver; a scrub that rewrites them is the "
        "over-redaction the masking doctrine forbids"
    )
