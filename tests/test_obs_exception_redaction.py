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
    — this pins the ``add_event`` attribute to the same contract. Drives the
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
    # The diagnostic template — the part that is not row data — survives.
    assert "duplicate key value violates unique constraint" in last_error


# ── repo guard: add_event attribute dicts ──────────────────────────


def test_no_raw_exception_text_in_span_event_attributes() -> None:
    """Repo-wide guard: ``add_event`` attribute dicts must not embed raw
    ``str()``/``repr()``/f-string renders of exception objects.

    ``str()`` of an asyncpg ``PostgresError`` appends the server's DETAIL
    line, which quotes row values — so an unredacted render inside a span
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
    feeds — exactly how ``job_error_message``/``infra_error_message``/
    ``job_error_traceback``/``infra_error_traceback`` (the terminal-write
    log in worker/_handlers.py) leaked the actor's exception unredacted.

    Suffix-scoped so classification fields (``error_class``, ``error_type``,
    ``job_error_class`` — class names, not exception text) never fire: the
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
                if kw.arg is None:  # Why: **kwargs splat — no field name to check.
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
    two characters ``\\n``, which the line-anchored scrub cannot see — and
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


def test_scrub_preserves_non_detail_escaped_newlines() -> None:
    """Only DETAIL/HINT/CONTEXT-shaped escaped lines are scrubbed — a
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
# by "=" (and does NOT need "://" — bare "host/db?password=…" must stay
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
    """Exact scrub outputs — the prefilter must not change a single byte."""
    from taskq.obs._redact_exc import _scrub_text

    assert _scrub_text(raw) == expected, label


def test_scrub_text_prefilter_disabled_redaction_still_skips_detail_regexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """With redaction off, clean text still runs only the URI masks — the
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
