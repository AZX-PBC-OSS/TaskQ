"""The re-attack pass over the review-UI fixes (73c8690f).

The reviewer's own pins left cells unpinned and claims under-proven. This
module attacks each claim where the original pass stopped:

1. the refresh-transport matrix, EXHAUSTIVE: every served page (the pass
   pinned 8 pages in "real-time mode" through a monkeypatch that FastAPI's
   ``Depends`` never reads, so those realtime cells were vacuous; the jobs
   page, queue detail, and batch detail had NO pin in either mode) - each
   cell pinned to its EXACT transport, forced through the real seam
   (``_factory.get_realtime_mode``, the function ``get_realtime_ctx``
   looks up at call time);
2. inline event handlers: the pass fixed ONE handler (the rate-limits
   bucket reset) - a whole-repo render scan finds the others (the job
   detail page's cancel/retry confirms interpolate ``job.id`` into inline
   ``onclick`` JS strings, the same attribute-entity-decodes-before-JS
   class the pass itself named);
3. the tojson edges: the ``</script>`` break-out and the entity
   round-trip, evaluated under Node - and the ONE config string the pass
   missed (``window.TASKQ_BASE_PATH`` is still HTML-escaped inside a
   script context);
4. the cap's uniformity: one LIMIT value across every capped surface, and
   a truncation notice on EVERY capped render that says so;
5. the offline pin: every page's script graph must be LOCAL (the pass
   moved Alpine off its CDN and left lucide on unpkg - the air-gapped
   deployment the Alpine move names renders every icon as an empty
   ``<i>`` again), and the prefix-deployment walk must cover the
   URL-bearing attributes ``src``/``href`` alone miss (``action``,
   ``hx-get``, ``sse-connect``).
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

pytest.importorskip("fastapi")

from fastapi import FastAPI
from fastapi.testclient import TestClient

from taskq.web.admin import create_router, setup_admin_state

from . import StubRecord
from .test_review_ui_pins import _ClockConn, _ClockPool

pytestmark = [pytest.mark.fastapi]


# ── scripted connections ──────────────────────────────────────────────────


def _job_row(job_id: str) -> StubRecord:
    return StubRecord(
        id=job_id,
        actor="send_email",
        queue="default",
        status="running",
        priority=0,
        attempt=1,
        max_attempts=3,
        retry_kind="transient",
        scheduled_at=datetime(2026, 9, 27, 12, 0, 0, tzinfo=UTC),
        started_at=datetime(2026, 9, 27, 12, 0, 1, tzinfo=UTC),
        finished_at=None,
        progress_state={"step": "stage", "percent": 50},
        progress_seq=2,
        payload=None,
        metadata=None,
        result=None,
        error_class=None,
        error_message=None,
        error_traceback=None,
        tags=[],
        trace_id=None,
        span_id=None,
        locked_by_worker="w12345678",
        lock_expires_at=datetime(2026, 9, 27, 12, 5, 0, tzinfo=UTC),
        cancel_requested_at=None,
        cancel_phase=0,
    )


_JOB_ID = "0198e2a7-9c1b-7c6a-b2f5-3f4a5b6c7d8e"
_BATCH_ID = "00000000-0000-0000-0000-000000000001"


def _make_client(pool: Any, *, base_path: str = "") -> TestClient:
    bundle = create_router(pool, base_path=base_path)  # pyright: ignore[reportArgumentType]  # Why: test duck-type pool.
    app = FastAPI()
    setup_admin_state(app, bundle)
    app.include_router(bundle.router)
    return TestClient(app)


# ── 1. the refresh-transport matrix, exhaustive ───────────────────────────
#
# The matrix the fixer pinned had holes: the 8 no-transport pages were
# pinned in "real-time mode" via _force_realtime, which setattr's a module
# attribute FastAPI's Depends-captured callable never reads - those cells
# rendered in POLLING mode and the pin held trivially. /jobs and the two
# detail pages (queue, batch) had no served-page pin in ANY mode. Every
# cell here is pinned to its exact transport, with the mode forced through
# the seam the dependency chain actually calls.


# page → (polling transport, realtime transport). Transports are named by
# the rendered evidence: "meta" = the meta refresh tag, "htmx" = an
# hx-trigger="every" poll, "admin.js" / "realtime.js" = that script tag.
_MATRIX: dict[str, tuple[dict[str, bool], dict[str, bool]]] = {
    "/queues": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": False, "htmx": True, "admin.js": False, "realtime.js": False},
    ),
    "/queues/default": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/jobs": (
        {"meta": False, "htmx": False, "admin.js": True, "realtime.js": False},
        {"meta": False, "htmx": False, "admin.js": True, "realtime.js": False},
    ),
    f"/jobs/{_JOB_ID}": (
        {"meta": False, "htmx": False, "admin.js": False, "realtime.js": True},
        {"meta": False, "htmx": False, "admin.js": False, "realtime.js": True},
    ),
    "/history": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/workers": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/leader": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/actors": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/batches": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    f"/batches/{_BATCH_ID}": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/schedules": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/rate-limits": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
    "/reservations": (
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
        {"meta": True, "htmx": False, "admin.js": False, "realtime.js": False},
    ),
}

_EVIDENCE = {
    "meta": 'http-equiv="refresh"',
    "htmx": 'hx-trigger="every',
    "admin.js": "static/admin.js",
    "realtime.js": "static/realtime.js",
}


def _cell_violations(html: str, expected: dict[str, bool]) -> list[str]:
    found = {name: marker in html for name, marker in _EVIDENCE.items()}
    violations = []
    for transport, should in expected.items():
        if found[transport] != should:
            violations.append(f"{transport} rendered={found[transport]} expected={should}")
    return violations


@pytest.mark.parametrize("page", sorted(_MATRIX), ids=str)
def test_refresh_matrix_polling_cell(page: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """POLLING mode: every page's exact transport, pinned."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))))
    html = client.get(page).text
    violations = _cell_violations(html, _MATRIX[page][0])
    assert not violations, f"{page} (polling): {'; '.join(violations)}"


@pytest.mark.parametrize("page", sorted(_MATRIX), ids=str)
def test_refresh_matrix_realtime_cell(page: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """REALTIME mode: every page's exact transport, pinned - through the
    real mode seam, so a regression that re-keys suppression on the badge
    (the frozen-page bug) trips the pin instead of sailing through."""
    import taskq.web.admin._factory as factory_module

    async def _realtime(redis_client: object | None = None) -> tuple[str, str]:
        return ("realtime", "real-time mode")

    monkeypatch.setattr(factory_module, "get_realtime_mode", _realtime)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))))
    html = client.get(page).text
    badge = re.search(r'data-mode="([^"]+)"', html)
    assert badge is not None and badge.group(1) == "realtime", (
        f"{page} must have rendered in REALTIME mode (got {badge and badge.group(1)!r}): "
        "a pin that silently renders polling mode proves nothing"
    )
    violations = _cell_violations(html, _MATRIX[page][1])
    assert not violations, f"{page} (realtime): {'; '.join(violations)}"


def test_degraded_mode_refreshes_like_polling(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The badge's THIRD state (polling-degraded) is a polling mode: the
    meta refresh runs and no page's JS poll takes it over."""
    import taskq.web.admin._factory as factory_module

    async def _degraded(redis_client: object | None = None) -> tuple[str, str]:
        return ("polling-degraded", "polling mode (Redis unavailable)")

    monkeypatch.setattr(factory_module, "get_realtime_mode", _degraded)
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))))
    for page in ("/queues", "/jobs", f"/jobs/{_JOB_ID}", "/history"):
        html = client.get(page).text
        badge = re.search(r'data-mode="([^"]+)"', html)
        assert badge is not None and badge.group(1) == "polling-degraded", page
        if page == "/queues":
            assert 'http-equiv="refresh"' in html, page
            assert 'hx-trigger="every' not in html, page
        elif page == "/history":
            assert 'http-equiv="refresh"' in html, page
        else:
            assert 'http-equiv="refresh"' not in html, (
                f"{page} owns its transport in every mode; a meta refresh "
                f"alongside it in degraded mode is the double-fetch"
            )


# ── 2. no served page ships an inline event handler ───────────────────────


def test_no_page_serves_an_inline_event_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EVERY rendered page, probed with hostile stored data (a bucket name
    carrying the JS-string breakout the rate-limits fix removed), must ship
    zero inline ``on*=`` handlers. The fixer removed the bucket reset's
    handler; the job detail page's cancel and retry confirms still
    interpolate a value into inline onclick JS strings - the same
    attribute-entity-decodes-before-the-JS-runs class, one missed instance
    away from the next keyed-bucket bug."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")

    class _HostileConn(_ClockConn):
        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "rate_limit_buckets" in query:
                return [
                    StubRecord(
                        bucket_name="emails'); alert(1); ('",
                        kind="token_bucket",
                        state={},
                        updated_at=datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
                    )
                ]
            return []

    client = _make_client(_ClockPool(_HostileConn(fetchrow=_job_row(_JOB_ID))))
    pages = [
        "/queues",
        "/queues/default",
        "/jobs",
        f"/jobs/{_JOB_ID}",
        "/history",
        "/workers",
        "/leader",
        "/actors",
        "/batches",
        f"/batches/{_BATCH_ID}",
        "/schedules",
        "/rate-limits",
        "/reservations",
    ]
    for page in pages:
        html = client.get(page).text
        handlers = re.findall(r"\son[a-z]+\s*=", html)
        assert not handlers, (
            f"{page} ships an inline event handler {handlers}: confirmations "
            "must ride the delegated-listener idiom (data attribute + submit "
            "listener), never an inline JS string"
        )


def test_job_detail_confirms_ride_data_not_inline_js(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The job detail page's cancel/retry confirms must carry the label as
    escaped DATA and confirm through the delegated listener - the idiom
    the rate-limits page was converted to."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))))
    html = client.get(f"/jobs/{_JOB_ID}").text
    assert 'data-confirm-label="Cancel job' in html, (
        "the cancel confirm's label must ride as a data attribute"
    )
    assert 'data-confirm-label="Retry job' not in html, "a running job renders no retry form"

    terminal_job = _job_row(_JOB_ID)
    terminal_job["status"] = "failed"
    client = _make_client(_ClockPool(_ClockConn(fetchrow=terminal_job)))
    html = client.get(f"/jobs/{_JOB_ID}").text
    assert 'data-confirm-label="Retry job' in html, (
        "the retry confirm's label must ride as a data attribute"
    )
    assert "addEventListener('submit'" in html or 'addEventListener("submit"' in html, (
        "the page must confirm through a delegated submit listener"
    )


# ── 3. the tojson edges ───────────────────────────────────────────────────


def _job_config(client: TestClient, **params: str) -> str:
    html = client.get("/jobs", params=params).text
    match = re.search(r"window\.__taskqJobConfig = (\{.*?\});", html, re.S)
    assert match is not None, "the jobs page must embed its JS config"
    return match.group(1)


def _node_eval(src: str) -> Any:
    node = shutil.which("node")
    if node is None:
        pytest.skip("the config's runtime value is evaluated under Node")
    result = subprocess.run(  # noqa: S603  # Why: fixed argv, no shell; the script is this test's own constant and the config arrives on stdin.
        [
            node,
            "-e",
            'const src = require("fs").readFileSync(0, "utf8");'
            " const window = {};"
            ' const cfg = eval("(" + src + ")");'
            " console.log(JSON.stringify(cfg));",
        ],
        input=src,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_config_survives_a_script_breakout_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A search carrying ``</script>`` must neither break out of the
    script block (tojson escapes it) nor lose a byte on the way to the
    machinery (evaluated under Node)."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn()))
    payload = "</script><script>alert(1)</script>"
    config = _job_config(client, search=payload)
    assert "</script>" not in config, (
        "a raw </script> inside the config block ends the script element: "
        "the rest of the payload renders as markup"
    )
    assert _node_eval(config)["search"] == payload


def test_config_round_trips_entity_shaped_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The double-encoding edge: a value that LOOKS like an entity (`&#39;`)
    and a value with a bare ampersand must reach the machinery verbatim -
    tojson inside autoescape must neither decode nor re-encode them."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn()))
    for raw in ("a&#39;b", "a&b", "a&#39;b&amp;c"):
        config = _job_config(client, search=raw)
        assert "&#39;" not in config.replace("\\u0026#39;", ""), (
            f"the entity-shaped value {raw!r} was re-encoded"
        )
        assert _node_eval(config)["search"] == raw, raw


def test_taskq_base_path_is_script_encoded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """job_detail.html's ``window.TASKQ_BASE_PATH = "{{ base_path }}"`` is
    the exact bug the tojson pass fixed on the jobs page: HTML escaping
    inside a <script> block mangles the value (an ``&`` in the prefix
    reaches realtime.js as ``&amp;`` and every fetch 404s). A config
    string in a script context is a JS literal, not an HTML attribute."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    base_path = "/taskq&env=prod"  # a prefix carrying an ampersand
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))), base_path=base_path)
    html = client.get(f"/jobs/{_JOB_ID}").text
    match = re.search(r"window\.TASKQ_BASE_PATH = (.*?);</script>", html, re.S)
    assert match is not None, "the page must embed its base path config"
    literal = match.group(1).strip()
    assert literal.startswith('"') and literal.endswith('"'), (
        f"TASKQ_BASE_PATH must be a JS string literal, got {literal!r}"
    )
    assert json.loads(literal) == base_path, (
        f"TASKQ_BASE_PATH rendered as {literal!r}; the JS literal must "
        f"evaluate to the raw prefix {base_path!r}"
    )
    assert "&amp;" not in literal, (
        "HTML entities never decode inside a script block: an escaped "
        "ampersand corrupts every URL realtime.js builds from it"
    )


# ── 4. the cap's uniformity ───────────────────────────────────────────────


def test_every_capped_surface_shares_one_cap() -> None:
    """The five capped read-only SQLs must share ONE limit value - a cap
    that drifted per page is a cap nobody can state in review."""
    from taskq.web.admin.ops import (
        _HELD_SLOTS_PAGE_SIZE,
        _RATE_LIMITS_PAGE_SIZE,
        _RESERVATIONS_PAGE_SIZE,
        _SCHEDULES_PAGE_SIZE,
    )
    from taskq.web.admin.workers import _WORKERS_PAGE_SIZE

    caps = {
        "_SCHEDULES_PAGE_SIZE": _SCHEDULES_PAGE_SIZE,
        "_RATE_LIMITS_PAGE_SIZE": _RATE_LIMITS_PAGE_SIZE,
        "_RESERVATIONS_PAGE_SIZE": _RESERVATIONS_PAGE_SIZE,
        "_HELD_SLOTS_PAGE_SIZE": _HELD_SLOTS_PAGE_SIZE,
        "_WORKERS_PAGE_SIZE": _WORKERS_PAGE_SIZE,
    }
    assert len(set(caps.values())) == 1, f"the read-only caps drifted: {caps}"


def test_every_capped_sql_limit_equals_its_constant() -> None:
    """The LIMIT in each SQL must BE its constant, not a literal that
    drifted away from the truncation arithmetic."""
    import re as _re

    from taskq.web.admin.ops import (
        _HELD_SLOTS_PAGE_SIZE,
        _HELD_SLOTS_SQL,
        _RATE_LIMITS_PAGE_SIZE,
        _RATE_LIMITS_SQL,
        _RESERVATIONS_PAGE_SIZE,
        _RESERVATIONS_SQL,
        _SCHEDULES_PAGE_SIZE,
        _SCHEDULES_SQL,
    )
    from taskq.web.admin.workers import _WORKERS_PAGE_SIZE, _WORKERS_SQL

    for name, sql, size in (
        ("_SCHEDULES_SQL", _SCHEDULES_SQL, _SCHEDULES_PAGE_SIZE),
        ("_RATE_LIMITS_SQL", _RATE_LIMITS_SQL, _RATE_LIMITS_PAGE_SIZE),
        ("_RESERVATIONS_SQL", _RESERVATIONS_SQL, _RESERVATIONS_PAGE_SIZE),
        ("_HELD_SLOTS_SQL", _HELD_SLOTS_SQL, _HELD_SLOTS_PAGE_SIZE),
        ("_WORKERS_SQL", _WORKERS_SQL, _WORKERS_PAGE_SIZE),
    ):
        match = _re.search(r"LIMIT (\d+)$", sql.strip())
        assert match is not None, f"{name} has no LIMIT"
        assert int(match.group(1)) == size, (
            f"{name}'s LIMIT ({match.group(1)}) drifted from its "
            f"truncation constant ({size}): the notice lies"
        )


def test_every_capped_page_announces_when_the_cap_bites(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every capped page says so when the cap bites (the pass pinned the
    schedules and held-slots notices only): workers, rate-limit buckets
    and reservation buckets get the same parametrized pin, plus the
    under-cap must not cry wolf."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")

    class _WorkersConn(_ClockConn):
        def __init__(self, n: int) -> None:
            self._n = n

        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "FROM" in query and "workers" in query:
                return [
                    StubRecord(
                        hostname=f"w-{i}",
                        pid=i,
                        queues=["default"],
                        last_seen_at=datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
                        is_leader=i == 0,
                        metadata=None,
                    )
                    for i in range(self._n)
                ]
            return []

    from taskq.web.admin.workers import _WORKERS_PAGE_SIZE

    client = _make_client(_ClockPool(_WorkersConn(_WORKERS_PAGE_SIZE)))
    html = client.get("/workers").text
    assert "Showing the" in html and str(_WORKERS_PAGE_SIZE) in html, (
        "a capped workers render must say the cap bit"
    )
    client = _make_client(_ClockPool(_WorkersConn(1)))
    assert "Showing the" not in client.get("/workers").text, (
        "an under-cap workers render must not cry wolf"
    )

    class _BucketsConn(_ClockConn):
        def __init__(self, n: int) -> None:
            self._n = n

        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "rate_limit_buckets" in query:
                return [
                    StubRecord(
                        bucket_name=f"b{i}",
                        kind="token_bucket",
                        state={},
                        updated_at=datetime(2026, 9, 27, 12, 0, tzinfo=UTC),
                    )
                    for i in range(self._n)
                ]
            return []

    from taskq.web.admin.ops import _RATE_LIMITS_PAGE_SIZE

    monkeypatch.setenv("TASKQ_ADMIN_UI_ALLOW_RATE_LIMIT_RESET", "true")
    client = _make_client(_ClockPool(_BucketsConn(_RATE_LIMITS_PAGE_SIZE)))
    html = client.get("/rate-limits").text
    assert "Showing the" in html and str(_RATE_LIMITS_PAGE_SIZE) in html, (
        "a capped rate-limits render must say the cap bit"
    )
    client = _make_client(_ClockPool(_BucketsConn(1)))
    assert "Showing the" not in client.get("/rate-limits").text, (
        "an under-cap rate-limits render must not cry wolf"
    )

    class _ReservationsConn(_ClockConn):
        def __init__(self, n: int) -> None:
            self._n = n

        async def fetch(self, query: str, *args: object) -> list[StubRecord]:
            if "GROUP BY bucket_name" in query:
                return [
                    StubRecord(
                        bucket_name=f"b{i}",
                        held_count=0,
                        free_count=0,
                        total_slots=0,
                    )
                    for i in range(self._n)
                ]
            return []

    from taskq.web.admin.ops import _RESERVATIONS_PAGE_SIZE

    client = _make_client(_ClockPool(_ReservationsConn(_RESERVATIONS_PAGE_SIZE)))
    html = client.get("/reservations").text
    assert "Showing the" in html and str(_RESERVATIONS_PAGE_SIZE) in html, (
        "a capped reservations render must say the cap bit"
    )
    client = _make_client(_ClockPool(_ReservationsConn(1)))
    assert "Showing the" not in client.get("/reservations").text, (
        "an under-cap reservations render must not cry wolf"
    )


# ── 5. the offline pin ────────────────────────────────────────────────────


def test_every_page_loads_only_local_scripts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Every <script src> on every page must resolve to the served
    /static dir. The pass moved Alpine off its CDN citing the air-gapped
    ops deployment and left lucide on unpkg - that same deployment renders
    every data-lucide icon as an EMPTY <i>, the defect the icon boot was
    supposed to fix."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))))
    pages = [
        "/queues",
        "/queues/default",
        "/jobs",
        f"/jobs/{_JOB_ID}",
        "/history",
        "/workers",
        "/leader",
        "/actors",
        "/batches",
        f"/batches/{_BATCH_ID}",
        "/schedules",
        "/rate-limits",
        "/reservations",
    ]
    for page in pages:
        for src in re.findall(r'<script[^>]*\bsrc="([^"]+)"', client.get(page).text):
            assert not src.startswith(("http://", "https://")), (
                f"{page} loads {src} from the public internet - the "
                "air-gapped deployment loses the feature it carries"
            )
            assert src.startswith("/static/"), (
                f"{page} loads {src}: scripts must come from the served static dir"
            )


def test_prefix_walk_covers_every_url_bearing_attribute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pass's prefix walk read only src=/href=. The forms (``action=``),
    the htmx navigation (``hx-get=``) and the SSE console
    (``sse-connect=``) carry URLs too - a prefix-blind URL there breaks
    only when deployed, which is where the pin has to hold."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    external_prefix = "/taskq"
    base_path = f"{external_prefix}/admin"
    bundle = create_router(_ClockPool(_ClockConn()), base_path=base_path)  # pyright: ignore[reportArgumentType]
    app = FastAPI(root_path=external_prefix)
    setup_admin_state(app, bundle)
    app.include_router(bundle.router, prefix="/admin")
    client = TestClient(app, root_path=external_prefix)

    pages = [
        "/taskq/admin/queues",
        "/taskq/admin/jobs",
        "/taskq/admin/history",
        "/taskq/admin/actors",
        "/taskq/admin/rate-limits",
        "/taskq/admin/schedules",
    ]
    url_attrs = re.compile(r'(?:\bsrc|\bhref|\baction|\bhx-get|\bhx-post|\bsse-connect)="([^"]+)"')
    checked = 0
    for page in pages:
        response = client.get(page)
        assert response.status_code == 200, page
        for url in url_attrs.findall(response.text):
            if url.startswith(("http://", "https://")):
                continue
            # every local URL must be ABSOLUTE from the root (a relative
            # "static/x" resolves against the page URL, which differs per
            # route - the ./static edge that passes a unit test and breaks
            # a deployment)
            assert url.startswith("/"), (
                f"{page} emits the relative URL {url!r}: relative URLs "
                "resolve against the page's own path, not the prefix"
            )
            assert url.startswith(base_path) or url.startswith(external_prefix), (
                f"{page} emits {url!r} without the external prefix"
            )
            checked += 1
    assert checked > 0, "the walk must have collected URL-bearing attributes"


# ── 6. the re-attack's own hardening: the cells the re-attack left unpinned ─
#
# Three pins over the re-attack's own fixes, found while attacking them:
# the base-path literal was pinned for the ampersand prefix only (the
# ``</script>`` break-out and the EMPTY root-deployment prefix were not),
# and the vendored lucide file had no integrity pin at all - a silently
# corrupted vendor is the air-gap failure the move was made to prevent,
# discovered only when the icons never render.

_LUCIDE_UNPKG_SHA256 = (
    # The hash of the bytes https://unpkg.com/lucide@0.544.0/dist/umd/lucide.min.js
    # served at the time of vendoring (verified by fetch + compare). unpkg
    # serves immutable versioned URLs, so a mismatch here means the vendored
    # copy was corrupted or silently re-vendored from something else - the
    # icons break offline either way, and the pin forces a conscious update.
    "72646e574ecc776f056949d914e5f461881e639b236910680e38b097d3561634"
)


def test_taskq_base_path_survives_a_script_breakout_prefix(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The base-path pin covered the ``&`` prefix; the ``</script>`` prefix
    is the same literal in the same script context and must be pinned too:
    tojson must escape it into the JS string, and the JS value must
    round-trip to the raw prefix."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    payload = "</script><script>alert(1)</script>"
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))), base_path=payload)
    html = client.get(f"/jobs/{_JOB_ID}").text
    match = re.search(r"window\.TASKQ_BASE_PATH = (.*?);</script>", html, re.S)
    assert match is not None, "the page must embed its base path config"
    literal = match.group(1).strip()
    assert "</script>" not in literal, (
        "a raw </script> inside the TASKQ_BASE_PATH script block ends the "
        "script element: the rest of the prefix renders as markup"
    )
    assert json.loads(literal) == payload, (
        f"TASKQ_BASE_PATH rendered as {literal!r}; the JS literal must "
        f"evaluate to the raw prefix {payload!r}"
    )


def test_taskq_base_path_empty_prefix_is_a_valid_root_deployment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A root deployment (no prefix) renders ``window.TASKQ_BASE_PATH = "";``
    - the literal must still be a JSON string (not ``null``/``undefined``
    or a bare word), and every URL the page builds from the concatenation
    must be root-absolute."""
    monkeypatch.setenv("TASKQ_ENVIRONMENT", "dev")
    client = _make_client(_ClockPool(_ClockConn(fetchrow=_job_row(_JOB_ID))), base_path="")
    html = client.get(f"/jobs/{_JOB_ID}").text
    match = re.search(r"window\.TASKQ_BASE_PATH = (.*?);</script>", html, re.S)
    assert match is not None, "the page must embed its base path config"
    literal = match.group(1).strip()
    assert literal == '""', (
        f"an empty base_path must render as the empty JS string literal, got {literal!r}"
    )
    for src in re.findall(r'<script[^>]*\bsrc="([^"]+)"', html):
        assert src.startswith("/static/"), (
            f"the root deployment's script src {src!r} must be root-absolute"
        )


def test_vendored_lucide_is_the_pinned_unpkg_build() -> None:
    """The vendored lucide.min.js must be byte-identical to the unpkg
    build the chrome's comment names, and must carry lucide's ISC license
    header - the repo's discipline for every vendored third-party asset.
    A silently corrupted vendor renders every icon as an empty ``<i>``
    offline; the hash pin makes that a red test instead of a discovery."""
    vendored = (
        Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "static" / "lucide.min.js"
    )
    assert vendored.exists(), "the vendored lucide asset must ship in the static dir"
    body = vendored.read_bytes()
    assert hashlib.sha256(body).hexdigest() == _LUCIDE_UNPKG_SHA256, (
        "lucide.min.js's bytes drifted from the pinned unpkg v0.544.0 UMD "
        "build - re-vendor consciously and update the pin, or restore the "
        "exact file (a corrupted vendor breaks the icons offline)"
    )
    header = body[:512].decode("utf-8", errors="replace")
    assert "@license lucide v0.544.0 - ISC" in header, (
        "the vendored lucide must carry its ISC license header with the "
        "pinned version (the repo's vendoring discipline)"
    )
    # The chrome's comment names the same version: the doc must not drift
    # from the asset it describes.
    chrome = (
        Path(__file__).resolve().parents[2] / "src" / "taskq" / "web" / "templates" / "_base.html"
    ).read_text()
    assert "lucide v0.544.0" in chrome, (
        "_base.html's vendoring comment must name the version the hash pin "
        "holds - a comment that drifted from the asset lies to the next reviewer"
    )
