"""The ``/insights`` page: the operational-insights SQL layer, rendered.

Every number on the page comes from :mod:`taskq.insights`' own
parameterized statements — this module writes NO SQL of its own.  A
hand-copied shape would drift from the module's UNION/window/index
discipline the first time one side changed; the route's whole job is to
bind the page's declared parameters (the window from
:data:`~taskq.insights.INSIGHTS_WINDOWS`' closed set, the per-actor
wait-grouping toggle), hand the module the caller-owned connection, and
render what comes back — including the confounds the module documents:

* the wait distribution's ``clean`` and ``deferred`` segments render
  SEPARATED, with the deferred segment's honesty label (its wait
  measures only the final leg);
* a utilization past the high-water mark (> 1) or a queue with due work
  nothing can serve renders a verdict BADGE — a visual judgment, not
  just numbers — and a healthy fleet renders none;
* the overprovisioning verdict names the window it was computed over
  (a single-window TRUE is a hypothesis);
* a drain estimate for a queue whose window carried no traffic says so
  instead of rendering an eta (eta=0 would read as "already drained");
* the cron ledger renders the runaway trend as a badge, which by the
  module's two-window rule can only render when fires outran clearance
  in BOTH the current and the prior window;
* the page renders only after ALL six reads return — a read that fails
  (a legacy schema whose archive drifted) fails the whole request
  loudly, never a half-rendered page that reads as a healthy fleet;
* the two fleet-sized tables (per-actor wait groups, actor backlog) are
  capped at the actors page's ``STATS_LIMIT`` with a visible note; the
  per-queue verdict sections stay uncapped, because a cap there could
  hide the very badge the page exists to show;
* the page resolves the deployment's realtime mode like every sibling
  page — the header badge and the meta-refresh decision tell the same
  story as the rest of the admin UI.
"""

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment

from taskq.insights import (
    INSIGHTS_WINDOWS,
    fetch_actor_backlog,
    fetch_cron_ledger,
    fetch_drain_estimates,
    fetch_overprovisioning,
    fetch_queue_imbalance,
    fetch_wait_distribution,
)
from taskq.settings import TaskQSettings
from taskq.web._pool import BoundedPool
from taskq.web.admin._actor_stats import STATS_LIMIT
from taskq.web.admin._constants import reject_unknown_query_params
from taskq.web.admin._factory import (
    get_admin_pool,
    get_realtime_ctx,
    get_schema,
    get_settings,
    get_templates,
)

#: The window the page opens on when the operator has not picked one —
#: the recency view, not the whole retained history (the module
#: deliberately has no all-time entry: on hypertables all-time IS the
#: archive retention).
_DEFAULT_WINDOW: str = "24h"


def resolve_insights_window(raw: str) -> timedelta:
    """Resolve the window selector to a duration; 400s on unknown values.

    Anything the page accepts must name a window in
    :data:`~taskq.insights.INSIGHTS_WINDOWS`: a typo that silently fell
    back to a default would show an operator one recency view while the
    URL claimed another — the wrong-but-plausible answer, not a clean
    input error (the same closed-set treatment
    :func:`taskq.web.admin._actor_stats.resolve_stats_window` gives the
    actors page's selector).
    """
    try:
        return INSIGHTS_WINDOWS[raw]
    except KeyError:
        raise HTTPException(
            status_code=400,
            detail=f"unknown insights window: {raw!r}; allowed: {', '.join(INSIGHTS_WINDOWS)}",
        ) from None


def _parse_per_actor(raw: str) -> bool:
    """Parse the per-actor wait-grouping toggle; 400s on anything its
    closed value space does not name. A mistyped toggle silently rendering
    the OTHER grouping is the undeclared-filter bug in toggle form."""
    if raw in ("true", "1"):
        return True
    if raw in ("false", "0"):
        return False
    raise HTTPException(
        status_code=400,
        detail=f"invalid per_actor value: {raw!r}; allowed: true, false",
    )


def _humanize_seconds(seconds: float | None) -> str | None:
    """A human duration for a seconds figure (``800`` → ``13m``); ``None``
    passes through so the template can render its dash."""
    if seconds is None:
        return None
    s = round(seconds)
    if s < 1:
        return "<1s"
    if s < 60:
        return f"{s}s"
    days, rem = divmod(s, 86400)
    hours, rem = divmod(rem, 3600)
    minutes = rem // 60
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h {minutes}m" if minutes else f"{hours}h"
    return f"{minutes}m"


def register(router: APIRouter) -> None:
    """Attach the insights page to *router*."""

    @router.get("/insights", response_class=HTMLResponse)
    async def insights_page(  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        request: Request,
        pool: BoundedPool = Depends(get_admin_pool),
        schema: str = Depends(get_schema),
        tmpl: Environment = Depends(get_templates),
        settings: TaskQSettings = Depends(get_settings),
        realtime_ctx: tuple[str, str] = Depends(get_realtime_ctx),
        window: str = Query(default=_DEFAULT_WINDOW),
        per_actor: str = Query(default="false"),
    ) -> HTMLResponse:
        # The page declares exactly two params — the window and the
        # per-actor wait toggle. Anything else is refused, never dropped
        # (the same undeclared-param contract every admin page serves).
        reject_unknown_query_params(request, ("window", "per_actor"))
        window_delta = resolve_insights_window(window)
        per_actor_on = _parse_per_actor(per_actor)

        # Every read is the insights module's own parameterized SQL; the
        # route binds no SQL text of its own. One checkout, six reads —
        # the page is one consistent-ish snapshot of the ledger.
        async with pool.acquire() as conn:
            wait_rows = await fetch_wait_distribution(
                conn, schema=schema, window=window_delta, per_actor=per_actor_on
            )
            imbalance_rows = await fetch_queue_imbalance(
                conn,
                schema=schema,
                worker_liveness_seconds=settings.admin_worker_liveness_seconds,
            )
            backlog_rows = await fetch_actor_backlog(conn, schema=schema)
            overprovisioning_rows = await fetch_overprovisioning(
                conn,
                schema=schema,
                window=window_delta,
                worker_liveness_seconds=settings.admin_worker_liveness_seconds,
            )
            drain_rows = await fetch_drain_estimates(conn, schema=schema, window=window_delta)
            ledger_rows = await fetch_cron_ledger(conn, schema=schema, window=window_delta)

        waits = [
            {
                "queue": r["queue"],
                "actor": r.get("actor"),
                "segment": r["segment"],
                "count": int(r["count"]),
                "p50": _humanize_seconds(r["p50_wait_s"]),
                "p95": _humanize_seconds(r["p95_wait_s"]),
                "max": _humanize_seconds(r["max_wait_s"]),
            }
            for r in wait_rows
        ]

        # The utilization verdict is the visual judgment the page exists
        # to render: past the high-water mark (> 1 — more due work than
        # one full wave of capacity) or with due work nothing can serve
        # (NULL utilization) is a pathological shape, and a healthy queue
        # earns no badge at all.
        imbalance = []
        for r in imbalance_rows:
            depth = int(r["depth"])
            utilization = r["utilization"]
            verdict: tuple[str, str] | None = None
            if utilization is not None and utilization > 1.0:
                verdict = ("over-capacity", f"Over capacity ({utilization:.0%})")
            elif utilization is None and depth > 0:
                verdict = ("unservable", "Unservable — no live capacity can serve this queue")
            imbalance.append(
                {
                    "queue": r["queue"],
                    "depth": depth,
                    "scheduled_depth": int(r["scheduled_depth"]),
                    "live_workers": int(r["live_workers"]),
                    "effective_capacity": int(r["effective_capacity"] or 0),
                    "utilization": f"{utilization:.0%}" if utilization is not None else None,
                    "oldest_due": _humanize_seconds(r["oldest_due_age_s"]),
                    "verdict": verdict,
                }
            )

        backlog = [
            {
                "actor": r["actor"],
                "queue": r["queue"],
                "backlog": int(r["backlog"]),
                "running": int(r["running"]),
                "max_concurrent": r["max_concurrent"],
                "saturation": (f"{r['saturation']:.0%}" if r["saturation"] is not None else None),
                "unservable": int(r["unservable_backlog"]),
            }
            for r in backlog_rows
        ]

        # The idle-fleet verdict names its window on the badge: a
        # single-window TRUE is a hypothesis, and the page says which
        # window the hypothesis is about.
        overprovisioning = [
            {
                "queue": r["queue"],
                "live_workers": int(r["live_workers"]),
                "depth": int(r["depth"]),
                "terminalisations": int(r["terminalisations"]),
                "flagged": bool(r["overprovisioned"]),
                "badge_label": f"Idle fleet · {window}",
            }
            for r in overprovisioning_rows
        ]

        # The has_traffic honesty: a window with no traffic renders the
        # caveat, NEVER an eta — eta=0 would read as "already drained".
        drains = [
            {
                "queue": r["queue"],
                "depth": int(r["depth"]),
                "terminalisations": int(r["terminalisations"]),
                "has_traffic": bool(r["has_traffic"]),
                "eta": _humanize_seconds(r["eta_seconds"]) if r["has_traffic"] else None,
                "scheduled_depth": int(r["scheduled_depth"]),
            }
            for r in drain_rows
        ]

        ledger = [
            {
                "actor": r["actor"],
                "cron_expr": r["cron_expr"],
                "enabled": bool(r["enabled"]),
                "fires_window": int(r["fires_window"]),
                "cleared_window": int(r["cleared_window"]),
                "fires_prior": int(r["fires_prior"]),
                "cleared_prior": int(r["cleared_prior"]),
                "outstanding": int(r["outstanding"]),
                "runaway": bool(r["runaway_trending"]),
            }
            for r in ledger_rows
        ]

        # The render cap, the actors page's STATS_LIMIT discipline: the
        # two fleet-sized tables (per-actor wait groups, actor backlog)
        # grow with the ACTORS, not with the pathology — uncapped, a
        # 500-actor fleet renders a thousand rows into the operator's
        # browser.  The cap is announced, never silent; the per-queue
        # verdict sections (imbalance, drain, overprovisioning) stay
        # uncapped on purpose — a cap there could hide the very badge
        # the page exists to show.
        waits_truncated = len(waits) > STATS_LIMIT
        if waits_truncated:
            waits = waits[:STATS_LIMIT]
        backlog_truncated = len(backlog) > STATS_LIMIT
        if backlog_truncated:
            backlog = backlog[:STATS_LIMIT]

        realtime_mode, mode_label = realtime_ctx
        html = tmpl.get_template("insights.html").render(
            window=window,
            windows=list(INSIGHTS_WINDOWS),
            per_actor=per_actor_on,
            waits=waits,
            waits_truncated=waits_truncated,
            backlog=backlog,
            backlog_truncated=backlog_truncated,
            render_limit=STATS_LIMIT,
            imbalance=imbalance,
            overprovisioning=overprovisioning,
            drains=drains,
            ledger=ledger,
            realtime_mode=realtime_mode,
            mode_label=mode_label,
        )
        return HTMLResponse(content=html)
