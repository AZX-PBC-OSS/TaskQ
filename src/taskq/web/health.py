"""FastAPI router for /jobs/health/{live,ready}.

GET /jobs/health/metrics is served by taskq.contrib.prometheus.create_metrics_router
(requires taskq[prometheus]) and must be mounted alongside this router.

Importing this module requires the `taskq[fastapi]` optional extra.
"""

import time
from typing import TYPE_CHECKING

import structlog
from fastapi import APIRouter, Response

from taskq import _json
from taskq.worker.health import (
    _check_live,  # pyright: ignore[reportPrivateUsage]  # Why: _check_live is a shared utility consumed by both transports (Unix socket + FastAPI); the underscore signals "internal to the health subsystem" not "private to health.py".
    build_ready_body,
    compute_health,
)

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

logger = structlog.get_logger("taskq.web.health")


def create_health_router(deps: "WorkerDeps") -> APIRouter:
    """Create a FastAPI router at /jobs/health/{live,ready}.

    Captures *deps* via closure — no FastAPI dependency injection.
    Mount alongside create_metrics_router (taskq.contrib.prometheus) for the
    full /jobs/health surface including Prometheus metrics.
    """

    router = APIRouter(prefix="/jobs/health")

    @router.get("/live")
    async def live() -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        t0 = time.perf_counter()
        ok, _msg = await _check_live()
        status_code = 200 if ok else 503
        body_dict: dict[str, str] = {"status": "ok"} if ok else {"status": "unresponsive"}
        body_bytes = _json.dumps(body_dict)

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        logger.debug(
            "health-request",
            endpoint="/jobs/health/live",
            status_code=status_code,
            response_time_ms=elapsed_ms,
        )

        return Response(
            content=body_bytes,
            media_type="application/json",
            status_code=status_code,
        )

    @router.get("/ready")
    async def ready() -> Response:  # pyright: ignore[reportUnusedFunction]  # Why: registered via FastAPI decorator; pyright cannot see the route registration.
        t0 = time.perf_counter()
        report = await compute_health(deps)
        body_bytes = build_ready_body(report, deps)
        status_code = 200 if report.ready else 503

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        logger.debug(
            "health-request",
            endpoint="/jobs/health/ready",
            status_code=status_code,
            response_time_ms=elapsed_ms,
        )

        return Response(
            content=body_bytes,
            media_type="application/json",
            status_code=status_code,
        )

    return router
