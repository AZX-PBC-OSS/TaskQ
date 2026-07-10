"""FastAPI router for GET /jobs/health/metrics (OTel Prometheus bridge).

Requires taskq[prometheus] and taskq[fastapi] optional extras.
The operator must configure a MeterProvider with a PrometheusMetricReader
before process start — this module does NOT configure the provider.
"""

try:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader as _  # noqa: F401
except ImportError as _exc:
    raise ImportError(
        "taskq[prometheus] is required to use the Prometheus metrics bridge. "
        "Install it with: pip install 'taskq[prometheus]'"
    ) from _exc

try:
    from fastapi import APIRouter, Response
except ImportError as _exc:
    raise ImportError(
        "taskq[fastapi] is required to use the Prometheus metrics bridge. "
        "Install it with: pip install 'taskq[fastapi]'"
    ) from _exc

from typing import TYPE_CHECKING

from prometheus_client import REGISTRY, CollectorRegistry, generate_latest

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"


def create_metrics_router(
    _deps: "WorkerDeps",
    *,
    registry: CollectorRegistry = REGISTRY,
) -> APIRouter:
    """Return a FastAPI router exposing GET /metrics in Prometheus text format.

    Mount alongside the health router (which owns /live, /ready):

        app.include_router(create_metrics_router(deps), prefix="/jobs/health")

    *registry* defaults to the prometheus-client global REGISTRY, which is
    where PrometheusMetricReader registers its collector.  Pass a custom
    CollectorRegistry in tests or when using an isolated registry.

    *_deps* is accepted for signature parity with create_health_router; it is
    not used by the metrics route because the OTel bridge reads directly from
    the MeterProvider the operator configured before process start.
    """

    def _generate() -> bytes:
        return generate_latest(registry)

    router = APIRouter()

    @router.get("/metrics")
    async def metrics() -> Response:  # pyright: ignore[reportUnusedFunction]
        return Response(content=_generate(), media_type=_CONTENT_TYPE, status_code=200)

    return router
