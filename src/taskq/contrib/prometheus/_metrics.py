"""FastAPI router for GET /jobs/health/metrics (OTel Prometheus bridge).

Requires taskq[prometheus] and taskq[fastapi] optional extras.

Provider wiring — who feeds the scrape
--------------------------------------

Every ``taskq_*`` / ``messaging_*`` series this endpoint can serve is an
OpenTelemetry instrument fed through the process-global ``MeterProvider``.
Measurements an OTel proxy instrument records before any SDK provider
exists are DROPPED (the proxy rebinds on ``set_meter_provider`` but does
not replay), so the provider must be wired at process start, never at
first scrape.

:func:`create_metrics_router` therefore wires one itself at router
creation — which is process start on the shipped serve path (``taskq ui
serve`` builds its routers inside the FastAPI lifespan, before uvicorn
accepts a connection). The wiring composes with, and never replaces, an
operator-configured provider:

- **Nothing configured** (the documented quick-start: no ``OTEL_*`` env
  vars, no operator SDK setup): a ``PrometheusMetricReader`` bound to the
  registry this router scrapes, held by a fresh ``MeterProvider``, is
  installed as the global provider and ``prometheus-metrics-provider-wired``
  is logged at INFO. From then on every obs instrument this process
  records reaches the scrape.
- **A bridge already serves this registry** (an operator wired a
  ``PrometheusMetricReader`` themselves, or this module already wired one
  for it): the existing wiring is left untouched.
- **An operator-configured SDK provider has no Prometheus bridge into
  this registry** (e.g. an OTLP-only pipeline): the provider is left
  untouched and ``prometheus-metrics-reader-missing`` is logged at
  WARNING at startup. Without it the mounted endpoint answers 200 with
  zero ``taskq_*`` series while the shipped ``rules.yaml`` alert set
  references exactly those series — a failure that looks like a success;
  the warning names the missing piece.
- **OTel emission is disabled** (``TASKQ_OTEL_ENABLED=false``, flipped by
  worker startup; only reachable when the router is mounted into a
  process that flipped it): no provider is installed, and the mount logs
  ``prometheus-metrics-otel-disabled`` at INFO so a scrape empty of
  ``taskq_*`` series is attributable to the switch rather than a defect.

Operators who want full control configure their own provider before
process start (see docs/guides/observability.md); this module then
changes nothing about their setup.
"""

try:
    from opentelemetry.exporter.prometheus import PrometheusMetricReader
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

from typing import TYPE_CHECKING, Literal

import structlog
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider as _SdkMeterProvider
from prometheus_client import REGISTRY, CollectorRegistry, generate_latest

from taskq.obs import otel_enabled

if TYPE_CHECKING:
    from taskq.worker.deps import WorkerDeps

_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

logger = structlog.get_logger("taskq.contrib.prometheus._metrics")

type PrometheusWiring = Literal[
    "already_bridged",
    "wired",
    "otel_disabled",
    "provider_without_bridge",
    "provider_set_blocked",
]
"""What :func:`ensure_prometheus_meter_provider` did about the scrape's feed.

Returned rather than only logged so an embedding application (and the
tests) can assert the outcome without scraping.
"""


def _registry_has_otel_bridge(registry: CollectorRegistry) -> bool:
    """True when an OTel→Prometheus bridge collector is already registered
    on *registry*.

    prometheus_client exposes no public collector listing, so this reads
    the registry's two private collector slots defensively. A registry
    shape this cannot see (a renamed internals slot) is treated as "no
    bridge": a second reader then registers without error and the scrape
    carries each OTel family twice — malformed exposition a Prometheus
    server rejects loudly at ingestion. The failure this helper exists to
    remove is the opposite shape: a 200 scrape that is silently empty of
    taskq_* series.
    """
    collectors = [
        *getattr(registry, "_collector_to_names", {}),  # pyright: ignore[reportPrivateUsage]  # Why: no public enumeration API; defensive getattr keeps a renamed slot from raising here (see docstring for the loud failure it defers to).
        *getattr(registry, "_collectors_without_names", ()),  # pyright: ignore[reportPrivateUsage]  # Why: as above.
    ]
    return any(
        type(c).__module__ == "opentelemetry.exporter.prometheus"
        and type(c).__name__ == "_CustomCollector"
        for c in collectors
    )


def ensure_prometheus_meter_provider(
    registry: CollectorRegistry = REGISTRY,
) -> PrometheusWiring:
    """Wire a ``PrometheusMetricReader``-backed ``MeterProvider`` if nothing
    feeds *registry* yet, and report what was done.

    Called by :func:`create_metrics_router` at router creation; embedding
    applications that serve their own scrape endpoint (no router) can call
    it directly at process start — BEFORE the worker records anything,
    because pre-provider proxy measurements are dropped.

    Never replaces an operator-configured provider: a registry with an
    OTel bridge already registered is left alone, an SDK provider without
    a bridge into *registry* is left alone (with a WARNING), and a global
    provider set through the OTel env-var entry point (or any earlier
    ``set_meter_provider``) wins the OTel set-once guard, which is
    detected and reported rather than silently shadowed.
    """
    if _registry_has_otel_bridge(registry):
        logger.debug("prometheus-metrics-bridge-present")
        return "already_bridged"

    if not otel_enabled():
        logger.info(
            "prometheus-metrics-otel-disabled",
            detail=(
                "the metrics route is mounted while OTel emission is disabled "
                "(TASKQ_OTEL_ENABLED=false): no provider was installed, so the "
                "scrape serves only collectors registered directly on the "
                "registry — every taskq_* series is suppressed by configuration"
            ),
        )
        return "otel_disabled"

    provider = metrics.get_meter_provider()
    if isinstance(provider, _SdkMeterProvider):
        # The operator configured the SDK (e.g. an OTLP pipeline) but no
        # PrometheusMetricReader feeds this registry, so the scrape cannot
        # serve the taskq_* series. Readers cannot be attached to a
        # constructed provider through any public API, so the honest move
        # is to say so at startup, not to shadow their provider.
        logger.warning(
            "prometheus-metrics-reader-missing",
            detail=(
                "a MeterProvider is already configured but no "
                "PrometheusMetricReader bridges it into the registry this "
                "endpoint scrapes: the route will answer 200 with zero "
                "taskq_* series, and the shipped rules.yaml alert set — "
                "which references those series — can never fire. Add "
                "PrometheusMetricReader(registry=...) to your MeterProvider "
                "at process start, or remove your own provider and let this "
                "router wire one."
            ),
        )
        return "provider_without_bridge"

    reader = PrometheusMetricReader(registry=registry)
    sdk_provider = _SdkMeterProvider(metric_readers=[reader])
    metrics.set_meter_provider(sdk_provider)
    if metrics.get_meter_provider() is not sdk_provider:
        # The OTel set-once guard was already consumed by a non-SDK
        # provider (a custom API-level provider, or one loaded through the
        # OTEL_PYTHON_METER_PROVIDER entry point). Our provider is inert:
        # taskq's instruments follow the global provider, so the scrape
        # would stay empty. Unregister the orphaned collector and say so.
        sdk_provider.shutdown()
        logger.warning(
            "prometheus-metrics-provider-blocked",
            detail=(
                "the global meter provider was already set to a non-SDK "
                "provider before this router was created, so the Prometheus "
                "bridge just constructed is inert and the scrape cannot "
                "serve taskq_* series. Set the provider once, at process "
                "start, with a PrometheusMetricReader attached."
            ),
        )
        return "provider_set_blocked"

    logger.info(
        "prometheus-metrics-provider-wired",
        detail=(
            "no meter provider was configured, so a PrometheusMetricReader-"
            "backed MeterProvider was installed as the process-global "
            "provider: every taskq_* / messaging_* instrument this process "
            "records from now on reaches this endpoint's scrape"
        ),
    )
    return "wired"


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

    Creating the router also ensures the scrape can actually serve the
    ``taskq_*`` series — see :func:`ensure_prometheus_meter_provider` for
    the wiring contract (auto-wire when nothing is configured, never
    replace an operator's provider, WARN at startup when a configured
    provider cannot feed this registry).

    *_deps* is accepted for signature parity with create_health_router; it
    is not used by the metrics route because the OTel bridge reads directly
    from the process-global MeterProvider.
    """
    ensure_prometheus_meter_provider(registry)

    def _generate() -> bytes:
        return generate_latest(registry)

    router = APIRouter()

    @router.get("/metrics")
    async def metrics() -> Response:  # pyright: ignore[reportUnusedFunction]
        return Response(content=_generate(), media_type=_CONTENT_TYPE, status_code=200)

    return router
