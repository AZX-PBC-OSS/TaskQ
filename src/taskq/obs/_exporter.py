"""SDK exporter auto-configuration for the ``taskq worker`` process.

TaskQ instruments itself through the OpenTelemetry **API** only: every
span and metric goes to the process-global providers, which stay the
API's no-op proxies until something installs SDK providers. The
standard environment variables (``OTEL_EXPORTER_OTLP_ENDPOINT``,
``OTEL_TRACES_EXPORTER``, ...) configure nothing by themselves — they
are read by the SDK's configurator, which ``opentelemetry-instrument``
runs at interpreter start and which nothing ran inside a stock worker.
A container with those variables set therefore exported nothing, health
stayed green, and every shipped alert rule was inert.

:func:`configure_exporters` closes that gap the way a distro does: when
the ``[otel]`` extra is installed and the operator has asked for an
exporter, it runs the SDK's own configurator
(``opentelemetry.sdk._configuration``, the machinery behind
``opentelemetry-instrument``) so the same variables mean the same thing
under ``taskq worker`` as under ``opentelemetry-instrument taskq
worker``. It never replaces a provider that is already set — an
embedding application, a vendor distro, or ``opentelemetry-instrument``
itself owns the process then — and it reports what it wired on one
startup line so an operator can verify the pipeline from the log alone.
"""

import importlib
import os
from dataclasses import dataclass
from typing import Literal, Protocol

import structlog
from opentelemetry import metrics, trace
from opentelemetry.environment_variables import (
    OTEL_LOGS_EXPORTER,
    OTEL_METRICS_EXPORTER,
    OTEL_TRACES_EXPORTER,
)
from opentelemetry.trace import ProxyTracerProvider

__all__ = [
    "ExporterWiring",
    "OtelExporterConfigurationError",
    "configure_exporters",
]

_log: structlog.stdlib.BoundLogger = structlog.get_logger("taskq.obs._exporter")

#: Spec'd variable names the SDK package exports from
#: ``opentelemetry.sdk.environment_variables`` — spelled out here because
#: the SDK is an optional extra and the trigger check must work (and warn)
#: without it.
_OTLP_ENDPOINT_ENV: str = "OTEL_EXPORTER_OTLP_ENDPOINT"
_SDK_DISABLED_ENV: str = "OTEL_SDK_DISABLED"
_PROMETHEUS_HOST_ENV: str = "OTEL_EXPORTER_PROMETHEUS_HOST"
_PROMETHEUS_PORT_ENV: str = "OTEL_EXPORTER_PROMETHEUS_PORT"

_SIGNAL_ENVS: tuple[str, ...] = (OTEL_TRACES_EXPORTER, OTEL_METRICS_EXPORTER, OTEL_LOGS_EXPORTER)

#: The exporter the specification selects for traces and metrics when the
#: signal's variable is unset. The Python SDK's configurator applies no
#: default of its own — ``opentelemetry-distro`` supplies it — so a bare
#: endpoint with no exporter variable would otherwise install providers
#: with nothing attached and export silence.
_SPEC_DEFAULT_EXPORTER: str = "otlp"

#: The SDK entry-point name of the Prometheus pull reader, and the package
#: that registers it.
_PROMETHEUS_EXPORTER: str = "prometheus"
_PROMETHEUS_MODULE: str = "opentelemetry.exporter.prometheus"

type ExporterWiring = Literal[
    "configured",
    "preconfigured",
    "none",
    "disabled",
    "sdk_disabled",
    "sdk_missing",
]
"""What :func:`configure_exporters` did about the process-global providers.

- ``configured``: SDK providers were installed from the environment (and
  ``metrics_port``); ``otel-exporter-configured`` names the exporters.
- ``preconfigured``: a real provider was already set — an embedding
  application, a vendor distro or ``opentelemetry-instrument`` owns the
  process — so nothing was touched.
- ``none``: nothing asked for an exporter; the API's no-op proxies stay.
- ``disabled``: ``TASKQ_OTEL_AUTOCONFIGURE=false``.
- ``sdk_disabled``: ``OTEL_SDK_DISABLED=true``.
- ``sdk_missing``: an exporter was requested but the package that provides
  it is not installed; ``otel-exporter-unavailable`` names the extra.
"""


class OtelExporterConfigurationError(RuntimeError):
    """The operator asked for an exporter the SDK could not build.

    Raised at worker startup — the earliest stage that can catch a
    misspelled exporter name or a protocol the installed exporters do not
    speak — so the worker refuses to start rather than run with a
    telemetry pipeline that looks configured and exports nothing.
    """


class _ExporterSettings(Protocol):
    """The slice of ``WorkerSettings`` this module reads.

    Structural, so the obs leaf never imports ``taskq.settings`` (which
    imports ``taskq.obs``); ``WorkerSettings`` satisfies it as-is.
    """

    @property
    def otel_autoconfigure(self) -> bool: ...

    @property
    def metrics_port(self) -> int | None: ...

    @property
    def health_host(self) -> str: ...


@dataclass(frozen=True)
class _ExporterPlan:
    """The exporter names one configurator run installs, per signal.

    ``sources`` names what asked for them (``env``, ``prometheus``);
    ``*_extra`` are the names the configurator must be handed on top of its
    own environment parse — it appends that parse itself, so an
    environment-selected name listed as an extra would be built twice.
    """

    traces: tuple[str, ...]
    metrics: tuple[str, ...]
    logs: tuple[str, ...]
    sources: tuple[str, ...]
    prometheus_port: int | None

    @property
    def requested(self) -> bool:
        return bool(self.sources)

    def extras(self, signal: Literal["traces", "metrics", "logs"]) -> list[str]:
        planned: tuple[str, ...] = getattr(self, signal)
        env_var = {
            "traces": OTEL_TRACES_EXPORTER,
            "metrics": OTEL_METRICS_EXPORTER,
            "logs": OTEL_LOGS_EXPORTER,
        }[signal]
        from_env = set(_env_names(env_var))
        return [name for name in planned if name not in from_env]


def _env_names(var: str) -> tuple[str, ...]:
    """The exporter names an ``OTEL_*_EXPORTER`` variable lists — the
    SDK's own parsing (comma-separated; ``none`` selects nothing)."""
    raw = os.environ.get(var, "").strip()
    if not raw or raw.lower() == "none":
        return ()
    return tuple(name.strip() for name in raw.split(",") if name.strip())


def _installed(module: str) -> bool:
    """Whether *module* imports — the real import, so a package whose
    parent is missing (``opentelemetry.sdk`` without the extra) answers
    False instead of raising out of a spec lookup."""
    try:
        importlib.import_module(module)
    except ImportError:
        return False
    return True


def _plan(settings: _ExporterSettings, *, prometheus_available: bool) -> _ExporterPlan:
    """Decide what to install from the environment and the settings.

    The defaults are the spec's: ``otlp`` for traces and metrics when an
    OTLP endpoint is set and the signal's own variable is not. Logs keep
    no default — TaskQ logs through structlog, and the OTel log bridge is
    an explicit choice. ``metrics_port`` adds the Prometheus pull reader
    when its package is installed; when it is not, the port is reported as
    unavailable and the rest of the plan stands.
    """
    endpoint_set = bool(os.environ.get(_OTLP_ENDPOINT_ENV, "").strip())
    env_requested = endpoint_set or any(var in os.environ for var in _SIGNAL_ENVS)

    traces = _env_names(OTEL_TRACES_EXPORTER)
    metric_names = _env_names(OTEL_METRICS_EXPORTER)
    if endpoint_set and OTEL_TRACES_EXPORTER not in os.environ:
        traces = (_SPEC_DEFAULT_EXPORTER,)
    if endpoint_set and OTEL_METRICS_EXPORTER not in os.environ:
        metric_names = (_SPEC_DEFAULT_EXPORTER,)

    sources: list[str] = ["env"] if env_requested else []
    prometheus_port: int | None = None
    if settings.metrics_port is not None and prometheus_available:
        sources.append(_PROMETHEUS_EXPORTER)
        prometheus_port = settings.metrics_port
        if _PROMETHEUS_EXPORTER not in metric_names:
            metric_names = (*metric_names, _PROMETHEUS_EXPORTER)

    return _ExporterPlan(
        traces=traces,
        metrics=metric_names,
        logs=_env_names(OTEL_LOGS_EXPORTER),
        sources=tuple(sources),
        prometheus_port=prometheus_port,
    )


def _provider_already_set() -> bool:
    """True when either global provider is a real one, not the API proxy.

    ``get_tracer_provider`` / ``get_meter_provider`` also honour the
    ``OTEL_PYTHON_*_PROVIDER`` entry-point variables on first call, so a
    provider selected that way is loaded here and counts as set — which is
    the right answer: the operator chose it, and the set-once guard would
    refuse ours anyway.
    """
    if not isinstance(trace.get_tracer_provider(), ProxyTracerProvider):
        return True
    # The metrics API keeps its proxy class private; its name is the
    # stable fact the API exposes about it.
    return type(metrics.get_meter_provider()).__name__ != "_ProxyMeterProvider"


def _warn_unavailable(extra: str, *, source: str, detail: str) -> None:
    _log.warning("otel-exporter-unavailable", extra=extra, source=source, detail=detail)


def configure_exporters(settings: _ExporterSettings) -> ExporterWiring:
    """Install SDK tracer and meter providers from the OTel environment.

    Called once by the ``taskq worker`` CLI after settings load and before
    the worker records anything: measurements a proxy instrument takes
    before a provider exists are dropped, not replayed. Embedding
    applications that host a worker without the CLI call this themselves
    at process start, or configure the SDK directly — a provider that is
    already set is never replaced.

    Raises :class:`OtelExporterConfigurationError` when the environment
    names an exporter the SDK cannot build; every other outcome is a
    return value plus one log line.
    """
    if not settings.otel_autoconfigure:
        _log.info("otel-exporter-autoconfigure-disabled", source="TASKQ_OTEL_AUTOCONFIGURE")
        return "disabled"
    if os.environ.get(_SDK_DISABLED_ENV, "").strip().lower() == "true":
        _log.info("otel-exporter-sdk-disabled", source=_SDK_DISABLED_ENV)
        return "sdk_disabled"

    prometheus_available = _installed(_PROMETHEUS_MODULE)
    if settings.metrics_port is not None and not prometheus_available:
        _warn_unavailable(
            "prometheus",
            source=_PROMETHEUS_EXPORTER,
            detail=(
                "TASKQ_METRICS_PORT is set but opentelemetry-exporter-prometheus is "
                "not installed, so no scrape listener will be bound; install "
                "taskq-py[prometheus]"
            ),
        )
    plan = _plan(settings, prometheus_available=prometheus_available)
    if not plan.requested:
        if settings.metrics_port is not None:
            return "sdk_missing"
        _log.info("otel-exporter-configured", traces="", metrics="", logs="", source="none")
        return "none"

    if _provider_already_set():
        _log.info(
            "otel-exporter-preconfigured",
            detail=(
                "a tracer or meter provider is already installed (an embedding "
                "application, a vendor distro, or opentelemetry-instrument); "
                "the worker leaves it untouched"
            ),
        )
        return "preconfigured"

    try:
        from opentelemetry.sdk._configuration import (
            _initialize_components,  # pyright: ignore[reportPrivateUsage]  # Why: the SDK ships its env-var configurator as a private module; it is the exact code path opentelemetry-instrument runs, and re-implementing it would drift from the SDK's own parsing of the same variables.
        )
    except ImportError:
        _warn_unavailable(
            "otel",
            source=",".join(plan.sources),
            detail=(
                "OTEL_* exporter variables are set but opentelemetry-sdk is not "
                "installed, so nothing will be exported; install taskq-py[otel]"
            ),
        )
        return "sdk_missing"

    if plan.prometheus_port is not None:
        # The SDK's Prometheus reader binds its listener from these two
        # variables and nothing else. TASKQ_METRICS_PORT is the explicit
        # opt-in, so it is authoritative for the port; the host follows the
        # worker's other TCP listener unless the operator addressed the
        # reader directly.
        os.environ[_PROMETHEUS_PORT_ENV] = str(plan.prometheus_port)
        os.environ.setdefault(_PROMETHEUS_HOST_ENV, settings.health_host)

    try:
        _initialize_components(
            trace_exporter_names=plan.extras("traces"),
            metric_exporter_names=plan.extras("metrics"),
            log_exporter_names=plan.extras("logs"),
        )
    except Exception as exc:
        raise OtelExporterConfigurationError(
            "OpenTelemetry exporter configuration failed: "
            f"{type(exc).__name__}: {exc}. Check OTEL_TRACES_EXPORTER, "
            "OTEL_METRICS_EXPORTER, OTEL_LOGS_EXPORTER and OTEL_EXPORTER_OTLP_PROTOCOL "
            "against the exporters installed by taskq-py[otel] (otlp, console) and "
            "taskq-py[prometheus] (prometheus), or set TASKQ_OTEL_AUTOCONFIGURE=false "
            "to configure the SDK yourself."
        ) from exc

    _log.info(
        "otel-exporter-configured",
        traces=",".join(plan.traces),
        metrics=",".join(plan.metrics),
        logs=",".join(plan.logs),
        source=",".join(plan.sources),
        prometheus_port=plan.prometheus_port,
        prometheus_host=(
            os.environ.get(_PROMETHEUS_HOST_ENV) if plan.prometheus_port is not None else None
        ),
    )
    return "configured"
