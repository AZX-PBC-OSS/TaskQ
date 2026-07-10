"""OTel-to-Prometheus bridge for TaskQ (requires taskq[prometheus]).

Importing this package without opentelemetry-exporter-prometheus installed
raises ImportError with a clear install instruction.  The guard lives in
_metrics.py so pyright can resolve the create_metrics_router symbol cleanly.
"""

from taskq.contrib.prometheus._metrics import create_metrics_router

__all__ = ["create_metrics_router"]
