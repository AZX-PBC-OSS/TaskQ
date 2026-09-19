"""Shared rate-limit decision logger.

Emits ``logger.debug("rate-limit-decision", **fields)`` on every call and
``logger.info("rate-limit-decision", **fields)`` additionally on denial;
a denial also bumps ``taskq.ratelimit.denials`` (backend label only ,
bucket names are caller-derived and unbounded, so they are not a metric
dimension). The ``style`` keyword argument is omitted from the log payload
when ``None`` so that token-bucket log lines remain byte-for-byte identical
to the pre-extraction output.
"""

from typing import Literal

import structlog

from taskq.obs import record_ratelimit_denial
from taskq.ratelimit.decision import RateLimitDecision

logger = structlog.get_logger("taskq.ratelimit._decision_log")


def log_decision(
    result: RateLimitDecision,
    *,
    style: Literal["log", "gcra"] | None = None,
) -> None:
    retry_after_seconds: float | None = (
        result.retry_after.total_seconds() if result.retry_after is not None else None
    )
    fields: dict[str, object] = {
        "bucket_name": result.bucket_name,
        "backend": result.backend,
        "allowed": result.allowed,
        "remaining": result.remaining,
        "retry_after_seconds": retry_after_seconds,
    }
    if style is not None:
        fields["style"] = style
    logger.debug("rate-limit-decision", **fields)
    if not result.allowed:
        logger.info("rate-limit-decision", **fields)
        record_ratelimit_denial(result.backend)
