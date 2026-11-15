"""Generic fixture support for future standalone adoption sample applications.

This package exports the hermetic paged-provider protocol with its local
in-memory implementation and its real-PostgreSQL shared implementation. It
is not a runnable workflow sample, does not wire a TaskQ workflow, and does
not claim adoption evidence. See ``examples.adoption.provider`` for the
snapshot/cursor policy, bounds, fault script, and exact counters;
``examples.adoption.pg_provider`` documents the durable scratch-schema
variant used across provider instances and connection pools.
"""

from examples.adoption.pg_provider import (
    PgPagedProvider,
    PgProviderRecordIdentity,
    PgProviderScenarioState,
)
from examples.adoption.provider import (
    LocalPagedProvider,
    PagedProvider,
    ProviderContractError,
    ProviderCursor,
    ProviderCursorExpired,
    ProviderError,
    ProviderFault,
    ProviderFaultPhase,
    ProviderFaultRule,
    ProviderFaultScript,
    ProviderMalformedRecord,
    ProviderPage,
    ProviderPageLimitError,
    ProviderPoison,
    ProviderPoisonFault,
    ProviderRateLimited,
    ProviderRateLimitedFault,
    ProviderRecord,
    ProviderRecordLimitExceeded,
    ProviderStats,
    ProviderUnavailable,
    ProviderUnavailableFault,
    ProviderUnknownCursor,
)

__all__ = [
    "LocalPagedProvider",
    "PagedProvider",
    "PgPagedProvider",
    "PgProviderRecordIdentity",
    "PgProviderScenarioState",
    "ProviderContractError",
    "ProviderCursor",
    "ProviderCursorExpired",
    "ProviderError",
    "ProviderFault",
    "ProviderFaultPhase",
    "ProviderFaultRule",
    "ProviderFaultScript",
    "ProviderMalformedRecord",
    "ProviderPage",
    "ProviderPageLimitError",
    "ProviderPoison",
    "ProviderPoisonFault",
    "ProviderRateLimited",
    "ProviderRateLimitedFault",
    "ProviderRecord",
    "ProviderRecordLimitExceeded",
    "ProviderStats",
    "ProviderUnavailable",
    "ProviderUnavailableFault",
    "ProviderUnknownCursor",
]
