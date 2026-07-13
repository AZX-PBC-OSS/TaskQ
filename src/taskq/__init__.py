"""TaskQ — async-native, Postgres-backed background job library.

Canonical imports:

    from taskq import actor, TaskQ, JobHandle, JobFailed, RetryPolicy
    from taskq import cron, ScheduleHandle
    from taskq.context import JobContext
    from taskq.di import ProviderRegistry, Scope
"""

import importlib.metadata

from taskq.actor import ActorFn, ActorFnWithCtx, ActorHandler, ActorRef, actor
from taskq.auth import (
    PgCredential,
    PgCredentialProvider,
    RedisCredential,
    RedisCredentialProvider,
)
from taskq.backend._protocol import (
    CancelPhase,
    DstStrategy,
    IdempotencyKey,
    IdentityKey,
    JobFilter,
    JobId,
    JobSortField,
    QueueMode,
    QueueName,
    RateLimitBackend,
    RetryKind,
    ScheduleRecord,
)
from taskq.batch import BatchCompletionStatus, BatchHandle, EnqueueItem, wait_for_batch
from taskq.client import CancelResult, JobEvent, JobHandle, JobsClient, TaskQ
from taskq.client._enqueuer import SubJobEnqueuer
from taskq.connections import ConnFactory, PoolFactory, RedisFactory, WorkerConnections
from taskq.context import JobContext
from taskq.cron import CronScheduleSpec, ScheduleHandle, cron
from taskq.exceptions import (
    ActorConfigDriftError,
    ActorConfigDriftList,
    BackpressureError,
    DependencyCycle,
    DIError,
    IllegalStateTransition,
    JobFailed,
    MaxPendingExceededError,
    MissingProvider,
    PartialBatchError,
    PayloadValidationError,
    ProgressTooLarge,
    ReservationUnavailable,
    ResultTooLarge,
    ResultUnavailable,
    RetryAfter,
    SchemaNotMigratedError,
    ScopeViolation,
    SingletonCollisionError,
    Snooze,
    SubEnqueueError,
    TaskQError,
    WorkerOwnershipMismatch,
)
from taskq.obs import ErrorReporter, NullErrorReporter
from taskq.progress import ProgressEvent
from taskq.retry import (
    Fail,
    JobRetryState,
    OnSuccess,
    Retry,
    RetryClassifier,
    RetryClassifierHook,
    RetryDecision,
    RetryOverride,
    RetryPolicy,
)
from taskq.scheduler import register_cron

__all__ = [
    "ActorConfigDriftError",
    "ActorConfigDriftList",
    "ActorFn",
    "ActorFnWithCtx",
    "ActorHandler",
    "ActorRef",
    "BackpressureError",
    "BatchCompletionStatus",
    "BatchHandle",
    "CancelPhase",
    "CancelResult",
    "ConnFactory",
    "CronScheduleSpec",
    "DIError",
    "DependencyCycle",
    "DstStrategy",
    "EnqueueItem",
    "ErrorReporter",
    "Fail",
    "IdempotencyKey",
    "IdentityKey",
    "IllegalStateTransition",
    "JobContext",
    "JobEvent",
    "JobFailed",
    "JobFilter",
    "JobHandle",
    "JobId",
    "JobRetryState",
    "JobSortField",
    "JobsClient",
    "MaxPendingExceededError",
    "MissingProvider",
    "NullErrorReporter",
    "OnSuccess",
    "PartialBatchError",
    "PayloadValidationError",
    "PgCredential",
    "PgCredentialProvider",
    "PoolFactory",
    "ProgressEvent",
    "ProgressTooLarge",
    "QueueMode",
    "QueueName",
    "RateLimitBackend",
    "RedisCredential",
    "RedisCredentialProvider",
    "RedisFactory",
    "ReservationUnavailable",
    "ResultTooLarge",
    "ResultUnavailable",
    "Retry",
    "RetryAfter",
    "RetryClassifier",
    "RetryClassifierHook",
    "RetryDecision",
    "RetryKind",
    "RetryOverride",
    "RetryPolicy",
    "ScheduleHandle",
    "ScheduleRecord",
    "SchemaNotMigratedError",
    "ScopeViolation",
    "SingletonCollisionError",
    "Snooze",
    "SubEnqueueError",
    "SubJobEnqueuer",
    "TaskQ",
    "TaskQError",
    "WorkerConnections",
    "WorkerOwnershipMismatch",
    "__version__",
    "actor",
    "cron",
    "register_cron",
    "wait_for_batch",
]

try:
    __version__ = importlib.metadata.version("taskq-py")
except importlib.metadata.PackageNotFoundError:
    __version__ = "0.1.0"
