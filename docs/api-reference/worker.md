# Worker

Worker process internals: dispatcher, consumer, heartbeat, leader election, shutdown.

`taskq.worker` itself is a lazy `__getattr__` shim (re-exports selected names from its
submodules on first access) and renders no useful members on its own. The directives below
target the concrete submodules instead. See also the [Workers guide](../guides/workers.md)
for a task-oriented walkthrough.

## Process entry point, registration, producer/consumer loops

::: taskq.worker.run

## Maintenance leader (election, sweeps, cron, prune/archive)

::: taskq.worker.leader

## Dispatch

::: taskq.worker.dispatch

## Heartbeat and cancellation

::: taskq.worker.heartbeat

::: taskq.worker.cancel

## Shutdown

::: taskq.worker.shutdown
