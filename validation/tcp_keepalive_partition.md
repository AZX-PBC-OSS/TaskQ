# TCP partition detection on dedicated connections (manual)

Excluded from CI because the keepalive detection window
(`TCP_KEEPIDLE + TCP_KEEPCNT * TCP_KEEPINTVL = 30 + 3 * 5 = 45 s`) is too long
for unit-test latency budgets.

## What it verifies

`notify_conn` and `leader_conn` carry `SO_KEEPALIVE` plus the §6.6 / §12.5
timing parameters on their underlying sockets. When PG becomes unreachable
mid-session (e.g. network partition), the kernel surfaces the failure on the
next read/write within ~45 s instead of hanging silently for hours.

The in-CI test `tests/test_worker_deps.py::test_keepalive_setsockopt_fires_on_dedicated_conns`
verifies that `SO_KEEPALIVE` is set on both sockets. This manual harness
verifies that the configured timing values actually trigger detection against
a paused PG container.

## How to run

Requires Docker (for `testcontainers`) and the project venv.

```sh
uv run python validation/tcp_keepalive_partition.py
```

Expected output:

```
Starting PG18 testcontainer...
WorkerDeps open. Pausing PG container to simulate partition...
notify_conn detected partition in 45.x s
leader_conn detected partition in 45.x s
PASS
```

## Failure modes and what they mean

- **Hangs past 60 s deadline** — keepalive parameters not applied to the
  socket. Inspect `_apply_keepalive` in `src/taskq/worker/deps.py` and the
  platform branch (`sys.platform == "linux"` vs `"darwin"`).
- **Probes never raise** — the container pause did not take effect. Confirm
  Docker daemon permissions and that `container.get_wrapped_container().pause()`
  succeeded.
- **Detection time ≪ 30 s** — keepalive isn't what surfaced the error;
  something else (asyncpg-internal timeout, container-network teardown) is
  doing it. Investigate.
