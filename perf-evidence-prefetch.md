# Prefetch slot accounting — peak rows locked, sub-ms throughput (#229)

Evidence for the prefetch sub-item of #229: the producer sized every
claim by `local_queue.maxsize - local_queue.qsize()` alone, so a worker
whose consumers were all busy and whose queue had drained looked fully
free and locked up to `max_concurrency` ADDITIONAL rows — up to **2x
the slot count** held `running` at once (double the crash-reclaim
exposure, pending work locked behind long jobs while peer workers
idled, gauges overstating occupancy by up to 2x).

## The change

`producer_loop` (src/taskq/worker/run.py) sizes claims by the genuinely
free slots:

```python
available = local_queue.maxsize - local_queue.qsize() - deps.active_jobs.count()
```

and the consumer loops wake the producer at the **deregister** that
actually frees the active slot (the second slot-release point, beside
the get()-point wake), so a finished job's replacement claim does not
wait for the fallback poll — the trade the issue named ("a finished
local row no longer triggers a claim until accounting settles") is
bounded by a same-tick wake, not a poll interval. Pinned by
`tests/test_producer_slot_accounting.py`: a fully-busy worker claims
nothing and arms no cooldown, claim size tracks the active count
exactly, the completion wake fires after the deregister, and a
saturated producer drains promptly as slots free.

## Measurement

`benchmarks/prefetch_slots.py` — one real `PostgresBackend`, real
`consumer_loop_stub` consumers (real terminal writes), one shared
slot-freed event, the claim cooldown and jitter verbatim; the two sides
differ ONLY in the availability expression (the A6 harness doctrine).
Engine: the test suite's tuned `postgres:18-alpine` container, 8
consumer slots (`max_concurrency=8`).

### Slow jobs (3 s) — the 2x locking the change removes

| side | jobs/s | peak running rows locked | claim rounds |
|---|---|---|---|
| OLD (queue-emptiness) | 2.6 | **16** (2x slots) | 4 |
| NEW (free slots) | 2.6 | **8** (slots) | 4 |

Throughput is identical by construction — the local queue never added
parallelism, only consumers execute — and the peak-locked number is the
whole story: OLD holds twice the slot count locked (every extra row is
reclaim work on a crash, and pending work no other worker can take),
NEW holds at most the slot count.

### Sub-millisecond jobs — the trade the issue named

2000 jobs through the stub sentinel (`stub_work_timeout=0`), three
repetitions:

| repetition | OLD jobs/s | NEW jobs/s | delta |
|---|---|---|---|
| 1 | 646.7 | 644.0 | −0.4% |
| 2 | 653.4 | 662.7 | +1.4% |
| 3 | 677.2 | 673.0 | −0.6% |

The regression for sub-millisecond actors is **within the run-to-run
noise band** (±1.5% across repetitions), and the claim-round counts are
identical (499 vs 500-501): the dispatch round trip the old prefetch
hid is re-hidden by the pipeline itself — the deregister-side wake
refills the freed slots in one batched claim per completion wave, and
the claim cooldown keeps the round count flat. Peak-locked on this
shape drops from 8-11 to 5-6 (the overclaim only reaches the full 2x
when jobs are slow enough to keep every consumer busy while the queue
drains — the 3 s shape above).

## Decision: no configuration knob

The issue proposed making the behavior configurable if the sub-ms
regression were material. Measured, it is not: the worst repetition
costs 0.6% on a dispatch-bound sub-ms workload, against a structural
halving of locked rows. A knob would re-expose the 2x locking (reclaim
exposure, head-of-line latency, gauge overstatement) as a supported
configuration for a 1% win on one workload shape — the arithmetic the
issue itself made (River/Oban claim `limit − running`). The
exact-slot-accounting behavior ships as the only behavior; an operator
who truly needs prefetch back has the escape hatch of raising
`TASKQ_MAX_CONCURRENCY` (which sizes the local queue) and accepting the
locked-row exposure knowingly.
