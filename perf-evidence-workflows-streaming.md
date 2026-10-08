# The streaming source's bands (T20) — the emit tx, the chain dispatch, the routed fork

The three T20 bands, measured ON THE BUILT CODE through the REAL
surfaces (`taskq.workflows._emit.emit_batch`, the backend's own
`_dispatch_batch` — the shipped claim SQL unchanged, `finalize_node`
carrying the chain's one-child ForkSpec) against the `postgres:18.6`
container on the repo's tuning (`jit=off`, `fsync=off`,
`synchronous_commit=off`), 30 rounds each (the emit: 5 pages). The
script: `scripts/measure_t20_streaming_bands.py`; raw output:
`.measurements/t20-streaming-bands.json` (+ the timestamped capture
beside it, per the capture law).

## The emit tx per page (THE new primitive)

40 chain starts + 40 edge rows + the cursor checkpoint on the source
row under the FULL claim fence — ONE transaction, while the source stays
`running`:

- **measured: p50 3.2 ms, max 10.4 ms** (5 pages) → band **7–14 ms/page**
  held with headroom. The streaming source adds ONE bounded tx per
  page, not per record; the emit's cost is page-width, never
  record-count-scale.

## The dispatch band @ the chain backlog

The REAL certified `_dispatch_batch` (unchanged) at a 200-chain backlog
(vs the 200-PLAIN baseline — the same queue mechanics, no workflow
shape):

| backlog | p50 | p95 | max |
|---|---|---|---|
| 200-chain | 7.50 ms | 9.09 ms | 19.5 ms |
| 200-plain (baseline) | 7.36 ms | 9.08 ms | 9.48 ms |
| 800-row mixed (200 chain + 600 plain) | 7.23 ms | 8.53 ms | 9.70 ms |

- **band ratio (chain/plain) at p50: 1.02×** — the chain-shaped backlog
  costs the SAME ms-class as plain jobs (the spike's prototype measured
  1.19×; the built code's fence EXISTS-probe is at least as cheap).
- **Depth-invariant at 800 rows** — the depth-bounding design holds on
  the built code (the band does not grow with the backlog).

## The chain-step fork tx (the routed finalize)

One chain row's worker pass through the ROUTER's shape: the fenced
terminal mark + the route's ONE child + its edge (tx1), then the
guarded decrement (tx2 — no join; the chain has none), via the REAL
`finalize_node`:

- **measured: p50 0.83 ms, p95 1.40 ms** (50 rounds) — the chain's
  per-hop cost is a single-digit-ms pair of transactions; the
  conditional edge rides the certified fork machinery at its native
  band.

## The verdict

The streaming does not degrade the dispatch band: the chain backlog
dispatches at the plain baseline's class, the emit tx is bounded per
page, and the routed fork is a sub-millisecond-class tx pair. The
spike's §7 conclusions carry onto the built code at better numbers.
