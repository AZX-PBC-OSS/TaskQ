# The streaming source's bands (T20) — the emit tx, the chain dispatch, the routed fork

The three T20 bands, measured ON THE BUILT CODE through the REAL
surfaces (`taskq.workflows._emit.emit_batch`, the backend's own
`_dispatch_batch` — the shipped claim SQL unchanged, `finalize_node`
carrying the chain's one-child ForkSpec) against the `postgres:18.6`
container on the repo's tuning (`jit=off`, `fsync=off`,
`synchronous_commit=off`), 30 rounds each (the emit: 5 pages). The
script: `scripts/measure_t20_streaming_bands.py`; raw output:
`.measurements/t20-streaming-bands.json` (+ the timestamped capture
beside it, per the capture law). THE TABLES BELOW CITE THE RANGE —
EVERY captured run on disk, not the friendliest one (an earlier
revision of this file cited the FIRST capture alone, whose numbers
were the lowest in the tree — the cherry-pick is corrected here; the
last row is THIS revision's own re-run, taken on a LOADED box (load
average 33, concurrent sessions), kept for its regime label, never
for its absolute values).

## The emit tx per page (THE new primitive)

40 chain starts + 40 edge rows + the cursor checkpoint on the source
row under the FULL claim fence — ONE transaction, while the source stays
`running`:

- **measured: p50 3.2–8.1 ms, max 10.4–20.6 ms across the five
  captured runs** (the quiet-box runs sit at the low end; the max TAIL's
  high end is the 05:25:19 capture's 20.6 ms — a p95-class page under
  concurrent sessions, named rather than averaged away; the loaded-box
  re-run at the next run's high end) → band **p50 3.2–8.1 ms/page, the
  max tail bounded at 20.6 ms**. The streaming source adds ONE bounded
  tx per page, not per record; the emit's cost is page-width, never
  record-count-scale. (An earlier revision of THIS file claimed "max
  10.4–14.5 ms" — the 05:25:19 capture's 20.569 max contradicts it; the
  figures state the artifacts' range.)

## The dispatch band @ the chain backlog

The REAL certified `_dispatch_batch` (unchanged) at a 200-chain backlog
(vs the 200-PLAIN baseline — the same queue mechanics, no workflow
shape). EVERY captured run on disk, with its regime:

| capture | chain p50 | plain p50 | ratio (chain/plain) |
|---|---|---|---|
| 2026-10-08T04:41Z | 7.50 ms | 7.36 ms | 1.02× |
| 2026-10-08T05:08Z | 9.75 ms | 9.64 ms | 1.01× |
| 2026-10-08T05:25Z (run …19) | 11.60 ms | 11.20 ms | 1.04× |
| 2026-10-08T05:25Z (run …25) | 10.78 ms | 11.80 ms | 0.91× |
| 2026-10-09T06:04Z (LOADED box, load avg 33) | 31.80 ms | 13.65 ms | 2.33× |

- **band ratio at p50: 0.91×–1.04× on the quiet-box runs** — the
  chain-shaped backlog costs the SAME ms-class as plain jobs; the
  loaded-box run's 2.33× is the LOAD's signature (the tx pairs contend
  with the box's neighbors — both arms inflate, the chain's two-tx
  shape first), never cited as the built code's verdict. (An earlier
  revision cited "the spike's prototype measured 1.19×" — no capture
  with that number exists in the evidence tree; the figure is DELETED —
  provenance or silence. The single-run "1.02×" the same revision
  cited as THE ratio was the first capture alone — the table above is
  the honest range.)
- **Depth-invariant at 800 rows** — the mixed backlog's band tracks the
  plain baseline across the captures (p50 7.23–13.46 ms, the same
  ms-class as the 200-plain arm at each regime; the depth-bounding
  design holds on the built code — the band does not grow with the
  backlog).

## The chain-step fork tx (the routed finalize)

One chain row's worker pass through the ROUTER's shape: the fenced
terminal mark + the route's ONE child + its edge (tx1), then the
guarded decrement (tx2 — no join; the chain has none), via the REAL
`finalize_node`:

- **measured: p50 0.83–4.96 ms, p95 1.40–7.79 ms across the five
  captured runs** (quiet-box low end; loaded-box high end) — the
  chain's per-hop cost is a single-digit-ms pair of transactions at
  every regime captured; the conditional edge rides the certified fork
  machinery at its native band.

## The verdict

The streaming does not degrade the dispatch band (0.91×–1.04× on the
quiet-box captures; the loaded-box run is the load's signature, labeled
as such above): the chain backlog dispatches at the plain baseline's
class, the emit tx is bounded per page, and the routed fork is a
single-digit-ms-class tx pair at every captured regime. The spike's §7
conclusions carry onto the built code at the captured numbers' range.
