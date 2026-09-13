// etl_small — I/O-ish actor: sleeps between "stages", emits progress,
// returns a typed result object. Proves the happy-path round trip:
// typed payload in (Zod-validated), progress/log events streamed,
// typed result out (re-validated Python-side via Pydantic TypeAdapter).
import { z } from "zod";

export const payload = z.object({
  rows: z.number().int().min(1).max(1_000_000),
  label: z.string(),
});

export const result = z.object({
  rows_processed: z.number().int(),
  label: z.string(),
  duration_ms: z.number().int(),
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function handler(payload: z.infer<typeof payload>, ctx: any) {
  const started = Date.now();
  const stages = 5;
  for (let step = 1; step <= stages; step++) {
    // I/O-ish wait (setTimeout sleep) — the event loop stays free so the
    // warm runtime can interleave other jobs' runs concurrently.
    await sleep(40);
    ctx.log(`stage ${step}/${stages} done`, { step });
    ctx.progress(step, Math.round((step / stages) * 100));
  }
  return {
    rows_processed: payload.rows,
    label: payload.label,
    duration_ms: Date.now() - started,
  };
}
