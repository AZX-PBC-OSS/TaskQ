// fanout — streams 5 sub-enqueue requests via ctx.subenqueue, then holds
// briefly before completing. Proves buffered-bridge sub-enqueue semantics:
// requests stream to the parent immediately, the parent BUFFERS them and
// only flushes on parent success (scenario 2); a runtime crash after the
// requests were delivered but before the done frame leaves 0 sub-jobs in
// the queue (scenario 3) because the parent discards the buffer.
import { z } from "zod";

export const payload = z.object({
  children: z.number().int().min(1).max(100),
  // ms to sleep after the sub-enqueue buffer is populated, before the
  // run finishes. The crash scenario kills the runtime inside this window.
  hold_ms: z.number().int().min(0).max(60_000).default(300),
});

export const result = z.object({
  requested: z.number().int(),
});

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function handler(payload: z.infer<typeof payload>, ctx: any) {
  const jobs = Array.from({ length: payload.children }, (_, i) => ({
    actor: "etl_small",
    payload: { rows: 100 + i, label: `child-${i}` },
  }));
  ctx.subenqueue(jobs);
  ctx.log(`streamed ${jobs.length} sub-enqueue requests; parent buffers until done`);
  ctx.progress(1, 50);
  await sleep(payload.hold_ms);
  ctx.progress(2, 100);
  return { requested: jobs.length };
}
