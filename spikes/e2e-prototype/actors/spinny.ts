// spinny — CPU busy loop for cancellation testing.
//
// Cooperative mode (default): runs in ~100ms busy chunks, yields to the
// event loop between chunks (so the incoming {op:"cancel"} line can be
// processed), polls ctx.cancelled() after each chunk, and returns early
// when a cancel was requested — the runtime classifies the run as
// cancelled (scenario 4).
//
// Pathological mode (env SPINNY_NO_COOPERATE=1): never yields the event
// loop and ignores the flag entirely — the cancel line sits unread in the
// pipe buffer, which is precisely why the parent must escalate to a
// process kill after its grace period (scenario 5).
import { z } from "zod";

export const payload = z.object({
  duration_ms: z.number().int().min(1).max(120_000),
});

export const result = z.object({
  iterations: z.number().int(),
  observed_cancel: z.boolean(),
});

const now = () => Number(process.hrtime.bigint() / 1_000_000n); // ms

const busyChunk = (ms: number) => {
  const until = now() + ms;
  let iters = 0;
  while (now() < until) iters++; // pure CPU — no await, no I/O
  return iters;
};

const yieldEventLoop = () => new Promise((r) => setImmediate(r));

export async function handler(payload: z.infer<typeof payload>, ctx: any) {
  const noCooperate = process.env.SPINNY_NO_COOPERATE === "1";
  const deadline = now() + payload.duration_ms;
  let iterations = 0;
  let observedCancel = false;

  while (now() < deadline) {
    iterations += busyChunk(100);
    if (noCooperate) continue; // never yields — event loop is pinned
    await yieldEventLoop();
    if (ctx.cancelled()) {
      observedCancel = true;
      ctx.log("cancel flag observed; returning early");
      break;
    }
    ctx.progress(0, Math.min(99, Math.round(((now() - (deadline - payload.duration_ms)) / payload.duration_ms) * 100)));
  }

  return { iterations, observed_cancel: observedCancel };
}
