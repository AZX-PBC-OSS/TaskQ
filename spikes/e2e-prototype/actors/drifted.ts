// etl_small variant with a DRIFTED payload schema for scenario 7.
// `label` is required here but optional in the registered (expected) schema,
// and `rows` lost its upper bound — canonical-JSON hash of this manifest
// entry cannot match the pre-registered one.
import { z } from "zod";
import { handler } from "./etl_small.ts";

export const payload = z.object({
  rows: z.number().int().min(1),
  label: z.string(),
});

export const result = z.object({
  rows_processed: z.number().int(),
  label: z.string(),
  duration_ms: z.number().int(),
});

export async function registerAll(defineActor: (def: any) => void) {
  defineActor({
    name: "etl_small", // same name as the honest runtime — that's the point
    queue: "etl",
    payload,
    result,
    retry: { max_attempts: 3 },
    handler,
  });
}
