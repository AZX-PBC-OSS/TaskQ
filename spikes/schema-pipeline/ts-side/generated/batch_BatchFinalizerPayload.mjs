import { z } from "zod"

export const BatchFinalizerPayload = z.object({ "batch_id": z.string().uuid(), "expected": z.number().int() })
