import { z } from "zod"

export const NestedPayload = z.object({ "address": z.any(), "tags": z.array(z.string()).optional() })
