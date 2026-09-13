import { z } from "zod"

export const UnionPayload = z.object({ "value": z.union([z.number().int(), z.string()]), "maybe": z.union([z.number(), z.null()]).default(null) })
