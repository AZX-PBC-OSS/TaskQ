import { z } from "zod"

export const ConstrainedStringPayload = z.object({ "slug": z.string().min(3).max(20), "label": z.string().regex(new RegExp("^[a-z]+$")).default("task") })
