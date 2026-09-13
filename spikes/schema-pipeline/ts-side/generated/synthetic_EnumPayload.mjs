import { z } from "zod"

export const EnumPayload = z.object({ "priority": z.any().default("medium") })
