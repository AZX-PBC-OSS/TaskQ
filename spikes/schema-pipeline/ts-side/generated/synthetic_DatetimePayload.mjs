import { z } from "zod"

export const DatetimePayload = z.object({ "started_at": z.string().datetime({ offset: true }).optional(), "due_date": z.string().date().default("2026-12-31") })
