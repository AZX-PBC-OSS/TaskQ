import { z } from "zod"

export const WordCountResult = z.object({ "word_count": z.number().int(), "char_count": z.number().int(), "processed_at": z.union([z.string(), z.null()]).default(null) })
