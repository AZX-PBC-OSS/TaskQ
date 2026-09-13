import { z } from "zod"

export const BatchCounterPayload = z.object({ "n": z.number().int().gte(1).lte(1000).describe("Number of child jobs").default(5), "steps": z.number().int().gte(1).lte(1000).describe("Steps per child").default(5) })
