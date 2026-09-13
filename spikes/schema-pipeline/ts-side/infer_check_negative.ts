// Part 3c NEGATIVE probe — must FAIL typecheck. Do not fix the errors:
// their existence proves the generated Zod schemas expose precise static
// types (wrong types / missing required fields are rejected at compile time).
import { z } from "zod";
import { BatchCounterPayload } from "./generated_ts/batch_BatchCounterPayload.ts";
import { WordCountResult } from "./generated_ts/sync_demo_WordCountResult.ts";

// wrong primitive type for n
const bad1: z.output<typeof BatchCounterPayload> = { n: "not-a-number", steps: 5 };

// output type requires both fields (defaults fill at parse time, not in types)
const bad2: z.output<typeof BatchCounterPayload> = { n: 1 };

// word_count is required
const bad3: z.infer<typeof WordCountResult> = { char_count: 2, processed_at: null };

// processed_at is string | null, not number
const bad4: z.infer<typeof WordCountResult> = {
  word_count: 1,
  char_count: 2,
  processed_at: 42,
};

console.log(bad1, bad2, bad3, bad4);
