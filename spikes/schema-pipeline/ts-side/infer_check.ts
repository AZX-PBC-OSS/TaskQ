// Part 3c POSITIVE probe — must typecheck under --strict.
// Proves json-schema-to-zod output carries precise, statically inferable types.
import { z } from "zod";
import { BatchCounterPayload } from "./generated_ts/batch_BatchCounterPayload.ts";
import { WordCountResult } from "./generated_ts/sync_demo_WordCountResult.ts";
import { NestedPayload } from "./generated_ts/synthetic_NestedPayload.ts";

// defaults -> optional on input, required on output
const counterIn: z.input<typeof BatchCounterPayload> = { n: 42 };
const counterOut: z.output<typeof BatchCounterPayload> = { n: 42, steps: 5 };
const nNum: number = counterOut.n;
const stepsNum: number = counterOut.steps;
const nOptional: number | undefined = counterIn.n;

// required + nullable anyOf -> string | null union survives
const wc: z.infer<typeof WordCountResult> = {
  word_count: 1,
  char_count: 2,
  processed_at: null,
};
const processed: string | null = wc.processed_at;

// the $ref-collapsed field: z.any() infers any — evidence for the README
const nested: z.infer<typeof NestedPayload> = { address: { whatever: true }, tags: ["a"] };
const addressIsAny: any = nested.address;

console.log(counterIn, counterOut, nNum, stepsNum, nOptional, wc, processed, nested, addressIsAny);
