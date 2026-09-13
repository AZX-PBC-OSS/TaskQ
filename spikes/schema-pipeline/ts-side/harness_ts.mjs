// Part 3a — parse the SAME fixture corpus through the generated Zod schemas
// and compare acceptance with the Python originals (recorded in corpus.json).
import { readFile, writeFile } from "node:fs/promises";
import { z } from "zod";
import { BatchCounterPayload } from "./generated/batch_BatchCounterPayload.mjs";
import { WordCountResult } from "./generated/sync_demo_WordCountResult.mjs";
import { NestedPayload } from "./generated/synthetic_NestedPayload.mjs";
import { DatetimePayload } from "./generated/synthetic_DatetimePayload.mjs";
import { ConstrainedStringPayload } from "./generated/synthetic_ConstrainedStringPayload.mjs";
import { BatchFinalizerPayload } from "./generated/batch_BatchFinalizerPayload.mjs";
import { UnionPayload } from "./generated/synthetic_UnionPayload.mjs";
import { EnumPayload } from "./generated/synthetic_EnumPayload.mjs";
import { EmptyPayload } from "./generated/advanced_EmptyPayload.mjs";

const SCHEMAS = {
  "batch.BatchCounterPayload": BatchCounterPayload,
  "sync_demo.WordCountResult": WordCountResult,
  "synthetic.NestedPayload": NestedPayload,
  "synthetic.DatetimePayload": DatetimePayload,
  "synthetic.ConstrainedStringPayload": ConstrainedStringPayload,
  "batch.BatchFinalizerPayload": BatchFinalizerPayload,
  "synthetic.UnionPayload": UnionPayload,
  "synthetic.EnumPayload": EnumPayload,
  "advanced.EmptyPayload": EmptyPayload,
};

const corpus = JSON.parse(await readFile(new URL("../fixtures/corpus.json", import.meta.url), "utf8"));

const results = { zodVersion: z.coerce ? "v4" : "v3", schemas: {} };
let mismatches = 0;

for (const [slug, schemaZod] of Object.entries(SCHEMAS)) {
  const c = corpus[slug];
  const entry = { valid_fixture: {}, invalid: {} };

  const vOk = schemaZod.safeParse(c.valid).success;
  entry.valid_fixture = {
    ts_accepts: vOk,
    python_accepts: c.original_accepts_valid,
    parity: vOk === c.original_accepts_valid,
  };

  for (const [name, { fixture, original_accepts }] of Object.entries(c.invalid)) {
    const tsOk = schemaZod.safeParse(fixture).success;
    const parity = tsOk === original_accepts;
    if (!parity) mismatches++;
    entry.invalid[name] = { ts_accepts: tsOk, python_accepts: original_accepts, parity };
  }
  results.schemas[slug] = entry;
}

results.total_acceptance_mismatches = mismatches;
await writeFile(new URL("./ts_result.json", import.meta.url), JSON.stringify(results, null, 2) + "\n");

for (const [slug, e] of Object.entries(results.schemas)) {
  const bad = [
    ...(!e.valid_fixture.parity ? ["valid_fixture"] : []),
    ...Object.entries(e.invalid).filter(([, v]) => !v.parity).map(([k]) => k),
  ];
  console.log(`${slug}: ${bad.length === 0 ? "OK" : "MISMATCH " + bad.join(",")}`);
}
console.log(`total acceptance mismatches: ${mismatches}`);
