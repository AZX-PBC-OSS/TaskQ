// Part 3a setup — convert 3 representative canonical schemas to Zod via
// json-schema-to-zod. Emits each module twice: generated/*.mjs (runtime
// harness) and generated_ts/*.ts (tsc type-inference check).
import { mkdir, writeFile } from "node:fs/promises";
import { jsonSchemaToZod } from "json-schema-to-zod";

const REPRESENTATIVES = [
  ["batch.BatchCounterPayload", "BatchCounterPayload"], // defaults + constrained ints
  ["sync_demo.WordCountResult", "WordCountResult"], // required + optional/nullable
  ["synthetic.NestedPayload", "NestedPayload"], // $defs/$ref nesting + list
  ["synthetic.DatetimePayload", "DatetimePayload"], // date/date-time formats
  ["synthetic.ConstrainedStringPayload", "ConstrainedStringPayload"], // pattern/min/max
  ["batch.BatchFinalizerPayload", "BatchFinalizerPayload"], // format: uuid + required
  ["synthetic.UnionPayload", "UnionPayload"], // int|str union + optional float
  ["synthetic.EnumPayload", "EnumPayload"], // str enum + default
  ["advanced.EmptyPayload", "EmptyPayload"], // degenerate empty object
];

const corpus = JSON.parse(await readFile());

async function readFile() {
  const { readFile: rf } = await import("node:fs/promises");
  return rf(new URL("../fixtures/corpus.json", import.meta.url), "utf8");
}

await mkdir(new URL("./generated/", import.meta.url), { recursive: true });
await mkdir(new URL("./generated_ts/", import.meta.url), { recursive: true });

for (const [slug, cls] of REPRESENTATIVES) {
  const schema = corpus[slug].schema;
  const code = jsonSchemaToZod(schema, { module: "esm", name: cls });
  await writeFile(new URL(`./generated/${slug.replace(".", "_")}.mjs`, import.meta.url), code);
  await writeFile(new URL(`./generated_ts/${slug.replace(".", "_")}.ts`, import.meta.url), code);
  console.log(`converted ${slug}`);
}
