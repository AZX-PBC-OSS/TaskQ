// Mitigation check for the json-schema-to-zod local-$ref collapse:
// inline $defs/$refs BEFORE conversion, then confirm the generated Zod
// schema validates enums and nested models correctly (parity restored).
import { readFile, writeFile } from "node:fs/promises";
import { jsonSchemaToZod } from "json-schema-to-zod";
import { z } from "zod";

function deref(node, defs) {
  if (Array.isArray(node)) return node.map((n) => deref(n, defs));
  if (node === null || typeof node !== "object") return node;
  if (typeof node.$ref === "string" && node.$ref.startsWith("#/$defs/")) {
    const name = node.$ref.replace("#/$defs/", "");
    return deref(defs[name], defs);
  }
  return Object.fromEntries(
    Object.entries(node).map(([k, v]) => [k, deref(v, defs)]),
  );
}

const corpus = JSON.parse(
  await readFile(new URL("../fixtures/corpus.json", import.meta.url), "utf8"),
);

const out = {};
for (const slug of ["synthetic.EnumPayload", "synthetic.NestedPayload"]) {
  const raw = corpus[slug].schema;
  const inlined = deref(raw, raw.$defs ?? {});
  delete inlined.$defs;
  const code = jsonSchemaToZod(inlined, { module: "esm", name: "Deref" });
  // execute the generated code via a temp module file (unique path: ESM cache)
  const tmp = new URL(`./_tmp_deref_${slug.replace(".", "_")}.mjs`, import.meta.url);
  await writeFile(tmp, code);
  const mod = await import(tmp.href);
  const schema = mod.Deref;

  const fixtures = [
    ["valid", corpus[slug].valid, corpus[slug].original_accepts_valid],
    ...Object.entries(corpus[slug].invalid).map(([n, f]) => [n, f.fixture, f.original_accepts]),
  ];
  let mismatches = 0;
  const detail = {};
  for (const [name, fixture, py] of fixtures) {
    const ts = schema.safeParse(fixture).success;
    const parity = ts === py;
    if (!parity) mismatches++;
    detail[name] = { ts_accepts: ts, python_accepts: py, parity };
  }
  out[slug] = { generated: code.trim(), mismatches, detail };
}
await writeFile(new URL("./deref_mitigation_report.json", import.meta.url), JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify(out, null, 2));
