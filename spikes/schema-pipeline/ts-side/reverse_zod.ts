// Part 3b — REVERSE direction: hand-written Zod (v4) schemas -> z.toJSONSchema().
// Documents exactly what Zod drops/changes per feature: refinements, brands,
// defaults, unions, constrained strings/ints.
import { writeFile } from "node:fs/promises";
import { z } from "zod";

// 1. plain object (with default + optional + constrained int)
const Plain = z.object({
  id: z.string(),
  retries: z.number().int().min(0).max(10).default(3),
  note: z.string().optional(),
});

// 2. refined: refinement over a constrained string
const Refined = z
  .object({
    email: z.string().min(3),
    age: z.number().int(),
  })
  .refine((v) => v.age >= 18, { message: "must be adult" })
  .refine((v) => !v.email.startsWith("nobody+"), { message: "no plus aliases" });

// 3. branded
const Branded = z.object({ id: z.string() }).brand<"UserId">();

// bonus probes for the matrix: unions + enum + datetime + nested object
const Union = z.union([z.string(), z.number()]);
const Enumish = z.enum(["low", "medium", "high"]);
const Datish = z.object({ at: z.iso.datetime(), d: z.iso.date() });
const Nested = z.object({
  address: z.object({ street: z.string(), zip_code: z.string().regex(/^\d{5}$/) }),
  tags: z.array(z.string()),
});

const out = {};
const tryJson = (name, schema, opts = {}) => {
  try {
    out[name] = { ok: true, json: z.toJSONSchema(schema, opts) };
  } catch (e) {
    out[name] = { ok: false, error: String(e) };
  }
};

tryJson("plain_output_io", Plain); // default io: "output"
tryJson("plain_input_io", Plain, { io: "input" });
tryJson("refined_output_io", Refined);
tryJson("refined_input_io", Refined, { io: "input" });
tryJson("branded", Branded);
tryJson("union", Union);
tryJson("enum", Enumish);
tryJson("datetime", Datish);
tryJson("nested", Nested);

await writeFile(new URL("./reverse_report.json", import.meta.url), JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify(out, null, 2));
