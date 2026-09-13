# Schema Pipeline Conformance Spike

Answers: *what survives the round trips* for the planned TaskQ schema pipeline —
pydantic v2 models → canonical JSON Schema → (a) Python codegen,
(b) one aggregate OpenAPI 3.1 doc, (c) TypeScript/Zod.

Everything here was executed for real. Reproduce with:

```bash
uv sync                                   # project venv (taskq importable)
uv run python spikes/schema-pipeline/collect_schemas.py            # 29 real schemas from examples/actors/
uv run python spikes/schema-pipeline/synthetic_models.py            # + 5 synthetic (features absent from examples)
uv run python spikes/schema-pipeline/export_corpus.py               # fixture corpus + Python acceptance
uv run python spikes/schema-pipeline/python-roundtrip/run_roundtrip.py
uv run python spikes/schema-pipeline/openapi-aggregate/build_openapi.py
cd spikes/schema-pipeline/ts-side && npm install && node generate_zod.mjs \
  && node harness_ts.mjs && node reverse_zod.ts && node deref_mitigation.mjs \
  && npx tsc --ignoreConfig --strict --noEmit --target es2022 --module esnext \
       --moduleResolution bundler --skipLibCheck --allowImportingTsExtensions infer_check.ts
```

## Corpus

34 schemas = 29 real (every `BaseModel` payload/result in `examples/actors/*.py`,
collected via import + `model_json_schema()`) + 5 synthetic
(`synthetic_models.py`: enum, nested/`$defs`, union, date/datetime, constrained
strings — features the example actors don't use yet).

Fixtures per schema: 1 valid instance generated from the canonical schema
(proven valid with the `jsonschema` Draft 2020-12 validator) + deterministic
mutants, each proven **invalid** against the canonical schema before use
(explicit-null, type flip, min/max violation, missing required, enum violation,
bad uuid format, garbage nested object). Acceptance comparison uses
`pydantic.TypeAdapter` on the Python side and `safeParse` on the TS side
against the *same* JSON fixtures (`fixtures/corpus.json`).

## Conformance matrix

OK = executed check passed · DRIFT = semantics changed · FAIL = validation lost

| feature (source) | Python → codegen → Python (datamodel-codegen, default flags) | same, `--strict-nullable` | Python → OpenAPI 3.1 → codegen (aggregate) | JSON Schema → Zod (json-schema-to-zod 2.8.1) | Zod 4 → JSON Schema (`z.toJSONSchema`) |
|---|---|---|---|---|---|
| defaults, plain (`n: int = 10`) | **DRIFT** — every defaulted field becomes `X \| None` (accepts explicit `null`); proven by null-mutant acceptance asymmetry | OK | same DRIFT → OK with `--strict-nullable` | OK (`z.number().int().gte(1).default(5)`) | OK (default kept; `io:"input"` vs `"output"` changes whether defaulted fields are `required`) |
| optional (`str \| None = None`) | OK (`anyOf [T, null]`) | OK | OK | OK (`.nullable()`) | OK (`anyOf`/type list) |
| required | OK | OK | OK | OK | OK |
| lists (`list[str]`) | OK | OK | OK | OK (`.array()`) | OK |
| nested models (`$defs`/`$ref`, synthetic) | OK (`Address` regenerated, constraints kept) | OK | OK (promoted to `components/schemas`, refs rewritten) | **FAIL — local `$ref` silently collapses to `z.any()`**: accepts `null`, missing, garbage objects | OK (plain nested objects; cycles/reuse unprobed) |
| enums (str-Enum, synthetic) | OK | OK | OK | **FAIL — same `z.any()` collapse** (pydantic emits Enum fields as `$ref → $defs`; all 3 mutants accepted) | OK (`enum` kept) |
| unions (`int \| str`, synthetic) | OK (`anyOf`) | OK | OK | OK (`.union()`) | OK — emitted as `type: ["string","number"]`, not `anyOf` |
| date / datetime (synthetic) | OK (`date` / `AwareDatetime`); format preserved | OK for plain defaults; **DRIFT for `default_factory` fields** (see below) | same | OK (`z.string().datetime({offset:true})` / `.date()`) | OK format + **adds a strict Z-only RFC3339 regex** — `+01:00`-style offsets would fail downstream pattern checks |
| constrained ints (`ge/le`) | constraints preserved as `conint(ge,le)` but buried in `anyOf`; field nullable | OK | OK | OK (`.int().gte().lte()`) | OK (`minimum`/`maximum`; ±MAX_SAFE_INTEGER bounds added for plain `.int()`) |
| constrained strings (`min/max/pattern`, synthetic) | OK | OK | OK | OK (`.min().max().regex()`) | OK (`minLength`/`maxLength`/`pattern`) |
| `format: uuid` | OK (`uuid.UUID`) | OK | OK | OK (`z.string().uuid()`) | not probed |
| empty models (`EmptyPayload`) | OK | OK | OK | OK (`z.object({})`) | n/a (degenerate) |
| `default_factory` (synthetic: `started_at`, `tags`) | **DRIFT** even with `--strict-nullable` — canonical JSON Schema cannot express a factory default; regen emits `X \| None = None` (accepts explicit `null`) | same DRIFT | same DRIFT | OK | OK (but Zod-side default is a *value*, not a factory) |

Scoreboard (all executed): Python→codegen→Python: **13/34** baseline,
**32/34** `--strict-nullable` · OpenAPI aggregate route: **13/34** baseline,
**32/34** `--strict-nullable` · JSON Schema→Zod: **7/9** representatives raw,
**9/9 after the deref mitigation below** · Zod→JSON Schema: all 7 probes
executed, 2 hazardous behaviors documented above.

## Verdicts

### (a) FORBIDDEN schema features for v1 (lint rules)

1. **`default_factory` in actor payload/result models.** The canonical JSON
   Schema layer cannot represent it, and it is the *only* feature that still
   drifts with `--strict-nullable` on both Python routes. Lint: reject
   `Field(default_factory=...)` in models registered as actor payload/result
   (require a plain default or required field).
2. **Un-annotated optional widening is unavoidable — pin `--strict-nullable`.**
   Not a model feature but a CI rule: run datamodel-codegen with
   `--strict-nullable` everywhere; without it every defaulted field silently
   becomes nullable (`13/34 → 32/34`).
3. **TS boundary must deref `$defs` before `json-schema-to-zod`** (build-step
   rule, verified in `ts-side/deref_mitigation.mjs`: enum + nested parity
   restored to 0 mismatches). Caveat found: a naive deref must *merge sibling
   keys* (`default` etc.) onto the inlined branch — naive replacement dropped
   `default: "medium"`. Everything else (enums, UUID, constrained ints/strings,
   unions, lists, nested models, datetime/date, optional/required, empty
   models) survives with those two mitigations — no further feature bans needed.

### (b) Does the aggregate OpenAPI approach work end-to-end?

**Yes.** 28 actor paths (POST `/actors/{name}`), 35 component schemas
(collision rename: two distinct `EmptyPayload` classes →
`advanced_EmptyPayload` / `ratelimit_EmptyPayload`; shared models deduped:
`TaggedPayload` referenced by 2 actors; `$defs` promoted + refs rewritten),
`openapi-spec-validator`: **VALID** (incl. `{"type": "null"}` responses for
`-> None` actors), datamodel-codegen `--input-file-type openapi`: rc=0, all
classes generated, and the acceptance profile is *identical* to the
per-schema JSON Schema route (13/34 baseline → 32/34 `--strict-nullable`).
The aggregate doc is a viable single artifact for storage/export/codegen.

### (c) Tool bugs / findings (issue-worthy)

1. **datamodel-code-generator renamed its entrypoint.** The PyPI package
   `datamodel-code-generator` no longer ships a `datamodel-code-generator`
   binary; `uvx datamodel-code-generator` fails outright ("An executable named
   `datamodel-code-generator` is not provided") — it is now
   `uvx --from datamodel-code-generator datamodel-codegen`. Also there is **no
   `--input-model-type` flag** (old docs/blog posts reference it); the current
   flags are `--input-file-type jsonschema` + `--output-model-type
   pydantic_v2.BaseModel`.
2. **json-schema-to-zod silently collapses local `$ref` → `z.any()`.** Any
   `$ref: "#/$defs/X"` inside a property (pydantic emits *both* nested models
   and Enum classes this way) becomes `z.any()` with no warning — nested-model
   and enum validation are lost entirely (mutants accepted). Either resolve
   refs before conversion or error loudly. (See `ts-side/harness_ts.mjs`
   mismatches + `deref_mitigation_report.json` for the verified workaround.)
3. **Zod `z.toJSONSchema` adds a Z-suffix-only RFC3339 regex** next to
   `format: date-time` — a schema-valid datetime with a numeric UTC offset
   (`+01:00`) fails the emitted `pattern`. Also: `additionalProperties: false`
   is emitted while `z.object` runtime *strips* unknown keys (schema claims
   stricter than runtime); `io: "input"` vs `"output"` changes `required` for
   defaulted fields. Pin expectations in tests if adopting.
4. **Pydantic `default_factory` is invisible in canonical JSON Schema** —
   upstream pydantic limitation that motivates rule (a)1 above.

## Artifact map

- `schemas/` — canonical JSON Schema (29 real + 5 synthetic) + `_index.json`
- `fixtures/corpus.json` — valid instance + proven-invalid mutants + Python
  acceptance flags per schema
- `harness.py` — instance generator, mutant factory, TypeAdapter acceptance
- `python-roundtrip/` — `run_roundtrip.py`, `generated/` (baseline),
  `generated_strict/`, `report.json` / `report_strict.json` / `report.md`
- `openapi-aggregate/` — `build_openapi.py`, `openapi.json`,
  `component_map.json`, `generated_from_openapi*.py`,
  `openapi_codegen_report*.json`
- `ts-side/` — `generate_zod.mjs`, `generated/` + `generated_ts/`,
  `harness_ts.mjs` + `ts_result.json`, `reverse_zod.ts` +
  `reverse_report.json`, `deref_mitigation.mjs` +
  `deref_mitigation_report.json`, `infer_check.ts` (passes `tsc --strict`),
  `infer_check_negative.ts` (fails with the 4 expected type errors — proving
  generated Zod schemas expose precise, statically inferable types)
