"""Part 2 — aggregate ALL actor schemas into ONE OpenAPI 3.1 document.

- Each real actor becomes POST /actors/{name}: payload schema as requestBody,
  result schema as the 200 response (None-returning actors -> {"type": "null"}).
- Synthetic feature-coverage models ship as standalone component schemas
  (x-synthetic: true) so $defs promotion / collision rename is exercised.
- $defs of nested models are promoted to components/schemas, refs rewritten.
- Component-name collisions (e.g. two EmptyPayload classes) are renamed with
  the defining module as prefix; the slug->component mapping is exported for
  the codegen acceptance check.

Outputs: openapi.json, component_map.json, validation.txt, generated_from_openapi.py,
         openapi_codegen_report.json
"""

from __future__ import annotations

import importlib
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

SPIKE = Path(__file__).resolve().parent
sys.path.insert(0, str(SPIKE.parents[2]))  # worktree root -> examples
sys.path.insert(0, str(SPIKE.parent))  # spike dir -> harness / synthetic_models

from harness import accepts, build_corpus, prop_drifts  # noqa: E402

from taskq import ActorRef  # noqa: E402

OUT_DOC = SPIKE / "openapi.json"
COMPONENT_MAP = SPIKE / "component_map.json"

ACTOR_MODULES = [
    "examples.actors.advanced",
    "examples.actors.basic",
    "examples.actors.batch",
    "examples.actors.chained",
    "examples.actors.di",
    "examples.actors.failure",
    "examples.actors.progress",
    "examples.actors.ratelimit",
    "examples.actors.realworld",
    "examples.actors.sync_demo",
    "examples.actors.tags_demo",
    "examples.actors.ticker",
]


def collect_actors() -> dict[str, ActorRef[Any, Any]]:
    actors: dict[str, ActorRef[Any, Any]] = {}
    for mod_name in ACTOR_MODULES:
        mod = importlib.import_module(mod_name)
        for obj in vars(mod).values():
            if isinstance(obj, ActorRef):
                actors[obj.name] = obj
    return dict(sorted(actors.items()))


def register_component(
    components: dict[str, Any],
    schema: dict[str, Any],
    base_name: str,
    module_slug: str,
    slug: str,
    mapping: dict[str, str],
    synthetic: bool = False,
) -> str:
    """Add a schema (and its $defs) to components; returns component name."""
    # promote $defs first
    for def_name, def_schema in (schema.get("$defs") or {}).items():
        target = def_name
        if target in components["schemas"] and components["schemas"][target] != def_schema:
            target = f"{module_slug}_{def_name}"
        components["schemas"][target] = def_schema
        mapping[f"{slug}#{def_name}"] = target
        schema = _rewrite_refs(schema, def_name, target)
    schema.pop("$defs", None)

    name = base_name
    if name in components["schemas"] and components["schemas"][name] != schema:
        name = f"{module_slug}_{base_name}"  # collision rename
    components["schemas"][name] = dict(schema)
    if synthetic:
        components["schemas"][name]["x-synthetic"] = True
    mapping[slug] = name
    return name


def _rewrite_refs(node: Any, old: str, new: str) -> Any:
    if isinstance(node, dict):
        out = {}
        for k, v in node.items():
            if k == "$ref" and v == f"#/$defs/{old}":
                out[k] = f"#/components/schemas/{new}"
            else:
                out[k] = _rewrite_refs(v, old, new)
        return out
    if isinstance(node, list):
        return [_rewrite_refs(x, old, new) for x in node]
    return node


def build() -> tuple[dict[str, Any], dict[str, str]]:
    actors = collect_actors()
    components: dict[str, Any] = {"schemas": {}}
    mapping: dict[str, str] = {}
    paths: dict[str, Any] = {}

    for ref in actors.values():
        payload_model = ref.payload_type
        module_slug = payload_model.__module__.split(".")[-1]
        result_schema = ref.result_adapter.json_schema()

        payload_component = register_component(
            components, payload_model.model_json_schema(), payload_model.__name__, module_slug,
            f"{module_slug}.{payload_model.__name__}", mapping,
        )
        result_component: str | None = None
        if result_schema.get("type") != "null":
            title = result_schema.get("title", "Result")
            # map the result component back to its canonical slug (file stem)
            canon = next(
                (f.stem for f in (SPIKE.parent / "schemas").glob(f"*.{title}.json")), None
            )
            result_component = register_component(
                components, result_schema, title, title,
                canon or f"result.{title}", mapping,
            )

        path = f"/actors/{ref.name}"
        operation: dict[str, Any] = {
            "operationId": f"enqueue_{ref.name}",
            "summary": ref.fn.__doc__.strip().splitlines()[0] if ref.fn.__doc__ else ref.name,
            "requestBody": {
                "required": True,
                "content": {
                    "application/json": {
                        "schema": {"$ref": f"#/components/schemas/{payload_component}"}
                    }
                },
            },
            "responses": {
                "200": {
                    "description": "Job enqueued",
                    "content": {"application/json": {}},
                }
            },
        }
        if result_component:
            operation["responses"]["200"]["content"]["application/json"]["schema"] = {
                "$ref": f"#/components/schemas/{result_component}"
            }
        else:
            operation["responses"]["200"]["content"]["application/json"]["schema"] = {
                "type": "null"
            }
        paths[path] = {"post": operation}

    # synthetic models as standalone components ($defs promotion + x-synthetic)
    import synthetic_models

    for model in synthetic_models.MODELS:
        register_component(
            components, model.model_json_schema(), model.__name__, "synthetic",
            f"synthetic.{model.__name__}", mapping, synthetic=True,
        )

    doc = {
        "openapi": "3.1.0",
        "info": {
            "title": "TaskQ actors (spike aggregate)",
            "version": "0.0.0-spike",
            "description": "Aggregate actor payload/result schemas for the schema-pipeline spike.",
        },
        "paths": paths,
        "components": components,
    }
    return doc, mapping


def main() -> None:
    doc, mapping = build()
    OUT_DOC.write_text(json.dumps(doc, indent=2) + "\n")
    COMPONENT_MAP.write_text(json.dumps(mapping, indent=2) + "\n")
    n_schemas = len(doc["components"]["schemas"])
    n_paths = len(doc["paths"])
    print(f"openapi.json: {n_paths} paths, {n_schemas} component schemas, "
          f"{sum(1 for k in mapping if not k.startswith('result.') and '#' not in k)} payload+synthetic components")

    # validate
    try:
        from openapi_spec_validator import validate as validate_doc

        validate_doc(doc)
        print("openapi-spec-validator: VALID")
        validation = "VALID"
    except Exception as exc:  # noqa: BLE001
        validation = f"INVALID: {exc}"
        print(f"openapi-spec-validator: {validation}")

    # codegen against the aggregate (baseline + --strict-nullable)
    sys.path.insert(0, str(SPIKE))
    report = None
    report_strict = None
    for label, gen_name, extra in [
        ("baseline", "generated_from_openapi.py", []),
        ("strict-nullable", "generated_from_openapi_strict.py", ["--strict-nullable"]),
    ]:
        gen = SPIKE / gen_name
        cmd = [
            "uvx", "--from", "datamodel-code-generator", "datamodel-codegen",
            "--input", str(OUT_DOC), "--input-file-type", "openapi",
            "--output-model-type", "pydantic_v2.BaseModel",
            "--target-python-version", "3.12", "--output", str(gen),
        ] + extra
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=300)  # noqa: S603
        print(f"aggregate codegen [{label}]: rc={r.returncode}")
        if r.returncode != 0:
            (SPIKE / f"codegen_stderr_{label}.txt").write_text(r.stderr)
            print(r.stderr[-1500:])
            rep = {"label": label, "codegen_rc": r.returncode, "schemas": {}}
        else:
            spec = importlib.util.spec_from_file_location(f"gen_from_openapi_{label}", gen)
            assert spec and spec.loader
            gen_mod = importlib.util.module_from_spec(spec)
            sys.modules[f"gen_from_openapi_{label}"] = gen_mod
            spec.loader.exec_module(gen_mod)

            SCHEMAS = SPIKE.parent / "schemas"
            rep = {"label": label, "codegen_rc": r.returncode, "schemas": {}}
            for f in sorted(SCHEMAS.glob("*.json")):
                if f.name.startswith("_"):
                    continue
                slug = f.stem
                canonical = json.loads(f.read_text())
                component_name = mapping.get(slug)
                if component_name is None or not hasattr(gen_mod, component_name):
                    rep["schemas"][slug] = {
                        "component": component_name, "note": "class not generated",
                    }
                    continue
                regen = getattr(gen_mod, component_name)
                regen_schema = regen.model_json_schema()

                corpus = build_corpus(canonical)
                orig_model = _original_model(slug)
                parity = []
                for mname, mfix in corpus["invalid"].items():
                    oa = accepts(orig_model, mfix)
                    ra = accepts(regen, mfix)
                    if oa != ra:
                        parity.append(
                            {"mutant": mname, "original_accepted": oa, "regen_accepted": ra}
                        )
                rep["schemas"][slug] = {
                    "component": component_name,
                    "schema_drifts": prop_drifts(canonical, regen_schema),
                    "regen_accepts_valid": accepts(regen, corpus["valid"]),
                    "parity_failures": parity,
                }
        if label == "baseline":
            report = rep
        else:
            report_strict = rep

    (SPIKE / "openapi_codegen_report.json").write_text(json.dumps(report, indent=2) + "\n")
    (SPIKE / "openapi_codegen_report_strict.json").write_text(
        json.dumps(report_strict, indent=2) + "\n"
    )

    for rep in (report, report_strict):  # type: ignore[arg-type]
        n_ok = sum(
            1 for e in rep["schemas"].values()  # type: ignore[union-attr]
            if e.get("component") and not e.get("schema_drifts")
            and e.get("regen_accepts_valid") and not e.get("parity_failures")
        )
        print(f"aggregate codegen acceptance [{rep['label']}]: {n_ok}/{len(rep['schemas'])} conformant")  # type: ignore[arg-type]
        for slug, e in rep["schemas"].items():  # type: ignore[union-attr]
            if not e.get("component"):
                print(f"  {slug}: {e.get('note')}")
            elif e.get("schema_drifts") or e.get("parity_failures") or not e.get("regen_accepts_valid"):
                print(f"  {slug} ({e['component']}): DRIFT {e.get('schema_drifts', '')} "
                      f"parity {e.get('parity_failures', '')}")


_ORIG_MODELS: dict[str, type] | None = None


def _original_model(slug: str) -> type:
    global _ORIG_MODELS
    if _ORIG_MODELS is None:
        _ORIG_MODELS = {}
        for mod_name in ACTOR_MODULES:
            m = importlib.import_module(mod_name)
            for name, obj in vars(m).items():
                if isinstance(obj, type) and obj.__module__ == mod_name and hasattr(obj, "model_json_schema"):
                    _ORIG_MODELS[f"{mod_name.split('.')[-1]}.{name}"] = obj
        import synthetic_models as sm

        for model in sm.MODELS:
            _ORIG_MODELS[f"synthetic.{model.__name__}"] = model
    return _ORIG_MODELS[slug]


if __name__ == "__main__":
    main()
