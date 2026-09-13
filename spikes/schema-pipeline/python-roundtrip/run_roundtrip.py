"""Part 1 — Python -> codegen -> Python roundtrip conformance.

For every canonical schema:
  1. regenerate a pydantic v2 model with datamodel-code-generator
  2. compare ORIGINAL vs REGENERATED model_json_schema() (defaults,
     required, descriptions, enums, constraints)
  3. run the fixture corpus (valid + proven-invalid mutants) through both
     with TypeAdapter and report acceptance parity.

Outputs: generated/*.py, report.json, report.md
"""

from __future__ import annotations

import importlib
import importlib.util
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

SPIKE = Path(__file__).resolve().parent
SCHEMAS = SPIKE.parent / "schemas"
GENERATED = SPIKE / "generated"
sys.path.insert(0, str(SPIKE.parents[2]))  # worktree root -> `examples` package
sys.path.insert(0, str(SPIKE.parent))  # spike dir -> harness / synthetic_models

from harness import accepts, build_corpus, prop_drifts  # noqa: E402

CODEGEN = ["uvx", "--from", "datamodel-code-generator", "datamodel-codegen"]
CODEGEN_FLAGS = [
    "--input-file-type",
    "jsonschema",
    "--output-model-type",
    "pydantic_v2.BaseModel",
    "--target-python-version",
    "3.12",
]

# slug -> (importable module, class name) for the ORIGINAL models
REGISTRY: dict[str, tuple[str, str]] = {}


def build_registry() -> None:
    for mod in [
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
    ]:
        m = importlib.import_module(mod)
        for name, obj in vars(m).items():
            if isinstance(obj, type) and obj.__module__ == mod and hasattr(obj, "model_json_schema"):
                REGISTRY[f"{mod.split('.')[-1]}.{name}"] = (mod, name)
    import synthetic_models

    for model in synthetic_models.MODELS:
        REGISTRY[f"synthetic.{model.__name__}"] = ("synthetic_models", model.__name__)


def run_codegen(out_dir: Path, extra_flags: list[str]) -> dict[str, str]:
    """Returns slug -> stderr ("" on success)."""
    out_dir.mkdir(exist_ok=True)
    errors = {}
    for f in sorted(SCHEMAS.glob("*.json")):
        if f.name.startswith("_"):
            continue
        out = out_dir / (f.stem.replace(".", "_") + ".py")
        cmd = CODEGEN + ["--input", str(f), "--output", str(out)] + CODEGEN_FLAGS + extra_flags
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=120)  # noqa: S603
        if r.returncode != 0:
            errors[f.stem] = r.stderr[-2000:]
    return errors


def load_model_class(py_file: Path, class_name: str) -> type:
    mod_name = "gen_" + py_file.stem
    spec = importlib.util.spec_from_file_location(mod_name, py_file)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[mod_name] = mod
    spec.loader.exec_module(mod)
    return getattr(mod, class_name)


def evaluate(out_dir: Path, extra_flags: list[str], label: str) -> dict[str, Any]:
    codegen_errors = run_codegen(out_dir, extra_flags)

    report: dict[str, Any] = {"label": label, "codegen_errors": codegen_errors, "schemas": {}}
    for f in sorted(SCHEMAS.glob("*.json")):
        if f.name.startswith("_"):
            continue
        slug = f.stem
        entry: dict[str, Any] = {"slug": slug}

        py_file = out_dir / (slug.replace(".", "_") + ".py")
        if slug in codegen_errors:
            entry["codegen"] = "FAIL"
            report["schemas"][slug] = entry
            continue

        orig_schema = json.loads(f.read_text())
        mod_name, class_name = REGISTRY[slug]
        orig_model = getattr(importlib.import_module(mod_name), class_name)

        regen_model = load_model_class(py_file, orig_schema["title"])
        regen_schema = regen_model.model_json_schema()

        entry["schema_drifts"] = prop_drifts(orig_schema, regen_schema)

        corpus = build_corpus(orig_schema)

        # sanity: valid fixture must satisfy the canonical schema and the original model
        orig_valid = accepts(orig_model, corpus["valid"])
        regen_valid = accepts(regen_model, corpus["valid"])

        mutants = []
        for mname, mfix in corpus["invalid"].items():
            oa = accepts(orig_model, mfix)
            ra = accepts(regen_model, mfix)
            mutants.append(
                {
                    "mutant": mname,
                    "original_accepted": oa,
                    "regen_accepted": ra,
                    "parity": oa == ra,
                }
            )
        parity = all(m["parity"] for m in mutants)
        entry.update(
            {
                "orig_accepts_valid": orig_valid,
                "regen_accepts_valid": regen_valid,
                "mutants": mutants,
                "rejection_parity": parity,
            }
        )
        report["schemas"][slug] = entry
    return report


def summarize(report: dict[str, Any]) -> list[str]:
    lines = [f"# Python roundtrip report — {report['label']}", ""]
    n_ok = 0
    for slug, e in report["schemas"].items():
        if e.get("codegen") == "FAIL":
            lines.append(f"- **{slug}**: codegen FAIL")
            continue
        drifts = e["schema_drifts"]
        status = "OK" if not drifts and e["rejection_parity"] and e["regen_accepts_valid"] else "DRIFT"
        if status == "OK":
            n_ok += 1
        lines.append(f"- **{slug}**: {status}")
        for d in drifts:
            lines.append(f"  - {d}")
        if not e["rejection_parity"]:
            for m in e["mutants"]:
                if not m["parity"]:
                    lines.append(
                        f"  - acceptance parity FAIL on {m['mutant']}: "
                        f"original {'accepts' if m['original_accepted'] else 'rejects'}, "
                        f"regen {'accepts' if m['regen_accepted'] else 'rejects'}"
                    )
        if not e["orig_accepts_valid"]:
            lines.append("  - WARNING: original model REJECTED the generated valid fixture")
        elif not e["regen_accepts_valid"]:
            lines.append("  - regenerated model REJECTED the valid fixture")
    lines.insert(1, f"\n{n_ok}/{len(report['schemas'])} schemas fully conformant.\n")
    return lines


def main() -> None:
    build_registry()
    baseline = evaluate(GENERATED, [], "baseline (default flags)")
    (SPIKE / "report.json").write_text(json.dumps(baseline, indent=2) + "\n")

    strict = evaluate(SPIKE / "generated_strict", ["--strict-nullable"], "--strict-nullable")
    (SPIKE / "report_strict.json").write_text(json.dumps(strict, indent=2) + "\n")

    out = summarize(baseline) + ["", "---", ""] + summarize(strict)
    (SPIKE / "report.md").write_text("\n".join(out) + "\n")
    print("\n".join(out))


if __name__ == "__main__":
    main()
