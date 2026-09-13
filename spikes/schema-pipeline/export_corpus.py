"""Export the fixture corpus + Python-original acceptance as JSON for the TS harness."""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

SPIKE = Path(__file__).resolve().parent
sys.path.insert(0, str(SPIKE.parents[1]))  # worktree root -> examples
sys.path.insert(0, str(SPIKE))

from harness import accepts, build_corpus  # noqa: E402

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


FULL_MODULE = {m.split(".")[-1]: m for m in ACTOR_MODULES}


def original_model(slug: str) -> type:
    module, cls = slug.rsplit(".", 1)
    if module == "synthetic":
        import synthetic_models

        return getattr(synthetic_models, cls)
    mod = importlib.import_module(FULL_MODULE[module])
    return getattr(mod, cls)


def main() -> None:
    out: dict[str, dict] = {}
    for f in sorted((SPIKE / "schemas").glob("*.json")):
        if f.name.startswith("_"):
            continue
        schema = json.loads(f.read_text())
        model = original_model(f.stem)
        corpus = build_corpus(schema)
        entry = {
            "schema": schema,
            "valid": corpus["valid"],
            "original_accepts_valid": accepts(model, corpus["valid"]),
            "invalid": {},
        }
        for name, fixture in corpus["invalid"].items():
            entry["invalid"][name] = {
                "fixture": fixture,
                "original_accepts": accepts(model, fixture),
            }
        out[f.stem] = entry

    dest = SPIKE / "fixtures" / "corpus.json"
    dest.write_text(json.dumps(out, indent=2) + "\n")
    print(f"wrote {len(out)} corpora -> {dest}")


if __name__ == "__main__":
    main()
