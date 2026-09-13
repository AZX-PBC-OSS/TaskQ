"""Collect canonical JSON Schema for every actor payload/result model.

Imports the real example actor modules (they import taskq; the @actor
decorators and ratelimit registry.register() calls run at import time but
do not touch the network/DB), then finds every pydantic BaseModel subclass
*defined in* each module and dumps its canonical JSON Schema via
model_json_schema().

Output: one JSON file per model at schemas/<module>.<model>.json
"""

from __future__ import annotations

import importlib
import json
import sys
from pathlib import Path

from pydantic import BaseModel

WORKTREE = Path(__file__).resolve().parents[2]
OUT = Path(__file__).resolve().parent / "schemas"

MODULES = [
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


def main() -> None:
    sys.path.insert(0, str(WORKTREE))
    OUT.mkdir(parents=True, exist_ok=True)
    collected: list[dict[str, str]] = []
    failed: list[str] = []

    for mod_name in MODULES:
        try:
            mod = importlib.import_module(mod_name)
        except Exception as exc:  # noqa: BLE001 - import guard for the spike
            failed.append(f"{mod_name}: {type(exc).__name__}: {exc}")
            continue

        for name, obj in vars(mod).items():
            if not isinstance(obj, type) or not issubclass(obj, BaseModel):
                continue
            # Only models *defined in this module* (skip imported re-exports
            # like CounterPayload imported into batch.py).
            if obj.__module__ != mod_name:
                continue
            schema = obj.model_json_schema()
            slug = f"{mod_name.split('.')[-1]}.{name}"
            (OUT / f"{slug}.json").write_text(json.dumps(schema, indent=2) + "\n")
            collected.append({"module": mod_name, "model": name, "file": f"{slug}.json"})

    (OUT / "_index.json").write_text(json.dumps(collected, indent=2) + "\n")
    print(f"collected {len(collected)} schemas -> {OUT}")
    if failed:
        print("FAILED IMPORTS:")
        for f in failed:
            print(f"  {f}")
        sys.exit(1)


if __name__ == "__main__":
    main()
