"""Codegen consumer: actor definitions are the source of truth for Python types.

Reads manifest.json (dumped by the runtime from `defineActor` metadata +
`z.toJSONSchema`), builds one aggregate JSON Schema document with named
definitions per actor payload/result, and emits Pydantic models via
datamodel-code-generator (uvx).

Usage (from the repo root):
    uv run python spikes/build-pipeline/codegen.py

The `--strict-nullable` flag is pinned deliberately: spike-schema-pipeline
proved that without it datamodel-codegen widens every defaulted field to
`X | None` (13/34 round-trip conformance -> 32/34 with the flag).
"""

import json
import subprocess
from pathlib import Path
from urllib.request import urlopen

HERE = Path(__file__).parent
MANIFEST = HERE / "manifest.json"
SCHEMA_OUT = HERE / "schemas.generated.json"
MODELS_OUT = HERE / "models_generated.py"


def kebab_to_pascal(name: str) -> str:
    return "".join(part.capitalize() for part in name.split("-"))


def main() -> int:
    manifest = json.loads(MANIFEST.read_text())
    print(f"manifest: {len(manifest['actors'])} actors from {manifest['runtime']['entrypoint']}")

    defs: dict = {}
    for actor in manifest["actors"]:
        for kind, schema in (
            ("payload", actor["payloadJsonSchema"]),
            ("result", actor["resultJsonSchema"]),
        ):
            # Name definitions after the actor so generated class names are
            # self-describing: send-welcome-email/payload -> SendWelcomeEmailPayload
            defs[f"{kebab_to_pascal(actor['name'])}{kind.capitalize()}"] = schema

    aggregate = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$defs": defs,
    }
    SCHEMA_OUT.write_text(json.dumps(aggregate, indent=2))
    print(f"wrote {SCHEMA_OUT.name}: {len(defs)} definitions")

    cmd = [
        "uvx",
        "--from",
        "datamodel-code-generator",
        "datamodel-codegen",
        "--input",
        str(SCHEMA_OUT),
        "--input-file-type",
        "jsonschema",
        "--output",
        str(MODELS_OUT),
        "--output-model-type",
        "pydantic_v2.BaseModel",
        "--strict-nullable",
        "--disable-timestamp",
    ]
    print(f"running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"wrote {MODELS_OUT.name}: {MODELS_OUT.stat().st_size} bytes")

    # Prove the generated models are real, importable pydantic models that
    # accept a payload matching the actor's Zod schema. email-validator is
    # injected ad hoc: z.email() -> format:email -> EmailStr, and pydantic's
    # EmailStr requires it at import time (a real dependency implication of
    # the pipeline — documented in the README).
    check = subprocess.run(
        [
            "uv",
            "run",
            "--with",
            "email-validator",
            "python",
            "-c",
            "import sys; sys.path.insert(0, r'%s'); "
            "import models_generated as m; "
            "p = m.SendWelcomeEmailPayload(userId='7d3f9c2a-1b4e-4c8d-9a0f-2e5d6b8a1c3e', email='author@example.com', locale='en'); "
            "r = m.SendWelcomeEmailResult(messageId='msg_1', acceptedAt='2026-09-12T00:00:00Z'); "
            "print('round-trip OK:', p.model_dump_json()); "
            "print('result OK:', r.model_dump_json())"
            % HERE,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    print(check.stdout.strip())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
