"""RED pass: execute every ```python fence from the docs against the real package.

One subprocess per example (fresh interpreter, fresh event loop, private PG
schema), 45 s wall-clock kill. Writes a JSON report for bucketing.
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from tests._docs_examples import REPO_ROOT, DocsExample, iter_examples, render_script

PG_DSN = "postgresql://taskq:taskq@localhost:5432/taskq"
REDIS_URL = "redis://localhost:6379/0"
TIMEOUT = 45


def schema_name(example_id: str) -> str:
    digest = hashlib.sha256(example_id.encode()).hexdigest()[:10]
    stem = Path(example_id.split(":")[0]).stem[:40].replace("-", "_")
    return f"docs_{stem}_{digest}"


def run_one(example: DocsExample) -> dict[str, object]:
    try:
        script = render_script(example.code)
    except SyntaxError as exc:
        return {
            "id": example.example_id,
            "outcome": "syntax_error",
            "detail": f"{type(exc).__name__}: {exc.msg} (line {exc.lineno})",
        }
    with tempfile.TemporaryDirectory(prefix="docsx-") as tmp:
        path = Path(tmp) / "example.py"
        path.write_text(script, encoding="utf-8")
        env = {
            **os.environ,
            "TASKQ_PG_DSN": PG_DSN,
            "TASKQ_SCHEMA_NAME": schema_name(example.example_id),
            "TASKQ_REDIS_URL": REDIS_URL,
            "DOTENV_DIR": tmp,
            "PYTHONPATH": str(REPO_ROOT / "src"),
        }
        try:
            proc = subprocess.run(  # noqa: S603
                [sys.executable, str(path)],
                cwd=tmp,
                env=env,
                capture_output=True,
                text=True,
                timeout=TIMEOUT,
            )
        except subprocess.TimeoutExpired:
            return {
                "id": example.example_id,
                "outcome": "timeout",
                "detail": f"no exit within {TIMEOUT}s",
                "stderr": "timeout",
            }
        out = (proc.stdout or "")[-2000:]
        err = (proc.stderr or "")[-3000:]
        if proc.returncode == 0:
            return {"id": example.example_id, "outcome": "pass", "stdout": out}
        return {
            "id": example.example_id,
            "outcome": "fail",
            "rc": proc.returncode,
            "stderr": err,
        }


def main() -> None:
    examples = iter_examples()
    print(f"{len(examples)} examples", flush=True)
    results = [run_one(e) for e in examples]
    passed = sum(1 for r in results if r["outcome"] == "pass")
    print(f"pass {passed}/{len(examples)}", flush=True)
    Path(sys.argv[1]).write_text(json.dumps(results, indent=1), encoding="utf-8")


if __name__ == "__main__":
    main()
