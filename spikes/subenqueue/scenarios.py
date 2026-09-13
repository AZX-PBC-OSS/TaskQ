"""Run the full scenario matrix: semantics (buffered|direct) x child behavior.

Writes one trace JSON per combo into traces/ and a combined summary to
stdout. Traces contain the actual InMemoryBackend state dump (list_jobs).
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
TRACES = HERE / "traces"

# (scenario_name, parent_extra_args)
SCENARIOS: list[tuple[str, list[str]]] = [
    ("success_5_subjobs", []),
    ("crash_after_3", ["--crash-after", "3"]),
    ("crash_after_0", ["--crash-after", "0"]),
    ("fail_after_5", ["--fail"]),
]


def main() -> None:
    summary: list[dict[str, object]] = []
    for semantics in ("buffered", "direct"):
        for name, extra in SCENARIOS:
            trace_path = TRACES / f"{name}_{semantics}.json"
            cmd = [
                sys.executable,
                str(HERE / "worker_parent.py"),
                "--semantics", semantics,
                "--trace", str(trace_path),
                *extra,
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True)
            if proc.returncode != 0:
                print(f"FAILED: {name}/{semantics}\n{proc.stderr}", file=sys.stderr)
                raise SystemExit(1)
            trace = json.loads(trace_path.read_text())
            sub_jobs = [j for j in trace["backend_jobs"] if j["actor"] == "spike_sub_actor"]
            parent = [j for j in trace["backend_jobs"] if j["actor"] == "spike_parent_actor"]
            summary.append({
                "scenario": name,
                "semantics": semantics,
                "child_returncode": trace["child_returncode"],
                "child_done_msg": trace["child_done_msg"],
                "subenqueues_received": trace["subenqueues_received"],
                "subjobs_enqueued": len(sub_jobs),
                "subjob_payloads": sorted(j["payload"]["n"] for j in sub_jobs),
                "parent_status": parent[0]["status"],
                "trace": trace_path.name,
            })
    (TRACES / "summary.json").write_text(json.dumps(summary, indent=2))
    for row in summary:
        print(json.dumps(row))


if __name__ == "__main__":
    main()
