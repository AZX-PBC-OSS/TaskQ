"""Inventory every ```python fence in docs/** and README.md.

Mechanical RED step for the docs-examples execution harness: emits one JSON
record per fence (source path, line range, first line of code) plus a naive
classification that the inventory run uses to bucket examples. The pytest
harness (tests/test_docs_examples.py) re-implements the extraction; this
script exists for the one-off red count and for tagging fences by hand.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

FENCE_RE = re.compile(r"^(?P<indent> *)```(?P<info>.*)$")


def extract(path: Path) -> list[dict[str, object]]:
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[dict[str, object]] = []
    i = 0
    while i < len(lines):
        m = FENCE_RE.match(lines[i])
        if m is None or not m.group("info").strip().startswith("python"):
            i += 1
            continue
        start = i  # 0-based fence-open line
        i += 1
        body: list[str] = []
        closed = False
        while i < len(lines):
            if FENCE_RE.match(lines[i]):
                closed = True
                break
            body.append(lines[i])
            i += 1
        if not closed:
            raise SystemExit(f"unclosed fence in {path} at line {start + 1}")
        close_line = i
        i += 1
        code = "\n".join(body)
        fragment = "..." in code or "…" in code
        shell = bool(re.search(r"^\s*(\$|>>>|\.\.\.)\s", code, re.M))
        out.append(
            {
                "path": str(path.relative_to(REPO)),
                "open_line": start + 1,
                "close_line": close_line + 1,
                "info": m.group("info").strip(),
                "first_code": next((ln for ln in body if ln.strip()), "")[:80],
                "lines": len(body),
                "fragment": fragment,
                "shell": shell,
            }
        )
    return out


def main() -> None:
    targets = sorted([*REPO.glob("docs/**/*.md"), REPO / "README.md"])
    records: list[dict[str, object]] = []
    for t in targets:
        if not t.exists():
            continue
        records.extend(extract(t))
    json.dump(records, sys.stdout, indent=1)
    print(f"\ntotal: {len(records)}", file=sys.stderr)


if __name__ == "__main__":
    main()
