"""In-process GIL sampler + flamegraph — py-spy fallback for macOS.

py-spy needs root on macOS (``task_for_pid``), which is unavailable here.
This script profiles the same workload in-process instead:

  - runs ``stress_dispatch.main()`` (the ~20s dispatch stress loop) in a
    worker thread
  - samples ``sys._current_frames()`` from the sampler thread at ~1kHz

Because the workload is a single CPU-bound Python thread, every sample
of the worker thread is a GIL-holding sample — the same population
py-spy ``--gil`` would report.

Outputs (in benchmarks/):
  - ``dispatch_gil.raw``    py-spy-style collapsed stacks (``a;b;c count``)
  - ``dispatch_flame.svg``  classic flamegraph rendered from those stacks
  - stdout: top-15 GIL-holding leaf frames by sample count

Usage: .venv/bin/python benchmarks/gil_sample.py
"""

import io
import os
import sys
import threading
import time
from collections import Counter
from xml.sax.saxutils import escape

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import stress_dispatch

OUT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_PATH = os.path.join(OUT_DIR, "dispatch_gil.raw")
SVG_PATH = os.path.join(OUT_DIR, "dispatch_flame.svg")

_worker_tid: int = 0
_samples: list[list[str]] = []
_stop = threading.Event()


def _frame_name(frame) -> str:  # type: ignore[no-untyped-def]
    name = frame.f_code.co_name
    filename = os.path.basename(frame.f_code.co_filename)
    return f"{filename}:{frame.f_lineno}({name})"


def _sample_loop() -> None:
    while not _stop.is_set():
        frames = sys._current_frames()
        for tid, frame in frames.items():
            if tid != _worker_tid:
                continue
            stack = []
            while frame is not None:
                stack.append(_frame_name(frame))
                frame = frame.f_back
            stack.reverse()  # root -> leaf
            _samples.append(stack)
        time.sleep(0.0005)


def _collapse(samples: list[list[str]]) -> dict[tuple[str, ...], int]:
    collapsed: Counter[tuple[str, ...]] = Counter()
    for stack in samples:
        collapsed[tuple(stack)] += 1
    return dict(collapsed)


def write_raw(collapsed: dict[tuple[str, ...], int]) -> None:
    with open(RAW_PATH, "w", encoding="utf-8") as f:
        for stack, count in sorted(collapsed.items(), key=lambda kv: -kv[1]):
            f.write(f"{';'.join(stack)} {count}\n")


def top_leaf_frames(collapsed: dict[tuple[str, ...], int], top: int = 15) -> None:
    leaves: Counter[str] = Counter()
    total = 0
    for stack, count in collapsed.items():
        total += count
        leaves[stack[-1]] += count
    print(f"\ntotal GIL-holding samples (worker thread): {total}")
    print(f"top {top} GIL-holding leaf frames:")
    print(f"{'count':>8}  {'pct':>6}  frame")
    for leaf, count in leaves.most_common(top):
        print(f"{count:>8}  {count / total * 100:5.1f}%  {leaf}")


# ── Flamegraph rendering (classic flamegraph.pl layout) ───────────────


def _color(depth: int, name: str) -> str:
    h = (hash(name) & 0xFF) / 255.0
    r = int(205 + 50 * h)
    g = int(90 + 90 * ((hash(name) >> 8) & 0xFF) / 255.0)
    b = int(40 + 30 * depth % 60)
    return f"rgb({min(r, 255)},{min(g, 230)},{b})"


def _build_tree(collapsed: dict[tuple[str, ...], int]) -> dict:
    root: dict = {"name": "root", "value": 0, "children": {}}
    for stack, count in collapsed.items():
        node = root
        node["value"] += count
        for frame in stack:
            child = node["children"].get(frame)
            if child is None:
                child = {"name": frame, "value": 0, "children": {}}
                node["children"][frame] = child
            child["value"] += count
            node = child
    return root


def write_flamegraph(collapsed: dict[tuple[str, ...], int]) -> None:
    total = sum(collapsed.values())
    root = _build_tree(collapsed)
    max_depth = max(len(stack) for stack in collapsed)
    width, row_h, pad = 1200, 16, 10
    height = (max_depth + 1) * row_h + 40

    out = io.StringIO()
    out.write(
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" '
        f'height="{height}" viewBox="0 0 {width} {height}" font-family="monospace">\n'
    )
    out.write(
        f"<text x='{width // 2}' y='18' text-anchor='middle' font-size='13'>"
        f"TaskQ dispatch stress — GIL-holding samples (total {total})</text>\n"
    )

    def emit(node: dict, x: float, w: float, depth: int) -> None:
        y = height - 24 - depth * row_h
        name = node["name"]
        pct = node["value"] / total * 100
        out.write(
            f"<g><title>{escape(name)} — {node['value']} samples "
            f"({pct:.2f}%)</title>"
            f"<rect x='{x:.1f}' y='{y:.1f}' width='{max(w - 0.4, 0.2):.1f}' "
            f"height='{row_h - 2}' fill='{_color(depth, name)}' rx='1'/>"
        )
        if w > 60:
            label = name if len(name) * 6.6 < w else name[: int(w / 6.6) - 2] + "…"
            out.write(
                f"<text x='{x + 2:.1f}' y='{y + row_h - 5:.1f}' "
                f"font-size='10' fill='#000'>{escape(label)}</text>"
            )
        out.write("</g>\n")
        child_x = x
        for child in sorted(node["children"].values(), key=lambda c: -c["value"]):
            cw = w * child["value"] / node["value"] if node["value"] else 0
            if cw >= 0.2:
                emit(child, child_x, cw, depth + 1)
            child_x += cw

    emit(root, pad, width - 2 * pad, 0)
    out.write("</svg>\n")
    with open(SVG_PATH, "w", encoding="utf-8") as f:
        f.write(out.getvalue())
    print(f"flamegraph: {SVG_PATH} ({os.path.getsize(SVG_PATH):,} bytes, depth {max_depth})")


def main() -> None:
    global _worker_tid

    def run_stress() -> None:
        import asyncio

        global _worker_tid
        _worker_tid = threading.get_ident()
        asyncio.run(stress_dispatch.main())

    t = threading.Thread(target=run_stress, name="stress-worker")
    t.start()
    sampler = threading.Thread(target=_sample_loop, name="gil-sampler")
    sampler.start()
    t.join()
    _stop.set()
    sampler.join()

    collapsed = _collapse(_samples)
    write_raw(collapsed)
    print(f"collapsed stacks: {len(collapsed):,} -> {RAW_PATH}")
    top_leaf_frames(collapsed)
    write_flamegraph(collapsed)


if __name__ == "__main__":
    main()
