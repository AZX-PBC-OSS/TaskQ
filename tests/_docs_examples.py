"""Mechanical extraction of Python example fences from the documentation.

Shared by the pytest harness (``tests/test_docs_examples.py``) and the
one-off inventory script (``scripts/docs_example_inventory.py``). Every
``​```python fence in ``docs/**/*.md`` and ``README.md`` is an example; its
fence info string decides how the harness treats it:

- ``​```python`` — executed. The example must run standalone: it may not
  reference names bound by an earlier fence, may not block indefinitely,
  and must finish quickly.
- ``​```python no-exec`` — deliberately not executed. Used for fragments
  that continue an earlier example's scope, pseudo-code, and
  interactive/long-running snippets. Anything after ``no-exec`` in the info
  string is a free-form reason for human readers.

Any other info string after ``python`` is a collection error: a new tag
must be added to this module explicitly, never silently ignored.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

#: Documentation sources scanned for example fences.
DOC_ROOTS: tuple[str, ...] = ("docs", "README.md")

_FENCE_OPEN = re.compile(r"^(?P<indent> *)```(?P<info>.*)$")
_EXEC = "exec"
_NO_EXEC = "no-exec"


@dataclass(frozen=True, slots=True)
class DocsExample:
    """One ```python fence extracted from a documentation source file."""

    #: Stable identifier, e.g. ``docs/guides/cron.md:80``.
    example_id: str
    #: Repo-relative path of the source document.
    path: str
    #: 1-based line numbers of the fence-open and fence-close lines.
    open_line: int
    close_line: int
    #: Raw info string between the backticks and the newline.
    info: str
    #: The example source, verbatim.
    code: str

    @property
    def executed(self) -> bool:
        """Whether the harness runs this example."""
        return _tag(self.info) == _EXEC


def _tag(info: str) -> str:
    """Parse a fence info string into its execution tag.

    ``python`` → exec; ``python no-exec ...`` → no-exec; anything else is a
    mistake the collection must surface rather than swallow.
    """
    parts = info.split()
    assert parts, "fence info string is empty"
    assert parts[0] == "python", f"unexpected fence language: {parts[0]!r}"
    if len(parts) == 1:
        return _EXEC
    if parts[1] == _NO_EXEC:
        return _NO_EXEC
    raise ValueError(
        f"unknown docs-example tag {parts[1]!r} in info string {info!r}; "
        f"expected bare 'python' or 'python no-exec [reason]'"
    )


def iter_examples(repo_root: Path = REPO_ROOT) -> list[DocsExample]:
    """Extract every ```python fence from the documentation, in file order."""
    paths: list[Path] = []
    for entry in DOC_ROOTS:
        root = repo_root / entry
        if root.is_dir():
            paths.extend(sorted(root.rglob("*.md")))
        elif root.exists():
            paths.append(root)

    examples: list[DocsExample] = []
    for path in paths:
        lines = path.read_text(encoding="utf-8").splitlines()
        i = 0
        while i < len(lines):
            match = _FENCE_OPEN.match(lines[i])
            if match is None or not match.group("info").strip().startswith("python"):
                i += 1
                continue
            open_line = i + 1
            info = match.group("info").strip()
            _tag(info)  # fail fast on unknown tags, even for non-executed fences
            i += 1
            body: list[str] = []
            while i < len(lines) and not _FENCE_OPEN.match(lines[i]):
                body.append(lines[i])
                i += 1
            if i == len(lines):
                raise ValueError(f"unclosed fence in {path} at line {open_line}")
            close_line = i + 1
            i += 1
            rel = path.relative_to(repo_root).as_posix()
            examples.append(
                DocsExample(
                    example_id=f"{rel}:{open_line}",
                    path=rel,
                    open_line=open_line,
                    close_line=close_line,
                    info=info,
                    code="\n".join(body),
                )
            )
    return examples


def render_script(code: str) -> str:
    """Return *code* as an executable script, wrapping top-level ``await``.

    The wrap is mechanical and deterministic: a fence that fails to compile
    only because it uses ``await`` at module level is a synchronous-looking
    async example, and gets an ``asyncio.run`` driver. Any other
    ``SyntaxError`` propagates — that is a broken example, not a wrap case.
    """
    try:
        compile(code, "<example>", "exec")
        return code
    except SyntaxError as exc:
        if exc.msg != "'await' outside function":
            raise
    indented = "\n".join(f"    {line}" if line.strip() else line for line in code.splitlines())
    return f"import asyncio\n\n\nasync def _docs_example_main() -> None:\n{indented}\n\nasyncio.run(_docs_example_main())\n"
