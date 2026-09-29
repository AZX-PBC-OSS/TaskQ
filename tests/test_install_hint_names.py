"""Every install command the product prints must name the published distribution.

The PyPI distribution is ``taskq-py``; the bare name ``taskq`` is an UNRELATED
distribution (docs/getting-started/installation.md's "Name check" warning, and
docs/guides/sso.md: "``taskq[saml]`` on PyPI resolves to an unrelated package
with no ``saml`` extra"). An error message that says
``pip install 'taskq[redis]'`` therefore tells the operator to install the
wrong package — the exact trap the docs warn about, planted by the product
itself, at the worst possible moment (the operator is mid-failure and will
paste the command as-is).

Prose references to an extra (``taskq[redis] is required``) are fine and are
the established house pattern (``src/taskq/aws.py``, ``src/taskq/aad.py`` keep
prose as ``taskq[aws]`` but give the install command as ``taskq-py[aws]``).
Only INSTALL COMMANDS are policed here: ``pip install``, ``uv add``, and
``uv tool install`` followed by the bare name.

The census is AST-based (see tests/_import_discipline.py for why substring
greps over source are the wrong tool), so it sees the message regardless of
how the string is composed, and it cannot be fooled by a comment.
"""

from __future__ import annotations

import ast
import asyncio
import re
import sys
from pathlib import Path

import pytest

SRC = Path(__file__).parent.parent / "src" / "taskq"

# Install-command shapes that must name taskq-py, as (group name, pattern)
# pairs. The negative lookahead rejects the bare distribution name; prose
# references (no install verb) never match.
_INSTALL_COMMANDS: tuple[tuple[str, str], ...] = (
    ("pip_install", r"pip3? install\s+['\"]?taskq(?!-py)"),
    ("uv_add", r"uv add\s+['\"]?taskq(?!-py)"),
    ("uv_tool_install", r"uv tool install\s+taskq(?!-py)"),
)
_COMBINED = re.compile("|".join(f"(?P<{name}>{pat})" for name, pat in _INSTALL_COMMANDS))
assert set(_COMBINED.groupindex) == {name for name, _ in _INSTALL_COMMANDS}


def _string_constants(tree: ast.AST) -> list[str]:
    return [
        node.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]


def _violations(text: str) -> list[str]:
    return [f"{match.group(0)!r} ({match.lastgroup})" for match in _COMBINED.finditer(text)]


def test_every_install_command_names_the_published_distribution() -> None:
    """The product's own error messages must not point at the wrong package."""
    offenders: list[str] = []
    for path in sorted(SRC.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for constant in _string_constants(tree):
            for violation in _violations(constant):
                offenders.append(f"{path.relative_to(SRC.parent)}: {violation}")
    assert not offenders, (
        "install commands name the UNRELATED 'taskq' distribution "
        "(must be 'taskq-py'):\n" + "\n".join(offenders)
    )


@pytest.mark.parametrize(
    ("trigger", "poison", "fragment"),
    [
        # The Redis-backed limiter's guard: poisoning the parent package is the
        # documented "extra truly absent" state (see
        # tests/test_worker_di_bootstrap.py for the find_spec-on-dotted-submodule
        # rationale). with_pg_fallback probes the extra on first call.
        (
            "call_with_pg_fallback",
            "redis",
            "taskq-py[redis]",
        ),
        # The SSO session cookie serializer: shared by the oidc and saml backends.
        (
            "construct_session_manager",
            "itsdangerous",
            "taskq-py[",
        ),
    ],
)
def test_guarded_import_hints_name_the_published_distribution(
    trigger: str,
    poison: str,
    fragment: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Behavioral pin: the reachable guards' hints really name taskq-py."""
    monkeypatch.setitem(sys.modules, poison, None)  # type: ignore[import-not-found]
    if trigger == "call_with_pg_fallback":
        from taskq.ratelimit._redis_utils import with_pg_fallback

        async def _never() -> object:  # pragma: no cover - the guard fires first
            raise AssertionError("the redis acquire must not run")

        with pytest.raises(ImportError, match=re.escape(fragment)):
            asyncio.run(
                with_pg_fallback(
                    _never,  # pyright: ignore[reportArgumentType]  # Why: the guard raises before the callables are touched.
                    _never,  # pyright: ignore[reportArgumentType]
                    bucket_name="install-hint-pin",
                    settings=None,
                )
            )
    else:
        from taskq.web.admin.auth._session import SessionManager

        with pytest.raises(ImportError, match=re.escape(fragment)):
            SessionManager(secret="install-hint-pin")  # pyright: ignore[reportCallIssue]  # Why: the guard raises in __post_init__ before the full signature matters.
