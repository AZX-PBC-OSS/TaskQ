"""`schema_name` must be validated even under `validate=False`.

`schema_name` used a dotenvmodel BUILT-IN `regex=` constraint. Built-in
constraints are skipped under `load_from_dict(..., validate=False)`, while
custom `validator=` hooks always run -- so the one setting that reaches raw SQL
as an interpolated identifier was the one whose guard could be skipped.

Not exploitable today, and saying otherwise would overstate it: `validate=False`
appears only in TaskQ's own test fixtures, never in `src/`, and every
interpolation site independently re-checks `_IDENT_RE`, which is real defence in
depth. This closes the landmine before a future config-reload path steps on it.
TaskQ had already fixed this exact class for `log_format`; `schema_name` was
never migrated.
"""

from __future__ import annotations

import pytest
from dotenvmodel import DotEnvModelError

from taskq.settings import TaskQSettings, WorkerSettings

_MALICIOUS = 'evil"; DROP SCHEMA public CASCADE; --'


@pytest.mark.parametrize("cls", [TaskQSettings, WorkerSettings])
def test_malicious_schema_rejected_under_validate_false(cls: type) -> None:
    """The regression: this loaded silently before."""
    with pytest.raises(DotEnvModelError):
        cls.load_from_dict({"TASKQ_SCHEMA_NAME": _MALICIOUS}, validate=False)


@pytest.mark.parametrize("cls", [TaskQSettings, WorkerSettings])
def test_malicious_schema_rejected_under_validate_true(cls: type) -> None:
    with pytest.raises(DotEnvModelError):
        cls.load_from_dict({"TASKQ_SCHEMA_NAME": _MALICIOUS}, validate=True)


@pytest.mark.parametrize("value", ["taskq\n", "1abc", "has space", "", "semi;colon", 'quo"te'])
def test_invalid_identifiers_rejected(value: str) -> None:
    r"""`"taskq\n"` belongs here only because both halves are in this PR.

    The validator delegates to `_IDENT_RE` rather than re-implementing the
    pattern. With `_IDENT_RE` still anchored `^...$`, Python's `$` matches
    before a trailing newline and this case passed; the `\A`/`\Z` fix is the
    other half of this change. Neither commit could assert the composed
    behaviour alone, which is why the two are reviewed together.
    """
    with pytest.raises(DotEnvModelError):
        WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": value}, validate=False)


@pytest.mark.parametrize("value", ["taskq", "my_schema", "_private", "S1"])
def test_valid_identifiers_still_load(value: str) -> None:
    s = WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": value}, validate=False)
    assert s.schema_name == value


def test_default_is_unchanged() -> None:
    assert WorkerSettings.load_from_dict({}, validate=False).schema_name == "taskq"


def test_error_message_names_the_field_and_the_rule() -> None:
    with pytest.raises(DotEnvModelError) as excinfo:
        WorkerSettings.load_from_dict({"TASKQ_SCHEMA_NAME": "1bad"}, validate=False)
    msg = str(excinfo.value)
    assert "schema_name" in msg
    assert "valid SQL identifier" in msg


def test_uses_a_validator_hook_not_a_builtin_constraint() -> None:
    """Pin the mechanism, not just the behaviour: reverting to `regex=` would
    reopen the bypass while keeping every behavioural test above green under
    validate=True."""
    from pathlib import Path

    src = (Path(__file__).resolve().parent.parent / "src" / "taskq" / "settings.py").read_text()
    idx = src.index("schema_name: str = Field(")
    field = src[idx : idx + 250]
    assert "validator=_schema_name_validator" in field
    assert "regex=" not in field, "built-in constraint is back; it is skipped by validate=False"
