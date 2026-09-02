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
    """`"taskq\n"` is included here only because both halves have landed.

    On the F14 branch alone it was accepted, because `_IDENT_RE` still used
    `^...$` and Python's `$` matches before a trailing newline. The anchors were
    fixed separately (`fix/ident-re-trailing-newline`). This validator delegates
    to `_IDENT_RE` rather than re-implementing the pattern, so on the merged
    tree the two compose and the newline is rejected here too -- which is the
    behaviour worth pinning, and could not be asserted on either branch alone.
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


# A source scan asserting `validator=_schema_name_validator` in the field
# definition used to live here, on the stated grounds that reverting to
# `regex=` "would keep every behavioural test above green". That reasoning was
# wrong, and measurably so: every test in this file loads with
# `validate=False`, which is precisely where a built-in constraint is skipped
# and a validator hook is not. Reverting the field to
# `regex=r"\A[A-Za-z_][A-Za-z0-9_]*\Z"` fails NINE tests here — both
# `test_malicious_schema_rejected_under_validate_false` parametrisations, all
# six `test_invalid_identifiers_rejected` cases and
# `test_error_message_names_the_field_and_the_rule` — each with "DID NOT RAISE
# DotEnvModelError". The behaviour is fully pinned; the scan added nothing
# except a second thing to update when the field moves.
