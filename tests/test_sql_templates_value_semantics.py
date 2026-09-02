"""``SqlTemplates`` value semantics — nothing in the suite named this type.

The rendered templates are built once per backend and handed to every
read, write and dispatch helper.  Immutability is what makes that sharing
safe, and ``slots`` is what makes an attribute typo an error instead of a
silently-ignored write of a template nobody reads.  Neither had a test.
"""

import dataclasses

import pytest

from taskq.backend._sql_templates import render


def test_rendered_templates_reject_assignment() -> None:
    """The shared instance cannot be re-pointed at different SQL."""
    templates = render("taskq_test")
    original = templates.mark_succeeded

    with pytest.raises(dataclasses.FrozenInstanceError):
        templates.mark_succeeded = "DELETE FROM jobs"  # type: ignore[misc]  # Why: assigning to a frozen dataclass field is the behaviour under test.

    assert templates.mark_succeeded == original


def test_rendered_templates_reject_unknown_attributes() -> None:
    """A misspelled template name fails loudly rather than landing in a dict.

    ``object.__setattr__`` bypasses the frozen guard, so this probes the
    ``slots`` half specifically: without it the stray name would be stored
    on an instance dict and read back as if it were a real template.
    """
    templates = render("taskq_test")

    with pytest.raises(AttributeError):
        object.__setattr__(templates, "mark_suceeded", "SELECT 1")

    assert not hasattr(templates, "__dict__")
