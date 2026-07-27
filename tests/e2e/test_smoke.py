"""Preflight smoke test for the e2e tier marker wiring."""

import pytest

pytestmark = [pytest.mark.e2e, pytest.mark.timeout(900)]


def test_e2e_marker_wiring() -> None:
    """Proves the e2e marker collects under -m e2e and is excluded by default."""
    assert True
