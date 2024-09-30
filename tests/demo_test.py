"""Demo version test."""

import pytest

from ontology_loader import __version__


def test_version_type():
    """Demo test."""
    assert type(__version__) == str
