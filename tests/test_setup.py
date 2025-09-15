"""Test basic project setup."""

import pytest
from nfl_analytics import __version__


def test_version() -> None:
    """Test that version is defined."""
    assert __version__ is not None


def test_imports() -> None:
    """Test that main modules can be imported."""
    from nfl_analytics import data, ingestion, analytics, ml, api
    
    assert data is not None
    assert ingestion is not None
    assert analytics is not None
    assert ml is not None
    assert api is not None