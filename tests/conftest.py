"""
pytest conftest for MeshReliable stress tests.

Runs device discovery once per session so that require_devices() works
when tests are collected by pytest (instead of only via the main() entry point).
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.dirname(__file__))


@pytest.fixture(scope="session", autouse=True)
def _discover_devices():
    """Populate node_ids at session start so hardware tests skip gracefully."""
    from test_stress import discover_devices
    discover_devices()
