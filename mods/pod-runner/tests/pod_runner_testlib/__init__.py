#
# Copyright 2026 Tabsdata Inc.
#
"""Test utilities for pod-runner integration tests."""
import os

from itest_common import get_session_id, make_test_dir as _make_test_dir

_SUBPROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..")
)
TARGET_DIR = os.path.join(_SUBPROJECT_ROOT, "target")


def make_test_dir(node_id: str) -> str:
    return _make_test_dir(node_id, TARGET_DIR)
