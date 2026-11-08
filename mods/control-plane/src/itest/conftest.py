#
# Copyright 2026 Tabsdata Inc.
#

import os

import pytest

from itest_common import (  # noqa: F401
    ControlPlaneProcess,
    find_free_port,
    pytest_configure,
    start_on_free_ports,
)
from control_plane_testlib import make_test_dir


# ---------------------------------------------------------------------------
# test_dir fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def test_dir(request) -> str:
    """Return a unique test directory under target/testrun.<ts>/..."""
    return make_test_dir(request.node.nodeid)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def bin_dir():
    env_val = os.environ.get("TD_BIN_FOLDER")
    if env_val:
        return env_val
    raise RuntimeError("TD_BIN_FOLDER environment variable must be set")


@pytest.fixture(scope="session")
def control_plane_bin(bin_dir):
    path = os.path.join(bin_dir, "control-plane")
    if not os.path.isfile(path):
        pytest.skip(f"control-plane binary not found at {path}")
    return path


@pytest.fixture
def free_port():
    return find_free_port()


@pytest.fixture
def free_port_pair():
    return find_free_port(), find_free_port()


# ---------------------------------------------------------------------------
# Convenience fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def control_plane(control_plane_bin, test_dir):
    """Starts a control-plane process and stops it after the test."""

    def start(api_port, kcm_port):
        cp = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp.start()
        return cp

    cp = start_on_free_ports(start, 2)
    yield cp
    cp.stop()
