#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for logging behavior."""

import os
import time


class TestLogFiles:
    """Each component writes to its own rotating log file."""

    def test_log_files_created(self, control_plane):
        log_dir = control_plane.log_dir
        # KCM log may take a moment to be written
        time.sleep(2)
        assert os.path.isfile(os.path.join(log_dir, "control-plane.log"))
        assert os.path.isfile(os.path.join(log_dir, "etcd.log"))
        assert os.path.isfile(os.path.join(log_dir, "kube-apiserver.log"))
        assert os.path.isfile(os.path.join(log_dir, "kube-controller-manager.log"))

    def test_control_plane_log_not_empty(self, control_plane):
        log_path = os.path.join(control_plane.log_dir, "control-plane.log")
        assert os.path.getsize(log_path) > 0
