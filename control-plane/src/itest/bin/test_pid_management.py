#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for PID file management."""

import glob
import os
import signal
import time

import psutil


class TestPidFilesOnStartup:
    """PID files are written for all managed processes."""

    def test_pid_files_exist(self, control_plane):
        pids_dir = control_plane.pids_dir
        assert os.path.isdir(pids_dir)
        pid_files = os.listdir(pids_dir)
        # Should have: control-plane, etcd, kube-apiserver, kube-controller-manager
        names = {f.split(".", 1)[1] for f in pid_files if "." in f}
        assert "control-plane" in names
        assert "etcd" in names
        assert "kube-apiserver" in names
        assert "kube-controller-manager" in names

    def test_pid_files_contain_valid_pids(self, control_plane):
        pids_dir = control_plane.pids_dir
        for pid_file in os.listdir(pids_dir):
            parts = pid_file.split(".", 1)
            assert len(parts) == 2, f"Bad PID file name: {pid_file}"
            pid = int(parts[0])
            assert psutil.pid_exists(pid), f"PID {pid} ({parts[1]}) not running"


class TestPidCleanupOnShutdown:
    """PID files are cleaned up after graceful shutdown."""

    def test_pids_removed_after_stop(self, control_plane_bin, test_dir, free_port_pair):
        from itest_common import ControlPlaneProcess

        api_port, kcm_port = free_port_pair
        cp = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp.start()
        pids_dir = cp.pids_dir

        # Verify PID files exist while running
        assert len(os.listdir(pids_dir)) >= 4

        cp.stop()

        # pids directory should be gone or empty
        if os.path.exists(pids_dir):
            assert len(os.listdir(pids_dir)) == 0

    def test_child_processes_dead_after_stop(self, control_plane_bin, test_dir, free_port_pair):
        from itest_common import ControlPlaneProcess

        api_port, kcm_port = free_port_pair
        cp = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp.start()
        pids_dir = cp.pids_dir

        # Collect PIDs while running
        child_pids = []
        for pid_file in os.listdir(pids_dir):
            parts = pid_file.split(".", 1)
            if len(parts) == 2:
                child_pids.append(int(parts[0]))

        cp.stop()
        time.sleep(1)

        # All child processes should be dead
        for pid in child_pids:
            assert not psutil.pid_exists(pid), f"PID {pid} still alive after shutdown"


class TestStaleProcessCleanup:
    """Stale processes from a previous run are killed on startup."""

    def test_stale_processes_killed_on_restart(
        self, control_plane_bin, test_dir, free_port_pair
    ):
        from itest_common import ControlPlaneProcess

        api_port, kcm_port = free_port_pair
        cp = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp.start()

        # Kill control-plane abruptly (simulate crash), children may survive
        cp.process.kill()
        cp.process.wait()
        time.sleep(1)

        # Start again on the same workdir with new ports
        from itest_common import find_free_port

        api_port2 = find_free_port()
        kcm_port2 = find_free_port()
        cp2 = ControlPlaneProcess(
            control_plane_bin, test_dir, api_port2, kcm_port2
        )
        cp2.start()
        try:
            # Should be running successfully
            assert cp2.process.poll() is None
        finally:
            cp2.stop()
