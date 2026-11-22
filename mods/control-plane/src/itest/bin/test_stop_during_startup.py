#
# Copyright 2026 Tabsdata Inc.
#

"""A stop requested at any point of startup leaves no child process behind."""

import os
import signal
import subprocess
import sys
import time

import psutil
import pytest

from itest_common import request_stop, start_process

CHILDREN = ("etcd", "kube-apiserver", "kube-controller-manager")

# How a process ends when the stop arrives before it handles one: SIGTERM's
# default action or, on Windows, CTRL_BREAK's (STATUS_CONTROL_C_EXIT), and
# STATUS_DLL_INIT_FAILED when the process is still loading as it arrives.
ENDED_BY_STOP = (
    (0xC000013A, 0xC0000142) if sys.platform == "win32" else (-signal.SIGTERM,)
)


def _pid_files(pids_dir):
    try:
        names = os.listdir(pids_dir)
    except FileNotFoundError:
        return {}
    pids = {}
    for name in names:
        pid, _, process = name.partition(".")
        if process in CHILDREN:
            pids[int(pid)] = process
    return pids


def _start(control_plane_bin, test_dir, free_port_pair, output=subprocess.DEVNULL):
    api_port, kcm_port = free_port_pair
    return start_process(
        [
            control_plane_bin,
            "-port", str(api_port),
            "-kcm-port", str(kcm_port),
            "-workdir", test_dir,
        ],
        stdout=output,
        stderr=subprocess.STDOUT,
    )


def _stop_and_check(process, pids_dir, seen, exit_codes=(0,)):
    request_stop(process)
    process.wait(timeout=120)
    seen.update(_pid_files(pids_dir))
    survivors = [
        f"{name}({pid})"
        for pid, name in seen.items()
        if psutil.pid_exists(pid)
        and psutil.Process(pid).status() != psutil.STATUS_ZOMBIE
    ]
    for pid in seen:
        try:
            psutil.Process(pid).kill()
        except psutil.NoSuchProcess:
            pass
    assert process.returncode in exit_codes, f"control-plane exited with {process.returncode}"
    assert not survivors, f"children left running: {survivors}"
    assert not _pid_files(pids_dir), "child PID files left behind"


def _pids_dir(test_dir):
    return os.path.join(test_dir, "data", "control-plane", "pids")


def test_stop_before_any_child(control_plane_bin, test_dir, free_port_pair):
    process = _start(control_plane_bin, test_dir, free_port_pair)
    # Before its handler is installed the process just ends, which is safe:
    # nothing has been started yet.
    _stop_and_check(process, _pids_dir(test_dir), {}, exit_codes=(0, *ENDED_BY_STOP))


def test_stop_once_etcd_and_api_server_run(control_plane_bin, test_dir, free_port_pair):
    process = _start(control_plane_bin, test_dir, free_port_pair)
    pids_dir = _pids_dir(test_dir)
    seen = {}
    deadline = time.time() + 60
    while time.time() < deadline:
        seen.update(_pid_files(pids_dir))
        if {"etcd", "kube-apiserver"} <= set(seen.values()):
            break
        assert process.poll() is None, "control-plane exited during startup"
        time.sleep(0.05)
    else:
        pytest.fail("etcd and the API server never started")
    _stop_and_check(process, pids_dir, seen)


def test_stop_right_when_ready(control_plane_bin, test_dir, free_port_pair):
    process = _start(control_plane_bin, test_dir, free_port_pair, output=subprocess.PIPE)
    for line in process.stdout:
        if b"Cluster is ready!" in line:
            break
    else:
        pytest.fail("control-plane exited before it was ready")
    _stop_and_check(process, _pids_dir(test_dir), _pid_files(_pids_dir(test_dir)))
