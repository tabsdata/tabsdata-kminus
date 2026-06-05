#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for static token authentication.

The control-plane generates tokens.csv once (on first startup) and reuses
it thereafter, so the kubeconfigs embed stable bearer tokens instead of
client certificates re-issued from a fresh CA on every start. Verifies
that kubeconfigs are identical across restarts and that credentials
captured during one run still authenticate against a later run.
"""

import os
import shutil
import sys

import pytest
import yaml
from itest_common import ControlPlaneProcess, find_free_port

KUBECONFIGS = ["kubeconfig.yaml", "vk-kubeconfig.yaml", "kubeconfig-kcm.yaml"]

TOKEN_USERS = ["admin", "virtual-kubelet", "system:kube-controller-manager"]


def _load_kubeconfig(kubeconfig_path):
    """Load kubeconfig and return a kubernetes client."""
    from kubernetes import client, config as k8s_config

    k8s_config.load_kube_config(config_file=kubeconfig_path)
    return client.CoreV1Api()


def _read_tokens_csv(config_dir):
    """Parse tokens.csv into {user: token}."""
    import csv

    path = os.path.join(config_dir, "tokens.csv")
    with open(path, newline="") as f:
        return {row[1]: row[0] for row in csv.reader(f) if len(row) >= 2}


class TestTokenFile:
    """tokens.csv is generated once with one token per user."""

    def test_tokens_csv_created(self, control_plane):
        path = os.path.join(control_plane.config_dir, "tokens.csv")
        assert os.path.isfile(path)

    def test_tokens_csv_has_all_users(self, control_plane):
        tokens = _read_tokens_csv(control_plane.config_dir)
        for user in TOKEN_USERS:
            assert user in tokens
            assert len(tokens[user]) == 64  # 32 random bytes, hex-encoded

    @pytest.mark.skipif(
        sys.platform == "win32", reason="POSIX permission bits not meaningful on Windows"
    )
    def test_tokens_csv_not_world_readable(self, control_plane):
        path = os.path.join(control_plane.config_dir, "tokens.csv")
        assert os.stat(path).st_mode & 0o077 == 0


class TestKubeconfigsUseTokens:
    """Kubeconfigs embed bearer tokens, not client certificates."""

    def test_kubeconfigs_have_token_auth(self, control_plane):
        tokens = _read_tokens_csv(control_plane.config_dir)
        for name in KUBECONFIGS:
            with open(os.path.join(control_plane.config_dir, name)) as f:
                config = yaml.safe_load(f)
            user = config["users"][0]["user"]
            assert "client-certificate-data" not in user
            assert user["token"] in tokens.values()

    def test_admin_kubeconfig_authenticates(self, control_plane):
        k8s = _load_kubeconfig(control_plane.kubeconfig)
        namespaces = [ns.metadata.name for ns in k8s.list_namespace().items]
        assert "default" in namespaces


class TestStableAcrossRestarts:
    """Tokens and kubeconfigs survive restarts unchanged."""

    def test_kubeconfigs_identical_across_restart(self, control_plane_bin, test_dir):
        api_port = find_free_port()
        kcm_port = find_free_port()

        # --- First run: capture tokens.csv and all kubeconfigs ---
        cp1 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp1.start()
        first = {}
        for name in ["tokens.csv"] + KUBECONFIGS:
            with open(os.path.join(cp1.config_dir, name), "rb") as f:
                first[name] = f.read()
        cp1.stop()

        # --- Second run: same workdir and ports, files must be identical ---
        cp2 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp2.start()
        try:
            for name, content in first.items():
                with open(os.path.join(cp2.config_dir, name), "rb") as f:
                    assert f.read() == content, f"{name} changed across restart"
        finally:
            cp2.stop()

    def test_saved_credentials_survive_restart(self, control_plane_bin, test_dir):
        """A kubeconfig copied during one run authenticates against the next.

        This is what cert-based kubeconfigs could not do: the fresh CA of
        each run invalidated every previously issued client certificate.
        """
        api_port = find_free_port()
        kcm_port = find_free_port()

        # --- First run: copy the admin kubeconfig elsewhere ---
        cp1 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp1.start()
        saved = os.path.join(test_dir, "saved-kubeconfig.yaml")
        shutil.copyfile(cp1.kubeconfig, saved)
        cp1.stop()

        # --- Second run: the saved copy still works ---
        cp2 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp2.start()
        try:
            k8s = _load_kubeconfig(saved)
            namespaces = [ns.metadata.name for ns in k8s.list_namespace().items]
            assert "default" in namespaces
        finally:
            cp2.stop()

    def test_controllers_work_after_restart(self, control_plane_bin, test_dir):
        """KCM controllers still reconcile after a restart.

        The restarted kube-controller-manager authenticates with the
        reused token from tokens.csv; a job driven to completion in the
        second run is deleted by the ttl-after-finished controller,
        proving the controllers are live. The client also uses the
        kubeconfig saved during the first run.
        """
        from test_controllers import (
            _is_not_found,
            _load_clients,
            _make_job,
            _mark_pod_succeeded,
            _wait_for,
            _wait_for_job_pod,
        )

        api_port = find_free_port()
        kcm_port = find_free_port()

        # --- First run: save the admin kubeconfig, then stop ---
        cp1 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp1.start()
        saved = os.path.join(test_dir, "saved-kubeconfig.yaml")
        shutil.copyfile(cp1.kubeconfig, saved)
        cp1.stop()

        # --- Second run: drive a job through the controllers ---
        cp2 = ControlPlaneProcess(control_plane_bin, test_dir, api_port, kcm_port)
        cp2.start()
        try:
            core, batch = _load_clients(saved)

            batch.create_namespaced_job(
                namespace="default",
                body=_make_job("restart-ttl-job", ttl_seconds_after_finished=1),
            )

            pod = _wait_for_job_pod(core, "restart-ttl-job")
            _mark_pod_succeeded(core, pod.metadata.name)

            # The job controller marks the job Complete, then the
            # ttl-after-finished controller deletes it 1s later.
            _wait_for(
                lambda: _is_not_found(
                    lambda: batch.read_namespaced_job(
                        name="restart-ttl-job", namespace="default"
                    )
                ),
                message="job restart-ttl-job to be deleted after restart",
            )
        finally:
            cp2.stop()
