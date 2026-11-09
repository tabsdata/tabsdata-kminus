#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for control-plane startup behavior."""

import os

import yaml


class TestKubeconfigGeneration:
    """Kubeconfigs are regenerated on every start."""

    def test_kubeconfigs_created(self, control_plane):
        config_dir = control_plane.config_dir
        assert os.path.isfile(os.path.join(config_dir, "kubeconfig.yaml"))
        assert os.path.isfile(os.path.join(config_dir, "vk-kubeconfig.yaml"))
        assert os.path.isfile(os.path.join(config_dir, "kubeconfig-kcm.yaml"))

    def test_admin_kubeconfig_has_valid_server(self, control_plane):
        path = os.path.join(control_plane.config_dir, "kubeconfig.yaml")
        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        server = config["clusters"][0]["cluster"]["server"]
        assert f":{control_plane.port}" in server

    def test_vk_kubeconfig_has_valid_server(self, control_plane):
        path = os.path.join(control_plane.config_dir, "vk-kubeconfig.yaml")
        with open(path, encoding="utf-8") as f:
            config = yaml.safe_load(f)
        server = config["clusters"][0]["cluster"]["server"]
        assert f":{control_plane.port}" in server


class TestDefaultDirectories:
    """Default directories are created under workdir."""

    def test_config_dir_created(self, control_plane):
        assert os.path.isdir(control_plane.config_dir)

    def test_log_dir_created(self, control_plane):
        assert os.path.isdir(control_plane.log_dir)

    def test_data_dir_created(self, control_plane):
        assert os.path.isdir(control_plane.data_dir)


class TestCustomDirectories:
    """Custom directories can be set via CLI flags."""

    def test_custom_configdir(self, control_plane_bin, test_dir, free_port_pair, tmp_path):
        from itest_common import ControlPlaneProcess

        custom_config = str(tmp_path / "my_config")
        api_port, kcm_port = free_port_pair
        cp = ControlPlaneProcess(
            control_plane_bin, test_dir, api_port, kcm_port,
            extra_args=["-configdir", custom_config],
        )
        cp.start()
        try:
            assert os.path.isdir(custom_config)
            assert os.path.isfile(os.path.join(custom_config, "kubeconfig.yaml"))
        finally:
            cp.stop()

    def test_custom_logdir(self, control_plane_bin, test_dir, free_port_pair, tmp_path):
        from itest_common import ControlPlaneProcess

        custom_log = str(tmp_path / "my_logs")
        api_port, kcm_port = free_port_pair
        cp = ControlPlaneProcess(
            control_plane_bin, test_dir, api_port, kcm_port,
            extra_args=["-logdir", custom_log],
        )
        cp.start()
        try:
            assert os.path.isdir(custom_log)
            assert os.path.isfile(os.path.join(custom_log, "control-plane.log"))
        finally:
            cp.stop()
