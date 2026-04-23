#
# Copyright 2026 Tabsdata Inc.
#

"""Integration tests for port availability checking."""

import socket
import subprocess
import os


class TestPortAvailability:
    """Startup fails with a clear error if a port is in use."""

    def test_fails_if_api_port_in_use(self, control_plane_bin, test_dir, free_port_pair):
        api_port, kcm_port = free_port_pair

        # Occupy the API server port
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        blocker.bind(("127.0.0.1", api_port))
        blocker.listen(1)

        try:
            proc = subprocess.run(
                [
                    control_plane_bin,
                    "-port", str(api_port),
                    "-kcm-port", str(kcm_port),
                    "-workdir", test_dir,
                ],
                capture_output=True,
                timeout=10,
            )
            assert proc.returncode != 0
            assert b"already in use" in proc.stderr
        finally:
            blocker.close()
