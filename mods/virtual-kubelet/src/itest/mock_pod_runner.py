#
# Copyright 2026 Tabsdata Inc.
#
"""Mock pod-runner HTTP server for virtual-kubelet integration tests.

Implements the pod-runner HTTP API with in-memory state so we can test
virtual-kubelet without a real pod-runner binary.
"""

import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs


class MockPodRunnerHandler(BaseHTTPRequestHandler):
    """Handles pod-runner HTTP API requests."""

    # Shared state across requests (set by the server)
    pods = {}  # key: "namespace/name" -> pod dict

    def log_message(self, format, *args):
        pass  # Suppress request logging

    def _send_json(self, data, status=200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def _parse_query(self):
        parsed = urlparse(self.path)
        return parsed.path, parse_qs(parsed.query)

    def do_GET(self):
        path, query = self._parse_query()

        if path == "/capacity":
            self._send_json({"cpu": "100", "memory": "100Gi", "pods": "100"})

        elif path == "/nodeConditions":
            self._send_json([
                {
                    "type": "Ready",
                    "status": "True",
                    "reason": "MockReady",
                    "message": "mock pod runner is ready",
                },
            ])

        elif path == "/nodeAddresses":
            self._send_json([
                {"type": "InternalIP", "address": "127.0.0.1"},
            ])

        elif path == "/getPods":
            pods = list(self.server.pods.values())
            self._send_json(pods)

        elif path == "/getPod":
            ns = query.get("namespace", [""])[0]
            name = query.get("name", [""])[0]
            key = f"{ns}/{name}"
            pod = self.server.pods.get(key)
            if pod:
                self._send_json(pod)
            else:
                self.send_response(404)
                self.end_headers()

        elif path == "/getPodStatus":
            ns = query.get("namespace", [""])[0]
            name = query.get("name", [""])[0]
            key = f"{ns}/{name}"
            pod = self.server.pods.get(key)
            if pod and "status" in pod:
                self._send_json(pod["status"])
            elif pod:
                self._send_json({
                    "phase": "Running",
                    "conditions": [
                        {"type": "Ready", "status": "True"},
                    ],
                })
            else:
                self.send_response(404)
                self.end_headers()

        elif path == "/getContainerLogs":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"mock log output\n")

        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        path, _ = self._parse_query()

        if path == "/createPod":
            body = self._read_body()
            pod = json.loads(body)
            ns = pod.get("metadata", {}).get("namespace", "default")
            name = pod.get("metadata", {}).get("name", "unknown")
            key = f"{ns}/{name}"
            # Set status to Running
            pod["status"] = {
                "phase": "Running",
                "conditions": [{"type": "Ready", "status": "True"}],
            }
            self.server.pods[key] = pod
            self._send_json(pod, status=201)
        else:
            self.send_response(404)
            self.end_headers()

    def do_PUT(self):
        path, _ = self._parse_query()

        if path == "/updatePod":
            body = self._read_body()
            pod = json.loads(body)
            ns = pod.get("metadata", {}).get("namespace", "default")
            name = pod.get("metadata", {}).get("name", "unknown")
            key = f"{ns}/{name}"
            self.server.pods[key] = pod
            self._send_json(pod)
        else:
            self.send_response(404)
            self.end_headers()

    def do_DELETE(self):
        path, query = self._parse_query()

        if path == "/deletePod":
            ns = query.get("namespace", [""])[0]
            name = query.get("name", [""])[0]
            key = f"{ns}/{name}"
            self.server.pods.pop(key, None)
            self.send_response(200)
            self.end_headers()
        else:
            self.send_response(404)
            self.end_headers()


class MockPodRunner:
    """Manages a mock pod-runner HTTP server in a background thread."""

    def __init__(self, host="127.0.0.1", port=0):
        self.server = HTTPServer((host, port), MockPodRunnerHandler)
        self.server.pods = {}
        self.thread = None

    @property
    def port(self):
        return self.server.server_address[1]

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def start(self):
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.server.shutdown()
        if self.thread:
            self.thread.join(timeout=5)

    @property
    def pods(self):
        return self.server.pods
