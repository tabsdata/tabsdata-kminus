#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#
"""Process monitor for kminus load tests.

Periodically samples CPU and memory usage of all kminus-related processes
(control-plane, pod-runner, virtual-kubelet, km, etcd, kube-apiserver,
kube-controller-manager). Reports peak and average utilization.
"""

import threading
import time
from dataclasses import dataclass, field

import psutil


KMINUS_BINARIES = {
    "control-plane", "pod-runner", "virtual-kubelet", "km",
    "etcd", "kube-apiserver", "kube-controller-manager",
}


@dataclass
class ProcessSample:
    name: str
    pid: int
    cpu_percent: float
    rss_mb: float
    vms_mb: float


@dataclass
class MonitorReport:
    """Aggregated monitoring results."""
    samples: list[list[ProcessSample]] = field(default_factory=list)
    duration_secs: float = 0.0

    @property
    def peak_total_rss_mb(self) -> float:
        if not self.samples:
            return 0.0
        return max(sum(s.rss_mb for s in snap) for snap in self.samples)

    @property
    def avg_total_rss_mb(self) -> float:
        if not self.samples:
            return 0.0
        totals = [sum(s.rss_mb for s in snap) for snap in self.samples]
        return sum(totals) / len(totals)

    @property
    def peak_total_cpu(self) -> float:
        if not self.samples:
            return 0.0
        return max(sum(s.cpu_percent for s in snap) for snap in self.samples)

    @property
    def peak_by_process(self) -> dict[str, float]:
        """Peak RSS per process name."""
        peaks: dict[str, float] = {}
        for snap in self.samples:
            for s in snap:
                if s.name not in peaks or s.rss_mb > peaks[s.name]:
                    peaks[s.name] = s.rss_mb
        return peaks

    def summary(self) -> str:
        lines = [
            f"Monitor duration: {self.duration_secs:.1f}s, {len(self.samples)} samples",
            f"Peak total RSS: {self.peak_total_rss_mb:.1f} MB",
            f"Avg total RSS:  {self.avg_total_rss_mb:.1f} MB",
            f"Peak total CPU: {self.peak_total_cpu:.1f}%",
            "Peak RSS by process:",
        ]
        for name, mb in sorted(self.peak_by_process.items()):
            lines.append(f"  {name:30s} {mb:8.1f} MB")
        return "\n".join(lines)


def _snapshot() -> list[ProcessSample]:
    """Take a snapshot of all kminus-related processes."""
    samples = []
    for proc in psutil.process_iter(["pid", "name", "cpu_percent", "memory_info"]):
        try:
            name = proc.info["name"]
            if name not in KMINUS_BINARIES:
                continue
            mem = proc.info["memory_info"]
            samples.append(ProcessSample(
                name=name,
                pid=proc.info["pid"],
                cpu_percent=proc.info["cpu_percent"] or 0.0,
                rss_mb=(mem.rss / 1024 / 1024) if mem else 0.0,
                vms_mb=(mem.vms / 1024 / 1024) if mem else 0.0,
            ))
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    return samples


class ProcessMonitor:
    """Background thread that periodically samples process metrics."""

    def __init__(self, interval_secs: float = 2.0):
        self.interval = interval_secs
        self._report = MonitorReport()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._start_time = 0.0

    def start(self):
        self._start_time = time.time()
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> MonitorReport:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._report.duration_secs = time.time() - self._start_time
        return self._report

    def _run(self):
        # Initial CPU sample (psutil needs two calls for accurate %)
        _snapshot()
        time.sleep(0.5)

        while not self._stop.is_set():
            snap = _snapshot()
            if snap:
                self._report.samples.append(snap)
            self._stop.wait(self.interval)


def check_orphan_processes() -> list[ProcessSample]:
    """Return any kminus-related processes still running."""
    return _snapshot()
