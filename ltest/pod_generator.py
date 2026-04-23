#!/usr/bin/env python3
#
# Copyright 2026 Tabsdata Inc.
#
"""Pod definition generator for load testing.

Generates random pod specs based on a config descriptor. Each pod gets:
- Variable number of regular containers, init containers, and sidecars
- Configurable failure probability per container
- Random durations and exit codes
- Optional exec readiness probes
- Expected completion status and nominal execution time

Output: a list of PodPlan objects, each containing the pod spec dict
and expected outcomes.
"""

import random
import uuid
from dataclasses import dataclass, field


@dataclass
class ContainerPlan:
    name: str
    duration_secs: int
    exit_code: int
    has_probe: bool
    is_init: bool = False
    is_sidecar: bool = False


@dataclass
class PodPlan:
    """A generated pod with its expected outcomes."""
    name: str
    uid: str
    spec: dict
    containers: list[ContainerPlan] = field(default_factory=list)
    expected_phase: str = "Succeeded"
    nominal_time_secs: float = 0.0


def _make_command(duration_secs: int, exit_code: int) -> list[str]:
    """Build a shell command that runs for duration_secs then exits."""
    if exit_code == 0:
        return ["sh", "-c", f"sleep {duration_secs}"]
    else:
        return ["sh", "-c", f"sleep {duration_secs} && exit {exit_code}"]


def _make_probe(cfg: dict) -> dict | None:
    """Optionally create an exec readiness probe."""
    return {
        "exec": {"command": ["true"]},
        "initialDelaySeconds": cfg.get("probe_initial_delay", 0),
        "periodSeconds": cfg.get("probe_period", 1),
        "timeoutSeconds": 5,
        "successThreshold": 1,
        "failureThreshold": 3,
    }


def _rand_range(section: dict) -> int:
    return random.randint(section["min"], section["max"])


def generate_pods(cfg: dict) -> list[PodPlan]:
    """Generate pod plans from config."""
    pod_count = cfg["pod_count"]
    failure_prob = cfg.get("failure_probability", 0.1)
    probe_prob = cfg.get("probe_probability", 0.3)

    plans = []

    for i in range(pod_count):
        pod_name = f"ltest-{i:04d}-{uuid.uuid4().hex[:6]}"
        pod_uid = f"uid-{pod_name}"

        n_containers = _rand_range(cfg["containers"])
        n_inits = _rand_range(cfg["init_containers"])
        n_sidecars = _rand_range(cfg["sidecars"])

        container_plans = []
        init_specs = []
        sidecar_specs = []
        container_specs = []

        # Init containers (sequential)
        for j in range(n_inits):
            dur = _rand_range(cfg["duration"])
            fail = random.random() < failure_prob
            exit_code = random.choice([1, 2, 137]) if fail else 0
            has_probe = False  # init containers don't get probes
            cp = ContainerPlan(
                name=f"init-{j}",
                duration_secs=dur,
                exit_code=exit_code,
                has_probe=has_probe,
                is_init=True,
            )
            container_plans.append(cp)
            init_specs.append({
                "name": cp.name,
                "image": "/dev/null",
                "command": _make_command(dur, exit_code),
            })

        # Sidecars (init containers with restartPolicy: Always)
        for j in range(n_sidecars):
            cp = ContainerPlan(
                name=f"sidecar-{j}",
                duration_secs=0,  # runs forever until main exits
                exit_code=0,
                has_probe=False,
                is_sidecar=True,
            )
            container_plans.append(cp)
            sidecar_specs.append({
                "name": cp.name,
                "image": "/dev/null",
                "command": ["sh", "-c", "trap exit TERM; while true; do sleep 3600 & wait $!; done"],
                "restartPolicy": "Always",
            })

        # Regular containers (parallel)
        for j in range(n_containers):
            dur = _rand_range(cfg["duration"])
            fail = random.random() < failure_prob
            exit_code = random.choice([1, 2, 137]) if fail else 0
            has_probe = random.random() < probe_prob
            cp = ContainerPlan(
                name=f"app-{j}",
                duration_secs=dur,
                exit_code=exit_code,
                has_probe=has_probe,
            )
            container_plans.append(cp)
            c_spec = {
                "name": cp.name,
                "image": "/dev/null",
                "command": _make_command(dur, exit_code),
            }
            if has_probe:
                c_spec["readinessProbe"] = _make_probe(cfg)
            container_specs.append(c_spec)

        # Nominal time (failure-aware) and expected phase.
        # Inits run sequentially; if any fails, the pod stops there and
        # mains never run. Otherwise all mains run in parallel (with
        # restartPolicy=Never) until all have terminated; the pod waits
        # for the longest one regardless of whether any failed.
        nominal_time = 0.0
        init_fail = False
        for c in container_plans:
            if not c.is_init:
                continue
            nominal_time += c.duration_secs
            if c.exit_code != 0:
                init_fail = True
                break
        mains = [c for c in container_plans
                 if not c.is_init and not c.is_sidecar]
        main_fail = any(c.exit_code != 0 for c in mains)
        if not init_fail and mains:
            nominal_time += max(c.duration_secs for c in mains)
        expected_phase = "Failed" if (init_fail or main_fail) else "Succeeded"

        # Build pod spec
        all_inits = sidecar_specs + init_specs  # sidecars are init containers
        pod_spec = {
            "metadata": {
                "name": pod_name,
                "namespace": "default",
                "uid": pod_uid,
            },
            "spec": {
                "restartPolicy": "Never",
                "containers": container_specs,
            },
        }
        if all_inits:
            pod_spec["spec"]["initContainers"] = all_inits

        plans.append(PodPlan(
            name=pod_name,
            uid=pod_uid,
            spec=pod_spec,
            containers=container_plans,
            expected_phase=expected_phase,
            nominal_time_secs=nominal_time,
        ))

    # Shuffle to dispatch in random order
    random.shuffle(plans)
    return plans
