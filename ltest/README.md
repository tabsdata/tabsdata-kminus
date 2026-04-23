<!--
  Copyright 2026 Tabsdata Inc.
-->

# kminus Load Test

A Python-based load test framework for kminus. Generates random pod
workloads, dispatches them as K8s Jobs, monitors resource usage, and
produces a Markdown report. Tests are saved to a work directory and
can be replayed exactly.

## Quick Start

```bash
# Install dependencies
pip install -r ltest/requirements.txt

# IMPORTANT: raise the file-descriptor limit in every shell that will
# run the kminus stack or the test runner. pod-runner spawns many
# subprocesses per pod, and at moderate concurrency macOS's default
# (usually 256) causes "Too many open files (os error 24)" which
# surfaces as unexplained pod failures (BackoffLimitExceeded).
ulimit -n 65536

# Start the kminus stack (in a shell that has the raised ulimit)
./target/darwin-arm64/km --workdir /tmp/km-ltest

# Generate and run a load test (saves to run1/)
python ltest/runner.py \
    --test ltest/test-input-100.yaml \
    --kubeconfig /tmp/km-ltest/config/kubeconfig.yaml \
    --workdir run1

# Replay the exact same test
python ltest/runner.py \
    --replay run1 \
    --kubeconfig /tmp/km-ltest/config/kubeconfig.yaml
```

## Usage

```
# Generate and run:
python runner.py --test <input.yaml> --kubeconfig <path> [--workdir <dir>]

# Replay a previous run:
python runner.py --replay <workdir> --kubeconfig <path>
```

| Flag | Description |
|------|-------------|
| `--test` | Test input YAML (generates pods, saves to workdir) |
| `--replay` | Replay from a saved workdir (exact same pods and order) |
| `--kubeconfig` | Kubeconfig for the target cluster (required) |
| `--node-name` | VK node name (default: `tdknode`) |
| `--workdir` | Directory for pods and plan (default: `ltest-run-<timestamp>/`) |

## Work Directory

On `--test`, the runner saves everything needed for replay:

```
run1/
├── plan.yaml                  # Dispatch order, delays, expected outcomes
├── pods/                      # Individual pod spec YAML files
│   ├── ltest-0000-ab12cd.yaml
│   ├── ltest-0001-ef34gh.yaml
│   └── ...
├── report-20260411T153000.md  # Report from first run
└── report-20260411T160000.md  # Report from replay
```

On `--replay`, the runner loads `plan.yaml` and the pod specs from
`pods/`, dispatches them in the exact same order at the same rate,
and writes a new timestamped report to the same directory.

## Test Input File

See `test-input-sample.yaml` for a complete example.

```yaml
pod_count: 50
dispatch_rate: 10

containers:    { min: 1, max: 3 }
init_containers: { min: 0, max: 2 }
sidecars:      { min: 0, max: 1 }
duration:      { min: 1, max: 10 }

failure_probability: 0.1
probe_probability: 0.3
test_timeout: 300
monitor_interval: 2
```

## Report

The Markdown report includes:

| Section | Content |
|---------|---------|
| Configuration | Test parameters, mode (generate/replay) |
| Summary | Succeeded/failed/timed-out, phase accuracy, timing |
| Timings | Per-pod aggregate stats for the three metrics |
| Slowest pods | Top 10 by `actual_time` |
| Phase Mismatches | One row per pod whose phase disagrees with the plan, with `fail_container` + `fail_exit_code`, `unterminated` containers, pod phase/reason, and Job failure reason/message |

The three per-pod metrics:

- **nominal_time** — expected execution time computed from the plan
  (inits sequential + max main; truncated when an init fails so
  mains never run).
- **actual_time** — first container `startedAt` → last container
  `finishedAt`, from pod status. Sidecars are excluded.
- **start_latency** — client submission → first container `startedAt`.

## Prerequisites

- Python 3.12+
- `pip install -r ltest/requirements.txt`
- A running kminus stack with accessible kubeconfig
- `ulimit -n 65536` in the shell running `km` *and* in the shell
  running `runner.py` (see Quick Start)