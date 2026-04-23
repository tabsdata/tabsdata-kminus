<!--
  Copyright 2026 Tabsdata Inc.
-->

# kminus Load Test

A load test framework for kminus that generates random pod workloads,
dispatches them, monitors execution, and produces detailed reports
comparing expected vs actual behavior.

## Testing Strategy

The load test validates that kminus correctly executes pods within
expected time bounds. Each test run:

1. **Generates** a set of pods with randomized configurations (container
   count, init containers, sidecars, durations, failure probability).
2. **Dispatches** all pods at a configurable rate.
3. **Monitors** pod status until all reach a terminal state or timeout.
4. **Verifies** that each pod's final phase (Succeeded/Failed) matches
   the expected phase based on container exit codes.
5. **Measures** execution overhead by comparing actual duration against
   nominal (expected) duration at both the pod level and the test level.

### Pod Nominal Time

Each pod has a **nominal time** — the expected wall-clock duration if
there were zero overhead:

```
pod_nominal = sum(init_container_durations) + max(container_durations)
```

- **Init containers** run sequentially, so their durations are summed.
- **Regular containers** run concurrently, so only the longest matters.
- **Sidecars** are excluded — they run alongside main containers and
  are terminated after all main containers exit.

### Test Timing

| Metric | Formula | Description |
|--------|---------|-------------|
| **Dispatch time** | `pod_count / dispatch_rate` | Time to submit all pods |
| **Max pod nominal** | `max(pod_nominal for all pods)` | Longest single pod |
| **Expected test time** | `dispatch_time + max_pod_nominal` | Expected wall-clock for the test (all pods run concurrently) |
| **Wall time** | *(measured)* | Actual wall-clock from first dispatch to last completion |
| **Test overhead** | `wall_time - expected_test_time` | Extra time beyond nominal |
| **Timeout** | `dispatch_time + 2 * max_pod_nominal` | Deadline after which pending pods are marked as timed out |

### Per-Pod Overhead

For each completed pod, overhead is measured two ways:

| Source | Formula | What it captures |
|--------|---------|------------------|
| **Runner** | `runner_completion - runner_dispatch - pod_nominal` | Includes polling latency (~0.3-0.5s) |
| **Server** | `server_duration - pod_nominal` | Pod-runner or K8s timestamps — no polling latency |

Server-measured overhead isolates the execution engine overhead (process
spawn, sidecar termination, status sync) from the test harness overhead.

### Start Latency

Time between pod submission and first observation of `Running` phase.
In K8s mode this includes scheduling; in PR mode it reflects process
spawn time.

## Modes

The runner supports two modes for submitting pods:

### PR Mode (`--mode pr`)

Starts a pod-runner process directly and submits pods via its
HTTPS/mTLS API. No Kubernetes cluster needed. The runner:

- Starts `pod-runner` with auto-generated PKI (all resolvers set to `none`).
- Reads `vk-prconfig.yaml` to obtain mTLS client certificates.
- Submits pods to `POST /createPod` and polls `GET /getPodStatus`.
- Computes server duration from container `startedAt`/`finishedAt` timestamps.

This mode measures pod-runner overhead in isolation, without K8s
scheduling, virtual-kubelet status sync, or Job controller latency.

### K8s Mode (`--mode k8s`)

Submits pods as Kubernetes Jobs to an existing cluster via the
Kubernetes API. Requires `--kubeconfig` pointing to a running kminus
stack. The runner:

- Wraps each pod spec in a K8s Job with `nodeName` targeting the VK node.
- Polls Job status via the Batch API.
- Uses Job `startTime`/`completionTime` for server duration.

This mode measures end-to-end overhead including K8s scheduling,
virtual-kubelet status sync (5s polling), and Job controller latency.

## Usage

### Prerequisites

```bash
pip install -r ltest/requirements.txt   # psutil, pyyaml, kubernetes
```

For PR mode, the `pod-runner` binary must be built:

```bash
cd new1 && make build   # builds target/darwin-arm64/pod-runner
```

For K8s mode, a running kminus stack is needed:

```bash
./target/darwin-arm64/km --workdir /tmp/km-ltest
```

### 1. Generate a Test Plan

```bash
python ltest/runner.py --test ltest/test-input-sample.yaml \
    --mode pr --workdir run1/
```

This generates random pod specs based on the input YAML, saves them to
`run1/pods/` and the dispatch plan to `run1/plan.yaml`, then runs the
test.

### 2. Run with an Existing Plan (Replay)

```bash
python ltest/runner.py --replay run1/ --mode pr
```

Replay loads `plan.yaml` and the saved pod specs from `run1/pods/`,
dispatches them in the exact same order at the same rate, and writes a
new timestamped report to the same directory. This allows comparing
results across code changes with identical workloads.

### 3. K8s Mode

```bash
# Generate and run against a live cluster:
python ltest/runner.py --test ltest/test-input-sample.yaml \
    --mode k8s --kubeconfig /tmp/km-ltest/config/kubeconfig.yaml

# Replay:
python ltest/runner.py --replay run1/ \
    --mode k8s --kubeconfig /tmp/km-ltest/config/kubeconfig.yaml
```

### 4. Regenerate Markdown from JSON

```bash
python ltest/runner.py --gen-report run1/report-20260411T130502.json
```

The JSON report is the source of truth. The markdown report is
generated from it. Use `--gen-report` to regenerate the markdown after
modifying the runner's formatting without rerunning the test.

### Command-Line Reference

| Flag | Description |
|------|-------------|
| `--test <input.yaml>` | Generate pods from input YAML and run |
| `--replay <workdir>` | Replay a saved test plan |
| `--gen-report <report.json>` | Regenerate markdown from a JSON report |
| `--mode pr\|k8s` | `pr` (default): direct pod-runner, `k8s`: via Kubernetes |
| `--kubeconfig <path>` | Kubeconfig (required for K8s mode) |
| `--node-name <name>` | VK node name for K8s mode (default: `tdknode`) |
| `--pod-runner <path>` | Pod-runner binary for PR mode (default: auto-detect) |
| `--workdir <dir>` | Work directory (default: `ltest-run-<timestamp>/`) |

## Test Input File

```yaml
pod_count: 50          # Total pods to generate
dispatch_rate: 10      # Max pods dispatched per second

containers:            # Regular containers per pod
  min: 1
  max: 3
init_containers:       # Init containers per pod
  min: 0
  max: 2
sidecars:              # Sidecar containers per pod
  min: 0
  max: 1
duration:              # Duration of each container's command (seconds)
  min: 1
  max: 10

failure_probability: 0.1   # Probability a container exits non-zero
probe_probability: 0.3     # Probability a container has a readiness probe
monitor_interval: 2        # Process monitoring sample interval (seconds)
```

## Work Directory

```
run1/
+-- plan.yaml                  # Dispatch order, timing, expected outcomes
+-- pods/                      # Individual pod spec YAML files
|   +-- ltest-0000-ab12cd.yaml
|   +-- ltest-0001-ef34gh.yaml
+-- _pod_runner/               # Pod-runner state (PR mode only)
+-- report-20260411T153000.md  # Markdown report
+-- report-20260411T153000.json # JSON report (machine-readable)
```

## Reports

Each test run produces two reports in the work directory:

### Markdown Report (`report-<timestamp>.md`)

Human-readable with tables:

| Section | Content |
|---------|---------|
| Configuration | Test parameters, mode, binary paths |
| Summary | Succeeded/failed/timed-out, phase accuracy |
| Timing | Expected vs actual test time, overhead |
| Overhead | Per-pod overhead stats (runner and server measured) |
| Start Latency | Dispatch to first Running observation |
| Slowest pods | Top 10 by actual time |
| All pods | Full table with containers, timing, correctness |
| Process monitoring | Peak/avg RSS, CPU, per-process memory |
| Orphan processes | Processes still running after test |

### JSON Report (`report-<timestamp>.json`)

Machine-readable with the same data:

```json
{
  "version": "0.1.0",
  "generated": "2026-04-11T19:54:41Z",
  "config": { "mode": "pr", "input": { ... } },
  "summary": {
    "total": 50,
    "succeeded": 45,
    "failed": 5,
    "expected_test_time_secs": 15.0,
    "wall_time_secs": 17.3,
    "test_overhead_secs": 2.3
  },
  "pods": [
    {
      "name": "ltest-0000-ab12cd",
      "nominal_time_secs": 5.0,
      "actual_time_secs": 5.1,
      "overhead_runner_secs": 0.1,
      "overhead_server_secs": 0.01,
      "phase_correct": true
    }
  ],
  "monitor": { ... },
  "orphans": []
}
```
