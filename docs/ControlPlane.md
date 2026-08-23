<!--
  Copyright 2026 Tabsdata Inc.
-->

# Control Plane

The control-plane binary (`control-plane`) runs a local Kubernetes control
plane on a single machine, without containers or VMs. It uses a modified
[envtest](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/envtest)
setup to start real Kubernetes components as native processes.

## How It Works

Standard envtest is designed for integration testing — it spins up a
temporary API server and etcd, runs tests, and tears down. The control
plane repurposes this machinery to run a persistent, usable single-machine
Kubernetes cluster:

- Configures envtest with custom API server flags (full API group
  enablement, service account issuer, OIDC audiences).
- Redirects subprocess output to rotating log files instead of
  discarding it.
- Starts kube-controller-manager as a separate process (envtest does
  not include it).
- Creates kubeconfigs with static bearer tokens for admin,
  virtual-kubelet, and the controller manager.
- Creates required namespaces (`kube-system`, `kube-node-lease`).
- Tracks all process PIDs for clean shutdown.

The kube-controller-manager runs with a limited set of controllers
(`--controllers=job,cronjob,ttl-after-finished,garbagecollector,deployment,replicaset`):
job and cronjob lifecycle, `Deployment` → `ReplicaSet` → `Pod` expansion,
deletion of finished jobs via `ttlSecondsAfterFinished`, and cascading
deletion of dependents via `ownerReferences`. Leader election is disabled
since this is a single-node setup.

There is **no scheduler** and the virtual-kubelet node carries a
`virtual-kubelet.io/provider:NoSchedule` taint, so workload pod templates
must pin themselves to the node with `spec.nodeName` (which bypasses
scheduling) and tolerate that taint. This applies to bare Pods as well as
the pod templates inside `Job`, `CronJob`, and `Deployment` objects. See
[`examples/deployment.yaml`](../examples/deployment.yaml) for a complete
Deployment manifest.

## Services Started

The control plane starts three Kubernetes components, in order:

| Service                     | Started By              | Default Port  |
|-----------------------------|-------------------------|---------------|
| **etcd**                    | envtest (child process) | `--etcd-port` |
| **kube-apiserver**          | envtest (child process) | 6443          |
| **kube-controller-manager** | control-plane (exec)    | 10257         |

All three binaries (`kube-apiserver`, `kube-controller-manager`, `etcd`)
must be present in the same directory as the `control-plane` binary.

## Tokens and Kubeconfigs

Client authentication uses static bearer tokens (kube-apiserver
`--token-auth-file`). The token file (`tokens.csv`) is generated with
random tokens on the first start and **reused on every later start**, so
the kubeconfigs are stable across restarts: a kubeconfig obtained during
one run keeps working after a restart, even if it was copied elsewhere
(e.g. imported into Headlamp or another client).

envtest still generates fresh serving certificates on every start, but
clients skip TLS verification (`insecure-skip-tls-verify: true`), so
this does not affect them.

If you need custom ports, pass them via CLI flags on each start. Note
that the API server host/port is embedded in the kubeconfigs, so they
are only stable across restarts that use the same port.

Under km none of this is manual: km allocates every port the stack binds from
its `--portstart`, etcd's two included (`portstart+5` client, `+6` peer), and
passes them in. Left at `0` the etcd ports fall back to envtest's random
allocation, which is what an instance created before km managed them does.

## Command-Line Options

| Flag            | Default             | Description                                                                         |
|-----------------|---------------------|-------------------------------------------------------------------------------------|
| `--port`        | `6443`              | kube-apiserver listen port                                                          |
| `--kcm-port`    | `10257`             | kube-controller-manager secure port                                                 |
| `--etcd-port`   | `0`                 | etcd client port; `0` lets envtest pick a random free one                           |
| `--etcd-peer-port` | `0`              | etcd peer port, bound but unused by a single node; `0` lets envtest pick             |
| `--workdir`     | `.`                 | Working directory — `data/control-plane/`, `config/`, `log/` are created under this |
| `--configdir`   | `<workdir>/config`  | Directory for generated kubeconfig and `cpsysconfig.yaml`                           |
| `--logdir`      | `<workdir>/log`     | Directory for rotating log files                                                    |
| `--logmaxsizemb`| `100`               | Max size of each log file in MB before rotation                                     |
| `--logmaxfiles` | `3`                 | Max number of log files to keep per component                                       |

## Directory Structure

### Data Directory (`<workdir>/data/control-plane/`)

Contains:

```
data/control-plane/
└── pids/                            PID files for all managed processes
    ├── <pid>.control-plane
    ├── <pid>.etcd
    ├── <pid>.kube-apiserver
    └── <pid>.kube-controller-manager
```

Each PID file is named `<pid>.<process-name>` and contains the process
ID as text. PID files are deleted individually as each process stops
during shutdown.

etcd also stores its data under this directory.

### Config Directory (`<workdir>/config/`)

Rewritten on each start (contents are stable across restarts on the
same port):

| File                  | Purpose                                                    |
|-----------------------|------------------------------------------------------------|
| `kubeconfig.yaml`     | Admin access (`system:masters` group)                      |
| `vk-kubeconfig.yaml`  | Virtual kubelet access (`system:masters` + `system:nodes`) |
| `kubeconfig-kcm.yaml` | kube-controller-manager access (internal)                  |

Each kubeconfig uses static bearer token authentication; the tokens come
from `tokens.csv` (see below), so file contents are identical across
restarts on the same port. TLS verification is skipped
(`insecure-skip-tls-verify: true`).

Persistent:

| File               | Purpose                                                     |
|--------------------|-------------------------------------------------------------|
| `cpsysconfig.yaml` | kube-controller-manager tunables (see below). Auto-created, user-editable. |
| `tokens.csv`       | Static auth tokens (`token,user,uid,"groups"` per line). Auto-created with random tokens on first start; delete it to rotate them. |

`cpsysconfig.yaml` fields:

```yaml
concurrentJobSyncs: 50   # kube-controller-manager --concurrent-job-syncs
kubeApiQps: 500          # --kube-api-qps
kubeApiBurst: 1000       # --kube-api-burst
```

### Log Directory (`<workdir>/log/`)

Rotating log files for each component:

| File                          | Source                                     |
|-------------------------------|--------------------------------------------|
| `control-plane.log`           | control-plane own log (klog-style format)  |
| `etcd.log`                    | etcd stdout/stderr                         |
| `kube-apiserver.log`          | kube-apiserver stdout/stderr               |
| `kube-controller-manager.log` | kube-controller-manager stdout/stderr      |

Logs are rotated using [lumberjack](https://github.com/natefinch/lumberjack).
When a log file reaches the max size, it is rotated and previous files are
kept up to the max file count. Rotation is not compressed.

## Startup and Shutdown

### Startup

1. Resolves working, config, and log directories.
2. Checks for PID files from a previous run — if matching processes are
   still running, they are killed and a warning is printed.
3. Validates that required binaries exist alongside it.
4. Checks that the API server and KCM ports are available.
5. Loads (or creates) `cpsysconfig.yaml` and `tokens.csv`.
6. Generates fresh serving certificates.
7. Starts etcd and kube-apiserver via envtest.
8. Writes PID files for all child processes.
9. Creates `kube-system` and `kube-node-lease` namespaces.
10. Waits for the API server to be ready (up to 15 seconds).
11. Generates kubeconfigs and starts kube-controller-manager.
12. Prints connection instructions and waits for `Ctrl+C` / `SIGTERM`.

### Shutdown

On `SIGTERM` or `Ctrl+C`:

1. Sends `SIGTERM` to kube-controller-manager and waits for exit.
2. Stops the envtest environment (terminates etcd and kube-apiserver).
3. Verifies all child processes are dead (force-kills with `SIGKILL`
   after 5 seconds if needed), then deletes their PID files.
4. Deletes own PID file and removes the pids directory.
