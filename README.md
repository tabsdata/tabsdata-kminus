<!--
  Copyright 2026 Tabsdata Inc.
-->

# KMinus

A lightweight, local Kubernetes environment that runs entirely on a single
machine without containers or VMs. kMinus bundles a real Kubernetes control
plane (API server, etcd, controller-manager) with a virtual kubelet that
executes pods as native processes, providing a fast and minimal Kubernetes
experience for development and testing.

## Components

- **control-plane** (Go) — Starts a local Kubernetes control plane using
  envtest (kube-apiserver + etcd) and kube-controller-manager. Generates
  kubeconfigs for admin access, virtual kubelet, and the controller manager.

- **virtual-kubelet** (Go) — A Kubernetes node provider that registers as a
  node in the cluster and runs pod workloads as native processes on the host
  instead of in containers.

- **pod-runner** (Rust) — The execution runtime that manages pod lifecycles
  as native host processes.

- **kminus** (Rust) — The distribution binary (`km`) that ties all
  components together and orchestrates them as child processes.

## Quick Start

```
make build
```

Binaries are placed under `target/<platform>/`. To run the full stack:

```
./target/<platform>/km
# in another shell:
export KUBECONFIG=./config/kubeconfig.yaml
kubectl apply -f my-job.yaml
```

See [BUILDING.md](BUILDING.md) for the full build system
documentation, and [docs/](docs/) for component design docs.

## License

Copyright 2026 Tabsdata Inc.
