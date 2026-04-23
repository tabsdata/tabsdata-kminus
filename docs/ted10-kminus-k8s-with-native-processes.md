<!--
  Copyright 2026 Tabsdata Inc.
-->

# TED10: KMinus (K-) (DRAFT)

* **Author(s)**: tucu

* **Date**: 2026-APR-06

* **Obsoleted on**: N/A

* **Relates**:
    * N/A

  ---

## TL;DR

KMinus (K-) is a single-host, lightweight multi-platform (macOS, Linux, Windows) Kubernetes system that runs pods as
native OS processes instead of containers. It unifies the execution model from development laptops to production
clusters by providing a real Kubernetes control plane without requiring Docker, VMs, or administrator privileges.

## Context

Tabsdata workers are defined as Kubernetes pods. In production these run on multi-node Kubernetes clusters. During
development, however, developers need to run the same pod definitions on their local machines without the overhead of a
full cluster, container runtime, or elevated privileges.

KMinus replaces the previous Tabsdata process supervisor with a system that accepts standard Kubernetes manifests and
executes them locally.

Tabsdata user' experience does not change for the existing public interfaces (CLI, UI, Python).

## Design

KMinus is composed of three main components orchestrated by a coordinator entry point (`km`):

### control-plane

A Go application that starts and manages three vanilla Kubernetes binaries sourced from the official `envtest` build (
the same binaries used for K8s integration testing):

- **kube-apiserver** -- standard Kubernetes API server
- **kube-controller-manager** -- handles Job and CronJob reconciliation (built from K8s v1.29.0 source)
- **etcd** -- key-value store for cluster state

These are unmodified upstream binaries, ensuring full compatibility with Kubernetes API.

### pod-runner

A Rust application that executes Kubernetes pods as native OS processes. It exposes a REST/HTTPS API with mTLS and 
manages the full pod lifecycle: spec validation, environment/secret/configmap resolution, volume setup, process 
execution with init containers and sidecars, health probes (exec, httpGet, tcpSocket), log rotation, restart 
policies, graceful shutdown, and Prometheus metrics.

### virtual-kubelet

A Go application implementing the Virtual Kubelet provider interface. It registers a virtual node with the Kubernetes
API server and bridges pod lifecycle operations to the pod-runner via REST/HTTPS with mTLS authentication.

This is the same pattern used by Azure ACI and AWS Fargate (which use Virtual Kubelet) -- Kubernetes sees a
regular node and schedules pods to it, and the virtual-kubelet translates those operations into pod-runner
API calls.

### Supported Kubernetes Subset

| Feature            | Support                               |
|--------------------|---------------------------------------|
| Workload types     | Pods, Jobs, CronJobs                  |
| Container images   | Local directories (no registry pulls) |
| Init containers    | Sequential; sidecars (restartPolicy: Always) run async |
| Restart policies   | Never, OnFailure                      |
| Volumes            | hostPath (Directory), emptyDir        |
| Secrets/ConfigMaps | Environment variables only            |
| Probes             | exec, httpGet, tcpSocket              |
| Networking         | Host network only                     |
| Metrics            | Prometheus on /metrics endpoint       |
| Security           | TLS/mTLS with auto-generated PKI      |

Not supported: Deployments, StatefulSets, DaemonSets, PVCs, container image pulls, grpc probes, network policies,
resource limit enforcement, security context enforcement.

### Usage Example

```bash
km &                                         # starts control-plane, pod-runner, virtual-kubelet
export KUBECONFIG=./config/kubeconfig.yaml

kubectl apply -f my-job.yaml                 # standard kubectl works
kubectl get pods
kubectl logs my-pod

kill %1                                      # graceful shutdown (SIGTERM)
```

##  Trade-offs

- No container isolation -- all processes run directly on the host OS, sharing the filesystem and network.
- Subset of K8s features -- only Jobs, CronJobs, and bare Pods are supported. This covers Tabsdata's requirements.
- Single-host only -- by design, Kminus targets one machine. Multi-node scheduling is deferred to production Kubernetes.

## Appendix 1: PoC with Python and Rust Pod Runner Implementations

Available at [Github tabsdata/kminus-poc](https://github.com/tabsdata/kminus-poc)
