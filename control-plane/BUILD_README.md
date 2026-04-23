<!--
  Copyright 2026 Tabsdata Inc.
-->

# control-plane Build

The control-plane binary requires several Kubernetes binaries at runtime:
**kube-apiserver**, **kube-controller-manager**, **etcd**, and **kubectl**.

These are fetched automatically during `make build` and cached locally so
subsequent builds don't re-download or re-compile them.

## Cache Layout

```
control-plane/cache/
├── k8s-source/                          K8s source (shared, cloned once)
│   └── kubernetes/
├── darwin-arm64/                        Per-platform pre-built binaries
│   ├── kube-apiserver
│   ├── kube-controller-manager
│   ├── etcd
│   └── kubectl
├── linux-amd64/
│   └── ...
└── windows-amd64/
    └── ...
```

## How Binaries Are Obtained

| Binary | Source |
|--------|--------|
| kube-apiserver | Downloaded via [setup-envtest](https://pkg.go.dev/sigs.k8s.io/controller-runtime/tools/setup-envtest) |
| etcd | Downloaded via setup-envtest |
| kubectl | Downloaded via setup-envtest |
| kube-controller-manager | Built from source — K8s repo is shallow-cloned and the binary is compiled with `CGO_ENABLED=0` per target platform |

The K8s version is determined by the `K8S_VERSION` variable in the Makefile
(default: `1.29`). setup-envtest resolves this to the latest patch release
(e.g., `v1.29.5`), and the same version is used to clone the K8s source.

## Cache Behavior

- **`make build`** — fetches binaries for the current platform if not cached,
  then copies them to `target/bin/` alongside the control-plane binary.
- **`make assembly`** — fetches for all target platforms, copies to each
  platform's assembly directory.
- **`make clean`** — removes build output but **preserves the cache**.
- **`make clean-all`** — removes build output **and** the cache.

The cache is gitignored and safe to delete at any time. It will be
re-populated on the next build.

## Prerequisites

- **Go 1.23+** — for building control-plane and kube-controller-manager
- **Git** — for cloning the Kubernetes source
- **setup-envtest** — installed automatically on first build via
  `go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.19`
