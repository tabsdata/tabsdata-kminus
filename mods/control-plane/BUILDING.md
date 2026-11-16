<!--
  Copyright 2026 Tabsdata Inc.
-->

# control-plane Build

The control-plane binary requires four Kubernetes binaries at runtime:
**kube-apiserver**, **kube-controller-manager**, **etcd**, and **kubectl**.

These are fetched automatically during `cargo make build` (run either from
the project root or from `mods/control-plane/`) and cached locally so
subsequent builds don't re-download or re-compile them.

## Cache Layout

The cache lives at `mods/control-plane/cache/`. Only the host platform
folder is populated — there is no cross-compilation:

```
mods/control-plane/cache/
├── k8s-source/                   K8s source (cloned once, shared)
│   └── kubernetes/
└── <os>-<arch>/                  host-platform binaries (e.g. darwin-arm64)
    ├── kube-apiserver
    ├── kube-controller-manager
    ├── etcd
    └── kubectl
```

After `cargo make build` succeeds, the four binaries are copied into
`mods/control-plane/target/` alongside the `control-plane` binary.

## How Binaries Are Obtained

| Binary                    | Source                                                                                                                                                  |
|---------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| kube-apiserver            | Downloaded via [setup-envtest](https://pkg.go.dev/sigs.k8s.io/controller-runtime/tools/setup-envtest)                                                   |
| etcd                      | Downloaded via setup-envtest                                                                                                                            |
| kubectl                   | Downloaded via setup-envtest                                                                                                                            |
| kube-controller-manager   | Built from source — the Kubernetes repo is shallow-cloned into `cache/k8s-source/kubernetes/` and the binary is compiled with `CGO_ENABLED=0`           |

The K8s version is configured by `K8S_VERSION` in the root `Makeroot.toml`
(default `1.29`). setup-envtest resolves it to the latest patch release
(e.g. `v1.29.5`), and the K8s source clone uses the same exact tag.

`make/make-km/scripts/fetch_k8s_deps.py` detects the host OS / arch automatically
(`darwin-arm64`, `linux-amd64`, …) and skips work entirely when the cache
already contains the binaries for that platform.

## Why `control-plane` Is Built Without cgo

The build sets `CGO_ENABLED=0` for the `control-plane` binary itself, as it
does for `virtual-kubelet`, so that both binaries are statically linked and
need no C toolchain on any build host.

`control-plane` uses **gopsutil v4**, which calls macOS system libraries through
`ebitengine/purego` rather than cgo. Do not go back to gopsutil v3: it pulls in
`github.com/shoenig/go-m1cpu`, whose cgo package initializer null-dereferences
on Apple Silicon generations that no longer publish its IORegistry keys,
killing the binary before `main()` runs on some hosts only.

Note this is separate from `kube-controller-manager`, which is compiled with
`CGO_ENABLED=0` for static-linking reasons and is unaffected by the above.

## Cache Behavior

- **`cargo make build`** — fetches binaries for the current platform if not
  cached, then copies them into `mods/control-plane/target/`.
- **`cargo make assembly`** — the top-level `assembly` task copies the
  cached host-platform K8s binaries into the root `target/dist/` along
  with the built module binaries.
- **`cargo make clean`** — removes `mods/control-plane/target/` but
  **preserves** the cache.
- **`cargo make clean -- --all`** — removes both `target/` and `cache/`.

The cache is gitignored and safe to delete at any time; the next build
will re-populate it.

## Prerequisites

- **Go** — any installed version; the build sets `GOTOOLCHAIN=go1.26.11`
  (`TABSDATA_GO_VERSION` in `make/make-km/fragments/shared/Makeroot.toml`,
  which os and ee load too), so Go runs exactly
  1.26.11, downloading it once if the installed version differs
- **Git** — for cloning the Kubernetes source
- **setup-envtest** — installed automatically on first build via
  `go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.19`
