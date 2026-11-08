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

The build sets `CGO_ENABLED=0` for the `control-plane` binary itself. This is a
workaround, not a design choice: `gopsutil` v3 pulls in the indirect dependency
`github.com/shoenig/go-m1cpu`, whose cgo package initializer reads M1-era
IORegistry keys (`voltage-states5-sram`) and null-dereferences on Apple Silicon
generations that no longer publish them — killing the binary before `main()`
runs. Disabling cgo drops that initializer from the build and lets `gopsutil`
take its pure-Go paths.

The clean fix is to upgrade to **gopsutil v4**, which removed `go-m1cpu`
entirely in favour of `ebitengine/purego` and needs no cgo. The `process` API
used by `control-plane` is unchanged between v3 and v4, so the migration is an
import-path bump; once done, `CGO_ENABLED=0` can be dropped from
`make/make-km/fragments/tasks/build_go.toml`.

Two things worth knowing before revisiting this:

- **Upgrading the Go toolchain does not help.** The offending cgo initializer is
  still compiled and still crashes, independent of Go version. (The build
  already runs exactly Go 1.26.10, through `GOTOOLCHAIN`.)
- **The crash is host-specific.** It does not reproduce on every Apple Silicon
  machine, so a successful local build is not evidence the problem is gone.

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

- **Go** — any installed version; the build sets `GOTOOLCHAIN=go1.26.10`
  (`TABSDATA_GO_VERSION` in `make/make-km/fragments/shared/Makeroot.toml`,
  which os and ee load too), so Go runs exactly
  1.26.10, downloading it once if the installed version differs
- **Git** — for cloning the Kubernetes source
- **setup-envtest** — installed automatically on first build via
  `go install sigs.k8s.io/controller-runtime/tools/setup-envtest@release-0.19`
