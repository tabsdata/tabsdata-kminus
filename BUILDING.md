<!--
  Copyright 2026 Tabsdata Inc.
-->

# kMinus — Build System

The build is driven by [cargo-make](https://github.com/sagiegurari/cargo-make).
Tasks are defined in `Maketask.toml` (root) and per module under
`mods/<m>/Maketask.toml`, with shared fragments under `make/make-km/` (each
module keeps its own under `mods/<m>/make/make-km/`).

## Project Layout

```
├── Makefile.toml             cargo-make entry point (extends Makeroot / Maketask)
├── Makeroot.toml             root environment (env files, scripts, hooks)
├── Maketask.toml             top-level tasks (build / test / clean / assembly / ...)
├── make/
│   └── make-km/
│       ├── fragments/        task / hook / plugin fragments; standalone/ is km's own
│       │                     make root; shared/ and the exported tasks/ are loaded by os/ee
│       ├── libraries/        duckscript helpers (log.ds, term.py)
│       ├── resources/        banners, fonts, images
│       ├── scripts/          build-time scripts (e.g. fetch_k8s_deps.py)
│       ├── settings/         *.env defaults loaded by Makeroot
│       └── tests/            tests of the build-time scripts
├── mods/
│   ├── control-plane/        Go — Kubernetes control plane
│   ├── virtual-kubelet/      Go — Virtual Kubelet provider
│   ├── pod-runner/           Rust — pod execution runtime
│   └── kminus/               Rust — distribution binary (`km`)
├── ltest/                    Python load-test harness
└── test/                     shared pytest helpers (pytest-noop.sh, etc.)
```

Each module produces its own `target/` next to its sources, and the root
`target/dist/` is the bundled output of `assembly`:

```
mods/control-plane/target/
  ├── control-plane                Go binary (current platform, native)
  ├── kube-apiserver               ┐
  ├── kube-controller-manager      │ K8s deps copied from
  ├── etcd                         │ mods/control-plane/cache/
  └── kubectl                      ┘
mods/virtual-kubelet/target/
  └── virtual-kubelet              Go binary
mods/pod-runner/target/<profile>/
  └── pod-runner                   Rust binary (debug / release / assembly)
mods/kminus/target/<profile>/
  └── km                           Rust binary

target/
  └── dist/                        collected binaries from `assembly`
       ├── control-plane
       ├── virtual-kubelet
       ├── pod-runner
       ├── km
       ├── kube-apiserver
       ├── kube-controller-manager
       ├── etcd
       └── kubectl
```

Builds are always **native** to the current platform — cross-compilation
is intentionally not supported.

## Tasks

Run from the repo root for the full workspace, or from a module folder
(`mods/<m>/`) to operate on that module only.

| Task                          | Description                                                                                  |
|-------------------------------|----------------------------------------------------------------------------------------------|
| `cargo make build`            | Compile all modules (debug profile by default)                                               |
| `cargo make test`             | Run unit tests (Go: `go test ./...`, Rust: `cargo nextest run --all-targets`)                |
| `cargo make itest`            | Build, then run integration tests (no-op for modules without an itest dir)                   |
| `cargo make clean`            | Remove `target/` for every module and the root `target/`                                     |
| `cargo make clean -- --all`   | Same as `clean`, plus remove the control-plane K8s cache                                     |
| `cargo make lock`             | Regenerate Cargo lockfiles for the Rust modules; no-op for the Go modules                    |
| `cargo make assembly`         | Build all modules and gather binaries into `target/dist/`                                    |
| `cargo make permit`           | Verify the required language toolchains (Python / Rust / Go) are available                   |

### Optimized builds

There is no separate `release` task. The optimized profile is selected via
cargo-make's `--profile`:

```
cargo make --profile assembly build
cargo make --profile assembly assembly
```

The `assembly` profile applies:

- Go: `-ldflags=-s -w -trimpath`
- Rust: the Cargo `assembly` profile, and the RUSTFLAGS loaded from
  `.cargo/rustflags-<os>.cfg`

Without `--profile assembly`, builds run in `dev` (debug) mode and
`assembly` will collect debug binaries into `target/dist/`.

### Per-module tasks

Each `mods/<m>/Maketask.toml` exposes the same task names as aliases for
the corresponding module-only task:

```
cd mods/control-plane && cargo make build      # build only the control-plane
cd mods/pod-runner    && cargo make test       # test only pod-runner
```

## Build Order

Dependencies are expressed through cargo-make task dependencies; modules
that have no inter-dependencies build in parallel:

1. **control-plane** and **virtual-kubelet** — Go, independent
2. **pod-runner** — Rust
3. **kminus** — Rust, brings everything together
4. **assembly** — collects all module binaries plus the cached K8s deps into
   `target/dist/`

## Prerequisites

- **Go** 1.23+
- **Rust** 1.94+ (edition 2024) and `cargo-nextest`
- **Python** 3.12+

The Python environment is the unified **tabsdata-os dev environment** —
this project does not maintain its own `requirements.txt`. Activate the
tabsdata-os dev environment before running build / test / itest tasks.
`cargo make permit` verifies that all three toolchains are reachable from
`PATH`.

## Notes

- Several settings in `Makeroot.toml` resolve under `${TABSDATA_ROOT}`, the
  sibling `tabsdata-os/` checkout. Helper scripts such as
  `check_native_libs.py` and `bump_version.py` live there and are invoked
  during the Rust build steps, so this project expects to live next to a
  tabsdata-os checkout.
- Rust builds use `cargo build --locked --workspace --all-features --bins
  --examples`; a stale `Cargo.lock` will fail the build.
- See [`mods/control-plane/BUILDING.md`](mods/control-plane/BUILDING.md)
  for details on how the control-plane fetches and caches its K8s
  dependencies.
