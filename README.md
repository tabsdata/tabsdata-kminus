<!--
  Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <img src="assets/images/tabsdata-logo-for-light-theme.svg#gh-light-mode-only" alt="Tabsdata" width="500">
    <img src="assets/images/tabsdata-logo-for-dark-theme.svg#gh-dark-mode-only" alt="Tabsdata" width="500">
</div>

<div align="center">
    <a href="https://tabsdata.com">Tabsdata</a> |
    <a href="https://github.com/tabsdata/tabsdata">tabsdata</a> |
    <a href="https://github.com/tabsdata/tabsdata-kubernetes">GitHub Repository</a>
</div>

# KMinus

A lightweight, local Kubernetes environment that runs entirely on a single machine, without
containers or virtual machines. The **kminus** stack bundles a real Kubernetes control plane (API
server, etcd and controller manager) with a virtual kubelet that runs pods as native processes,
which gives a fast and minimal Kubernetes for development and testing.

It holds the orchestration components of [tabsdata](https://github.com/tabsdata/tabsdata):
native-process pods (`km`). Every local tabsdata instance runs on it.

## Components

| Component | Language | What it does |
|---|---|---|
| control‑plane | Go | Starts a local Kubernetes control plane<br>Runs kube‑apiserver, etcd, kube‑controller‑manager<br>Writes the kubeconfigs of the administrator and the components |
| virtual‑kubelet | Go | Registers as a node in the cluster<br>Runs the pods as native processes on the host, not in containers |
| pod‑runner | Rust | Manages the lifecycle of each pod as native host processes |
| kminus | Rust | The distribution binary, `km`<br>Starts the other components as child processes<br>Orchestrates them |

# Running kminus

Build **kminus** first, as [Developing KMinus](DEVELOPING.md) describes. The binaries land in
`target/dist`.

Create an instance in a folder that doesn't exist yet.

macOS and Linux:

```bash
./target/dist/km create --workdir ~/km-demo
```

Windows:

```powershell
.\target\dist\km.exe create --workdir $HOME\km-demo
```

Start it. The command stays in the foreground until you stop the instance, so leave this
terminal open and use another one for the next steps.

macOS and Linux:

```bash
./target/dist/km start --workdir ~/km-demo
```

Windows:

```powershell
.\target\dist\km.exe start --workdir $HOME\km-demo
```

Point **kubectl** at the instance, and apply a manifest.

macOS and Linux:

```bash
export KUBECONFIG=~/km-demo/config/kubeconfig.yaml
```

```bash
./target/dist/kubectl apply -f examples/deployment.yaml
```

Windows:

```powershell
$env:KUBECONFIG = "$HOME\km-demo\config\kubeconfig.yaml"
```

```powershell
.\target\dist\kubectl.exe apply -f examples\deployment.yaml
```

Check the instance, and stop it when you're done.

macOS and Linux:

```bash
./target/dist/km status --workdir ~/km-demo
```

```bash
./target/dist/km stop --workdir ~/km-demo
```

Windows:

```powershell
.\target\dist\km.exe status --workdir $HOME\km-demo
```

```powershell
.\target\dist\km.exe stop --workdir $HOME\km-demo
```

The [examples](examples/README.md) explain the conventions a pod must follow to run on **kminus**.

# Building kminus from Source

The **kminus** stack is meant to be built and used together with
[tabsdata](https://github.com/tabsdata/tabsdata), checked out side by side. Its
[Developing Tabsdata](https://github.com/tabsdata/tabsdata/blob/main/DEVELOPING.md) guide covers
the software to install, the clone, the build environment and the toolchain.

[Developing KMinus](DEVELOPING.md) adds what is specific to this repository: its layout, its
tasks, its build profiles and its tests.

# Design Documents

- [Control Plane](docs/ControlPlane.md)
- [Virtual Kubelet](docs/VirtualKubelet.md)
- [Pod Runner: User Guide](docs/PodRunnerUserGuide.md)
- [Pod Runner: Implementation](docs/PodRunnerImpl.md)
- [KMinus: Component Messages](docs/KmComponentMessages.md)
- [KMinus: Readiness and Liveness](docs/KmReadinessLiveness.md)

# License

Licensed under the [Apache License, Version 2.0](LICENSE).
