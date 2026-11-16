<!--
Copyright 2026 Tabsdata Inc.
-->

# tabsdata-km

kMinus: a local Kubernetes environment that runs on a single machine without
containers or VMs. A real control plane (Go: envtest kube-apiserver + etcd,
kube-controller-manager) plus a virtual kubelet that runs pods as native host
processes (Go `virtual-kubelet`, Rust `pod-runner`), orchestrated by the Rust
`km` binary. It runs on Windows, Linux and macOS. Details: `README.md` and
`DEVELOPING.md`.

Shared multi-repo rules (commits, comments, cross-platform compatibility),
hosted in the ee sibling checkout (import is skipped when ee is absent):
@../tabsdata-ee/variant/devutils/agents/GUIDELINES.md
