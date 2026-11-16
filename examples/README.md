<!--
  Copyright 2026 Tabsdata Inc.
-->

<div align="center">
    <img src="../assets/images/tabsdata-logo-for-light-theme.svg#gh-light-mode-only" alt="Tabsdata" width="500">
    <img src="../assets/images/tabsdata-logo-for-dark-theme.svg#gh-dark-mode-only" alt="Tabsdata" width="500">
</div>

# Examples

Runnable manifests for a **kminus** instance. Start an instance and point **kubectl** at it, as
[Running kminus](../README.md#running-kminus) describes, then apply a manifest. Run the commands
below from the root of `tabsdata-km`.

## The kminus pod-placement convention

The **kminus** stack is a single-node stack with **no scheduler**, and its virtual-kubelet node
carries a `virtual-kubelet.io/provider:NoSchedule` taint. Every workload pod template, whether a
bare `Pod` or the template inside a `Job`, `CronJob` or `Deployment`, must therefore:

1. **Pin to the node** with `spec.nodeName: tdknode`, which bypasses scheduling.
2. **Tolerate the taint** with a `virtual-kubelet.io/provider` toleration.

Pods that omit either one stay `Pending` forever: without `nodeName` nothing assigns them to the
node, and the virtual-kubelet only runs pods already pinned to it.

## Manifests

| File | What it shows |
|---|---|
| [deployment.yaml](deployment.yaml) | A `Deployment`, with its ReplicaSet and pods<br>The required `nodeName` and toleration |

### deployment.yaml

Apply the deployment, and wait until it's rolled out.

macOS and Linux:

```bash
./target/dist/kubectl apply -f examples/deployment.yaml
```

```bash
./target/dist/kubectl rollout status deployment/hello
```

Windows:

```powershell
.\target\dist\kubectl.exe apply -f examples\deployment.yaml
```

```powershell
.\target\dist\kubectl.exe rollout status deployment/hello
```

Scale it to three pods, and list them.

macOS and Linux:

```bash
./target/dist/kubectl scale deployment/hello --replicas=3
```

```bash
./target/dist/kubectl get pods -l app=hello
```

Windows:

```powershell
.\target\dist\kubectl.exe scale deployment/hello --replicas=3
```

```powershell
.\target\dist\kubectl.exe get pods -l app=hello
```

Delete it, which also deletes its ReplicaSet and pods.

macOS and Linux:

```bash
./target/dist/kubectl delete -f examples/deployment.yaml
```

Windows:

```powershell
.\target\dist\kubectl.exe delete -f examples\deployment.yaml
```
