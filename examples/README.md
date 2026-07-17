<!--
  Copyright 2026 Tabsdata Inc.
-->

# Examples

Runnable manifests for a Tabsdata K- (kminus) instance. Start a stack, then
apply a manifest against its admin kubeconfig:

```sh
km create  --workdir /tmp/km
km start   --workdir /tmp/km
export KUBECONFIG=/tmp/km/config/kubeconfig.yaml
```

## The kminus pod-placement convention

kminus is a single-node stack with **no scheduler**, and its virtual-kubelet
node carries a `virtual-kubelet.io/provider:NoSchedule` taint. Every workload
pod template — whether a bare `Pod` or the template inside a `Job`,
`CronJob`, or `Deployment` — must therefore:

1. **pin to the node** with `spec.nodeName: tdknode` (bypasses scheduling), and
2. **tolerate the taint** with a `virtual-kubelet.io/provider` toleration.

Pods that omit either one stay `Pending` forever: without `nodeName` nothing
assigns them to the node, and the virtual-kubelet only runs pods already
pinned to it.

## Manifests

| File                              | What it shows                                            |
|-----------------------------------|----------------------------------------------------------|
| [`deployment.yaml`](deployment.yaml) | A `Deployment` (→ ReplicaSet → Pods) with the required `nodeName` + toleration. |

### deployment.yaml

```sh
kubectl apply -f examples/deployment.yaml
kubectl rollout status deployment/hello
kubectl get pods -l app=hello

# scale
kubectl scale deployment/hello --replicas=3
kubectl get pods -l app=hello

# clean up (cascades to the ReplicaSet and Pods)
kubectl delete -f examples/deployment.yaml
```
