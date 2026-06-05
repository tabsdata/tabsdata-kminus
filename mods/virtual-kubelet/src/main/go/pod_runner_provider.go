/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PodRunnerProvider implements the virtual-kubelet Provider interface.
type PodRunnerProvider struct {
	nodeName         string
	providerEndpoint string
	httpClient       *http.Client
	kubeletPort      int32
}

func NewPodRunnerProvider(nodeName, providerEndpoint string, httpClient *http.Client, kubeletPort int32) *PodRunnerProvider {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &PodRunnerProvider{
		nodeName:         nodeName,
		providerEndpoint: providerEndpoint,
		kubeletPort:      kubeletPort,
		httpClient:       httpClient,
	}
}

func (p *PodRunnerProvider) CreatePod(ctx context.Context, pod *v1.Pod) error {
	podJSON, err := json.Marshal(pod)
	if err != nil {
		return fmt.Errorf("failed to marshal pod: %w", err)
	}

	resp, err := p.httpClient.Post(
		fmt.Sprintf("%s/createPod", p.providerEndpoint),
		"application/json",
		bytes.NewReader(podJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to call createPod: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("createPod failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

func (p *PodRunnerProvider) UpdatePod(ctx context.Context, pod *v1.Pod) error {
	podJSON, err := json.Marshal(pod)
	if err != nil {
		return fmt.Errorf("failed to marshal pod: %w", err)
	}

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPut,
		fmt.Sprintf("%s/updatePod", p.providerEndpoint),
		bytes.NewReader(podJSON),
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call updatePod: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("updatePod failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

func (p *PodRunnerProvider) DeletePod(ctx context.Context, pod *v1.Pod) error {
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodDelete,
		fmt.Sprintf("%s/deletePod?namespace=%s&name=%s", p.providerEndpoint, pod.Namespace, pod.Name),
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call deletePod: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("deletePod failed: %s - %s", resp.Status, string(body))
	}

	return nil
}

func (p *PodRunnerProvider) GetPod(ctx context.Context, namespace, name string) (*v1.Pod, error) {
	resp, err := p.httpClient.Get(
		fmt.Sprintf("%s/getPod?namespace=%s&name=%s", p.providerEndpoint, namespace, name),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to call getPod: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("getPod failed: %s - %s", resp.Status, string(body))
	}

	var pod v1.Pod
	if err := json.NewDecoder(resp.Body).Decode(&pod); err != nil {
		return nil, fmt.Errorf("failed to decode pod: %w", err)
	}

	return &pod, nil
}

func (p *PodRunnerProvider) GetPodStatus(ctx context.Context, namespace, name string) (*v1.PodStatus, error) {
	resp, err := p.httpClient.Get(
		fmt.Sprintf("%s/getPodStatus?namespace=%s&name=%s", p.providerEndpoint, namespace, name),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to call getPodStatus: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("getPodStatus failed: %s - %s", resp.Status, string(body))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var status v1.PodStatus
	if err := json.Unmarshal(bodyBytes, &status); err != nil {
		return nil, fmt.Errorf("failed to decode status: %w", err)
	}

	return &status, nil
}

func (p *PodRunnerProvider) GetPods(ctx context.Context) ([]*v1.Pod, error) {
	resp, err := p.httpClient.Get(fmt.Sprintf("%s/getPods", p.providerEndpoint))
	if err != nil {
		return nil, fmt.Errorf("failed to call getPods: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("getPods failed: %s - %s", resp.Status, string(body))
	}

	var pods []*v1.Pod
	if err := json.NewDecoder(resp.Body).Decode(&pods); err != nil {
		return nil, fmt.Errorf("failed to decode pods: %w", err)
	}

	return pods, nil
}

// PodStatusEntry is the response from GET /getAllPodStatuses.
// Contains namespace, name, uid, phase, and the full pod status.
// Used by the sync loop — one bulk call replaces N individual
// getPodStatus calls.
//
// UID is the pod-runner's identity for the pod (the kube pod UID it was
// created with). The sync loop compares it against the live kube pod's
// UID: a pod's (namespace, name) is reused across incarnations, so a
// mismatch means the pod-runner is still holding a *previous*
// incarnation under this name. Older pod-runners that don't send it
// leave UID == "", which the loop treats as "can't tell" and skips the
// staleness check (graceful degradation across the version skew).
type PodStatusEntry struct {
	Namespace string       `json:"namespace"`
	Name      string       `json:"name"`
	UID       string       `json:"uid"`
	Phase     string       `json:"phase"`
	Status    v1.PodStatus `json:"status"`
}

// GetAllPodStatuses fetches the current status of every pod from the
// pod-runner in a single HTTP call. Returns namespace, name, phase,
// and full pod status for each pod.
func (p *PodRunnerProvider) GetAllPodStatuses(ctx context.Context) ([]PodStatusEntry, error) {
	resp, err := p.httpClient.Get(fmt.Sprintf("%s/getAllPodStatuses", p.providerEndpoint))
	if err != nil {
		return nil, fmt.Errorf("failed to call getAllPodStatuses: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("getAllPodStatuses failed: %s - %s", resp.Status, string(body))
	}

	var entries []PodStatusEntry
	if err := json.NewDecoder(resp.Body).Decode(&entries); err != nil {
		return nil, fmt.Errorf("failed to decode pod statuses: %w", err)
	}

	return entries, nil
}

func (p *PodRunnerProvider) ConfigureNode(ctx context.Context, node *v1.Node) {
	node.Status.Capacity = p.capacity()
	node.Status.Allocatable = p.capacity()
	node.Status.Conditions = p.nodeConditions()
	node.Status.Addresses = p.nodeAddresses()
	node.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{Port: p.kubeletPort},
	}
	node.Status.NodeInfo = v1.NodeSystemInfo{
		OperatingSystem: "Linux",
		Architecture:    "amd64",
		KubeletVersion:  "v1.29.0-vk-minimal",
	}
}

func (p *PodRunnerProvider) capacity() v1.ResourceList {
	resp, err := p.httpClient.Get(fmt.Sprintf("%s/capacity", p.providerEndpoint))
	if err != nil {
		return defaultCapacity()
	}
	defer resp.Body.Close()

	var capacity map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&capacity); err != nil {
		return defaultCapacity()
	}

	result := v1.ResourceList{}
	if cpu, ok := capacity["cpu"]; ok {
		result[v1.ResourceCPU] = resource.MustParse(cpu)
	}
	if memory, ok := capacity["memory"]; ok {
		result[v1.ResourceMemory] = resource.MustParse(memory)
	}
	if pods, ok := capacity["pods"]; ok {
		result[v1.ResourcePods] = resource.MustParse(pods)
	}

	return result
}

func defaultCapacity() v1.ResourceList {
	return v1.ResourceList{
		v1.ResourceCPU:    resource.MustParse("100"),
		v1.ResourceMemory: resource.MustParse("100Gi"),
		v1.ResourcePods:   resource.MustParse("100"),
	}
}

func (p *PodRunnerProvider) nodeConditions() []v1.NodeCondition {
	resp, err := p.httpClient.Get(fmt.Sprintf("%s/nodeConditions", p.providerEndpoint))
	if err != nil {
		return defaultNodeConditions()
	}
	defer resp.Body.Close()

	var conditions []v1.NodeCondition
	if err := json.NewDecoder(resp.Body).Decode(&conditions); err != nil {
		return defaultNodeConditions()
	}

	return conditions
}

func defaultNodeConditions() []v1.NodeCondition {
	now := metav1.NewTime(time.Now())
	return []v1.NodeCondition{
		{
			Type:               v1.NodeReady,
			Status:             v1.ConditionTrue,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "KubeletReady",
			Message:            "kubelet is ready",
		},
		{
			Type:               v1.NodeMemoryPressure,
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "KubeletHasSufficientMemory",
			Message:            "kubelet has sufficient memory",
		},
		{
			Type:               v1.NodeDiskPressure,
			Status:             v1.ConditionFalse,
			LastHeartbeatTime:  now,
			LastTransitionTime: now,
			Reason:             "KubeletHasNoDiskPressure",
			Message:            "kubelet has no disk pressure",
		},
	}
}

func (p *PodRunnerProvider) nodeAddresses() []v1.NodeAddress {
	resp, err := p.httpClient.Get(fmt.Sprintf("%s/nodeAddresses", p.providerEndpoint))
	if err != nil {
		return defaultNodeAddresses()
	}
	defer resp.Body.Close()

	var addresses []v1.NodeAddress
	if err := json.NewDecoder(resp.Body).Decode(&addresses); err != nil {
		return defaultNodeAddresses()
	}

	return addresses
}

func defaultNodeAddresses() []v1.NodeAddress {
	return []v1.NodeAddress{
		{Type: v1.NodeInternalIP, Address: "192.168.1.101"},
	}
}

func (p *PodRunnerProvider) GetContainerLogs(ctx context.Context, namespace, podName, containerName string, opts api.ContainerLogOpts) (io.ReadCloser, error) {
	url := fmt.Sprintf("%s/getContainerLogs?podName=%s&namespace=%s&containerName=%s",
		p.providerEndpoint, podName, namespace, containerName)

	resp, err := p.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to get container logs: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("getContainerLogs failed: %s - %s", resp.Status, string(body))
	}

	return resp.Body, nil
}

func (p *PodRunnerProvider) RunInContainer(ctx context.Context, namespace, podName, containerName string, cmd []string, attach api.AttachIO) error {
	return fmt.Errorf("RunInContainer not supported")
}

func (p *PodRunnerProvider) AttachToContainer(ctx context.Context, namespace, podName, containerName string, attach api.AttachIO) error {
	return fmt.Errorf("AttachToContainer not supported")
}

func (p *PodRunnerProvider) NotifyPods(ctx context.Context, notifier func(*v1.Pod)) {
}

func (p *PodRunnerProvider) NotifyNodeStatus(ctx context.Context, notifier func(*v1.Node)) {
	go func() {
		p.sendNodeStatus(notifier)

		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.sendNodeStatus(notifier)
			}
		}
	}()
}

func (p *PodRunnerProvider) sendNodeStatus(notifier func(*v1.Node)) {
	n := &v1.Node{}
	n.Name = p.nodeName
	n.Status.Capacity = p.capacity()
	n.Status.Allocatable = p.capacity()
	n.Status.Conditions = p.nodeConditions()
	n.Status.Addresses = p.nodeAddresses()
	n.Status.DaemonEndpoints = v1.NodeDaemonEndpoints{
		KubeletEndpoint: v1.DaemonEndpoint{Port: p.kubeletPort},
	}
	n.Status.NodeInfo = v1.NodeSystemInfo{
		OperatingSystem: "Linux",
		Architecture:    "amd64",
		KubeletVersion:  "v1.29.0-vk-minimal",
	}
	notifier(n)
}

func (p *PodRunnerProvider) Ping(ctx context.Context) error {
	resp, err := p.httpClient.Get(fmt.Sprintf("%s/capacity", p.providerEndpoint))
	if err != nil {
		return fmt.Errorf("provider ping failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("provider ping returned status: %s", resp.Status)
	}

	return nil
}
