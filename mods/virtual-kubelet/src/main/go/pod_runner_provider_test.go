/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	neturl "net/url"
	"strings"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func parseLogsQuery(t *testing.T, rawURL string) neturl.Values {
	t.Helper()
	u, err := neturl.Parse(rawURL)
	if err != nil {
		t.Fatalf("failed to parse URL %q: %v", rawURL, err)
	}
	if u.Path != "/getContainerLogs" {
		t.Fatalf("unexpected path %q", u.Path)
	}
	return u.Query()
}

func TestContainerLogsURLBasic(t *testing.T) {
	p := NewPodRunnerProvider("node", "https://127.0.0.1:1", nil, 10250)
	q := parseLogsQuery(t, p.containerLogsURL("ns", "pod", "ctr", api.ContainerLogOpts{}))

	if q.Get("podName") != "pod" || q.Get("namespace") != "ns" || q.Get("containerName") != "ctr" {
		t.Fatalf("missing identification params: %v", q)
	}
	for _, absent := range []string{"follow", "tailLines", "previous", "timestamps"} {
		if q.Has(absent) {
			t.Fatalf("param %q should be absent when not requested: %v", absent, q)
		}
	}
}

func TestContainerLogsURLForwardsOpts(t *testing.T) {
	p := NewPodRunnerProvider("node", "https://127.0.0.1:1", nil, 10250)
	q := parseLogsQuery(t, p.containerLogsURL("ns", "pod", "ctr", api.ContainerLogOpts{
		Follow:     true,
		Tail:       25,
		Previous:   true,
		Timestamps: true,
	}))

	if q.Get("follow") != "true" {
		t.Fatalf("follow not forwarded: %v", q)
	}
	if q.Get("tailLines") != "25" {
		t.Fatalf("tailLines not forwarded: %v", q)
	}
	if q.Get("previous") != "true" {
		t.Fatalf("previous not forwarded: %v", q)
	}
	if q.Get("timestamps") != "true" {
		t.Fatalf("timestamps not forwarded: %v", q)
	}
}

func TestContainerLogsURLEscapesParams(t *testing.T) {
	p := NewPodRunnerProvider("node", "https://127.0.0.1:1", nil, 10250)
	url := p.containerLogsURL("name space", "pod&x=1", "ctr", api.ContainerLogOpts{})

	if strings.Contains(url, "pod&x=1") {
		t.Fatalf("params not escaped: %s", url)
	}
	q := parseLogsQuery(t, url)
	if q.Get("podName") != "pod&x=1" || q.Get("namespace") != "name space" {
		t.Fatalf("escaped params do not round-trip: %v", q)
	}
}

func TestStreamClientHasNoTimeout(t *testing.T) {
	p := NewPodRunnerProvider("node", "https://127.0.0.1:1", nil, 10250)
	if p.streamClient.Timeout != 0 {
		t.Fatalf("stream client must not have a whole-request timeout, got %v", p.streamClient.Timeout)
	}
}

func termPod(name, uid string, deleting bool) *v1.Pod {
	p := &v1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "default", Name: name, UID: types.UID(uid),
	}}
	if deleting {
		now := metav1.Now()
		p.DeletionTimestamp = &now
	}
	return p
}

func names(pods []*v1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}

func TestPodsToFinalizeSelectsDoneTerminatingPods(t *testing.T) {
	pods := []*v1.Pod{
		termPod("running-deleting", "uid-run", true),   // active → keep
		termPod("terminal-deleting", "uid-term", true), // not active → finalize
		termPod("gone-deleting", "uid-gone", true),     // absent from provider → finalize
		termPod("running-alive", "uid-alive", false),   // not deleting → skip
	}
	// Provider reports the running pod (non-terminal) and the terminal one.
	active := map[string]bool{"uid-run": true}

	got := names(podsToFinalize(pods, active))
	want := map[string]bool{"terminal-deleting": true, "gone-deleting": true}
	if len(got) != 2 {
		t.Fatalf("expected 2 pods to finalize, got %v", got)
	}
	for _, n := range got {
		if !want[n] {
			t.Fatalf("unexpected pod selected for finalize: %s (got %v)", n, got)
		}
	}
}

func TestPodsToFinalizeSkipsNonTerminating(t *testing.T) {
	pods := []*v1.Pod{termPod("alive", "uid-1", false)}
	if got := podsToFinalize(pods, map[string]bool{}); len(got) != 0 {
		t.Fatalf("non-terminating pod must never be finalized, got %v", names(got))
	}
}

func TestPodsToFinalizeKeepsActiveTerminating(t *testing.T) {
	// A pod still running in the provider must NOT be finalized even though
	// it is Terminating — it should terminate within its grace period.
	pods := []*v1.Pod{termPod("running-deleting", "uid-run", true)}
	active := map[string]bool{"uid-run": true}
	if got := podsToFinalize(pods, active); len(got) != 0 {
		t.Fatalf("active terminating pod must not be finalized, got %v", names(got))
	}
}
