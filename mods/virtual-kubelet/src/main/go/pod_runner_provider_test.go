/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	neturl "net/url"
	"strings"
	"testing"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
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
