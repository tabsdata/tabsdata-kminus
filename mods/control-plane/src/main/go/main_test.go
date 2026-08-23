/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

var hexToken = regexp.MustCompile(`^[0-9a-f]{64}$`)

// seededToken must be deterministic and shaped like the random tokens
// (64 lowercase hex chars = 32 bytes).
func TestSeededTokenDeterministicAndShaped(t *testing.T) {
	a := seededToken("alice")
	b := seededToken("alice")
	if a != b {
		t.Fatalf("seededToken not deterministic: %q != %q", a, b)
	}
	if !hexToken.MatchString(a) {
		t.Fatalf("seededToken has unexpected shape: %q", a)
	}
	if seededToken("alice") == seededToken("bob") {
		t.Fatalf("seededToken collided for different seeds")
	}
}

// fixedAdminToken is off unless the env var is exactly "true" (case-insensitive),
// and when on it matches the username-seeded token.
func TestFixedAdminToken(t *testing.T) {
	t.Setenv(fixedAuthTokenEnv, "")
	if got := fixedAdminToken(); got != "" {
		t.Fatalf("expected no fixed token when env unset, got %q", got)
	}
	t.Setenv(fixedAuthTokenEnv, "false")
	if got := fixedAdminToken(); got != "" {
		t.Fatalf("expected no fixed token when env=false, got %q", got)
	}
	t.Setenv(fixedAuthTokenEnv, "TRUE")
	got := fixedAdminToken()
	if got == "" {
		t.Fatalf("expected a fixed token when env=TRUE")
	}
	if want := seededToken(currentUsername()); got != want {
		t.Fatalf("fixed token %q != seededToken(username) %q", got, want)
	}
}

func readTokensCSV(t *testing.T, path string) map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open tokens.csv: %v", err)
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		t.Fatalf("parse tokens.csv: %v", err)
	}
	m := make(map[string]string)
	for _, rec := range records {
		if len(rec) >= 2 {
			m[rec[1]] = rec[0]
		}
	}
	return m
}

// With the dev override on, a freshly created tokens.csv gets the deterministic
// admin token while the other users get random ones.
func TestLoadOrCreateTokensFixedAdminOnCreate(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(fixedAuthTokenEnv, "true")

	tokens, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens: %v", err)
	}
	want := seededToken(currentUsername())
	if tokens[adminUserName] != want {
		t.Fatalf("admin token %q != fixed %q", tokens[adminUserName], want)
	}
	if !hexToken.MatchString(tokens[vkUserName]) || tokens[vkUserName] == want {
		t.Fatalf("vk token should be random hex, got %q", tokens[vkUserName])
	}
	// The persisted file must match the returned map (apiserver auth == kubeconfig).
	onDisk := readTokensCSV(t, filepath.Join(dir, tokensFile))
	if onDisk[adminUserName] != want {
		t.Fatalf("tokens.csv admin %q != fixed %q", onDisk[adminUserName], want)
	}
}

// With the override on over an EXISTING tokens.csv, the admin token is rewritten
// to the fixed value while the other users' tokens are preserved.
func TestLoadOrCreateTokensFixedAdminOverridesExisting(t *testing.T) {
	dir := t.TempDir()

	// First run without the override: fully random tokens.
	first, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (first): %v", err)
	}
	vkBefore := first[vkUserName]

	// Second run with the override: admin becomes fixed, vk unchanged.
	t.Setenv(fixedAuthTokenEnv, "true")
	second, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (second): %v", err)
	}
	want := seededToken(currentUsername())
	if second[adminUserName] != want {
		t.Fatalf("admin token not overridden: %q != %q", second[adminUserName], want)
	}
	if second[vkUserName] != vkBefore {
		t.Fatalf("vk token changed: %q != %q", second[vkUserName], vkBefore)
	}
	onDisk := readTokensCSV(t, filepath.Join(dir, tokensFile))
	if onDisk[adminUserName] != want {
		t.Fatalf("tokens.csv not rewritten with fixed admin: %q", onDisk[adminUserName])
	}
	if onDisk[vkUserName] != vkBefore {
		t.Fatalf("tokens.csv vk token changed: %q != %q", onDisk[vkUserName], vkBefore)
	}
}

// End-to-end: with the override on, the deterministic admin token flows all
// the way into kubeconfig.yaml (the client credential the user consumes).
func TestFixedAdminTokenReachesKubeconfig(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(fixedAuthTokenEnv, "true")

	tokens, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens: %v", err)
	}
	kubeconfig := filepath.Join(dir, "kubeconfig.yaml")
	if err := writeKubeconfig(kubeconfig, "https://127.0.0.1:6443", adminUserName, tokens[adminUserName]); err != nil {
		t.Fatalf("writeKubeconfig: %v", err)
	}
	data, err := os.ReadFile(kubeconfig)
	if err != nil {
		t.Fatalf("read kubeconfig: %v", err)
	}
	want := "token: " + seededToken(currentUsername())
	if !strings.Contains(string(data), want) {
		t.Fatalf("kubeconfig.yaml missing fixed admin token %q:\n%s", want, data)
	}
}

// Without the override, tokens stay random and the file is created once and
// reused verbatim across calls.
func TestLoadOrCreateTokensRandomStable(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(fixedAuthTokenEnv, "")

	first, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (first): %v", err)
	}
	second, err := loadOrCreateTokens(dir)
	if err != nil {
		t.Fatalf("loadOrCreateTokens (second): %v", err)
	}
	for _, u := range tokenUsers {
		if !hexToken.MatchString(first[u.name]) {
			t.Fatalf("%s token not random hex: %q", u.name, first[u.name])
		}
		if first[u.name] != second[u.name] {
			t.Fatalf("%s token not stable across calls: %q != %q", u.name, first[u.name], second[u.name])
		}
	}
}

// writeHeartbeat must create the directory and emit parseable JSON, since
// km treats an unparseable beat as a miss.
func TestWriteHeartbeatCreatesParseableBeat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "heartbeat.json")

	writeHeartbeat(path, statusDegraded, `etcd: GET "x" failed`, func(string, ...any) {})

	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("heartbeat not written: %v", err)
	}
	var beat struct {
		TS     int64  `json:"ts"`
		Status string `json:"status"`
		Detail string `json:"detail"`
	}
	if err := json.Unmarshal(body, &beat); err != nil {
		t.Fatalf("heartbeat is not valid JSON: %v (%s)", err, body)
	}
	if beat.Status != statusDegraded {
		t.Errorf("status = %q, want %q", beat.Status, statusDegraded)
	}
	if beat.Detail != `etcd: GET "x" failed` {
		t.Errorf("detail = %q, want the quoted message verbatim", beat.Detail)
	}
	if beat.TS == 0 {
		t.Error("ts not stamped")
	}
}

// An empty path is how a standalone run disables the heartbeat: it must be
// a no-op rather than an error or a stray file.
func TestWriteHeartbeatSkipsEmptyPath(t *testing.T) {
	warned := false
	writeHeartbeat("", statusReady, "", func(string, ...any) { warned = true })
	if warned {
		t.Error("empty heartbeat path warned instead of being ignored")
	}
}

func TestHTTPProbeAcceptsSuccessAndRejectsErrors(t *testing.T) {
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer healthy.Close()
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer sick.Close()

	client := &http.Client{}
	if err := httpProbe(client, healthy.URL)(context.Background()); err != nil {
		t.Errorf("healthy probe failed: %v", err)
	}
	if err := httpProbe(client, sick.URL)(context.Background()); err == nil {
		t.Error("a 500 must fail the probe")
	}
	if err := httpProbe(client, "http://127.0.0.1:1")(context.Background()); err == nil {
		t.Error("a refused connection must fail the probe")
	}
}

// probeAll must report every failing child, not just the first, so one
// warning line names everything that is wrong.
func TestProbeAllReportsEveryFailure(t *testing.T) {
	ok := func(context.Context) error { return nil }
	boom := func(msg string) func(context.Context) error {
		return func(context.Context) error { return errors.New(msg) }
	}

	healthy := []childProbe{{name: "a", check: ok}, {name: "b", check: ok}}
	if detail, gone := probeAll(context.Background(), healthy); detail != "" || gone {
		t.Errorf("healthy round reported %q (gone=%v)", detail, gone)
	}

	mixed := []childProbe{
		{name: "etcd", check: boom("no route")},
		{name: "kube-apiserver", check: ok},
		{name: "kube-controller-manager", check: boom("refused")},
	}
	detail, _ := probeAll(context.Background(), mixed)
	if !strings.Contains(detail, "etcd: no route") ||
		!strings.Contains(detail, "kube-controller-manager: refused") {
		t.Errorf("detail = %q, want both failures named", detail)
	}
	if strings.Contains(detail, "kube-apiserver") {
		t.Errorf("detail = %q, must not name the healthy child", detail)
	}
}

// A cancelled context must fail the round immediately: the monitor loop
// relies on that to stop probing the instant a shutdown is requested.
func TestProbeAllFailsFastOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	probes := []childProbe{{name: "slow", check: func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}}}
	if detail, _ := probeAll(ctx, probes); detail == "" {
		t.Error("a cancelled context must fail the probe round")
	}
}

// The miss budget is only spent by *consecutive* failures: one good round in
// between must put the count back to zero.
func TestMonitorRoundCountsOnlyConsecutiveFailures(t *testing.T) {
	const maxMissed = 4

	missed, action := monitorRound("etcd: refused", 0, maxMissed)
	if missed != 1 || action != actionContinue {
		t.Fatalf("first failure = (%d, %v), want (1, continue)", missed, action)
	}

	missed, action = monitorRound("etcd: refused", missed, maxMissed)
	if missed != 2 || action != actionContinue {
		t.Fatalf("second failure = (%d, %v), want (2, continue)", missed, action)
	}

	missed, action = monitorRound("", missed, maxMissed)
	if missed != 0 || action != actionContinue {
		t.Fatalf("healthy round = (%d, %v), want (0, continue)", missed, action)
	}

	missed, action = monitorRound("etcd: refused", missed, maxMissed)
	if missed != 1 || action != actionContinue {
		t.Fatalf("failure after recovery = (%d, %v), want (1, continue)", missed, action)
	}
}

func TestMonitorRoundShutsDownExactlyAtTheLimit(t *testing.T) {
	const maxMissed = 3
	missed := 0
	for round := 1; round <= maxMissed; round++ {
		var action monitorAction
		missed, action = monitorRound("kube-controller-manager: refused", missed, maxMissed)
		wantShutdown := round == maxMissed
		if (action == actionShutdown) != wantShutdown {
			t.Fatalf("round %d: action = %v, wantShutdown = %v", round, action, wantShutdown)
		}
		if missed != round {
			t.Fatalf("round %d: missed = %d", round, missed)
		}
	}
}

// A misconfigured 0 must not mean "fail before anything can answer".
func TestMonitorRoundClampsANonPositiveLimit(t *testing.T) {
	missed, action := monitorRound("etcd: refused", 0, 0)
	if missed != 1 || action != actionShutdown {
		t.Errorf("limit 0 = (%d, %v), want (1, shutdown) after one failure", missed, action)
	}
	if missed, action := monitorRound("", 0, 0); missed != 0 || action != actionContinue {
		t.Errorf("healthy round with limit 0 = (%d, %v), want (0, continue)", missed, action)
	}
}

// km's readiness gate waits on the first beat, so rounds before it must not
// be an interval apart.
func TestProbeWaitIsFastUntilTheFirstHealthyRound(t *testing.T) {
	interval := 15 * time.Second
	if got := probeWait(false, interval); got != startupRetry {
		t.Errorf("probeWait(not yet healthy) = %v, want %v", got, startupRetry)
	}
	if got := probeWait(true, interval); got != interval {
		t.Errorf("probeWait(healthy) = %v, want %v", got, interval)
	}
}

func TestChildProcessNameTrimsTheWindowsSuffix(t *testing.T) {
	if got := childProcessName("etcd.exe"); got != "etcd" {
		t.Errorf("childProcessName(etcd.exe) = %q, want etcd", got)
	}
	if got := childProcessName("etcd"); got != "etcd" {
		t.Errorf("childProcessName(etcd) = %q, want etcd", got)
	}
}

// The pid check has to be exact about the two ways a recorded pid can lie:
// the process is gone, or the pid now belongs to something else.
func TestProcessGoneGuardsAgainstPidReuse(t *testing.T) {
	self, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		t.Fatalf("cannot inspect this test process: %v", err)
	}
	name, err := self.Name()
	if err != nil {
		t.Fatalf("cannot read this process's name: %v", err)
	}

	if processGone(childProcessName(name), os.Getpid()) {
		t.Error("this very process must not read as gone")
	}
	if !processGone("definitely-not-this-process", os.Getpid()) {
		t.Error("a pid whose process has a different name must read as gone")
	}
}

// A child that has already exited must be reported before any probe runs.
func TestChildDeathPrefersTheReportedExit(t *testing.T) {
	exited := make(chan string, 1)
	exited <- "kube-controller-manager exited: exit status 1"

	death := childLost(exited, nil, true)
	if !strings.Contains(death, "exit status 1") {
		t.Errorf("death = %q, want the exit status", death)
	}
}

func TestChildDeathReportsAMissingPid(t *testing.T) {
	// An empty channel must not block the check.
	exited := make(chan string, 1)

	if death := childLost(exited, nil, true); death != "" {
		t.Errorf("no children = %q, want no death", death)
	}

	alive := []childProcess{{name: "self", pid: os.Getpid()}}
	if death := deadChildProcess(alive); death == "" {
		t.Error("a pid whose name does not match must read as a death")
	}

	// A pid above the platform maximum cannot exist.
	gone := []childProcess{{name: "etcd", pid: 0x7FFFFFFF}}
	death := deadChildProcess(gone)
	if !strings.Contains(death, "etcd") || !strings.Contains(death, "no longer running") {
		t.Errorf("death = %q, want etcd named as not running", death)
	}
}

// The tick must not stretch a startup retry, or the first beat — which km's
// readiness gate waits on — arrives a whole tick late.
func TestTickWaitYieldsToACloserRound(t *testing.T) {
	if got := tickWait(250*time.Millisecond, processTick); got != 250*time.Millisecond {
		t.Errorf("tickWait(250ms) = %v, want 250ms", got)
	}
	if got := tickWait(10*time.Second, processTick); got != processTick {
		t.Errorf("tickWait(10s) = %v, want %v", got, processTick)
	}
	// An overdue round must still tick rather than spin.
	if got := tickWait(-time.Second, processTick); got != processTick {
		t.Errorf("tickWait(overdue) = %v, want %v", got, processTick)
	}
}

// A refused connection is definitive: the child stopped listening, so the
// round must say so rather than letting the miss budget run.
func TestProbeAllFlagsARefusedConnectionAsGone(t *testing.T) {
	client := &http.Client{}
	probes := []childProbe{{
		name:  "kube-apiserver",
		check: httpProbe(client, "http://127.0.0.1:1"),
	}}

	detail, gone := probeAll(context.Background(), probes)
	if detail == "" {
		t.Fatal("a refused connection must fail the round")
	}
	if !gone {
		t.Errorf("detail = %q, want it flagged as a listener that is gone", detail)
	}
}

// A child that answers but is unhealthy, or is merely slow, must not be
// treated as gone — that is what the miss budget is for.
func TestProbeAllDoesNotFlagAnUnhealthyChildAsGone(t *testing.T) {
	sick := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer sick.Close()

	probes := []childProbe{{name: "etcd", check: httpProbe(&http.Client{}, sick.URL)}}
	detail, gone := probeAll(context.Background(), probes)
	if detail == "" {
		t.Fatal("a 503 must fail the round")
	}
	if gone {
		t.Error("a 503 is unhealthy, not gone")
	}
}

// A closed port is detected at tick speed, which is what catches a child that
// closed its listener and is still shutting down — a plain `kill` on
// kube-apiserver, for instance.
func TestChildLostReportsAClosedListener(t *testing.T) {
	// Bind then close, so the port is known to have nothing on it.
	probe := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	addr := strings.TrimPrefix(probe.URL, "http://")
	probe.Close()

	listeners := []childListener{{name: "kube-apiserver", addr: addr}}
	death := childLost(make(chan string, 1), listeners, true)
	if !strings.Contains(death, "no longer listening") {
		t.Errorf("death = %q, want the closed listener reported", death)
	}
}

// Before a child has ever answered, a refused port only means it is still
// binding — tearing the stack down for that would break every startup.
func TestChildLostIgnoresListenersBeforeTheFirstHealthyRound(t *testing.T) {
	listeners := []childListener{{name: "kube-apiserver", addr: "127.0.0.1:1"}}
	if death := childLost(make(chan string, 1), listeners, false); death != "" {
		t.Errorf("death = %q, want silence until the child has served", death)
	}
}

// A live listener must not be reported, or the stack would stop while healthy.
func TestChildLostAcceptsALiveListener(t *testing.T) {
	live := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer live.Close()

	listeners := []childListener{{name: "etcd", addr: strings.TrimPrefix(live.URL, "http://")}}
	if death := childLost(make(chan string, 1), listeners, true); death != "" {
		t.Errorf("death = %q, want no death for a listening child", death)
	}
}

// A pinned port becomes etcd's client URL; an unset one leaves the choice to
// envtest, which is what instances created before km managed the port do.
func TestFixedEtcdURLOnlyWhenPinned(t *testing.T) {
	if got := fixedEtcdURL(portUnset); got != nil {
		t.Errorf("fixedEtcdURL(%q) = %v, want nil so envtest allocates", portUnset, got)
	}

	got := fixedEtcdURL("10305")
	if got == nil {
		t.Fatal("a pinned port must produce a URL")
	}
	if got.Scheme != "http" || got.Host != "127.0.0.1:10305" {
		t.Errorf("fixedEtcdURL(10305) = %v, want http://127.0.0.1:10305", got)
	}
}
