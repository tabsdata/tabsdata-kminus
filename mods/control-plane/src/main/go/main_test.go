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
	"strconv"
	"strings"
	"sync"
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

// ---------------------------------------------------------------------------
// etcd backend maintenance
// ---------------------------------------------------------------------------

// fakeEtcd serves the three maintenance endpoints etcdMaintainer calls and
// records which were hit. Defragmentation returns the backend to its live
// size, the way a real one does once compaction has freed the pages.
type fakeEtcd struct {
	mu          sync.Mutex
	size, inUse int64
	alarmed     bool
	calls       []string
}

func (f *fakeEtcd) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls = append(f.calls, r.URL.Path)
		switch r.URL.Path {
		case etcdStatusPath:
			body := map[string]any{
				"header":      map[string]any{"member_id": "42"},
				"dbSize":      strconv.FormatInt(f.size, 10),
				"dbSizeInUse": strconv.FormatInt(f.inUse, 10),
			}
			if f.alarmed {
				body["errors"] = []string{"memberID:42 alarm:NOSPACE "}
			}
			json.NewEncoder(w).Encode(body)
		case etcdDefragPath:
			f.size = f.inUse
			w.Write([]byte("{}"))
		case etcdAlarmPath:
			f.alarmed = false
			w.Write([]byte("{}"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeEtcd) called(path string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.calls {
		if c == path {
			return true
		}
	}
	return false
}

func (f *fakeEtcd) forget() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = nil
}

func (f *fakeEtcd) arm() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.alarmed = true
}

func (f *fakeEtcd) backend() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.size
}

// quietMaintainer builds a maintainer against base with the logs discarded.
func quietMaintainer(base string, cfg cpSysConfig) *etcdMaintainer {
	return countingMaintainer(base, cfg, nil)
}

// countingMaintainer is quietMaintainer with a heartbeat callback that
// increments beats, so the pre-pause beat can be asserted on.
func countingMaintainer(base string, cfg cpSysConfig, beats *int) *etcdMaintainer {
	silent := func(string, ...any) {}
	var beat func()
	if beats != nil {
		beat = func() { *beats++ }
	}
	// No data directory: publishing the storage report is not under test here,
	// and an empty storagePath makes publish a no-op.
	return newEtcdMaintainer(&http.Client{}, base, "", "", cfg, silent, silent, beat)
}

func TestEtcdStatusReadsTheGatewayStringEncoding(t *testing.T) {
	var st etcdStatus
	body := `{"header":{"member_id":"42"},"dbSize":"466894848","dbSizeInUse":"2867200"}`
	if err := json.Unmarshal([]byte(body), &st); err != nil {
		t.Fatalf("decoding a status reply failed: %v", err)
	}
	if st.size() != 466894848 {
		t.Fatalf("size() = %d, want 466894848", st.size())
	}
	if st.inUse() != 2867200 {
		t.Fatalf("inUse() = %d, want 2867200", st.inUse())
	}
	if st.noSpace() {
		t.Fatalf("noSpace() true with no alarms reported")
	}
}

// The alarm arrives as free text in the errors list, not as a typed field.
func TestEtcdStatusSpotsANoSpaceAlarmAmongOtherErrors(t *testing.T) {
	st := etcdStatus{Errors: []string{"memberID:9 alarm:CORRUPT ", "memberID:9 alarm:NOSPACE "}}
	if !st.noSpace() {
		t.Fatalf("noSpace() false with a NOSPACE alarm present")
	}
	if (etcdStatus{Errors: []string{"memberID:9 alarm:CORRUPT "}}).noSpace() {
		t.Fatalf("noSpace() true for an unrelated alarm")
	}
}

// Recovery ignores the size and cadence gates: the backend here is far below
// etcdDefragMinBytes, and the cluster is read-only until the alarm is
// disarmed, which neither compaction nor defragmentation does by itself.
func TestEtcdMaintainerClearsANoSpaceAlarm(t *testing.T) {
	fake := &fakeEtcd{size: 100 << 20, inUse: 90 << 20, alarmed: true}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 128, EtcdDefragMins: 60})

	m.run(context.Background(), time.Now())

	if !fake.called(etcdDefragPath) {
		t.Fatalf("an armed alarm did not trigger defragmentation")
	}
	if !fake.called(etcdAlarmPath) {
		t.Fatalf("an armed alarm was never disarmed")
	}
	fake.mu.Lock()
	stillArmed := fake.alarmed
	fake.mu.Unlock()
	if stillArmed {
		t.Fatalf("alarm still armed after recovery")
	}
}

// Disabling the periodic pass must not disable recovery: a read-only cluster
// is shut down by the probe loop, which is worse than a pause.
func TestEtcdMaintainerRecoversEvenWithDefragDisabled(t *testing.T) {
	fake := &fakeEtcd{size: 100 << 20, inUse: 90 << 20, alarmed: true}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 128, EtcdDefragMins: 0})

	m.run(context.Background(), time.Now())

	if !fake.called(etcdAlarmPath) {
		t.Fatalf("alarm left armed when periodic defragmentation is disabled")
	}
}

// A quota too small for the live data re-arms immediately; without the rate
// limit that would defragment on every probe round.
func TestEtcdMaintainerRateLimitsAlarmRecovery(t *testing.T) {
	fake := &fakeEtcd{size: 100 << 20, inUse: 99 << 20, alarmed: true}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 100, EtcdDefragMins: 60})
	start := time.Now()

	m.run(context.Background(), start)
	if !fake.called(etcdDefragPath) {
		t.Fatalf("first recovery did not run")
	}

	fake.arm()
	fake.forget()
	m.run(context.Background(), start.Add(etcdAlarmRetryInterval-time.Second))
	if fake.called(etcdDefragPath) {
		t.Fatalf("recovery retried inside the rate limit")
	}

	fake.forget()
	m.run(context.Background(), start.Add(etcdAlarmRetryInterval+time.Second))
	if !fake.called(etcdDefragPath) {
		t.Fatalf("recovery never retried after the rate limit elapsed")
	}
}

func TestEtcdMaintainerSkipsDefragBelowTheSizeGate(t *testing.T) {
	// Almost entirely free, but too small to be worth pausing the cluster.
	fake := &fakeEtcd{size: etcdDefragMinBytes - 1, inUse: 1 << 20}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60})

	m.run(context.Background(), time.Now())

	if fake.called(etcdDefragPath) {
		t.Fatalf("defragmented a backend below etcdDefragMinBytes")
	}
}

func TestEtcdMaintainerSkipsDefragWhenLittleIsFree(t *testing.T) {
	size := etcdDefragMinBytes * 2
	// Ten per cent free: compaction has little to give back here.
	fake := &fakeEtcd{size: size, inUse: size - size/10}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60})

	m.run(context.Background(), time.Now())

	if fake.called(etcdDefragPath) {
		t.Fatalf("defragmented a backend that was mostly live data")
	}
}

func TestEtcdMaintainerDefragsWhenTheBackendIsMostlyFree(t *testing.T) {
	size := etcdDefragMinBytes * 2
	fake := &fakeEtcd{size: size, inUse: size / 10}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60})

	m.run(context.Background(), time.Now())

	if !fake.called(etcdDefragPath) {
		t.Fatalf("a mostly-free backend was not defragmented")
	}
	if got := fake.backend(); got != size/10 {
		t.Fatalf("backend after defragmentation = %d, want %d", got, size/10)
	}
}

func TestEtcdMaintainerHonoursTheDefragCadence(t *testing.T) {
	size := etcdDefragMinBytes * 2
	fake := &fakeEtcd{size: size, inUse: size / 10}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60})
	start := time.Now()

	m.run(context.Background(), start)
	if !fake.called(etcdDefragPath) {
		t.Fatalf("the first round did not defragment")
	}

	// Refill so the gates would pass again, and confirm the cadence holds.
	fake.mu.Lock()
	fake.size = size
	fake.mu.Unlock()
	fake.forget()
	m.run(context.Background(), start.Add(59*time.Minute))
	if fake.called(etcdDefragPath) {
		t.Fatalf("defragmented again before the cadence elapsed")
	}

	fake.forget()
	m.run(context.Background(), start.Add(61*time.Minute))
	if !fake.called(etcdDefragPath) {
		t.Fatalf("did not defragment once the cadence elapsed")
	}
}

func TestEtcdMaintainerNeverDefragsWhenDisabled(t *testing.T) {
	size := etcdDefragMinBytes * 2
	fake := &fakeEtcd{size: size, inUse: size / 10}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 0})

	m.run(context.Background(), time.Now())
	m.run(context.Background(), time.Now().Add(24*time.Hour))

	if fake.called(etcdDefragPath) {
		t.Fatalf("defragmented with etcdDefragMins set to 0")
	}
}

// An unreachable etcd is the probes' business, not the maintainer's: it must
// return quietly rather than fail the round.
func TestEtcdMaintainerToleratesAnUnreachableEtcd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60})
	m.run(context.Background(), time.Now())
}

// ---------------------------------------------------------------------------
// cpsysconfig.yaml etcd fields
// ---------------------------------------------------------------------------

func TestClampEtcdReplacesNonsenseWithDefaults(t *testing.T) {
	defaults := defaultCPSysConfig()
	cfg := cpSysConfig{EtcdQuotaMB: 0, EtcdDefragMins: -1}
	cfg.clampEtcd()
	if cfg.EtcdQuotaMB != defaults.EtcdQuotaMB {
		t.Fatalf("EtcdQuotaMB = %d, want the default %d", cfg.EtcdQuotaMB, defaults.EtcdQuotaMB)
	}
	if cfg.EtcdDefragMins != defaults.EtcdDefragMins {
		t.Fatalf("EtcdDefragMins = %d, want the default %d",
			cfg.EtcdDefragMins, defaults.EtcdDefragMins)
	}
}

// 0 is a real setting for the cadence — "never defragment" — and must survive
// clamping, unlike 0 for the quota, which etcd would read as its own default.
func TestClampEtcdKeepsZeroMeaningDisabled(t *testing.T) {
	cfg := cpSysConfig{EtcdQuotaMB: 100, EtcdDefragMins: 0}
	cfg.clampEtcd()
	if cfg.EtcdDefragMins != 0 {
		t.Fatalf("EtcdDefragMins = %d, want 0 to keep meaning disabled", cfg.EtcdDefragMins)
	}
	if cfg.EtcdQuotaMB != 100 {
		t.Fatalf("EtcdQuotaMB = %d, want the configured 100", cfg.EtcdQuotaMB)
	}
}

// A cpsysconfig.yaml written before etcd was tunable must keep working, with
// the new fields defaulted rather than zeroed.
func TestLoadOrCreateCPSysConfigDefaultsEtcdFieldsInAnOlderFile(t *testing.T) {
	dir := t.TempDir()
	older := "concurrentJobSyncs: 7\nkubeApiQps: 11\nkubeApiBurst: 22\n"
	if err := os.WriteFile(filepath.Join(dir, "cpsysconfig.yaml"), []byte(older), 0644); err != nil {
		t.Fatalf("writing the older config failed: %v", err)
	}

	cfg, err := loadOrCreateCPSysConfig(dir)
	if err != nil {
		t.Fatalf("loading the older config failed: %v", err)
	}

	if cfg.ConcurrentJobSyncs != 7 {
		t.Fatalf("ConcurrentJobSyncs = %d, want the file's 7", cfg.ConcurrentJobSyncs)
	}
	defaults := defaultCPSysConfig()
	if cfg.EtcdQuotaMB != defaults.EtcdQuotaMB {
		t.Fatalf("EtcdQuotaMB = %d, want the default %d", cfg.EtcdQuotaMB, defaults.EtcdQuotaMB)
	}
	if cfg.EtcdDefragMins != defaults.EtcdDefragMins {
		t.Fatalf("EtcdDefragMins = %d, want the default %d",
			cfg.EtcdDefragMins, defaults.EtcdDefragMins)
	}
}

func TestLoadOrCreateCPSysConfigWritesTheEtcdFields(t *testing.T) {
	dir := t.TempDir()
	if _, err := loadOrCreateCPSysConfig(dir); err != nil {
		t.Fatalf("creating the config failed: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "cpsysconfig.yaml"))
	if err != nil {
		t.Fatalf("reading the created config failed: %v", err)
	}
	for _, key := range []string{"etcdQuotaMb", "etcdDefragMins"} {
		if !strings.Contains(string(data), key) {
			t.Fatalf("created config does not mention %s:\n%s", key, data)
		}
	}
}

func TestHumanBytesScalesUnits(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{512, "512B"},
		{2048, "2.0KiB"},
		{5 << 20, "5.0MiB"},
		{int64(8192) << 20, "8.0GiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.in); got != c.want {
			t.Fatalf("humanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}

// Defragmentation blocks the supervision loop, so km must see a fresh beat
// going into it rather than one already most of the way to stale.
func TestEtcdMaintainerBeatsBeforeTheDefragPause(t *testing.T) {
	size := etcdDefragMinBytes * 2
	fake := &fakeEtcd{size: size, inUse: size / 10}
	srv := fake.start(t)
	beats := 0
	m := countingMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60}, &beats)

	m.run(context.Background(), time.Now())

	if !fake.called(etcdDefragPath) {
		t.Fatalf("expected this round to defragment")
	}
	if beats != 1 {
		t.Fatalf("heartbeat published %d times before the pause, want 1", beats)
	}
}

// A round that decides against defragmenting must not touch the heartbeat:
// the loop publishes its own beat straight afterwards.
func TestEtcdMaintainerDoesNotBeatWithoutAPause(t *testing.T) {
	fake := &fakeEtcd{size: etcdDefragMinBytes - 1, inUse: 1 << 20}
	srv := fake.start(t)
	beats := 0
	m := countingMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60}, &beats)

	m.run(context.Background(), time.Now())

	if beats != 0 {
		t.Fatalf("heartbeat published %d times with no pause taken, want 0", beats)
	}
}

// A shutdown caused by a full backend has to say so, and say what to change.
// The probes cannot: all they report is a 503 from /health.
func TestEtcdMaintainerDiagnosesAFullBackend(t *testing.T) {
	fake := &fakeEtcd{size: 100 << 20, inUse: 99 << 20, alarmed: true}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 100, EtcdDefragMins: 60})

	if m.diagnosis() != "" {
		t.Fatalf("diagnosed a full backend before observing one")
	}
	m.run(context.Background(), time.Now())

	why := m.diagnosis()
	for _, want := range []string{"etcd backend is full", "etcdQuotaMb", "cpsysconfig.yaml"} {
		if !strings.Contains(why, want) {
			t.Fatalf("diagnosis %q does not mention %q", why, want)
		}
	}
}

// Once the backend is healthy again the diagnosis must stop being offered, or
// every later failure would be blamed on a quota that is no longer the issue.
func TestEtcdMaintainerStopsDiagnosingOnceRecovered(t *testing.T) {
	fake := &fakeEtcd{size: 100 << 20, inUse: 10 << 20, alarmed: true}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 8192, EtcdDefragMins: 60})
	start := time.Now()

	m.run(context.Background(), start)
	if m.diagnosis() == "" {
		t.Fatalf("no diagnosis while the alarm was armed")
	}

	// fakeEtcd disarms on the alarm call, so the next round sees it clear.
	m.run(context.Background(), start.Add(time.Second))
	if why := m.diagnosis(); why != "" {
		t.Fatalf("still diagnosing a full backend after recovery: %q", why)
	}
}

// The report km reads must land as a complete file with the numbers the
// backend actually reported, since `km status` has no other way to see them.
func TestEtcdMaintainerPublishesTheStorageReport(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeEtcd{size: 300 << 20, inUse: 40 << 20}
	srv := fake.start(t)
	silent := func(string, ...any) {}
	m := newEtcdMaintainer(&http.Client{}, srv.URL, dir, "",
		cpSysConfig{EtcdQuotaMB: 4096, EtcdDefragMins: 60}, silent, silent, nil)

	m.run(context.Background(), time.Now())

	data, err := os.ReadFile(filepath.Join(dir, etcdStorageFile))
	if err != nil {
		t.Fatalf("no storage report published: %v", err)
	}
	var got etcdStorageReport
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("storage report is not valid JSON: %v", err)
	}
	if got.DBSize != 300<<20 || got.DBSizeInUse != 40<<20 {
		t.Fatalf("report sizes = %d/%d, want %d/%d",
			got.DBSize, got.DBSizeInUse, 300<<20, 40<<20)
	}
	if got.QuotaBytes != int64(4096)<<20 {
		t.Fatalf("report quota = %d, want %d", got.QuotaBytes, int64(4096)<<20)
	}
	if got.DefragMins != 60 {
		t.Fatalf("report defragMins = %d, want 60", got.DefragMins)
	}
	if !got.Fsync {
		t.Fatalf("report says fsync is off; these instances are durable")
	}
	if got.DataDir != dir {
		t.Fatalf("report dataDir = %q, want %q", got.DataDir, dir)
	}
	if got.TS == 0 {
		t.Fatalf("report carries no timestamp, so km cannot age it")
	}
}

// Both ends of the instance's life publish, so `km status` on a stopped
// instance still shows the size it was left at.
func TestEtcdMaintainerLogStoragePublishes(t *testing.T) {
	dir := t.TempDir()
	fake := &fakeEtcd{size: 12 << 20, inUse: 8 << 20}
	srv := fake.start(t)
	lines := 0
	count := func(string, ...any) { lines++ }
	m := newEtcdMaintainer(&http.Client{}, srv.URL, dir, "",
		cpSysConfig{EtcdQuotaMB: 4096, EtcdDefragMins: 0}, count, count, nil)

	m.logStorage(context.Background(), "at shutdown")

	if lines == 0 {
		t.Fatalf("logStorage logged nothing")
	}
	if _, err := os.Stat(filepath.Join(dir, etcdStorageFile)); err != nil {
		t.Fatalf("logStorage published no report: %v", err)
	}
}

// A quota default that drifts silently would be a durability surprise, so it
// is pinned: 4 GiB, deliberately chosen over etcd's own 2 GiB.
func TestDefaultEtcdQuotaIs4GiB(t *testing.T) {
	if got := defaultCPSysConfig().EtcdQuotaMB; got != 4096 {
		t.Fatalf("default EtcdQuotaMB = %d, want 4096", got)
	}
}

// filepath.Join("", name) yields a bare relative name, which would drop the
// report into the working directory. Empty in, empty out.
func TestStorageReportPathStaysEmptyWithoutADataDir(t *testing.T) {
	if got := storageReportPath(""); got != "" {
		t.Fatalf("storageReportPath(\"\") = %q, want \"\"", got)
	}
	if got := storageReportPath("/wd/data/cp"); got != filepath.Join("/wd/data/cp", etcdStorageFile) {
		t.Fatalf("storageReportPath = %q, unexpected", got)
	}
}

// A maintainer with no data directory must hold no path at all: a relative
// one would drop the report into whatever the working directory happens to
// be, which is how it once landed in the source tree.
func TestEtcdMaintainerWithoutDataDirHasNoStoragePath(t *testing.T) {
	fake := &fakeEtcd{size: 1 << 20, inUse: 1 << 20}
	srv := fake.start(t)
	m := quietMaintainer(srv.URL, cpSysConfig{EtcdQuotaMB: 4096, EtcdDefragMins: 60})

	if m.storagePath != "" {
		t.Fatalf("storagePath = %q with no data dir; publish would write there", m.storagePath)
	}
	// Exercises publish through the guard: it must be a no-op, not a write.
	m.run(context.Background(), time.Now())
}

// ---------------------------------------------------------------------------
// km message channel
// ---------------------------------------------------------------------------

// km sorts message lines as plain strings, so the timestamp must be fixed
// width, fixed precision and always UTC or the ordering silently breaks.
func TestMessageTimeFormatSortsChronologically(t *testing.T) {
	earlier := time.Date(2026, 8, 22, 10, 0, 0, 1_000_000, time.UTC).Format(messageTimeFormat)
	later := time.Date(2026, 8, 22, 10, 0, 0, 2_000_000, time.UTC).Format(messageTimeFormat)
	nextSecond := time.Date(2026, 8, 22, 10, 0, 1, 0, time.UTC).Format(messageTimeFormat)

	if !(earlier < later && later < nextSecond) {
		t.Fatalf("timestamps do not sort chronologically: %q %q %q", earlier, later, nextSecond)
	}
	if len(earlier) != len(nextSecond) {
		t.Fatalf("timestamps vary in width (%q vs %q); string ordering would break",
			earlier, nextSecond)
	}
	if !strings.HasSuffix(earlier, "Z") {
		t.Fatalf("timestamp %q is not UTC", earlier)
	}
}

func TestWriteMessageAppendsNdjson(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, messagesDir), 0755); err != nil {
		t.Fatalf("creating the messages dir failed: %v", err)
	}

	writeMessage(dir, messageStartPhase, messageLevelInfo, "first %d", 1)
	writeMessage(dir, messageStartPhase, messageLevelWarn, "second")

	data, err := os.ReadFile(filepath.Join(dir, messagesDir, "start-control-plane.json"))
	if err != nil {
		t.Fatalf("no message file written: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 ndjson lines, got %d: %q", len(lines), data)
	}

	var first componentMessage
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatalf("line 1 is not valid JSON: %v", err)
	}
	if first.Message != "first 1" {
		t.Fatalf("message = %q, want %q", first.Message, "first 1")
	}
	if first.Binary != "control-plane" {
		t.Fatalf("binary = %q, want control-plane", first.Binary)
	}
	if first.Level != messageLevelInfo {
		t.Fatalf("level = %q, want %q", first.Level, messageLevelInfo)
	}
	if first.Time == "" {
		t.Fatalf("no timestamp, so km cannot order the line")
	}

	var second componentMessage
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatalf("line 2 is not valid JSON: %v", err)
	}
	if second.Level != messageLevelWarn {
		t.Fatalf("level = %q, want %q", second.Level, messageLevelWarn)
	}
}

// The start and stop sets must not land in the same file, or km would
// reprint the startup messages as the stack comes down.
func TestWriteMessageSeparatesPhases(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, messagesDir), 0755); err != nil {
		t.Fatalf("creating the messages dir failed: %v", err)
	}

	writeMessage(dir, messageStartPhase, messageLevelInfo, "up")
	writeMessage(dir, messageStopPhase, messageLevelInfo, "down")

	for name, want := range map[string]string{
		"start-control-plane.json": "up",
		"stop-control-plane.json":  "down",
	} {
		data, err := os.ReadFile(filepath.Join(dir, messagesDir, name))
		if err != nil {
			t.Fatalf("%s missing: %v", name, err)
		}
		if !strings.Contains(string(data), want) {
			t.Fatalf("%s does not carry %q: %s", name, want, data)
		}
	}
}

// Reporting must never be able to break a start or a stop.
func TestWriteMessageSurvivesAMissingDirectory(t *testing.T) {
	writeMessage(t.TempDir(), messageStartPhase, messageLevelInfo, "no messages dir here")
	writeMessage("", messageStartPhase, messageLevelInfo, "no workdir at all")
}

// The etcd summary reaches km's channel, and a backend close to its quota is
// raised to WARN with the setting to change named in the text.
func TestLogStorageReportsThroughTheMessageChannel(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, messagesDir), 0755); err != nil {
		t.Fatalf("creating the messages dir failed: %v", err)
	}
	// 3.9 GiB live against a 4 GiB quota: over etcdQuotaNearlyFull.
	fake := &fakeEtcd{size: 4090 << 20, inUse: 4000 << 20}
	srv := fake.start(t)
	silent := func(string, ...any) {}
	m := newEtcdMaintainer(&http.Client{}, srv.URL, dir, dir,
		cpSysConfig{EtcdQuotaMB: 4096, EtcdDefragMins: 60}, silent, silent, nil)

	m.logStorage(context.Background(), messageStartPhase)

	data, err := os.ReadFile(filepath.Join(dir, messagesDir, "start-control-plane.json"))
	if err != nil {
		t.Fatalf("etcd summary never reached the message channel: %v", err)
	}
	var got componentMessage
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &got); err != nil {
		t.Fatalf("message is not valid JSON: %v", err)
	}
	if got.Level != messageLevelWarn {
		t.Fatalf("level = %q, want %q for a backend near its quota", got.Level, messageLevelWarn)
	}
	for _, want := range []string{"etcd backend", "quota", "etcdQuotaMb"} {
		if !strings.Contains(got.Message, want) {
			t.Fatalf("message %q does not mention %q", got.Message, want)
		}
	}
}

// A backend with room reports at INFO and does not nag about the quota.
func TestLogStorageReportsInfoWhenTheBackendHasRoom(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, messagesDir), 0755); err != nil {
		t.Fatalf("creating the messages dir failed: %v", err)
	}
	fake := &fakeEtcd{size: 12 << 20, inUse: 8 << 20}
	srv := fake.start(t)
	silent := func(string, ...any) {}
	m := newEtcdMaintainer(&http.Client{}, srv.URL, dir, dir,
		cpSysConfig{EtcdQuotaMB: 4096, EtcdDefragMins: 60}, silent, silent, nil)

	m.logStorage(context.Background(), messageStartPhase)

	data, _ := os.ReadFile(filepath.Join(dir, messagesDir, "start-control-plane.json"))
	var got componentMessage
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &got); err != nil {
		t.Fatalf("message is not valid JSON: %v", err)
	}
	if got.Level != messageLevelInfo {
		t.Fatalf("level = %q, want %q", got.Level, messageLevelInfo)
	}
	if strings.Contains(got.Message, "etcdQuotaMb") {
		t.Fatalf("message nags about the quota with room to spare: %q", got.Message)
	}
}

// ---------------------------------------------------------------------------
// Startup budget
// ---------------------------------------------------------------------------

// Coming up gets its own budget because it is the expensive part: bringing up
// etcd, an API server and a controller manager is nothing like the work a
// steady-state probe round measures.
func TestStartupBudgetIsItsOwnSetting(t *testing.T) {
	if got := startupBudget(120); got != 2*time.Minute {
		t.Fatalf("startupBudget(120) = %s, want 2m", got)
	}
	if got := startupBudget(30); got != 30*time.Second {
		t.Fatalf("startupBudget(30) = %s, want 30s", got)
	}
}

// The regression this exists to prevent. probeWait spaces startup rounds at
// startupRetry, so applying the steady-state miss budget to them allowed
// (maxMissed-1) * 250ms — monitorRound shuts down *on* the last miss, not
// after it. Observed in the field: kube-controller-manager took ~785ms to
// answer /healthz and the control plane shut down on the fourth round.
func TestStartupBudgetClearsTheObservedKcmStartup(t *testing.T) {
	const observedKcmStartup = 785 * time.Millisecond

	byRounds := startupRetry * (defaultMaxMissedHeartbeats - 1)
	if byRounds >= observedKcmStartup {
		t.Fatalf("counting rounds allowed %s, so it would not have failed at %s",
			byRounds, observedKcmStartup)
	}

	budget := startupBudget(defaultStartupSecs)
	if budget < 100*observedKcmStartup {
		t.Fatalf("startup budget %s leaves little room over the %s kcm needed",
			budget, observedKcmStartup)
	}
}

// 0 would fail the very first round, before any child could answer, so it
// falls back to the default rather than being taken literally.
func TestStartupBudgetFallsBackOnANonPositiveSetting(t *testing.T) {
	want := time.Duration(defaultStartupSecs) * time.Second
	for _, secs := range []int{0, -1} {
		if got := startupBudget(secs); got != want {
			t.Fatalf("startupBudget(%d) = %s, want the default %s", secs, got, want)
		}
	}
}

// The default has to be well clear of a real cold start, which the log that
// prompted this showed reaching ready in about 4s once it got the chance.
func TestDefaultStartupSecsIsGenerous(t *testing.T) {
	if defaultStartupSecs < 60 {
		t.Fatalf("defaultStartupSecs = %d, too tight for a cold start", defaultStartupSecs)
	}
	steadyState := defaultHeartbeatSecs * defaultMaxMissedHeartbeats
	if defaultStartupSecs <= steadyState {
		t.Fatalf("startup budget %ds is no more generous than the steady state (%ds)",
			defaultStartupSecs, steadyState)
	}
}

// probeWait keeps startup rounds fast — that is what makes the budget
// wall-clock rather than a round count, so it is pinned here beside it.
func TestProbeWaitStaysFastUntilHealthyWhichIsWhyTheBudgetIsWallClock(t *testing.T) {
	if got := probeWait(false, 15*time.Second); got != startupRetry {
		t.Fatalf("probeWait before healthy = %s, want %s", got, startupRetry)
	}
	if got := probeWait(true, 15*time.Second); got != 15*time.Second {
		t.Fatalf("probeWait once healthy = %s, want the interval", got)
	}
}
