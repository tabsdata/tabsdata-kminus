/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	// Must be built with CGO_ENABLED=0: gopsutil v3 pulls in go-m1cpu, whose cgo
	// package initializer reads M1-era IORegistry keys ("voltage-states5-sram")
	// and null-derefs on Apple Silicon generations that no longer publish them,
	// killing this binary before main() runs. Without cgo that initializer is
	// not compiled in and gopsutil takes its pure-Go paths.
	//
	// This is a workaround, not a fix: it constrains the whole binary to satisfy
	// one transitive dependency. The clean fix is to upgrade to gopsutil v4,
	// which dropped go-m1cpu entirely in favour of ebitengine/purego and needs
	// no cgo. The process API used here (Processes, PidExists, NewProcess, Ppid,
	// Name) is unchanged between v3 and v4, so the migration is an import-path
	// bump; once done, CGO_ENABLED=0 can be dropped from build_go.toml.
	//
	// Note a newer Go toolchain does NOT help: go-m1cpu's cgo initializer is
	// still compiled and still crashes, regardless of Go version.
	"github.com/shirou/gopsutil/v3/process"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// cpSysConfig contains tunables for the control-plane's subprocesses.
// Persisted in cpsysconfig.yaml — created with defaults on first startup,
// user-editable thereafter. Fields missing from an existing file keep their
// default, so a file written by an earlier build stays valid unchanged.
type cpSysConfig struct {
	ConcurrentJobSyncs int `yaml:"concurrentJobSyncs"`
	KubeAPIQPS         int `yaml:"kubeApiQps"`
	KubeAPIBurst       int `yaml:"kubeApiBurst"`

	// EtcdQuotaMB caps etcd's backend file. Crossing it arms a NOSPACE alarm
	// that makes the cluster read-only, and etcd reports an armed alarm as
	// unhealthy — so without the recovery in etcdMaintainer it would also
	// exhaust the probe miss budget and take the control plane down. etcd's
	// own default is 2 GiB, which one large load test can reach.
	EtcdQuotaMB int `yaml:"etcdQuotaMb"`

	// EtcdDefragMins is how often the backend is considered for
	// defragmentation; 0 disables it. Compaction — which kube-apiserver runs
	// every 5 minutes by default — only marks pages reusable inside the file:
	// the file itself never shrinks, and those pages stay charged against
	// EtcdQuotaMB. Defragmentation rewrites the file without them, so the peak
	// size of a load test that finished days ago stops costing quota forever.
	// It pauses every etcd read and write while it runs, which is why it is
	// periodic and size-gated rather than continuous. Setting 0 disables only
	// this periodic pass: a NOSPACE alarm is still recovered from, because the
	// alternative to that pause is a control plane the probe loop shuts down.
	EtcdDefragMins int `yaml:"etcdDefragMins"`
}

func defaultCPSysConfig() cpSysConfig {
	return cpSysConfig{
		ConcurrentJobSyncs: 50,
		KubeAPIQPS:         500,
		KubeAPIBurst:       1000,
		EtcdQuotaMB:        4096,
		EtcdDefragMins:     60,
	}
}

// clampEtcd replaces out-of-range etcd tunables with their defaults. etcd
// reads a non-positive quota as "use the 2 GiB default", which is the cliff
// EtcdQuotaMB exists to move; a negative interval would defragment every
// round. Neither is what a typo in the file was meant to say.
func (c *cpSysConfig) clampEtcd() {
	defaults := defaultCPSysConfig()
	if c.EtcdQuotaMB <= 0 {
		c.EtcdQuotaMB = defaults.EtcdQuotaMB
	}
	if c.EtcdDefragMins < 0 {
		c.EtcdDefragMins = defaults.EtcdDefragMins
	}
}

// loadOrCreateCPSysConfig reads cpsysconfig.yaml or creates it with defaults.
func loadOrCreateCPSysConfig(configDir string) (cpSysConfig, error) {
	path := filepath.Join(configDir, "cpsysconfig.yaml")
	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return cpSysConfig{}, fmt.Errorf("read %s: %w", path, err)
		}
		cfg := defaultCPSysConfig()
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return cpSysConfig{}, fmt.Errorf("parse %s: %w", path, err)
		}
		cfg.clampEtcd()
		return cfg, nil
	}
	cfg := defaultCPSysConfig()
	data, _ := yaml.Marshal(&cfg)
	header := []byte("# Control-plane system configuration.\n" +
		"# Tunables for the kube-controller-manager and etcd subprocesses.\n\n")
	if err := os.WriteFile(path, append(header, data...), 0644); err != nil {
		return cpSysConfig{}, fmt.Errorf("write %s: %w", path, err)
	}
	return cfg, nil
}

// tokensFile is the kube-apiserver static token file (--token-auth-file),
// created with random tokens on first startup and reused thereafter so
// the kubeconfigs (and their credentials) stay stable across restarts.
// Pure CSV — the apiserver's parser does not support comment lines.
const tokensFile = "tokens.csv"

// fixedAuthTokenEnv, when set to "true", is a DEVELOPMENT-ONLY override that
// makes the admin token deterministic — derived from the current OS username
// instead of random. It yields a stable, predictable admin credential in
// kubeconfig.yaml across fresh instances and machines (handy for scripting
// and tests) at the cost of secrecy. Never enable it for a real deployment.
const fixedAuthTokenEnv = "TD_DEV_FIXED_AUTH_TOKEN"

// Authenticated identities provisioned in tokens.csv.
const (
	adminUserName = "admin"
	vkUserName    = "virtual-kubelet"
	kcmUserName   = "system:kube-controller-manager"
)

// Heartbeat defaults, mirroring km's own so a standalone run behaves like
// a supervised one.
const (
	defaultHeartbeatSecs       = 15
	defaultMaxMissedHeartbeats = 4

	// defaultStartupSecs is how long the children get, in total, to all
	// answer their health probes before this process gives up on coming up.
	//
	// Its own budget rather than heartbeatSecs * maxMissedHeartbeats, which
	// describes a running control plane and is far too tight for a starting
	// one: bringing up etcd, an API server and a controller manager is the
	// expensive part, and on a cold or loaded machine it is nothing like the
	// work a steady-state round measures. km overrides this with the value
	// from kmsysconfig.yaml.
	//
	// Generous rather than tuned, because overrunning it is not how a failed
	// start is normally caught: a child that exits is noticed within a tick,
	// by pid, and ends the wait immediately. This bounds only the other case
	// — a child that stays alive and never becomes healthy.
	defaultStartupSecs = 120
)

// Heartbeat statuses. Only statusReady is accepted as healthy by km.
// Kept in step with the virtual-kubelet copy; see the note above writeHeartbeat.
const (
	statusReady    = "ready"
	statusDegraded = "degraded"
)

// heartbeatTmpSuffix names the staging file the beat is written to before
// being renamed into place.
const heartbeatTmpSuffix = ".tmp"

// portUnset is the port value meaning "not pinned by km": envtest allocates a
// free one instead.
const portUnset = "0"

// etcdPeerURLsFlag is etcd's peer-listen flag, overridden by argument because
// envtest exposes no field for it.
const etcdPeerURLsFlag = "listen-peer-urls"

// etcd storage tuning. envtest passes etcd nothing but its URLs and data
// directory, so everything else is etcd's own default — chosen for a
// throwaway test environment rather than an instance that is kept.
const (
	// etcdAutoCompactionRetention is a safety net under kube-apiserver's
	// compactor, which trims revision history every --etcd-compaction-interval
	// (5m by default). etcd compacts nothing on its own unless told to, so if
	// that compactor ever stops, history grows until the quota is reached.
	etcdAutoCompactionRetention = "30m"

	// etcdMaxWALs and etcdMaxSnapshots bound the write-ahead log and snapshot
	// files, which are separate from the backend and untouched by
	// defragmentation. Each WAL segment is preallocated at 64 MB, so the count
	// is a fixed disk cost paid whether or not the instance is busy: a freshly
	// created etcd already occupies its first segments holding no data at all.
	etcdMaxWALs      = 3
	etcdMaxSnapshots = 3

	// etcdSnapshotCount is lowered from etcd's default of 100000 because WAL
	// segments only become eligible for purging once a snapshot has passed
	// them: at the default, a quiet instance retains every segment it has
	// written for a very long time.
	etcdSnapshotCount = 10000
)

// km's message channel. A component with something the operator should see
// writes it here as well as into its own log, and km prints the collected set
// once the stack is up and again as it comes down. Reporting only: readiness
// stays with the heartbeat, and a message that cannot be written is dropped
// rather than allowed to affect a start or a stop.
const (
	// messagesDir sits under the workdir and is emptied by km at startup.
	messagesDir = "messages"

	// messageStartPhase and messageStopPhase are the filename prefixes that
	// keep the two sets apart: km reads <phase><binary>.json.
	messageStartPhase = "start-"
	messageStopPhase  = "stop-"

	// messageTimeFormat is RFC 3339 in UTC at fixed millisecond precision.
	// km sorts these as strings, so the precision and the zone must not vary.
	messageTimeFormat = "2006-01-02T15:04:05.000Z07:00"
)

// Levels km understands. Anything else is shown as written.
const (
	messageLevelInfo  = "INFO"
	messageLevelWarn  = "WARN"
	messageLevelError = "ERROR"
)

// componentMessage is one newline-delimited JSON record in a message file.
type componentMessage struct {
	Time    string `json:"time"`
	Binary  string `json:"binary"`
	Level   string `json:"level"`
	Message string `json:"message"`
}

// writeMessage appends one message for km to print. Best-effort throughout:
// the directory is km's to create, and a control plane that cannot report is
// still a working control plane.
func writeMessage(workdir, phase, level, format string, args ...any) {
	if workdir == "" {
		return
	}
	record, err := json.Marshal(componentMessage{
		Time:    time.Now().UTC().Format(messageTimeFormat),
		Binary:  "control-plane",
		Level:   level,
		Message: fmt.Sprintf(format, args...),
	})
	if err != nil {
		return
	}
	path := filepath.Join(workdir, messagesDir, phase+"control-plane.json")
	// Appended, so several messages accumulate across a phase without the
	// writer having to hold or re-read what it already said.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(record, '\n'))
}

// etcdStorageFile is the backend report the control-plane publishes into its
// data directory for `km status` to read. km speaks no HTTP and, when ports
// are random, does not know the one envtest gave etcd — so the process that
// does know publishes, exactly as it already does for pids and heartbeats.
// Written tmp-then-renamed so a reader never sees half a file.
const etcdStorageFile = "etcd-storage.json"

// etcdReportTimeout bounds the status call behind a startup or shutdown
// report. Short: a report is never worth delaying either one for.
const etcdReportTimeout = 5 * time.Second

// etcdFsync records that this control plane runs etcd with fsync enabled.
// envtest passes --unsafe-no-fsync=true unconditionally, which suits an
// environment that is thrown away after a test and not one whose state is
// meant to survive: without fsync an unclean host shutdown can lose or
// corrupt recently acknowledged writes. km disables that flag, so this is a
// constant rather than a setting — durability here is not a tunable.
const etcdFsync = true

// etcdStorageReport is what km reads. A new, single-purpose contract rather
// than a wider heartbeat: the {ts,status,detail} beat has four independent
// implementations, and km treats this file as optional, so a missing or
// unreadable one degrades to "unknown" instead of failing anything.
type etcdStorageReport struct {
	TS          int64  `json:"ts"`
	DBSize      int64  `json:"dbSize"`
	DBSizeInUse int64  `json:"dbSizeInUse"`
	QuotaBytes  int64  `json:"quotaBytes"`
	DefragMins  int    `json:"defragMins"`
	Fsync       bool   `json:"fsync"`
	DataDir     string `json:"dataDir"`
}

// etcd maintenance endpoints, served over the v3 HTTP gateway that etcd
// enables by default. Used in preference to the etcd client library, which
// would be a new module dependency for three calls against a local,
// unauthenticated, plaintext listener.
const (
	// etcdStatusPath reports dbSize, dbSizeInUse and any armed alarms.
	etcdStatusPath = "/v3/maintenance/status"
	// etcdDefragPath rewrites the backend without its free pages, blocking
	// every read and write until it finishes.
	etcdDefragPath = "/v3/maintenance/defragment"
	// etcdAlarmPath lists and disarms alarms.
	etcdAlarmPath = "/v3/maintenance/alarm"
)

const (
	// etcdDefragMinBytes is the backend size below which periodic
	// defragmentation is skipped: there is not enough to reclaim to be worth
	// pausing the cluster for.
	etcdDefragMinBytes int64 = 512 << 20

	// etcdDefragMinFreeRatio is the share of the backend that must be free
	// pages before periodic defragmentation runs. Half means the file has
	// grown to twice the data it actually holds.
	etcdDefragMinFreeRatio = 0.5

	// etcdAlarmRetryInterval rate-limits NOSPACE recovery, which ignores both
	// the size gate and the defragmentation cadence. Without it, a quota that
	// is genuinely too small for the live data — where defragmenting reclaims
	// nothing and the alarm re-arms at once — would defragment every round.
	etcdAlarmRetryInterval = 5 * time.Minute

	// etcdMaintenanceTimeout bounds one status, defragment or disarm call.
	// Generous because defragmentation rewrites the entire backend.
	etcdMaintenanceTimeout = 2 * time.Minute

	// etcdQuotaNearlyFull is the share of the quota still live after a
	// recovery above which the quota itself is reported as too small: at that
	// point the space is real data and no defragmentation will help.
	etcdQuotaNearlyFull = 0.8
)

// fixedEtcdURL renders etcd's client URL for a pinned port, or nil to leave
// the choice to envtest.
func fixedEtcdURL(port string) *url.URL {
	if port == portUnset {
		return nil
	}
	return &url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort("127.0.0.1", port),
	}
}

// kcmName is the kube-controller-manager's binary and process name, used for
// its pid file, its probe label and its liveness check.
const kcmName = "kube-controller-manager"

// kcmStopTimeout bounds the wait for kube-controller-manager to exit after
// SIGTERM at shutdown.
const kcmStopTimeout = 10 * time.Second

// envtestStopTimeout bounds envtest's own attempt to stop etcd and the API
// server at shutdown.
const envtestStopTimeout = 5 * time.Second

// listenerDialTimeout bounds one tick-speed port check. Generous for
// loopback, where a listening socket accepts immediately.
const listenerDialTimeout = time.Second

// processTick is how often the children are checked for still being
// processes, in between the slower probe rounds. An exited child is
// definitive and costs nothing to see, so it should not wait out an interval.
const processTick = time.Second

// probeTimeout bounds a single child probe, kept well under the heartbeat
// interval so a wedged child fails its round instead of stalling it.
const probeTimeout = 5 * time.Second

// startupRetry spaces probe rounds until the first success: km's readiness
// gate waits on that first beat, so it must not be an interval away.
const startupRetry = 250 * time.Millisecond

// Health endpoints of the children. etcd predates the /healthz convention.
const (
	etcdHealthPath      = "/health"
	kcmHealthPath       = "/healthz"
	apiServerReadyzPath = "/readyz"
)

// tokenUsers defines the users provisioned in tokens.csv and the groups
// embedded in each token's identity.
var tokenUsers = []struct {
	name   string
	groups string
}{
	{adminUserName, "system:masters"},
	{vkUserName, "system:masters,system:nodes"},
	{kcmUserName, "system:masters"},
}

// writeHeartbeat, the status/suffix constants above, and the wait rule
// (probeWait here, heartbeatWait there) are duplicated near-verbatim in the
// virtual-kubelet: mods/virtual-kubelet/src/main/go/main.go.
//
// Why there is no common package yet: these are two separate Go modules —
// "control-plane" and "minimal-vk", each with its own go.mod — so sharing
// ~40 lines means adding a third module plus require/replace wiring in both,
// or vendoring a copy that drifts anyway. At two consumers that costs more
// than the duplication; extract it when a third Go component needs to beat.
//
// What must stay in step across the copies: the {ts,status,detail} shape, the
// tmp-then-rename publish, the ".tmp" suffix, and the status strings — km
// accepts only "ready", so a typo here reads as a permanently degraded
// component rather than an error. The same contract is implemented again in
// Rust, by km (mods/kminus/src/system/heartbeat.rs, the reader) and the
// pod-runner (mods/pod-runner/src/server/heartbeat.rs), which no Go package
// can unify — docs/KmReadinessLiveness.md is the source of truth for it.
//
// writeHeartbeat publishes one beat for km to read. Failures are logged and
// never fatal: km sees the file go stale and decides what to do.
func writeHeartbeat(path, status, detail string, warn func(string, ...any)) {
	if path == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		warn("cannot create heartbeat directory %s: %v", filepath.Dir(path), err)
		return
	}
	body, err := json.Marshal(map[string]any{
		"ts":     time.Now().Unix(),
		"status": status,
		"detail": detail,
	})
	if err != nil {
		warn("cannot encode heartbeat: %v", err)
		return
	}
	// Write then rename, so km never reads a half-written beat and counts it
	// as a miss. Rename replaces the target on Windows as well as POSIX.
	tmp := path + heartbeatTmpSuffix
	if err := os.WriteFile(tmp, body, 0644); err != nil {
		warn("cannot write heartbeat %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		warn("cannot publish heartbeat %s: %v", path, err)
		os.Remove(tmp)
	}
}

// childProbe is the health check for one control-plane child.
type childProbe struct {
	name  string
	check func(ctx context.Context) error
}

// listenerGone reports whether a probe error means nothing is listening any
// more, as opposed to a slow or unhealthy child. A refused connection is as
// definitive as a dead pid: the socket is closed, and no amount of waiting
// will bring it back on its own.
//
// The errno comparison is platform-split; see connectionRefused.
func listenerGone(err error) bool {
	return connectionRefused(err)
}

// httpProbe expects a 2xx from url — etcd's /health and the
// kube-controller-manager's /healthz, both of which need no credentials.
func httpProbe(client *http.Client, url string) func(context.Context) error {
	return func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return fmt.Errorf("GET %s returned %s", url, resp.Status)
		}
		return nil
	}
}

// probeAll runs every probe and returns a description of the failures, or
// "" when all children are healthy. Each probe gets its own timeout so one
// wedged child cannot consume the whole round.
//
// `gone` is set when a failure means a child has stopped listening
// altogether, which the caller acts on immediately instead of spending the
// miss budget on something that cannot recover.
func probeAll(ctx context.Context, probes []childProbe) (detail string, gone bool) {
	var failures []string
	for _, probe := range probes {
		probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
		err := probe.check(probeCtx)
		cancel()
		if err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", probe.name, err))
			gone = gone || listenerGone(err)
		}
	}
	return strings.Join(failures, "; "), gone
}

// childProcess is a child the control-plane can only check by pid: envtest
// owns etcd's and the API server's `exec.Cmd`, not this process.
type childProcess struct {
	name string
	pid  int
}

// childProcessName normalises a process name for comparison, trimming the
// Windows executable suffix so one spelling works on every platform.
func childProcessName(name string) string {
	if n := len(name) - len(".exe"); n > 0 && strings.EqualFold(name[n:], ".exe") {
		return name[:n]
	}
	return name
}

// processGone reports whether a recorded child is no longer running.
//
// Two guards matter: a zombie still "exists" but is dead, and would otherwise
// read as alive until something reaps it; and a recycled pid belongs to a
// different process, so the name is compared too (as killStaleProcesses does).
func processGone(name string, pid int) bool {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return true
	}
	if statuses, err := p.Status(); err == nil {
		for _, status := range statuses {
			if status == process.Zombie {
				return true
			}
		}
	}
	running, err := p.Name()
	if err != nil {
		return true
	}
	return childProcessName(running) != name
}

// childListener is a child's listening address. Checked at tick speed
// alongside the pids, because a closed port is just as cheap to detect — and
// catches what a pid cannot: a child shutting down gracefully closes its
// listener first and can stay alive for a long time afterwards, which is
// exactly what a plain `kill` on kube-apiserver looks like.
type childListener struct {
	name string
	addr string
}

// listenerRefused reports whether nothing accepts connections on addr.
//
// Only a refused connection counts: a dial timeout means the port may still
// be there but slow, which is the interval probe's business, not a reason to
// tear the stack down.
func listenerRefused(addr string) bool {
	conn, err := net.DialTimeout("tcp", addr, listenerDialTimeout)
	if err != nil {
		return listenerGone(err)
	}
	conn.Close()
	return false
}

// watchExit reaps `cmd` and reports how it ended on a buffered channel.
//
// Waiting in a goroutine buys two things a health probe cannot: the exit
// status, so the log says why rather than just "connection refused", and the
// reaping itself — otherwise a child that dies early stays a zombie until the
// control-plane exits.
func watchExit(name string, cmd *exec.Cmd) <-chan string {
	exited := make(chan string, 1)
	go func() {
		if err := cmd.Wait(); err != nil {
			exited <- fmt.Sprintf("%s exited: %v", name, err)
			return
		}
		exited <- fmt.Sprintf("%s exited cleanly, which it must not do while the control plane runs", name)
	}()
	return exited
}

// childLost describes a child that is definitively lost, or "" when nothing is
// conclusively wrong. Unlike a failed probe these answers cannot improve on
// their own — an exited process is not coming back and a refused port is not
// reopening — so the caller acts at once instead of spending the miss budget.
//
// Only the cheap checks live here, because this runs on every tick: a
// non-blocking channel receive for the child this process owns, and a loopback
// dial per listener (tens of microseconds). `served` gates the ports, since
// before a child has ever answered a refused connection only means it is still
// binding.
func childLost(exited <-chan string, listeners []childListener, served bool) string {
	select {
	case detail := <-exited:
		return detail
	default:
	}
	if !served {
		return ""
	}
	for _, listener := range listeners {
		if listenerRefused(listener.addr) {
			return fmt.Sprintf("%s is no longer listening on %s", listener.name, listener.addr)
		}
	}
	return ""
}

// deadChildProcess names a recorded child whose process is gone, or "" while
// they are all running.
//
// This rides the probe round rather than the tick: gopsutil reads a process's
// name and state by invoking `ps` on some platforms, which costs milliseconds
// and a subprocess per child — 85x the price of the port check above, for an
// answer the port check almost always reaches first. What it adds is naming a
// process that has died, and catching one whose port something else has since
// taken over.
func deadChildProcess(procs []childProcess) string {
	for _, child := range procs {
		if processGone(child.name, child.pid) {
			return fmt.Sprintf("%s (pid %d) is no longer running", child.name, child.pid)
		}
	}
	return ""
}

// monitorAction is what the supervision loop does after a probe round.
type monitorAction int

const (
	actionContinue monitorAction = iota
	actionShutdown
)

// monitorRound folds one probe round into the new consecutive-miss count and
// the action to take: a healthy round clears the count, and the limit is
// reached only by consecutive failures. Pure — the logging, the heartbeat and
// the waiting are the caller's.
func monitorRound(detail string, missed, maxMissed int) (int, monitorAction) {
	if maxMissed < 1 {
		// 0 would fail the very first round, before any child can answer.
		maxMissed = 1
	}
	if detail == "" {
		return 0, actionContinue
	}
	missed++
	if missed >= maxMissed {
		return missed, actionShutdown
	}
	return missed, actionContinue
}

// tickWait is how long to sleep before looking again: one process tick, or
// less when the next probe round falls sooner — so a 250ms startup retry is
// not stretched out to a whole tick.
func tickWait(untilNextRound, tick time.Duration) time.Duration {
	if untilNextRound > 0 && untilNextRound < tick {
		return untilNextRound
	}
	return tick
}

// startupBudget is how long the children get, in total, to all answer their
// health probes before the control plane gives up on coming up.
//
// Wall clock, not a count of rounds. probeWait spaces startup rounds at
// startupRetry (250ms) so km's readiness gate is not rounded up to a whole
// interval; applying the steady-state miss budget to rounds that fast allowed
// about a second, and kube-controller-manager routinely needs longer than
// that to answer /healthz on a busy machine. The control plane was shutting
// itself down over a child that was merely still starting.
//
// A non-positive setting falls back to the default rather than failing the
// first round, which is what 0 would otherwise mean.
func startupBudget(startupSecs int) time.Duration {
	if startupSecs < 1 {
		startupSecs = defaultStartupSecs
	}
	return time.Duration(startupSecs) * time.Second
}

// probeWait spaces the next round: fast until the first healthy one, since
// km's readiness gate is waiting on that first beat and would otherwise sit
// out a whole interval.
func probeWait(everHealthy bool, interval time.Duration) time.Duration {
	if everHealthy {
		return interval
	}
	return startupRetry
}

func currentUsername() string {
	if u, err := user.Current(); err == nil && u.Username != "" {
		return u.Username
	}
	for _, key := range []string{"USER", "USERNAME"} {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return "unknown"
}

// creates a fixed token from the given seed.
func seededToken(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	rng := mathrand.New(mathrand.NewSource(int64(binary.BigEndian.Uint64(sum[:8]))))
	raw := make([]byte, 32)
	rng.Read(raw)
	return hex.EncodeToString(raw)
}

func fixedAdminToken() string {
	if !strings.EqualFold(os.Getenv(fixedAuthTokenEnv), "true") {
		return ""
	}
	return seededToken(currentUsername())
}

// writeTokensFile writes tokens.csv from the token map, one record per
// provisioned user: token,user,uid,"group1,group2". The apiserver reads this
// as its --token-auth-file, so it must always match the kubeconfigs.
func writeTokensFile(path string, tokens map[string]string) error {
	var lines []string
	for _, u := range tokenUsers {
		lines = append(lines, fmt.Sprintf("%s,%s,%s,%q", tokens[u.name], u.name, u.name, u.groups))
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}

// loadOrCreateTokens reads tokens.csv or creates it with fresh random
// tokens. Returns a map of user name -> token. File format (one record
// per user): token,user,uid,"group1,group2".
//
// When the TD_DEV_FIXED_AUTH_TOKEN dev override is enabled, the admin token is
// forced to a deterministic value derived from the current OS username. This
// applies on every startup — including over an existing tokens.csv, which is
// rewritten so the apiserver's --token-auth-file stays in sync with the admin
// kubeconfig — while the other users keep their existing (random) tokens.
func loadOrCreateTokens(configDir string) (map[string]string, error) {
	path := filepath.Join(configDir, tokensFile)
	fixedAdmin := fixedAdminToken()

	if _, err := os.Stat(path); err == nil {
		file, err := os.Open(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		defer file.Close()
		reader := csv.NewReader(file)
		reader.FieldsPerRecord = -1
		records, err := reader.ReadAll()
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
		tokens := make(map[string]string)
		for _, record := range records {
			if len(record) >= 2 {
				tokens[record[1]] = record[0]
			}
		}
		for _, u := range tokenUsers {
			if tokens[u.name] == "" {
				return nil, fmt.Errorf("%s: no token for user %q (delete the file to regenerate it)", path, u.name)
			}
		}
		if fixedAdmin != "" && tokens[adminUserName] != fixedAdmin {
			tokens[adminUserName] = fixedAdmin
			if err := writeTokensFile(path, tokens); err != nil {
				return nil, err
			}
		}
		return tokens, nil
	}

	tokens := make(map[string]string)
	for _, u := range tokenUsers {
		if u.name == adminUserName && fixedAdmin != "" {
			tokens[u.name] = fixedAdmin
			continue
		}
		raw := make([]byte, 32)
		if _, err := rand.Read(raw); err != nil {
			return nil, fmt.Errorf("generate token: %w", err)
		}
		tokens[u.name] = hex.EncodeToString(raw)
	}
	if err := writeTokensFile(path, tokens); err != nil {
		return nil, err
	}
	return tokens, nil
}

// writeKubeconfig writes a token-based kubeconfig. Token auth keeps the
// file contents stable across restarts, unlike client certificates,
// which envtest signs with a fresh in-memory CA on every start.
func writeKubeconfig(path, host, userLabel, token string) error {
	content := fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: local
contexts:
- context:
    cluster: local
    user: %s
  name: default
current-context: default
kind: Config
users:
- name: %s
  user:
    token: %s
`, host, userLabel, userLabel, token)
	return os.WriteFile(path, []byte(content), 0600)
}

// writePid records a pid file. A failure here means teardown and the next
// start cannot find this process, so it is reported rather than dropped.
func writePid(path string, pid int) {
	if err := os.WriteFile(path, []byte(strconv.Itoa(pid)), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "WARNING: cannot write pid file %s: %v\n", path, err)
	}
}

// writeChildPids records the pids of the children envtest started, which it
// does not expose. Returns what it found: anything missing cannot be reaped at
// teardown, cleaned up by the next start, or watched while running, so the
// caller warns.
func writeChildPids(pidsDir string) []childProcess {
	var found []childProcess
	myPid := int32(os.Getpid())
	procs, err := process.Processes()
	if err != nil {
		return found
	}
	for _, p := range procs {
		ppid, err := p.Ppid()
		if err != nil || ppid != myPid {
			continue
		}
		running, err := p.Name()
		if err != nil {
			continue
		}
		name := childProcessName(running)
		switch name {
		case "etcd", "kube-apiserver":
			writePid(filepath.Join(pidsDir, fmt.Sprintf("%d.%s", p.Pid, name)), int(p.Pid))
			found = append(found, childProcess{name: name, pid: int(p.Pid)})
			now := time.Now()
			log.Printf("I%s %s [+] %s pid: %d", now.Format("0102"), now.Format("15:04:05.000000"), name, p.Pid)
		}
	}
	return found
}

// processAlive reports whether a process with the given pid is running.
func processAlive(pid int) bool {
	exists, err := process.PidExists(int32(pid))
	return err == nil && exists
}

// waitForDeath waits up to timeout for a process to exit.
// Returns true if the process is gone.
func waitForDeath(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !processAlive(pid) {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// killAndWait requests graceful termination, waits, then force-kills if needed.
func killAndWait(pid int) {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return
	}
	requestStopPid(pid)
	if waitForDeath(pid, 5*time.Second) {
		return
	}
	proc.Kill()
	waitForDeath(pid, 2*time.Second)
}

// pidFileForProcess finds the PID file for a given process name.
func pidFileForProcess(pidsDir, processName string) (path string, pid int) {
	entries, err := os.ReadDir(pidsDir)
	if err != nil {
		return "", 0
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), "."+processName) {
			parts := strings.SplitN(e.Name(), ".", 2)
			if p, err := strconv.Atoi(parts[0]); err == nil {
				return filepath.Join(pidsDir, e.Name()), p
			}
		}
	}
	return "", 0
}

// stopAndCleanPid kills a process by name using its PID file, waits for
// it to die, then removes the PID file.
func stopAndCleanPid(pidsDir, processName string, info func(string, ...any)) {
	path, pid := pidFileForProcess(pidsDir, processName)
	if path == "" {
		return
	}
	// Check if still alive
	if !processAlive(pid) {
		os.Remove(path)
		return
	}
	info("[+] Force killing %s (pid: %d)...", processName, pid)
	killAndWait(pid)
	os.Remove(path)
}

func newRotatingLog(logDir, name string, maxSizeMB, maxFiles int) *lumberjack.Logger {
	return &lumberjack.Logger{
		Filename:   filepath.Join(logDir, name+".log"),
		MaxSize:    maxSizeMB,
		MaxBackups: maxFiles - 1,
		Compress:   false,
	}
}

// killStaleProcesses checks for PID files from a previous run. If the
// process still exists and its name matches the PID file, it is killed.
func killStaleProcesses(pidsDir string, warn func(string, ...any)) {
	entries, err := os.ReadDir(pidsDir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		name := entry.Name()
		parts := strings.SplitN(name, ".", 2)
		if len(parts) != 2 {
			continue
		}
		pid, err := strconv.Atoi(parts[0])
		if err != nil {
			continue
		}
		processName := parts[1]

		p, err := process.NewProcess(int32(pid))
		if err != nil {
			// Process no longer exists, clean up stale PID file
			os.Remove(filepath.Join(pidsDir, name))
			continue
		}

		pName, err := p.Name()
		if err != nil || childProcessName(pName) != processName {
			// PID reused by a different process, clean up stale PID file
			os.Remove(filepath.Join(pidsDir, name))
			continue
		}

		warn("Warning: killing stale %s process (pid: %d)", processName, pid)
		killAndWait(pid)
		os.Remove(filepath.Join(pidsDir, name))
	}
}

// reportCtx bounds a storage report so neither startup nor shutdown can be
// delayed by an etcd that is slow to answer.
func reportCtx(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, etcdReportTimeout)
}

// humanBytes renders a byte count for a log line.
func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// etcdStatus is the subset of /v3/maintenance/status this needs. The HTTP
// gateway renders 64-bit fields as JSON strings, hence the string types.
type etcdStatus struct {
	Header struct {
		MemberID string `json:"member_id"`
	} `json:"header"`
	DBSize      string   `json:"dbSize"`
	DBSizeInUse string   `json:"dbSizeInUse"`
	Errors      []string `json:"errors"`
}

// size is the backend file on disk; inUse is the part of it holding live
// data. The difference is free pages: reusable by etcd, not returned to the
// filesystem, and still counted against the backend quota.
func (s etcdStatus) size() int64 {
	n, _ := strconv.ParseInt(s.DBSize, 10, 64)
	return n
}

func (s etcdStatus) inUse() int64 {
	n, _ := strconv.ParseInt(s.DBSizeInUse, 10, 64)
	return n
}

// noSpace reports whether a NOSPACE alarm is armed. etcd renders alarms into
// the status "errors" list as free text, e.g. "memberID:12 alarm:NOSPACE ".
func (s etcdStatus) noSpace() bool {
	for _, e := range s.Errors {
		if strings.Contains(e, "NOSPACE") {
			return true
		}
	}
	return false
}

// etcdMaintainer keeps etcd's backend file within its quota.
//
// Two jobs on different cadences. Every round it reads the backend status and
// clears a NOSPACE alarm if one is armed. That is the urgent one: etcd
// reports an armed alarm as unhealthy, so an untended alarm spends the probe
// miss budget and shuts down a control plane whose only fault is a full
// backend — and neither compaction nor defragmentation disarms it, so nothing
// else recovers on its own.
//
// On its own slower cadence it defragments, which is what actually returns
// disk. Compaction leaves the pages it frees inside the file, where they stay
// charged against the quota, so a single large load test otherwise reserves
// its peak size for the life of the instance.
type etcdMaintainer struct {
	client *http.Client
	// base is etcd's client URL with no trailing slash.
	base  string
	quota int64
	// every is the defragmentation cadence; zero disables it.
	every time.Duration
	// nextDefrag and nextRecovery are the earliest each may next run.
	nextDefrag   time.Time
	nextRecovery time.Time
	// alarmed and alarmInUse hold the last observed NOSPACE state, so that a
	// shutdown can name a full backend as its cause. The probes never can:
	// all they see is /health answering "ALARM NOSPACE" with a 503.
	alarmed    bool
	alarmInUse int64
	info       func(string, ...any)
	warn       func(string, ...any)
	// storagePath is where the report km reads is published, or "" for none.
	storagePath string
	// dataDir is etcd's data directory, echoed into the report so `km status`
	// can name the directory whose size it is describing.
	dataDir string
	// workdir roots km's messages directory, where the startup and shutdown
	// summaries go. Empty disables reporting.
	workdir string
	// defragMins is echoed into the report so km can show the cadence.
	defragMins int
	// beat republishes the readiness heartbeat, or is nil to publish none.
	// Defragmentation blocks the supervision loop, and km judges this process
	// by how old its last beat is: entering the pause with a fresh one gives
	// the pause km's whole detection window rather than whatever remains of
	// it. Reporting ready across a pause the control plane chose, and will
	// come back from, is the honest reading — the alternative is being killed
	// mid-maintenance for doing maintenance.
	beat func()
}

func newEtcdMaintainer(client *http.Client, base, dataDir, workdir string, cfg cpSysConfig,
	info, warn func(string, ...any), beat func()) *etcdMaintainer {
	return &etcdMaintainer{
		client:      client,
		base:        strings.TrimSuffix(base, "/"),
		quota:       int64(cfg.EtcdQuotaMB) << 20,
		every:       time.Duration(cfg.EtcdDefragMins) * time.Minute,
		storagePath: storageReportPath(dataDir),
		dataDir:     dataDir,
		workdir:     workdir,
		defragMins:  cfg.EtcdDefragMins,
		info:        info,
		warn:        warn,
		beat:        beat,
	}
}

// storageReportPath is where the report is published, or "" when there is no
// data directory to publish into. Empty must stay empty: filepath.Join("", x)
// returns a bare relative name, which would drop the report into whatever the
// working directory happens to be.
func storageReportPath(dataDir string) string {
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, etcdStorageFile)
}

// publish writes the storage report km reads. Best-effort and silent on
// failure: `km status` losing a line is not worth a warning per round, and
// the probes already cover anything that actually threatens the instance.
func (m *etcdMaintainer) publish(st etcdStatus) {
	if m.storagePath == "" {
		return
	}
	body, err := json.Marshal(etcdStorageReport{
		TS:          time.Now().Unix(),
		DBSize:      st.size(),
		DBSizeInUse: st.inUse(),
		QuotaBytes:  m.quota,
		DefragMins:  m.defragMins,
		Fsync:       etcdFsync,
		DataDir:     m.dataDir,
	})
	if err != nil {
		return
	}
	tmp := m.storagePath + heartbeatTmpSuffix
	if err := os.WriteFile(tmp, body, 0644); err != nil {
		return
	}
	if err := os.Rename(tmp, m.storagePath); err != nil {
		os.Remove(tmp)
	}
}

// logStorage logs one line about the backend, republishes the `km status`
// report and hands the same summary to km's message channel, which is where
// the operator actually sees it: this runs at both ends of the instance's
// life, where there is no probe round to piggyback on. `phase` is one of
// messageStartPhase or messageStopPhase.
func (m *etcdMaintainer) logStorage(ctx context.Context, phase string) {
	when := "at startup"
	if phase == messageStopPhase {
		when = "at shutdown"
	}
	var st etcdStatus
	if err := m.call(ctx, etcdStatusPath, "{}", &st); err != nil {
		m.warn("could not read etcd backend size %s: %v", when, err)
		writeMessage(m.workdir, phase, messageLevelWarn,
			"etcd backend size unavailable %s: %v", when, err)
		return
	}
	m.publish(st)
	fsync := "off"
	if etcdFsync {
		fsync = "on"
	}
	defrag := "disabled"
	if m.defragMins > 0 {
		defrag = fmt.Sprintf("every %dm", m.defragMins)
	}
	summary := fmt.Sprintf("etcd backend: %s on disk, %s live, %s quota, fsync %s, defrag %s",
		humanBytes(st.size()), humanBytes(st.inUse()), humanBytes(m.quota), fsync, defrag)
	m.info("[+] etcd backend %s: %s on disk, %s live, %s quota, fsync %s, defrag %s",
		when, humanBytes(st.size()), humanBytes(st.inUse()),
		humanBytes(m.quota), fsync, defrag)
	m.info("[+] etcd data dir: %s", m.dataDir)

	// The backend nearing its quota is the one thing here worth raising the
	// level for: it is the condition that eventually turns the cluster
	// read-only, and it is fixed by editing a file the operator owns.
	level := messageLevelInfo
	if m.quota > 0 && float64(st.inUse()) > float64(m.quota)*etcdQuotaNearlyFull {
		level = messageLevelWarn
		summary += " — live data is near the quota; raise etcdQuotaMb in cpsysconfig.yaml"
	}
	writeMessage(m.workdir, phase, level, "%s", summary)
}

// call posts a JSON body to one of etcd's maintenance endpoints and decodes
// the reply into out, which may be nil.
func (m *etcdMaintainer) call(ctx context.Context, path, body string, out any) error {
	ctx, cancel := context.WithTimeout(ctx, etcdMaintenanceTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, m.base+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := m.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s returned %s", path, resp.Status)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// run performs one maintenance round. Called before the probe round so a
// recovery lands before etcd's health is consulted. Every failure is logged
// and swallowed: the probes decide the control plane's fate, and an
// unreachable etcd is their business rather than this one's.
func (m *etcdMaintainer) run(ctx context.Context, now time.Time) {
	var st etcdStatus
	if err := m.call(ctx, etcdStatusPath, "{}", &st); err != nil {
		// Routine while etcd is still coming up; the probes report it if not.
		return
	}
	size, inUse := st.size(), st.inUse()
	m.alarmed, m.alarmInUse = st.noSpace(), inUse
	m.publish(st)

	if st.noSpace() {
		if now.Before(m.nextRecovery) {
			return
		}
		m.nextRecovery = now.Add(etcdAlarmRetryInterval)
		m.clearNoSpace(ctx, st, size, inUse)
		return
	}

	if m.every == 0 || now.Before(m.nextDefrag) {
		return
	}
	// Re-armed even when the gates below decline, so the check itself stays
	// on the configured cadence rather than running every round.
	m.nextDefrag = now.Add(m.every)
	if size < etcdDefragMinBytes {
		return
	}
	if float64(size-inUse) < float64(size)*etcdDefragMinFreeRatio {
		return
	}
	m.defrag(ctx, "reclaiming free pages", size, inUse)
}

// diagnosis explains an unhealthy etcd when a full backend is the cause, or
// returns "" when it is not. Recovery is automatic and usually silent, so
// reaching this means it could not keep up — which happens when the live data
// itself no longer fits, and no amount of defragmenting will change that.
// Whoever reads the log needs the setting to change, not the alarm's name.
func (m *etcdMaintainer) diagnosis() string {
	if !m.alarmed {
		return ""
	}
	return fmt.Sprintf("etcd backend is full: %s of live data against the %s "+
		"etcdQuotaMb in cpsysconfig.yaml — raise etcdQuotaMb and restart, or "+
		"delete finished pods and jobs to bring the live data back under it",
		humanBytes(m.alarmInUse), humanBytes(m.quota))
}

// clearNoSpace recovers from a NOSPACE alarm: defragment to give the backend
// room, then disarm. Both steps are needed and the order matters —
// defragmentation is permitted while the alarm is armed but does not clear
// it, and until it is disarmed every write is still rejected.
func (m *etcdMaintainer) clearNoSpace(ctx context.Context, st etcdStatus, size, inUse int64) {
	m.warn("etcd NOSPACE alarm armed: backend %s, live %s, quota %s — recovering",
		humanBytes(size), humanBytes(inUse), humanBytes(m.quota))
	m.defrag(ctx, "clearing NOSPACE", size, inUse)

	disarm := fmt.Sprintf(`{"action":"DEACTIVATE","memberID":%q,"alarm":"NOSPACE"}`,
		st.Header.MemberID)
	if err := m.call(ctx, etcdAlarmPath, disarm, nil); err != nil {
		m.warn("etcd NOSPACE disarm failed, cluster stays read-only: %v", err)
		return
	}

	var after etcdStatus
	if err := m.call(ctx, etcdStatusPath, "{}", &after); err != nil {
		m.info("[+] etcd NOSPACE alarm disarmed")
		return
	}
	m.info("[+] etcd NOSPACE alarm disarmed, backend now %s", humanBytes(after.size()))
	if m.quota > 0 && float64(after.inUse()) > float64(m.quota)*etcdQuotaNearlyFull {
		m.warn("etcd holds %s of live data against a %s quota: this is data rather "+
			"than fragmentation, so the alarm will return — raise etcdQuotaMb in "+
			"cpsysconfig.yaml, or delete finished pods and jobs",
			humanBytes(after.inUse()), humanBytes(m.quota))
	}
}

// defrag rewrites the backend without its free pages. Every etcd read and
// write blocks until it returns, so the elapsed time is logged.
func (m *etcdMaintainer) defrag(ctx context.Context, why string, size, inUse int64) {
	m.info("[+] Defragmenting etcd (%s): %s on disk, %s live",
		why, humanBytes(size), humanBytes(inUse))
	if m.beat != nil {
		m.beat()
	}
	start := time.Now()
	if err := m.call(ctx, etcdDefragPath, "{}", nil); err != nil {
		m.warn("etcd defragmentation failed: %v", err)
		return
	}
	elapsed := time.Since(start).Round(time.Millisecond)
	var after etcdStatus
	if err := m.call(ctx, etcdStatusPath, "{}", &after); err != nil {
		m.info("[+] etcd defragmented in %s", elapsed)
		return
	}
	m.info("[+] etcd defragmented in %s: %s -> %s",
		elapsed, humanBytes(size), humanBytes(after.size()))
}

func main() {
	os.Exit(run())
}

func run() int {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	port := fs.String("port", "6443", "API server listen port")
	kcmPort := fs.String("kcm-port", "10257", "kube-controller-manager secure port")
	etcdPort := fs.String("etcd-port", "0",
		"etcd client port; 0 lets envtest pick a random free one")
	etcdPeerPort := fs.String("etcd-peer-port", "0",
		"etcd peer port, bound but unused by a single node; 0 lets envtest pick")
	workDir := fs.String("workdir", ".", "Working directory (data/control-plane/ created under this)")
	configDir := fs.String("configdir", "", "Directory for config files (default: <workdir>/config)")
	logDir := fs.String("logdir", "", "Log directory (default: <workdir>/log)")
	logMaxSizeMB := fs.Int("logmaxsizemb", 100, "Max log file size in MB before rotation")
	logMaxFiles := fs.Int("logmaxfiles", 3, "Max number of log files to keep")
	heartbeatFile := fs.String("heartbeat-file", "",
		"File to publish the readiness heartbeat into (disabled when empty)")
	heartbeatSecs := fs.Int("heartbeat-secs", defaultHeartbeatSecs,
		"Seconds between health probe rounds")
	maxMissedHeartbeats := fs.Int("max-missed-heartbeats", defaultMaxMissedHeartbeats,
		"Consecutive failed probe rounds tolerated before shutting down")
	startupSecs := fs.Int("startup-secs", defaultStartupSecs,
		"Seconds for the children to all become healthy before giving up on startup")
	fs.Parse(os.Args[1:])

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot resolve own executable path: %v\n", err)
		return 1
	}
	baseDir := filepath.Dir(exe)

	// Resolve workdir to absolute
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot resolve the current directory: %v\n", err)
		return 1
	}
	wd := *workDir
	if !filepath.IsAbs(wd) {
		wd = filepath.Join(cwd, wd)
	}

	dataDir := filepath.Join(wd, "data", "control-plane")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot create data dir %s: %v\n", dataDir, err)
		return 1
	}

	// Config dir (default: <workdir>/config)
	outDir := *configDir
	if outDir == "" {
		outDir = filepath.Join(wd, "config")
	}
	if !filepath.IsAbs(outDir) {
		outDir = filepath.Join(cwd, outDir)
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot create config dir %s: %v\n", outDir, err)
		return 1
	}

	// Load or create cpsysconfig.yaml
	cpSys, err := loadOrCreateCPSysConfig(outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	// Load or create tokens.csv — static auth tokens, generated once and
	// reused so the kubeconfigs stay valid across restarts.
	tokens, err := loadOrCreateTokens(outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		return 1
	}

	// Log dir (default: <workdir>/log)
	ld := *logDir
	if ld == "" {
		ld = filepath.Join(wd, "log")
	}
	if !filepath.IsAbs(ld) {
		ld = filepath.Join(cwd, ld)
	}
	if err := os.MkdirAll(ld, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot create log dir %s: %v\n", ld, err)
		return 1
	}

	// Setup control-plane's own rotating log (log package → file only)
	kmLog := newRotatingLog(ld, "control-plane", *logMaxSizeMB, *logMaxFiles)
	log.SetOutput(kmLog)
	log.SetFlags(0)
	log.SetPrefix("")

	// klog-like format: I0405 01:28:07.931074] message
	kfmt := func(level byte, format string, args ...any) string {
		now := time.Now()
		msg := fmt.Sprintf(format, args...)
		return fmt.Sprintf("%c%s %s %s", level, now.Format("0102"), now.Format("15:04:05.000000"), msg)
	}
	info := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		fmt.Println(msg)
		log.Println(kfmt('I', "%s", msg))
	}
	// Warnings and errors were previously logged at 'I' like everything
	// else, which made a failing probe indistinguishable from progress.
	warn := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		fmt.Println("WARNING: " + msg)
		log.Println(kfmt('W', "%s", msg))
	}
	fail := func(format string, args ...any) {
		msg := fmt.Sprintf(format, args...)
		fmt.Fprintln(os.Stderr, "ERROR: "+msg)
		log.Println(kfmt('E', "%s", msg))
	}

	// PID directory — kill stale processes from a previous run
	pidsDir := filepath.Join(dataDir, "pids")
	if err := os.MkdirAll(pidsDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot create pids dir %s: %v\n", pidsDir, err)
		return 1
	}
	killStaleProcesses(pidsDir, warn)

	// Write own PID file
	writePid(filepath.Join(pidsDir, fmt.Sprintf("%d.control-plane", os.Getpid())), os.Getpid())
	defer func() {
		if path, _ := pidFileForProcess(pidsDir, "control-plane"); path != "" {
			os.Remove(path)
		}
		os.Remove(pidsDir)
	}()

	info("--- Local Control Plane ---")
	info("Binary Dir:  %s", baseDir)
	info("Work Dir:    %s", wd)
	info("Config Dir:  %s", outDir)
	info("Data Dir:    %s", dataDir)
	info("Log Dir:     %s", ld)

	// Determine extension based on OS
	ext := ""
	if runtime.GOOS == "windows" {
		ext = ".exe"
	}

	// Check if asset binaries exist next to td_km
	if _, err := os.Stat(filepath.Join(baseDir, "kube-apiserver"+ext)); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "CRITICAL: kube-apiserver%s not found next to td_km!\n", ext)
		return 1
	}

	if _, err := os.Stat(filepath.Join(baseDir, "etcd"+ext)); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "CRITICAL: etcd%s not found next to td_km!\n", ext)
		return 1
	}

	kcmPath := filepath.Join(baseDir, "kube-controller-manager"+ext)
	if _, err := os.Stat(kcmPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "CRITICAL: kube-controller-manager%s not found next to td_km!\n", ext)
		return 1
	}

	// Check if ports are available. etcd's are only listed when km pinned
	// them; left at 0 they are envtest's to allocate, and by definition free.
	portsToCheck := []struct{ name, port string }{
		{"API server", *port},
		{kcmName, *kcmPort},
	}
	if *etcdPort != portUnset {
		portsToCheck = append(portsToCheck, struct{ name, port string }{"etcd", *etcdPort})
	}
	if *etcdPeerPort != portUnset {
		portsToCheck = append(portsToCheck,
			struct{ name, port string }{"etcd peer", *etcdPeerPort})
	}
	for _, p := range portsToCheck {
		ln, err := net.Listen("tcp", "127.0.0.1:"+p.port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "CRITICAL: %s port %s is already in use: %v\n", p.name, p.port, err)
			return 1
		}
		ln.Close()
	}

	// Create rotating log writers for subprocess output
	etcdLog := newRotatingLog(ld, "etcd", *logMaxSizeMB, *logMaxFiles)
	apiserverLog := newRotatingLog(ld, "kube-apiserver", *logMaxSizeMB, *logMaxFiles)

	// Point envtest to the directory containing the binaries
	os.Setenv("KUBEBUILDER_ASSETS", baseDir)

	// envtest regenerates serving certs on every start (clients use
	// insecure-skip-tls-verify, so it does not matter), while client auth
	// uses the static tokens from tokens.csv — the kubeconfigs written
	// below are stable across restarts.
	info("[+] Generating kubeconfigs...")

	testEnv := &envtest.Environment{
		// Stated rather than inherited from envtest's 20s default, so the
		// budget for bringing etcd and the API server up is visible here.
		ControlPlaneStartTimeout: startupBudget(*startupSecs),
		// Deliberately short: stopAndCleanPid below force-kills whatever
		// envtest could not stop, so a long wait here only adds latency to
		// exactly the case where a child is wedged and will not go quietly —
		// a dead etcd leaving the API server unable to exit, for instance.
		ControlPlaneStopTimeout: envtestStopTimeout,
		ControlPlane: envtest.ControlPlane{
			APIServer: &envtest.APIServer{
				Out: apiserverLog,
				Err: apiserverLog,
				SecureServing: envtest.SecureServing{
					ListenAddr: envtest.ListenAddr{Address: "127.0.0.1", Port: *port},
				},
			},
			Etcd: &envtest.Etcd{
				Out:     etcdLog,
				Err:     etcdLog,
				DataDir: dataDir,
				// Set only when km pinned it: envtest allocates a random
				// free port whenever this is nil, which is the fallback for
				// instances created before km managed etcd's ports.
				URL: fixedEtcdURL(*etcdPort),
			},
		},
	}
	// The peer URL has to go through arguments: envtest keeps it in a private
	// field and overwrites it with a fresh random port on every Start, so
	// there is nothing to set — but a flag the caller sets replaces the
	// default it would otherwise pass.
	if *etcdPeerPort != portUnset {
		testEnv.ControlPlane.Etcd.Configure().
			Set(etcdPeerURLsFlag, fmt.Sprintf("http://127.0.0.1:%s", *etcdPeerPort))
	}

	// envtest hands etcd nothing but its URLs and data directory, so every
	// storage setting below would otherwise be etcd's own default: no
	// compaction of its own, and a 2 GiB backend quota whose breach arms a
	// cluster-wide read-only alarm. Fine for a test environment that is thrown
	// away; not for an instance that is kept.
	testEnv.ControlPlane.Etcd.Configure().
		// envtest adds --unsafe-no-fsync=true whenever the etcd binary accepts
		// it, trading durability for speed on the assumption that the data dies
		// with the test. These instances are durable, so the flag is dropped
		// entirely and etcd fsyncs as it normally would. Disable removes it
		// rather than setting false, so the argument never reaches etcd at all.
		// Not exposed as a tunable: an instance whose state is meant to survive
		// does not get to opt out of the writes actually reaching the disk.
		Disable("unsafe-no-fsync").
		Set("quota-backend-bytes", strconv.FormatInt(int64(cpSys.EtcdQuotaMB)<<20, 10)).
		Set("auto-compaction-mode", "periodic").
		Set("auto-compaction-retention", etcdAutoCompactionRetention).
		Set("max-wals", strconv.Itoa(etcdMaxWALs)).
		Set("max-snapshots", strconv.Itoa(etcdMaxSnapshots)).
		Set("snapshot-count", strconv.Itoa(etcdSnapshotCount))

	testEnv.ControlPlane.GetAPIServer().Configure().
		Set("runtime-config", "api/all=true,coordination.k8s.io/v1=true").
		Set("api-audiences", "https://kubernetes.default.svc").
		Set("service-account-issuer", "https://kubernetes.default.svc").
		Set("token-auth-file", filepath.Join(outDir, tokensFile))

	// Before any child starts: an unhandled SIGTERM ends this process at once,
	// leaving etcd and the API server running. Startup checks for one between
	// its steps; the supervision loop below takes over once it is done.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	interrupted := func() bool {
		select {
		case <-sigChan:
			info("\n[+] Shutdown requested")
			return true
		default:
			return false
		}
	}

	// Cancelled as run returns, after the deferred shutdown below has had its
	// go: kube-controller-manager is started under it, so a stop that outlives
	// its SIGTERM still gets killed.
	ctx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()

	if interrupted() {
		return 0
	}

	// Start the Server
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start: %v\n", err)
		return 1
	}

	// From here on every return stops the children, whatever the reason.
	var kcmCmd *exec.Cmd
	var kcmExited <-chan string
	var maintainer *etcdMaintainer
	defer func() {
		info("\n[+] Shutting down...")

		// Before anything is stopped: this is the last moment etcd can answer,
		// and the size it finishes at is what the next start inherits.
		if maintainer != nil {
			shutdownCtx, cancelShutdownReport := reportCtx(context.Background())
			maintainer.logStorage(shutdownCtx, messageStopPhase)
			cancelShutdownReport()
		}

		// Stop kube-controller-manager. The watcher goroutine owns Wait(), so
		// take its result rather than calling Wait() again, which would error —
		// and skip the whole dance when it has already exited and been reaped.
		if kcmCmd != nil && kcmCmd.Process != nil && !processGone(kcmName, kcmCmd.Process.Pid) {
			info("[+] Stopping kube-controller-manager...")
			if err := requestStop(kcmCmd.Process); err != nil {
				warn("cannot signal kube-controller-manager: %v", err)
			}
			select {
			case <-kcmExited:
			case <-time.After(kcmStopTimeout):
				warn("kube-controller-manager did not exit within %s", kcmStopTimeout)
			}
		}

		// Stop the test environment (etcd + kube-apiserver)
		if err := testEnv.Stop(); err != nil {
			info("Warning: %v (processes may have already stopped)", err)
		}

		// Ensure all child processes are actually dead before removing PID files.
		// envtest.Stop() may return before children have fully exited.
		for _, name := range []string{"kube-controller-manager", "kube-apiserver", "etcd"} {
			stopAndCleanPid(pidsDir, name, info)
		}

		info("[+] Stopped successfully")
	}()

	// Write PID files for etcd and kube-apiserver (envtest child processes)
	envtestChildren := writeChildPids(pidsDir)
	if len(envtestChildren) < 2 {
		warn("found only %d of the 2 envtest children under this process; the rest cannot be watched or reaped by pid",
			len(envtestChildren))
	}

	fmt.Println("\n[+] API Server Listening:", cfg.Host)
	info("[+] etcd listening: %s (peer %s)",
		testEnv.ControlPlane.Etcd.URL.Host, *etcdPeerPort)

	// Create a client to set up required namespaces
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		return 1
	}

	if interrupted() {
		return 0
	}

	// Create kube-node-lease namespace
	info("[+] Creating kube-node-lease namespace...")
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-node-lease"},
	}, metav1.CreateOptions{})
	if err != nil {
		info("Warning: Could not create kube-node-lease namespace: %v\n", err)
	} else {
		info("[+] kube-node-lease namespace created")
	}

	// Create kube-system namespace
	info("[+] Creating kube-system namespace...")
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "kube-system"},
	}, metav1.CreateOptions{})
	if err != nil {
		info("Warning: Could not create kube-system namespace: %v\n", err)
	} else {
		info("[+] kube-system namespace created")
	}

	if interrupted() {
		return 0
	}

	// Wait for API server to be fully ready
	info("[+] Waiting for API server to be ready...")
	for i := 0; i < 30; i++ {
		if interrupted() {
			return 0
		}
		_, err := client.Discovery().ServerVersion()
		if err == nil {
			info("[+] API server is ready")
			break
		}
		if i == 29 {
			fmt.Fprintf(os.Stderr, "CRITICAL: API server did not become ready: %v\n", err)
			return 1
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Generate KCM kubeconfig
	kcmConfigPath := filepath.Join(outDir, "kubeconfig-kcm.yaml")
	if err := writeKubeconfig(kcmConfigPath, cfg.Host, "kcm", tokens[kcmUserName]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write KCM kubeconfig: %v\n", err)
		return 1
	}

	if interrupted() {
		return 0
	}

	// Start kube-controller-manager with rotating log
	kcmLog := newRotatingLog(ld, "kube-controller-manager", *logMaxSizeMB, *logMaxFiles)
	info("[+] Starting kube-controller-manager...")
	kcmCmd = exec.CommandContext(ctx, kcmPath,
		"--kubeconfig", kcmConfigPath,
		"--leader-elect=false",
		"--controllers=job,cronjob,ttl-after-finished,garbagecollector,deployment,replicaset",
		fmt.Sprintf("--concurrent-job-syncs=%d", cpSys.ConcurrentJobSyncs),
		fmt.Sprintf("--kube-api-qps=%d", cpSys.KubeAPIQPS),
		fmt.Sprintf("--kube-api-burst=%d", cpSys.KubeAPIBurst),
		"--bind-address=127.0.0.1",
		"--secure-port="+*kcmPort,
	)
	kcmCmd.Stdout = kcmLog
	kcmCmd.Stderr = kcmLog
	kcmCmd.SysProcAttr = ownGroup()

	if err := kcmCmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start kube-controller-manager: %v\n", err)
		return 1
	}
	writePid(filepath.Join(pidsDir, fmt.Sprintf("%d.kube-controller-manager", kcmCmd.Process.Pid)), kcmCmd.Process.Pid)
	info("[+] kube-controller-manager started (pid: %d)", kcmCmd.Process.Pid)
	// This process owns kcm, so its exit status is available — unlike the
	// envtest pair, which can only be checked by pid.
	kcmExited = watchExit(kcmName, kcmCmd)

	// Generate VK kubeconfig
	vkConfigPath := filepath.Join(outDir, "vk-kubeconfig.yaml")
	if err := writeKubeconfig(vkConfigPath, cfg.Host, "vk-user", tokens[vkUserName]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write VK kubeconfig: %v\n", err)
		return 1
	}
	info("[+] VK Kubeconfig written to: %s", vkConfigPath)

	// Generate admin kubeconfig
	configPath := filepath.Join(outDir, "kubeconfig.yaml")
	if err := writeKubeconfig(configPath, cfg.Host, "admin", tokens[adminUserName]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write admin kubeconfig: %v\n", err)
		return 1
	}
	info("[+] Admin Kubeconfig written to: %s", configPath)

	info("\nTo connect, run:")
	info("export KUBECONFIG=%s", configPath)
	info("\n[+] Cluster is ready!")
	info("Press Ctrl+C to stop.")

	// Supervise the children from here on. Each round probes all three and
	// republishes the heartbeat km watches; km never probes them itself,
	// since only this process knows the port envtest gave etcd.

	// Derived from ctx so a shutdown aborts an in-flight probe at once,
	// separate from ctx so it does not kill kube-controller-manager before
	// its graceful stop below.
	probeCtx, cancelProbes := context.WithCancel(ctx)

	plainClient := &http.Client{}
	// kube-controller-manager serves HTTPS with a certificate it generates
	// for itself, so there is no CA to pin: verification is skipped for the
	// health probe, which carries no credentials and reads no data.
	selfSignedClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	etcdAddr := testEnv.ControlPlane.Etcd.URL.Host
	listeners := []childListener{
		{name: "etcd", addr: etcdAddr},
		{name: "kube-apiserver", addr: net.JoinHostPort("127.0.0.1", *port)},
		{name: kcmName, addr: net.JoinHostPort("127.0.0.1", *kcmPort)},
	}

	probes := []childProbe{
		{
			name:  "etcd",
			check: httpProbe(plainClient, testEnv.ControlPlane.Etcd.URL.String()+etcdHealthPath),
		},
		{
			// The API server is probed through the same authenticated client
			// the cluster uses, so this also proves tokens.csv still matches.
			name: "kube-apiserver",
			check: func(ctx context.Context) error {
				_, err := client.Discovery().RESTClient().
					Get().AbsPath(apiServerReadyzPath).DoRaw(ctx)
				return err
			},
		},
		{
			name: kcmName,
			check: httpProbe(selfSignedClient,
				fmt.Sprintf("https://127.0.0.1:%s%s", *kcmPort, kcmHealthPath)),
		},
	}

	interval := time.Duration(*heartbeatSecs) * time.Second
	info("[+] Probing %d children every %ds, %d missed rounds tolerated",
		len(probes), *heartbeatSecs, *maxMissedHeartbeats)
	addrs := make([]string, 0, len(listeners))
	for _, listener := range listeners {
		addrs = append(addrs, listener.addr)
	}
	info("[+] Checking listeners every %s: %s", processTick, strings.Join(addrs, " "))
	pids := make([]string, 0, len(envtestChildren))
	for _, child := range envtestChildren {
		pids = append(pids, fmt.Sprintf("%s(%d)", child.name, child.pid))
	}
	info("[+] Checking child pids every %ds: %s", *heartbeatSecs, strings.Join(pids, " "))
	info("[+] Watching %s(%d) continuously (this process owns it)", kcmName, kcmCmd.Process.Pid)
	if cpSys.EtcdDefragMins > 0 {
		info("[+] etcd backend: %s quota, defragmentation considered every %dm",
			humanBytes(int64(cpSys.EtcdQuotaMB)<<20), cpSys.EtcdDefragMins)
	} else {
		info("[+] etcd backend: %s quota, defragmentation disabled",
			humanBytes(int64(cpSys.EtcdQuotaMB)<<20))
	}
	if *heartbeatFile == "" {
		info("[+] No -heartbeat-file given; readiness heartbeat disabled")
	}

	exitCode := 0
	missed := 0
	everHealthy := false
	shuttingDown := false
	// Coming up is bounded by wall clock, not by round count: see
	// startupBudget. Fixed here rather than recomputed so a slow first round
	// does not extend it.
	startupAllowance := startupBudget(*startupSecs)
	startupStarted := time.Now()
	startupDeadline := startupStarted.Add(startupAllowance)

	// Only beats once the children have been healthy at least once: km's
	// readiness gate opens on the first ready beat, so beating before then
	// would report a control plane that is still coming up as usable.
	maintainer = newEtcdMaintainer(plainClient,
		testEnv.ControlPlane.Etcd.URL.String(), dataDir, wd, cpSys, info, warn,
		func() {
			if everHealthy {
				writeHeartbeat(*heartbeatFile, statusReady, "", warn)
			}
		})

	startupCtx, cancelStartupReport := reportCtx(ctx)
	maintainer.logStorage(startupCtx, messageStartPhase)
	cancelStartupReport()
	// Two cadences in one loop: the children are checked for still being
	// processes every tick, while the probe round runs when it comes due.
	nextRound := time.Now()
	// A child found definitively lost is handled the same wherever it is
	// noticed: say so, publish it, and stop.
	lost := func(detail string) {
		fail("%s; shutting down the control plane", detail)
		writeHeartbeat(*heartbeatFile, statusDegraded, detail, warn)
		exitCode = 1
	}

	for !shuttingDown {
		// Cheap checks, every tick. A definitive loss does not go through the
		// miss budget: there is nothing to wait for and nothing left to probe.
		if death := childLost(kcmExited, listeners, everHealthy); death != "" {
			lost(death)
			break
		}

		if !time.Now().Before(nextRound) {
			// Costly checks, once per round.
			if death := deadChildProcess(envtestChildren); death != "" {
				lost(death)
				break
			}
			// Ahead of the probes on purpose: etcd reports an armed NOSPACE
			// alarm as unhealthy, so clearing it here is what keeps a full
			// backend from spending the miss budget and taking the control
			// plane down over something recoverable in place.
			maintainer.run(probeCtx, time.Now())

			detail, gone := probeAll(probeCtx, probes)
			// Once a child has been healthy, a refused connection means its
			// listener is gone for good — no point spending the miss budget.
			if gone && everHealthy {
				lost(detail)
				break
			}
			// The probe only reports that etcd answered /health with a 503.
			// Name the cause and the fix while the round is still failing, so
			// it reaches both this log and the beat km reports on.
			if why := maintainer.diagnosis(); why != "" && detail != "" {
				detail += "; " + why
			}

			var action monitorAction
			missed, action = monitorRound(detail, missed, *maxMissedHeartbeats)
			// Until the first healthy round the miss count is the wrong
			// yardstick — rounds are 250ms apart, so it would allow about a
			// second. Give the children the full startup allowance instead.
			if !everHealthy && detail != "" {
				if time.Now().Before(startupDeadline) {
					action = actionContinue
				} else {
					action = actionShutdown
				}
			}

			if detail == "" {
				if !everHealthy {
					info("[+] All children healthy")
					everHealthy = true
				}
				writeHeartbeat(*heartbeatFile, statusReady, "", warn)
			} else {
				// Before the first healthy round this is just a child still
				// binding its port, not a fault worth a warning.
				if everHealthy {
					warn("health probe round failed (%d/%d): %s", missed, *maxMissedHeartbeats, detail)
				} else {
					info("[.] Waiting for children (%s/%s): %s",
						time.Since(startupStarted).Round(time.Second),
						startupAllowance, detail)
				}
				writeHeartbeat(*heartbeatFile, statusDegraded, detail, warn)
			}

			if action == actionShutdown {
				if why := maintainer.diagnosis(); why != "" {
					fail("%s", why)
				}
				if everHealthy {
					fail("%d consecutive failed probe rounds; shutting down the control plane", missed)
				} else {
					fail("children did not all become healthy within %s; shutting down the control plane",
						startupAllowance)
				}
				exitCode = 1
				break
			}
			nextRound = time.Now().Add(probeWait(everHealthy, interval))
		}

		select {
		case <-sigChan:
			info("\n[+] Shutdown requested")
			cancelProbes()
			shuttingDown = true
		case <-time.After(tickWait(time.Until(nextRound), processTick)):
		}
	}
	cancelProbes()

	return exitCode
}
