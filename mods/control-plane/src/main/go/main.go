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
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
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

// cpSysConfig contains tunables for the kube-controller-manager subprocess.
// Persisted in cpsysconfig.yaml — created with defaults on first startup,
// user-editable thereafter.
type cpSysConfig struct {
	ConcurrentJobSyncs int `yaml:"concurrentJobSyncs"`
	KubeAPIQPS         int `yaml:"kubeApiQps"`
	KubeAPIBurst       int `yaml:"kubeApiBurst"`
}

func defaultCPSysConfig() cpSysConfig {
	return cpSysConfig{
		ConcurrentJobSyncs: 50,
		KubeAPIQPS:         500,
		KubeAPIBurst:       1000,
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
		return cfg, nil
	}
	cfg := defaultCPSysConfig()
	data, _ := yaml.Marshal(&cfg)
	header := []byte("# Control-plane system configuration.\n" +
		"# Tunables for the kube-controller-manager subprocess.\n\n")
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
	return strings.TrimSuffix(name, ".exe")
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
	proc.Signal(syscall.SIGTERM)
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

func main() {
	fs := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	port := fs.String("port", "6443", "API server listen port")
	kcmPort := fs.String("kcm-port", "10257", "kube-controller-manager secure port")
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
	fs.Parse(os.Args[1:])

	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot resolve own executable path: %v\n", err)
		os.Exit(1)
	}
	baseDir := filepath.Dir(exe)

	// Resolve workdir to absolute
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot resolve the current directory: %v\n", err)
		os.Exit(1)
	}
	wd := *workDir
	if !filepath.IsAbs(wd) {
		wd = filepath.Join(cwd, wd)
	}

	dataDir := filepath.Join(wd, "data", "control-plane")
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "CRITICAL: cannot create data dir %s: %v\n", dataDir, err)
		os.Exit(1)
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
		os.Exit(1)
	}

	// Load or create cpsysconfig.yaml
	cpSys, err := loadOrCreateCPSysConfig(outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
	}

	// Load or create tokens.csv — static auth tokens, generated once and
	// reused so the kubeconfigs stay valid across restarts.
	tokens, err := loadOrCreateTokens(outDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %v\n", err)
		os.Exit(1)
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
		os.Exit(1)
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
		os.Exit(1)
	}
	killStaleProcesses(pidsDir, warn)

	// Write own PID file
	writePid(filepath.Join(pidsDir, fmt.Sprintf("%d.control-plane", os.Getpid())), os.Getpid())

	info("--- Local Control Plane ---")
	info("Binary Dir:  %s", baseDir)
	info("Work Dir:    %s", wd)
	info("Config Dir:  %s", outDir)
	info("Data Dir:    %s", dataDir)
	info("Log Dir:     %s", ld)

	// Determine extension based on OS
	ext := ""
	if os.Getenv("GOOS") == "windows" || os.PathSeparator == '\\' {
		ext = ".exe"
	}

	// Check if asset binaries exist next to td_km
	if _, err := os.Stat(filepath.Join(baseDir, "kube-apiserver"+ext)); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "CRITICAL: kube-apiserver%s not found next to td_km!\n", ext)
		os.Exit(1)
	}

	if _, err := os.Stat(filepath.Join(baseDir, "etcd"+ext)); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "CRITICAL: etcd%s not found next to td_km!\n", ext)
		os.Exit(1)
	}

	kcmPath := filepath.Join(baseDir, "kube-controller-manager"+ext)
	if _, err := os.Stat(kcmPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "CRITICAL: kube-controller-manager%s not found next to td_km!\n", ext)
		os.Exit(1)
	}

	// Check if ports are available
	for _, p := range []struct{ name, port string }{
		{"API server", *port},
		{"kube-controller-manager", *kcmPort},
	} {
		ln, err := net.Listen("tcp", "127.0.0.1:"+p.port)
		if err != nil {
			fmt.Fprintf(os.Stderr, "CRITICAL: %s port %s is already in use: %v\n", p.name, p.port, err)
			os.Exit(1)
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
		ControlPlaneStartTimeout: time.Duration(*heartbeatSecs**maxMissedHeartbeats) * time.Second,
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
			},
		},
	}
	testEnv.ControlPlane.GetAPIServer().Configure().
		Set("runtime-config", "api/all=true,coordination.k8s.io/v1=true").
		Set("api-audiences", "https://kubernetes.default.svc").
		Set("service-account-issuer", "https://kubernetes.default.svc").
		Set("token-auth-file", filepath.Join(outDir, tokensFile))

	// Start the Server
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start: %v\n", err)
		os.Exit(1)
	}

	// Write PID files for etcd and kube-apiserver (envtest child processes)
	envtestChildren := writeChildPids(pidsDir)
	if len(envtestChildren) < 2 {
		warn("found only %d of the 2 envtest children under this process; the rest cannot be watched or reaped by pid",
			len(envtestChildren))
	}

	fmt.Println("\n[+] API Server Listening:", cfg.Host)

	// Create a client to set up required namespaces
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	// Cancelled as main returns; kube-controller-manager is started under it,
	// so a stop that outlives its SIGTERM still gets killed.
	ctx, cancelAll := context.WithCancel(context.Background())
	defer cancelAll()

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

	// Wait for API server to be fully ready
	info("[+] Waiting for API server to be ready...")
	for i := 0; i < 30; i++ {
		_, err := client.Discovery().ServerVersion()
		if err == nil {
			info("[+] API server is ready")
			break
		}
		if i == 29 {
			fmt.Fprintf(os.Stderr, "CRITICAL: API server did not become ready: %v\n", err)
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Generate KCM kubeconfig
	kcmConfigPath := filepath.Join(outDir, "kubeconfig-kcm.yaml")
	if err := writeKubeconfig(kcmConfigPath, cfg.Host, "kcm", tokens[kcmUserName]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write KCM kubeconfig: %v\n", err)
		os.Exit(1)
	}

	// Start kube-controller-manager with rotating log
	kcmLog := newRotatingLog(ld, "kube-controller-manager", *logMaxSizeMB, *logMaxFiles)
	info("[+] Starting kube-controller-manager...")
	kcmCmd := exec.CommandContext(ctx, kcmPath,
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

	if err := kcmCmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start kube-controller-manager: %v\n", err)
		os.Exit(1)
	}
	writePid(filepath.Join(pidsDir, fmt.Sprintf("%d.kube-controller-manager", kcmCmd.Process.Pid)), kcmCmd.Process.Pid)
	info("[+] kube-controller-manager started (pid: %d)", kcmCmd.Process.Pid)
	// This process owns kcm, so its exit status is available — unlike the
	// envtest pair, which can only be checked by pid.
	kcmExited := watchExit(kcmName, kcmCmd)

	// Generate VK kubeconfig
	vkConfigPath := filepath.Join(outDir, "vk-kubeconfig.yaml")
	if err := writeKubeconfig(vkConfigPath, cfg.Host, "vk-user", tokens[vkUserName]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write VK kubeconfig: %v\n", err)
		os.Exit(1)
	}
	info("[+] VK Kubeconfig written to: %s", vkConfigPath)

	// Generate admin kubeconfig
	configPath := filepath.Join(outDir, "kubeconfig.yaml")
	if err := writeKubeconfig(configPath, cfg.Host, "admin", tokens[adminUserName]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write admin kubeconfig: %v\n", err)
		os.Exit(1)
	}
	info("[+] Admin Kubeconfig written to: %s", configPath)

	info("\nTo connect, run:")
	info("export KUBECONFIG=%s", configPath)
	info("\n[+] Cluster is ready!")
	info("Press Ctrl+C to stop.")

	// Supervise the children from here on. Each round probes all three and
	// republishes the heartbeat km watches; km never probes them itself,
	// since only this process knows the port envtest gave etcd.
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

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
	if *heartbeatFile == "" {
		info("[+] No -heartbeat-file given; readiness heartbeat disabled")
	}

	exitCode := 0
	missed := 0
	everHealthy := false
	shuttingDown := false
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
			detail, gone := probeAll(probeCtx, probes)
			// Once a child has been healthy, a refused connection means its
			// listener is gone for good — no point spending the miss budget.
			if gone && everHealthy {
				lost(detail)
				break
			}
			var action monitorAction
			missed, action = monitorRound(detail, missed, *maxMissedHeartbeats)

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
					info("[.] Waiting for children (%d/%d): %s", missed, *maxMissedHeartbeats, detail)
				}
				writeHeartbeat(*heartbeatFile, statusDegraded, detail, warn)
			}

			if action == actionShutdown {
				fail("%d consecutive failed probe rounds; shutting down the control plane", missed)
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

	info("\n[+] Shutting down...")

	// Stop kube-controller-manager. The watcher goroutine owns Wait(), so take
	// its result rather than calling Wait() again, which would error — and skip
	// the whole dance when it has already exited and been reaped.
	if kcmCmd.Process != nil && !processGone(kcmName, kcmCmd.Process.Pid) {
		info("[+] Stopping kube-controller-manager...")
		if err := kcmCmd.Process.Signal(syscall.SIGTERM); err != nil {
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

	// Clean up own PID and pids directory
	path, _ := pidFileForProcess(pidsDir, "control-plane")
	if path != "" {
		os.Remove(path)
	}
	os.Remove(pidsDir)

	info("[+] Stopped successfully")

	if exitCode != 0 {
		os.Exit(exitCode)
	}
}
