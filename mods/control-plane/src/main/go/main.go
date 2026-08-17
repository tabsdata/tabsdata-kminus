/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	mathrand "math/rand"
	"net"
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

func writePid(path string, pid int) {
	os.WriteFile(path, []byte(strconv.Itoa(pid)), 0644)
}

func writeChildPids(pidsDir string) {
	myPid := int32(os.Getpid())
	procs, err := process.Processes()
	if err != nil {
		return
	}
	for _, p := range procs {
		ppid, err := p.Ppid()
		if err != nil || ppid != myPid {
			continue
		}
		name, err := p.Name()
		if err != nil {
			continue
		}
		switch name {
		case "etcd", "kube-apiserver":
			writePid(filepath.Join(pidsDir, fmt.Sprintf("%d.%s", p.Pid, name)), int(p.Pid))
			now := time.Now()
			log.Printf("I%s %s [+] %s pid: %d", now.Format("0102"), now.Format("15:04:05.000000"), name, p.Pid)
		}
	}
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
		if err != nil || pName != processName {
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
	fs.Parse(os.Args[1:])

	exe, _ := os.Executable()
	baseDir := filepath.Dir(exe)

	// Resolve workdir to absolute
	wd := *workDir
	if !filepath.IsAbs(wd) {
		cwd, _ := os.Getwd()
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
		cwd, _ := os.Getwd()
		outDir = filepath.Join(cwd, outDir)
	}
	os.MkdirAll(outDir, 0755)

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
		cwd, _ := os.Getwd()
		ld = filepath.Join(cwd, ld)
	}
	os.MkdirAll(ld, 0755)

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

	// PID directory — kill stale processes from a previous run
	pidsDir := filepath.Join(dataDir, "pids")
	os.MkdirAll(pidsDir, 0755)
	killStaleProcesses(pidsDir, info)

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
	writeChildPids(pidsDir)

	fmt.Println("\n[+] API Server Listening:", cfg.Host)

	// Create a client to set up required namespaces
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create client: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()

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

	// Wait for interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	info("\n[+] Shutting down...")

	// Stop kube-controller-manager
	if kcmCmd.Process != nil {
		info("[+] Stopping kube-controller-manager...")
		kcmCmd.Process.Signal(syscall.SIGTERM)
		kcmCmd.Wait()
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
}
