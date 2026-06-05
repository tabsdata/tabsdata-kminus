/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"context"
	"encoding/base64"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

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

// waitForDeath waits up to timeout for a process to exit.
// Returns true if the process is gone.
func waitForDeath(pid int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err != nil {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// killAndWait sends SIGTERM, waits, then SIGKILL if needed.
func killAndWait(pid int) {
	syscall.Kill(pid, syscall.SIGTERM)
	if waitForDeath(pid, 5*time.Second) {
		return
	}
	syscall.Kill(pid, syscall.SIGKILL)
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
	if err := syscall.Kill(pid, 0); err != nil {
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

	// Certs and configs are regenerated on every start — envtest generates
	// a fresh CA each time so previous client certificates are not valid.
	info("[+] Generating certificates and kubeconfigs...")

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
		Set("service-account-issuer", "https://kubernetes.default.svc")

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
	kcmUser, err := testEnv.AddUser(envtest.User{
		Name:   "system:kube-controller-manager",
		Groups: []string{"system:masters"},
	}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create KCM user: %v\n", err)
		os.Exit(1)
	}

	kcmKubeconfig := fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: local
contexts:
- context:
    cluster: local
    user: kcm
  name: default
current-context: default
kind: Config
users:
- name: kcm
  user:
    client-certificate-data: %s
    client-key-data: %s
`, cfg.Host,
		base64.StdEncoding.EncodeToString(kcmUser.Config().CertData),
		base64.StdEncoding.EncodeToString(kcmUser.Config().KeyData))

	kcmConfigPath := filepath.Join(outDir, "kubeconfig-kcm.yaml")
	os.WriteFile(kcmConfigPath, []byte(kcmKubeconfig), 0644)

	// Start kube-controller-manager with rotating log
	kcmLog := newRotatingLog(ld, "kube-controller-manager", *logMaxSizeMB, *logMaxFiles)
	info("[+] Starting kube-controller-manager...")
	kcmCmd := exec.CommandContext(ctx, kcmPath,
		"--kubeconfig", kcmConfigPath,
		"--leader-elect=false",
		"--controllers=job,cronjob,ttl-after-finished,garbagecollector",
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
	user, err := testEnv.AddUser(envtest.User{
		Name:   "virtual-kubelet",
		Groups: []string{"system:masters", "system:nodes"},
	}, nil)
	if err != nil {
		panic(err)
	}

	vkKubeconfig := fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: local
contexts:
- context:
    cluster: local
    user: vk-user
  name: default
current-context: default
kind: Config
users:
- name: vk-user
  user:
    client-certificate-data: %s
    client-key-data: %s
`, cfg.Host,
		base64.StdEncoding.EncodeToString(user.Config().CertData),
		base64.StdEncoding.EncodeToString(user.Config().KeyData))

	vkConfigPath := filepath.Join(outDir, "vk-kubeconfig.yaml")
	os.WriteFile(vkConfigPath, []byte(vkKubeconfig), 0644)
	info("[+] VK Kubeconfig written to: %s", vkConfigPath)

	// Generate admin kubeconfig
	adminUser, err := testEnv.AddUser(envtest.User{
		Name:   "admin",
		Groups: []string{"system:masters"},
	}, nil)
	if err != nil {
		panic(err)
	}

	kubeconfigContent := fmt.Sprintf(`apiVersion: v1
clusters:
- cluster:
    server: %s
    insecure-skip-tls-verify: true
  name: local
contexts:
- context:
    cluster: local
    user: admin
  name: default
current-context: default
kind: Config
preferences: {}
users:
- name: admin
  user:
    client-certificate-data: %s
    client-key-data: %s
`, cfg.Host,
		base64.StdEncoding.EncodeToString(adminUser.Config().CertData),
		base64.StdEncoding.EncodeToString(adminUser.Config().KeyData))

	configPath := filepath.Join(outDir, "kubeconfig.yaml")
	os.WriteFile(configPath, []byte(kubeconfigContent), 0644)
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
