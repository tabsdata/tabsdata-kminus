/*
 * Copyright 2026 Tabsdata Inc.
 */

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	nodeapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	"golang.org/x/time/rate"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

var (
	kubeconfig     = flag.String("kubeconfig", "", "Path to kubeconfig file (required)")
	logLevel       = flag.String("log-level", "info", "Log level (trace, debug, info, warn, error)")
	vkPRConfigPath = flag.String("prconfig", "", "Path to VK client config YAML (required)")
	vkConfigDir    = flag.String("configdir", "", "Config directory (default: <workdir>/config)")
	vkWorkDir      = flag.String("workdir", ".", "Working directory (data/virtual-kubelet/ created under this)")
	vkLogDir       = flag.String("logdir", "", "Log directory (default: <workdir>/log)")
	kubeletPort    = flag.String("kubelet-port", "10250", "Kubelet API server port")
	vkLogMaxSizeMB = flag.Int("logmaxsizemb", 100, "Max log file size in MB before rotation")
	vkLogMaxFiles  = flag.Int("logmaxfiles", 3, "Max number of log files to keep")
	heartbeatFile  = flag.String("heartbeat-file", "",
		"File to publish the readiness heartbeat into (disabled when empty)")
	heartbeatSecs = flag.Int("heartbeat-secs", defaultHeartbeatSecs,
		"Seconds between readiness heartbeats")
)

// vkSysConfig contains tunables for the virtual-kubelet runtime.
// Persisted in vksysconfig.yaml — created with defaults on first
// startup, user-editable thereafter.
type vkSysConfig struct {
	SyncSec int `yaml:"syncSec"`
	// SyncStaleSec is the age at which a missing sync round makes this
	// kubelet report degraded. Held apart from SyncSec because the two
	// answer different questions: how often to sync, and how long a node
	// this size may take to get through a round. Zero falls back to
	// syncStaleFactor*SyncSec, so existing config files keep working.
	SyncStaleSec         int `yaml:"syncStaleSec"`
	PodControllerWorkers int `yaml:"podControllerWorkers"`
	PodControllerQPS     int `yaml:"podControllerQps"`
	PodControllerBurst   int `yaml:"podControllerBurst"`
	SyncMaxWorkers       int `yaml:"syncMaxWorkers"`
	ClientQPS            int `yaml:"clientQps"`
	ClientBurst          int `yaml:"clientBurst"`
}

func defaultVKSysConfig() vkSysConfig {
	return vkSysConfig{
		SyncSec:              1,
		SyncStaleSec:         30,
		PodControllerWorkers: 100,
		PodControllerQPS:     500,
		PodControllerBurst:   1000,
		SyncMaxWorkers:       50,
		ClientQPS:            500,
		ClientBurst:          1000,
	}
}

// loadOrCreateVKSysConfig reads vksysconfig.yaml or creates it with defaults.
func loadOrCreateVKSysConfig(configDir string) (vkSysConfig, error) {
	path := filepath.Join(configDir, "vksysconfig.yaml")
	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return vkSysConfig{}, fmt.Errorf("read %s: %w", path, err)
		}
		cfg := defaultVKSysConfig()
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return vkSysConfig{}, fmt.Errorf("parse %s: %w", path, err)
		}
		return cfg, nil
	}
	cfg := defaultVKSysConfig()
	data, _ := yaml.Marshal(&cfg)
	header := []byte("# Virtual-kubelet system configuration.\n" +
		"# Sync interval, worker counts, and K8s client rate limits.\n\n")
	if err := os.WriteFile(path, append(header, data...), 0644); err != nil {
		return vkSysConfig{}, fmt.Errorf("write %s: %w", path, err)
	}
	return cfg, nil
}

// vkPRConfig represents vk-prconfig.yaml
type vkPRConfig struct {
	URL       string          `yaml:"url"`
	NodeName  string          `yaml:"nodeName"`
	Namespace string          `yaml:"namespace"`
	Server    vkServerConfig  `yaml:"server"`
	Client    *vkClientConfig `yaml:"client"`
}

type vkServerConfig struct {
	TLS vkServerTLSConfig `yaml:"tls"`
}

type vkServerTLSConfig struct {
	CertificateAuthority     string `yaml:"certificate-authority"`
	CertificateAuthorityData string `yaml:"certificate-authority-data"`
	InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
}

type vkClientConfig struct {
	ID                 string `yaml:"id"`
	Certificate        string `yaml:"certificate"`
	CertificateData    string `yaml:"certificate-data"`
	CertificateKey     string `yaml:"certificate-key"`
	CertificateKeyData string `yaml:"certificate-key-data"`
}

func loadVKPRConfig(path string) (*vkPRConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read vk-prconfig: %w", err)
	}
	cfg := &vkPRConfig{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse vk-prconfig: %w", err)
	}
	return cfg, nil
}

// resolveData returns the PEM bytes from a file path or base64 data field.
func resolveData(filePath, b64Data string) ([]byte, error) {
	if b64Data != "" {
		return base64.StdEncoding.DecodeString(b64Data)
	}
	if filePath != "" {
		return os.ReadFile(filePath)
	}
	return nil, nil
}

func buildPodRunnerHTTPClient(caPEM, clientCertPEM, clientKeyPEM []byte, insecureSkipVerify bool) (*http.Client, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecureSkipVerify,
	}

	if len(caPEM) > 0 {
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA cert PEM")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if len(clientCertPEM) > 0 && len(clientKeyPEM) > 0 {
		cert, err := tls.X509KeyPair(clientCertPEM, clientKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed to parse client cert/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: tlsConfig,
		},
	}, nil
}

// Heartbeat defaults, mirroring km's own so a standalone run behaves like a
// supervised one.
const defaultHeartbeatSecs = 15

// Heartbeat statuses. Only statusReady is accepted as healthy by km.
// Kept in step with the control-plane copy; see the note above writeHeartbeat.
const (
	statusReady    = "ready"
	statusDegraded = "degraded"
)

// heartbeatTmpSuffix names the staging file the beat is written to before
// being renamed into place.
const heartbeatTmpSuffix = ".tmp"

// syncStaleFactor scales the pod status sync interval into the age at which
// a missing sync round makes this kubelet unhealthy. Two skipped rounds
// absorb a slow tick; three mean the loop is not running.
const syncStaleFactor = 3

// lastSyncUnixNano is stamped by the pod status sync loop at the end of
// every completed round. The heartbeat requires it to be recent, so the
// readiness signal comes from the loop that does the work rather than from
// a timer that would keep ticking after the work stopped.
var lastSyncUnixNano atomic.Int64

// markSyncCompleted records that a pod status sync round finished.
func markSyncCompleted() {
	lastSyncUnixNano.Store(time.Now().UnixNano())
}

// sinceLastSync reports how long ago a sync round last completed. Before the
// first round it is the time since the process started counting, which is
// deliberately large: nothing has been synced yet.
func sinceLastSync() time.Duration {
	stamp := lastSyncUnixNano.Load()
	if stamp == 0 {
		return time.Duration(math.MaxInt64)
	}
	return time.Since(time.Unix(0, stamp))
}

// writeHeartbeat, the status/suffix constants above, and the wait rule
// (heartbeatWait here, probeWait there) are duplicated near-verbatim in the
// control-plane: mods/control-plane/src/main/go/main.go.
//
// Why there is no common package yet: these are two separate Go modules —
// "minimal-vk" and "control-plane", each with its own go.mod — so sharing
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
func writeHeartbeat(logger *logrus.Logger, file, status, detail string) {
	if file == "" {
		return
	}
	if err := os.MkdirAll(filepath.Dir(file), 0755); err != nil {
		logger.Errorf("cannot create heartbeat directory %s: %v", filepath.Dir(file), err)
		return
	}
	body, err := json.Marshal(map[string]any{
		"ts":     time.Now().Unix(),
		"status": status,
		"detail": detail,
	})
	if err != nil {
		logger.Errorf("cannot encode heartbeat: %v", err)
		return
	}
	// Write then rename, so km never reads a half-written beat and counts it
	// as a miss. Rename replaces the target on Windows as well as POSIX.
	tmp := file + heartbeatTmpSuffix
	if err := os.WriteFile(tmp, body, 0644); err != nil {
		logger.Errorf("cannot write heartbeat %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, file); err != nil {
		logger.Errorf("cannot publish heartbeat %s: %v", file, err)
		os.Remove(tmp)
	}
}

// nodeLookupTimeout bounds the node lookup behind each beat. Without it a
// wedged API server — one whose etcd is gone, say — blocks the heartbeat loop
// and the beat simply stops, which tells km that this kubelet is unreachable
// but not why. With it, the beat reports the timeout instead.
const nodeLookupTimeout = 5 * time.Second

// heartbeatStartupRetry spaces beats before the first healthy one, since km's
// readiness gate is waiting on it and would otherwise sit out a whole
// interval.
const heartbeatStartupRetry = 250 * time.Millisecond

// nodeRegistrationTimeout bounds the wait for the node controller to register
// this kubelet's node. Startup continues on expiry: registration is not a
// precondition for serving, only for being scheduled onto.
const nodeRegistrationTimeout = 5 * time.Second

// nodeRegistrationPoll spaces the registration lookups. Registration usually
// lands well inside the first few beats, and km's cluster-start gate is
// waiting on this.
const nodeRegistrationPoll = 100 * time.Millisecond

// beatDetail reports why this kubelet is not ready, or "" when it is. Split
// from the loop so the decision is testable without an API server: nodeErr is
// the node lookup's result, syncAge the age of the last completed sync round.
func beatDetail(nodeName string, nodeErr error, syncAge, syncStale time.Duration) string {
	if nodeErr != nil {
		return fmt.Sprintf("node %q not registered: %v", nodeName, nodeErr)
	}
	if syncAge > syncStale {
		// Before the first round syncAge is unbounded, so this also covers
		// "has never synced".
		return fmt.Sprintf("pod status sync last completed a round over %s ago", syncStale)
	}
	return ""
}

// heartbeatWait spaces the next beat.
func heartbeatWait(everHealthy bool, interval time.Duration) time.Duration {
	if everHealthy {
		return interval
	}
	return heartbeatStartupRetry
}

// runHeartbeat publishes the readiness beat km watches, until ctx is done.
//
// A beat is written only while this kubelet is doing its job: its node is
// still registered with the API server and the pod status sync has completed
// a round recently. The node check is the one that matters — a node
// controller that exited takes the node registration with it, which is
// exactly the failure km could not otherwise see.
func runHeartbeat(
	ctx context.Context,
	logger *logrus.Logger,
	nodes corev1.NodeInterface,
	nodeName string,
	file string,
	interval time.Duration,
	syncStale time.Duration,
) {
	logger.Infof("Readiness heartbeat every %s into %s (sync must be under %s old)",
		interval, file, syncStale)

	everHealthy := false
	for {
		// ctx cancellation surfaces as a lookup error first; the select
		// below is what actually exits.
		// Sampled before the lookup: the lookup is allowed to take
		// nodeLookupTimeout, and charging that wait to the sync loop would
		// let a slow API server read as a stalled sync.
		syncAge := sinceLastSync()
		lookupCtx, cancelLookup := context.WithTimeout(ctx, nodeLookupTimeout)
		_, nodeErr := nodes.Get(lookupCtx, nodeName, metav1.GetOptions{})
		cancelLookup()
		detail := beatDetail(nodeName, nodeErr, syncAge, syncStale)

		if detail == "" {
			if !everHealthy {
				logger.Info("Node registered and syncing; reporting ready")
				everHealthy = true
			}
			writeHeartbeat(logger, file, statusReady, "")
		} else {
			if everHealthy {
				logger.Warnf("Reporting degraded: %s", detail)
			} else {
				logger.Debugf("Not ready yet: %s", detail)
			}
			writeHeartbeat(logger, file, statusDegraded, detail)
		}

		select {
		case <-ctx.Done():
			logger.Info("Readiness heartbeat stopped")
			return
		case <-time.After(heartbeatWait(everHealthy, interval)):
		}
	}
}

// klogFormatter outputs klog-like format: I0405 01:28:07.931074] message
type klogFormatter struct{}

func (f *klogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	level := 'I'
	switch entry.Level {
	case logrus.WarnLevel:
		level = 'W'
	case logrus.ErrorLevel, logrus.FatalLevel, logrus.PanicLevel:
		level = 'E'
	}
	ts := entry.Time
	return []byte(fmt.Sprintf("%c%s %s %s\n",
		level, ts.Format("0102"), ts.Format("15:04:05.000000"), entry.Message)), nil
}

func writePidFile(path string, pid int) {
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte(strconv.Itoa(pid)), 0644)
}

// podsToFinalize selects Terminating pods (a deletionTimestamp is set) whose
// containers the provider is no longer running — i.e. whose UID is not in
// `activeUIDs`, the set of pods the pod-runner currently reports with a
// non-terminal phase (Pending/Running/…). A pod the provider reports as
// terminal, or no longer reports at all (already cleaned up), is done and
// safe to remove immediately.
//
// Why this exists: when a *running* pod is deleted, the upstream
// PodController calls the provider's DeletePod and then enqueues the
// API-server removal with a delay equal to the pod's
// DeletionGracePeriodSeconds (podcontroller.go:575). The pod-runner kills
// the container and drops the pod from its status feed within milliseconds,
// but the object then sits in Terminating for the full grace period (30s by
// default) before the library removes it. A real kubelet instead finalizes
// as soon as the containers stop. We restore that behavior by force-deleting
// (grace 0) these pods ourselves; whichever delete lands first wins and the
// other is a harmless NotFound.
func podsToFinalize(pods []*v1.Pod, activeUIDs map[string]bool) []*v1.Pod {
	var out []*v1.Pod
	for _, pod := range pods {
		if pod.DeletionTimestamp == nil {
			continue
		}
		if activeUIDs[string(pod.UID)] {
			// Provider is still running this incarnation — let it
			// terminate within its grace period rather than yanking it.
			continue
		}
		out = append(out, pod)
	}
	return out
}

// finalizeTerminatingPods force-deletes (grace 0, UID precondition) the
// Terminating pods whose work the provider has finished, so they don't
// linger in Terminating for the full grace period. See [podsToFinalize].
func finalizeTerminatingPods(
	ctx context.Context,
	pods corev1.PodsGetter,
	lister listersv1.PodLister,
	activeUIDs map[string]bool,
	logger *logrus.Logger,
) {
	all, err := lister.List(labels.Everything())
	if err != nil {
		logger.WithError(err).Debug("finalize: failed to list pods from informer")
		return
	}
	for _, pod := range podsToFinalize(all, activeUIDs) {
		opts := *metav1.NewDeleteOptions(0)
		opts.Preconditions = metav1.NewUIDPreconditions(string(pod.UID))
		err := pods.Pods(pod.Namespace).Delete(ctx, pod.Name, opts)
		switch {
		case err == nil:
			logger.Infof("finalize: force-deleted terminating pod %s/%s (provider done)",
				pod.Namespace, pod.Name)
		case apierrors.IsNotFound(err) || apierrors.IsConflict(err):
			// Already removed (library queue or a newer incarnation) — fine.
		default:
			logger.WithError(err).Warnf("finalize: failed to force-delete %s/%s",
				pod.Namespace, pod.Name)
		}
	}
}

func main() {
	flag.Parse()

	// Resolve --kubeconfig from env if not set
	if *kubeconfig == "" {
		if env := os.Getenv("KUBECONFIG"); env != "" {
			kubeconfig = &env
		}
	}
	if *kubeconfig == "" {
		fmt.Println("ERROR: --kubeconfig is required (or set KUBECONFIG env var)")
		flag.Usage()
		os.Exit(1)
	}

	// Resolve --prconfig from env if not set
	if *vkPRConfigPath == "" {
		if env := os.Getenv("PRCONFIG"); env != "" {
			vkPRConfigPath = &env
		}
	}
	if *vkPRConfigPath == "" {
		fmt.Println("ERROR: --prconfig is required (or set PRCONFIG env var)")
		flag.Usage()
		os.Exit(1)
	}

	// Resolve workdir and create data/virtual-kubelet/
	wd, _ := filepath.Abs(*vkWorkDir)
	vkDataDir := filepath.Join(wd, "data", "virtual-kubelet")
	pidsDir := filepath.Join(vkDataDir, "pids")
	os.MkdirAll(pidsDir, 0755)
	writePidFile(filepath.Join(pidsDir, fmt.Sprintf("%d.virtual-kubelet", os.Getpid())), os.Getpid())

	// Resolve configdir and load vksysconfig.yaml
	configDir := *vkConfigDir
	if configDir == "" {
		configDir = filepath.Join(wd, "config")
	}
	if !filepath.IsAbs(configDir) {
		configDir, _ = filepath.Abs(configDir)
	}
	os.MkdirAll(configDir, 0755)
	vkSys, err := loadOrCreateVKSysConfig(configDir)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	// Load vk-prconfig
	vkCfg, err := loadVKPRConfig(*vkPRConfigPath)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		os.Exit(1)
	}

	if vkCfg.URL == "" {
		fmt.Println("ERROR: vk-prconfig must have a 'url' field")
		os.Exit(1)
	}

	// Read node identity from config (with defaults)
	vkNodeName := vkCfg.NodeName
	if vkNodeName == "" {
		vkNodeName = "virtual-node"
	}
	vkNamespace := vkCfg.Namespace
	if vkNamespace == "" {
		vkNamespace = "default"
	}

	// Build TLS-configured HTTP client from vk-prconfig
	insecureSkip := vkCfg.Server.TLS.InsecureSkipTLSVerify

	caPEM, err := resolveData(vkCfg.Server.TLS.CertificateAuthority, vkCfg.Server.TLS.CertificateAuthorityData)
	if err != nil {
		fmt.Printf("ERROR: failed to resolve server CA: %v\n", err)
		os.Exit(1)
	}

	var clientCertPEM, clientKeyPEM []byte
	if vkCfg.Client != nil {
		clientCertPEM, err = resolveData(vkCfg.Client.Certificate, vkCfg.Client.CertificateData)
		if err != nil {
			fmt.Printf("ERROR: failed to resolve client cert: %v\n", err)
			os.Exit(1)
		}
		clientKeyPEM, err = resolveData(vkCfg.Client.CertificateKey, vkCfg.Client.CertificateKeyData)
		if err != nil {
			fmt.Printf("ERROR: failed to resolve client key: %v\n", err)
			os.Exit(1)
		}
	}

	// Setup logging with rotating file
	ld := *vkLogDir
	if ld == "" {
		ld = filepath.Join(wd, "log")
	}
	if !filepath.IsAbs(ld) {
		ld, _ = filepath.Abs(ld)
	}
	os.MkdirAll(ld, 0755)

	vkLog := &lumberjack.Logger{
		Filename:   filepath.Join(ld, "virtual-kubelet.log"),
		MaxSize:    *vkLogMaxSizeMB,
		MaxBackups: *vkLogMaxFiles - 1,
		Compress:   false,
	}

	// Logrus writes to rotating log file only; fmt.Print goes to stdout for bootstrap info
	logger := logrus.New()
	logger.SetOutput(vkLog)
	logger.SetFormatter(&klogFormatter{})
	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		level = logrus.InfoLevel
	}
	logger.SetLevel(level)
	ctx := context.Background()

	clientId := "anonymous"
	if vkCfg.Client != nil && vkCfg.Client.ID != "" {
		clientId = vkCfg.Client.ID
	}

	fmt.Printf("Starting Minimal Virtual Kubelet\n")
	fmt.Printf("  Node Name: %s\n", vkNodeName)
	fmt.Printf("  Namespace: %s\n", vkNamespace)
	fmt.Printf("  Pod Runner URL: %s\n", vkCfg.URL)
	fmt.Printf("  Kubeconfig: %s\n", *kubeconfig)
	fmt.Printf("  VK PR Config: %s (client=%s)\n", *vkPRConfigPath, clientId)

	// Create Kubernetes client
	config, err := buildKubeConfig(*kubeconfig)
	if err != nil {
		logger.Fatal(fmt.Errorf("failed to build kubeconfig: %w", err))
	}

	// Bump client QPS/Burst to handle bulk status updates without
	// hitting the default rate limiter (5 QPS / 10 burst).
	config.QPS = float32(vkSys.ClientQPS)
	config.Burst = vkSys.ClientBurst

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		logger.Fatal(fmt.Errorf("failed to create kubernetes client: %w", err))
	}

	// Create our minimal provider with TLS-configured HTTP client
	prHTTPClient, err := buildPodRunnerHTTPClient(caPEM, clientCertPEM, clientKeyPEM, insecureSkip)
	if err != nil {
		logger.Fatal(fmt.Errorf("failed to build pod-runner HTTP client: %w", err))
	}
	kubeletPortNum, _ := strconv.ParseInt(*kubeletPort, 10, 32)
	provider := NewPodRunnerProvider(vkNodeName, vkCfg.URL, prHTTPClient, int32(kubeletPortNum))

	// Create node controller
	nodeRunner, err := node.NewNodeController(
		provider,
		&v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: vkNodeName,
				Labels: map[string]string{
					"type":                   "virtual-kubelet",
					"kubernetes.io/role":     "agent",
					"kubernetes.io/hostname": vkNodeName,
					"beta.kubernetes.io/os":  "linux",
					"alpha.service-controller.kubernetes.io/exclude-balancer": "true",
				},
			},
			Spec: v1.NodeSpec{
				Taints: []v1.Taint{
					{
						Key:    "virtual-kubelet.io/provider",
						Value:  "minimal",
						Effect: v1.TaintEffectNoSchedule,
					},
				},
			},
		},
		clientset.CoreV1().Nodes(),
		node.WithNodeEnableLeaseV1(clientset.CoordinationV1().Leases(v1.NamespaceNodeLease), int32(40)),
		node.WithNodeStatusUpdateErrorHandler(func(ctx context.Context, err error) error {
			logger.WithError(err).Error("node status update failed")
			return nil
		}),
	)
	if err != nil {
		logger.WithError(err).Fatal("failed to create node controller")
	}

	// Create event recorder
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&corev1.EventSinkImpl{
		Interface: clientset.CoreV1().Events(""),
	})
	recorder := eventBroadcaster.NewRecorder(
		scheme.Scheme,
		v1.EventSource{Component: "virtual-kubelet", Host: vkNodeName},
	)

	// Create pod informer, filtered to pods assigned to this node — like
	// a real kubelet, pods pinned to another (or nonexistent) node are
	// never run here; they stay Pending. This also keeps apiserver log
	// routing consistent: `kubectl logs` resolves the kubelet through the
	// pod's spec.nodeName, so a pod this node ran but isn't assigned to
	// would have unreachable logs.
	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		1*time.Minute,
		informers.WithNamespace(vkNamespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", vkNodeName).String()
		}),
	)
	podInformer := podInformerFactory.Core().V1().Pods()

	// ConfigMaps/secrets/services need an unfiltered factory:
	// spec.nodeName is not a valid field selector for those resources.
	scmInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		1*time.Minute,
		informers.WithNamespace(vkNamespace),
	)
	configMapInformer := scmInformerFactory.Core().V1().ConfigMaps()
	secretInformer := scmInformerFactory.Core().V1().Secrets()
	serviceInformer := scmInformerFactory.Core().V1().Services()

	// Create pod controller
	// The PodController work queues default to client-go's
	// DefaultControllerRateLimiter, whose token bucket caps the global
	// drain at 10 qps / 100 burst — so all PodControllerWorkers share a
	// 10/s ceiling and pod create/delete/status-sync back up at scale.
	// Override with a configurable bucket (podControllerQps/Burst) so
	// throughput tracks the worker count instead.
	podRateLimiter := func() workqueue.RateLimiter {
		return workqueue.NewMaxOfRateLimiter(
			workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
			&workqueue.BucketRateLimiter{
				Limiter: rate.NewLimiter(rate.Limit(vkSys.PodControllerQPS), vkSys.PodControllerBurst),
			},
		)
	}

	podController, err := node.NewPodController(node.PodControllerConfig{
		PodClient:                            clientset.CoreV1(),
		PodInformer:                          podInformer,
		ConfigMapInformer:                    configMapInformer,
		SecretInformer:                       secretInformer,
		ServiceInformer:                      serviceInformer,
		EventRecorder:                        recorder,
		Provider:                             provider,
		SyncPodsFromKubernetesRateLimiter:    podRateLimiter(),
		DeletePodsFromKubernetesRateLimiter:  podRateLimiter(),
		SyncPodStatusFromProviderRateLimiter: podRateLimiter(),
	})
	if err != nil {
		logger.WithError(err).Fatal("failed to create pod controller")
	}

	// Start informers
	podInformerFactory.Start(ctx.Done())
	scmInformerFactory.Start(ctx.Done())

	// Wait for informer caches to sync
	logger.Info("Waiting for informer caches to sync...")
	for _, factory := range []informers.SharedInformerFactory{podInformerFactory, scmInformerFactory} {
		for informerType, ok := range factory.WaitForCacheSync(ctx.Done()) {
			if !ok {
				logger.Fatalf("Failed to sync cache for %v", informerType)
			}
		}
	}
	logger.Info("Informer caches synced successfully")

	// Setup signal handling for graceful shutdown
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Start node controller
	go func() {
		if err := nodeRunner.Run(ctx); err != nil {
			logger.WithError(err).Error("node controller exited with error")
			cancel()
		}
	}()

	// Wait for the node to register, polling so startup costs only what
	// registration actually takes.
	logger.Info("Waiting for node to be registered...")
	registrationDeadline := time.Now().Add(nodeRegistrationTimeout)
	for {
		_, err = clientset.CoreV1().Nodes().Get(ctx, vkNodeName, metav1.GetOptions{})
		if err == nil {
			logger.Info("Node registered successfully")
			break
		}
		if time.Now().After(registrationDeadline) {
			logger.WithError(err).Warn("Node not yet registered, but continuing...")
			break
		}
		select {
		case <-ctx.Done():
			logger.Warn("Cancelled while waiting for node registration")
			return
		case <-time.After(nodeRegistrationPoll):
		}
	}

	// Start kubelet API server (serves kubectl logs)
	apiHandler := nodeapi.PodHandler(nodeapi.PodHandlerConfig{
		GetContainerLogs:  provider.GetContainerLogs,
		RunInContainer:    provider.RunInContainer,
		AttachToContainer: provider.AttachToContainer,
		GetPods:           provider.GetPods,
	}, false)
	tlsCert, err := generateSelfSignedCert()
	if err != nil {
		logger.WithError(err).Fatal("failed to generate self-signed cert for kubelet API")
	}
	kubeletServer := &http.Server{
		Handler: apiHandler,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{tlsCert},
		},
	}
	kubeletAddr := ":" + *kubeletPort
	ln, err := tls.Listen("tcp", kubeletAddr, kubeletServer.TLSConfig)
	if err != nil {
		logger.WithError(err).Fatalf("failed to listen on %s for kubelet API", kubeletAddr)
	}
	go func() {
		logger.Infof("Kubelet API server listening on %s (TLS)", kubeletAddr)
		if err := kubeletServer.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.WithError(err).Error("kubelet API server error")
		}
	}()
	go func() {
		<-ctx.Done()
		kubeletServer.Close()
	}()

	// Start pod controller
	logger.Info("Starting pod controller...")

	// Start background status sync loop
	syncInterval := time.Duration(vkSys.SyncSec) * time.Second
	if syncInterval <= 0 {
		syncInterval = 2 * time.Second
	}
	syncStale := time.Duration(vkSys.SyncStaleSec) * time.Second
	if syncStale <= 0 {
		syncStale = syncStaleFactor * syncInterval
	}
	// -----------------------------------------------------------------------
	// Pod status sync loop
	//
	// Synchronizes pod phase changes from pod-runner → K8s API server.
	//
	// DESIGN:
	//
	// The naive approach (what this replaced) was:
	//   per tick: GetPods() + N×GetPodStatus() + N×K8s Get()
	// That's O(N) HTTP calls per tick even when nothing changed —
	// at 1000 pods with 2s ticks, that's 2000+ calls/sec to the
	// K8s API server, each ~5ms, so the tick can't finish in time.
	//
	// The optimized approach uses two techniques:
	//
	// 1. LIGHTWEIGHT BULK FETCH: A single call to GetAllPodPhases()
	//    returns {namespace, name, phase} for every pod (~50 KB for
	//    1000 pods, vs ~5 MB for GetPods with full specs/statuses).
	//
	// 2. LOCAL PHASE CACHE: A map[key]phase tracks the last phase
	//    we successfully pushed to the K8s API. If the pod-runner
	//    reports the same phase, we skip the K8s Get+UpdateStatus
	//    entirely. K8s calls only fire on actual phase transitions
	//    (typically 0-few per tick in steady state).
	//
	// Cost comparison (1000 pods, steady state):
	//   Before: 1001 pod-runner calls + 1000 K8s GETs → 2-10 sec/tick
	//   After:  1 pod-runner call + 0 K8s calls       → ~10 ms/tick
	// -----------------------------------------------------------------------
	logger.Infof("Pod status sync interval: %s", syncInterval)
	podLister := podInformer.Lister()
	go func() {
		ticker := time.NewTicker(syncInterval)
		defer ticker.Stop()

		// Phase cache: tracks the last phase we successfully synced to
		// the K8s API for each pod incarnation. Keyed by the kube pod
		// UID — keying by "namespace/name" alone would leak stale
		// entries across pod replacement (delete + recreate with the
		// same name), causing the new pod's status to be skipped if it
		// happens to match the old pod's last cached phase.
		phaseCache := make(map[string]string)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				roundStart := time.Now()
				// Step 1: One bulk call to get all pod statuses
				// from pod-runner. This replaces both GetAllPodPhases
				// and per-pod GetPodStatus — zero per-pod HTTP calls.
				entries, err := provider.GetAllPodStatuses(ctx)
				if err != nil {
					// Warn, not debug: this path leaves the sync timestamp
					// unstamped, so a persistent failure here is what the
					// readiness beat reports as a stalled sync. Silent would
					// mean diagnosing the watchdog instead of the cause.
					logger.WithError(err).Warnf("Failed to get pod statuses for sync: %v", err)
					continue
				}
				// The round is live from here: the loop is turning and the
				// pod-runner answered. Stamped before the per-pod work below,
				// which is skipped entirely when there are no pods to sync.
				markSyncCompleted()

				// Finalize Terminating pods whose containers the provider has
				// already finished, so they don't sit in Terminating for the
				// full grace period (see finalizeTerminatingPods). A pod is
				// "active" only while the pod-runner reports it with a
				// non-terminal phase.
				activeUIDs := make(map[string]bool, len(entries))
				for _, e := range entries {
					if e.Phase != "Succeeded" && e.Phase != "Failed" {
						activeUIDs[e.UID] = true
					}
				}
				finalizeTerminatingPods(ctx, clientset.CoreV1(), podLister, activeUIDs, logger)

				// Step 2: Collect pods that may need a status push. The
				// phase-only check can't run yet because we don't have
				// the kube UID until step 3 fetches the live pod —
				// dedup happens after the Get returns the UID.
				type syncItem struct {
					entry PodStatusEntry
				}
				toSync := make([]syncItem, 0, len(entries))
				for _, entry := range entries {
					toSync = append(toSync, syncItem{entry: entry})
				}

				if len(toSync) == 0 {
					continue
				}

				// Step 3: Push status updates to K8s API concurrently.
				// Status is already fetched in step 1 — only K8s
				// Get+UpdateStatus calls remain (2 per pod, not 3).
				maxWorkers := vkSys.SyncMaxWorkers
				type syncResult struct {
					uidKey string
					phase  string
				}
				results := make(chan syncResult, len(toSync))
				sem := make(chan struct{}, maxWorkers)
				cacheMu := sync.Mutex{}
				var wg sync.WaitGroup

				for _, item := range toSync {
					wg.Add(1)
					sem <- struct{}{}
					go func(e PodStatusEntry) {
						defer wg.Done()
						defer func() { <-sem }()

						for attempt := 0; attempt < 3; attempt++ {
							latestPod, err := clientset.CoreV1().Pods(e.Namespace).Get(ctx, e.Name, metav1.GetOptions{})
							if err != nil {
								break
							}
							uidKey := string(latestPod.UID)

							// Stale-incarnation eviction. The pod-runner keys
							// pods by uid, but the kube control plane reuses
							// (namespace, name) across incarnations, each with a
							// fresh uid. If the live kube pod for this name has a
							// different uid than the pod-runner entry, the runner
							// is still holding a *previous* incarnation: its delete
							// was never delivered — the upstream PodController skips
							// the provider delete for terminal pods (it deletes them
							// from the API server only), and dangling-pod GC can't
							// spot a uid mismatch under a reused name. Such a stale
							// entry shadows the current pod in the runner's
							// name-keyed lookups: getPod (which drives the lib's
							// createOrUpdatePod, blocking the new pod's create) and
							// getAllPodStatuses (this loop, which would otherwise
							// push the old pod's status onto the new one).
							//
							// So evict it by name and skip the entry; the next sync
							// finds the current pod and the lib creates it cleanly.
							// e.UID == "" means an older pod-runner that doesn't
							// report uids — skip the check rather than evict blindly.
							// (create_pod keeps at most one incarnation per name in
							// the runner, so the name-based delete can't hit the
							// current pod.)
							if e.UID != "" && uidKey != e.UID {
								logger.Warnf("pod-runner holds stale incarnation of %s/%s (runner uid %s, live kube uid %s); evicting", e.Namespace, e.Name, e.UID, uidKey)
								stale := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: e.Namespace, Name: e.Name}}
								if delErr := provider.DeletePod(ctx, stale); delErr != nil {
									logger.WithError(delErr).Warnf("failed to evict stale incarnation of %s/%s", e.Namespace, e.Name)
								}
								return
							}

							cacheMu.Lock()
							cached, ok := phaseCache[uidKey]
							cacheMu.Unlock()
							if ok && cached == e.Phase {
								return
							}
							latestPod.Status = e.Status
							_, err = clientset.CoreV1().Pods(e.Namespace).UpdateStatus(ctx, latestPod, metav1.UpdateOptions{})
							if err == nil {
								results <- syncResult{uidKey: uidKey, phase: e.Phase}
								return
							}
							if attempt < 2 {
								time.Sleep(time.Duration(100*(attempt+1)) * time.Millisecond)
							} else {
								logger.WithError(err).Warnf("Failed to update status for %s/%s after retries", e.Namespace, e.Name)
							}
						}
					}(item.entry)
				}

				wg.Wait()
				close(results)

				// Update cache from successful results.
				synced := 0
				for r := range results {
					cacheMu.Lock()
					phaseCache[r.uidKey] = r.phase
					cacheMu.Unlock()
					synced++
				}
				// Round latency is logged unconditionally, and separately from
				// the update count: a round that changed nothing still says
				// whether the loop is keeping up, which is what the readiness
				// beat is judging. A round at or past the staleness budget is
				// the one that will trip it, so it is warned about by name.
				roundLatency := time.Since(roundStart)
				if synced > 0 {
					logger.Infof("Synced %d pod status updates in %s (%d pods)",
						synced, roundLatency, len(entries))
				} else {
					logger.Debugf("Sync round completed in %s (%d pods, no updates)",
						roundLatency, len(entries))
				}
				if roundLatency >= syncStale {
					logger.Warnf(
						"Sync round took %s over %d pods, at or past the %s staleness budget; "+
							"raise 'syncStaleSec' in vksysconfig.yaml if this node is simply this big",
						roundLatency, len(entries), syncStale)
				}
			}
		}
	}()

	if *heartbeatFile != "" {
		go runHeartbeat(
			ctx,
			logger,
			clientset.CoreV1().Nodes(),
			vkNodeName,
			*heartbeatFile,
			time.Duration(*heartbeatSecs)*time.Second,
			syncStale,
		)
	} else {
		logger.Info("No --heartbeat-file given; readiness heartbeat disabled")
	}

	if err := podController.Run(ctx, vkSys.PodControllerWorkers); err != nil && ctx.Err() == nil {
		logger.WithError(err).Fatal("pod controller exited with error")
	}

	// Clean up PID files
	os.RemoveAll(pidsDir)
	logger.Info("Virtual Kubelet shutting down...")
}

func buildKubeConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}

	config, err := rest.InClusterConfig()
	if err == nil {
		return config, nil
	}

	if home := os.Getenv("HOME"); home != "" {
		return clientcmd.BuildConfigFromFlags("", home+"/.kube/config")
	}

	return nil, fmt.Errorf("no kubeconfig found")
}

func generateSelfSignedCert() (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "virtual-kubelet"},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}, nil
}
