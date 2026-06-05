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
	"flag"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	nodeapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
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
)

// vkSysConfig contains tunables for the virtual-kubelet runtime.
// Persisted in vksysconfig.yaml — created with defaults on first
// startup, user-editable thereafter.
type vkSysConfig struct {
	SyncSec              int `yaml:"syncSec"`
	PodControllerWorkers int `yaml:"podControllerWorkers"`
	SyncMaxWorkers       int `yaml:"syncMaxWorkers"`
	ClientQPS            int `yaml:"clientQps"`
	ClientBurst          int `yaml:"clientBurst"`
}

func defaultVKSysConfig() vkSysConfig {
	return vkSysConfig{
		SyncSec:              1,
		PodControllerWorkers: 100,
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
	podController, err := node.NewPodController(node.PodControllerConfig{
		PodClient:         clientset.CoreV1(),
		PodInformer:       podInformer,
		ConfigMapInformer: configMapInformer,
		SecretInformer:    secretInformer,
		ServiceInformer:   serviceInformer,
		EventRecorder:     recorder,
		Provider:          provider,
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

	// Give the node time to register
	logger.Info("Waiting for node to be registered...")
	time.Sleep(5 * time.Second)

	// Verify node exists
	_, err = clientset.CoreV1().Nodes().Get(ctx, vkNodeName, metav1.GetOptions{})
	if err != nil {
		logger.WithError(err).Warn("Node not yet registered, but continuing...")
	} else {
		logger.Info("Node registered successfully")
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
				// Step 1: One bulk call to get all pod statuses
				// from pod-runner. This replaces both GetAllPodPhases
				// and per-pod GetPodStatus — zero per-pod HTTP calls.
				entries, err := provider.GetAllPodStatuses(ctx)
				if err != nil {
					logger.WithError(err).Debug("Failed to get pod statuses for sync")
					continue
				}

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
				if synced > 0 {
					logger.Infof("Synced %d pod status updates", synced)
				}
			}
		}
	}()

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
