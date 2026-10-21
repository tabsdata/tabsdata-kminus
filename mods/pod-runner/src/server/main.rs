// Copyright 2026 Tabsdata Inc.

use std::sync::Arc;

use clap::Parser;
use file_rotate::{ContentLimit, FileRotate, compression::Compression, suffix::AppendCount};
use tokio::sync::watch;
use tracing::{info, warn};

use pod_runner::config::cli::CliArgs;
use pod_runner::config::resolve::resolve_config;
use pod_runner::logic::disk_usage::{DiskSizePodTask, DiskSizeRunnerTask};
use pod_runner::logic::{
    CleanupConfig, DefaultExecutorProvider, ExecutorProvider, PodLogic, ResolverSet,
};
use pod_runner::metrics::Metrics;
use pod_runner::server::app::{AppState, create_router};
use pod_runner::store::PodStore;
use pod_runner::store::file::{FileStore, FileStoreConfig};
use pod_runner::store::memory::MemoryStore;
use pod_runner::store::multiplex::MultiplexStore;

#[tokio::main]
async fn main() {
    // Before CliArgs::parse: a re-exec'd graceful-shutdown helper must
    // never reach the normal CLI (see
    // kcommon::signal::ctrl_break_helper_hook).
    kcommon::signal::ctrl_break_helper_hook();
    // Parse CLI arguments and resolve configuration first so we know
    // the log directory. Logging is initialised below once we've
    // decided where to write.
    let cli = CliArgs::parse();
    // A bad config is the operator's problem, not a crash: report it on
    // stderr and exit. Panicking would render the message through `Debug`,
    // which escapes the newlines that make a multi-line complaint readable,
    // and would bury it under a backtrace note.
    let pr_config = match resolve_config(&cli) {
        Ok(config) => config,
        Err(e) => {
            eprintln!("pod-runner: configuration error: {e}");
            std::process::exit(1);
        }
    };

    // Set up tracing to a size-rotated log file under
    // <logdir>/pod-runner.log, matching control-plane
    // (`control-plane.log`) and virtual-kubelet
    // (`virtual-kubelet.log`) which use lumberjack with the same
    // MB/file-count semantics. Rotated files are suffixed `.1`,
    // `.2`, ... (newest first) up to `--logmaxfiles`.
    let log_dir = pr_config.log_dir.clone();
    std::fs::create_dir_all(&log_dir).ok();
    let log_path = log_dir.join("pod-runner.log");
    let max_size = (cli.logmaxsizemb as usize) * 1024 * 1024;
    let max_files = cli.logmaxfiles.max(1) as usize;
    let writer = FileRotate::new(
        &log_path,
        AppendCount::new(max_files - 1),
        ContentLimit::Bytes(max_size),
        Compression::None,
        #[cfg(unix)]
        None,
    );
    // Wrap in a Mutex so tracing's MakeWriter can clone a guard per
    // event, and use synchronous writes so log lines are visible
    // immediately (important for startup detection by external
    // supervisors / test harnesses).  Logging volume here is modest
    // (~80 lines/sec at peak load), so the sync overhead is fine.
    let writer = std::sync::Mutex::new(writer);

    tracing_subscriber::fmt()
        .with_env_filter(cli.loglevel.env_filter())
        .with_writer(writer)
        .with_ansi(false)
        .init();

    info!(
        log = %log_path.display(),
        max_size_mb = cli.logmaxsizemb,
        max_files = cli.logmaxfiles,
        loglevel = %cli.loglevel,
        "Logging initialized (size-based rotation)",
    );

    // Installed before the first container is spawned. A panic aborts
    // without unwinding, and the container processes this runner started
    // outlive it: they hold their pods' work dirs and, for an embedded
    // server, a port. Killing by kill token reaches them all, including
    // the ones that escaped their process group.
    kcommon::panic::install_with_teardown("pod-runner", || {
        let sweep = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        if let Ok(sweep) = sweep {
            sweep.block_on(async {
                pod_runner::process::sweep::kill_orphans(
                    &pod_runner::process::sweep::runner_prefix(),
                )
                .await;
            });
        }
    });

    // Install rustls crypto provider (needed by kube client in resolvers)
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Kill any orphaned processes from a previous run. The PID dir is fixed
    // across restarts, so it holds what a crashed run left.
    let stale_killed = pod_runner::process::executor::cleanup_stale_pids(&pr_config.pid_dir);
    if stale_killed > 0 {
        warn!(stale_killed, "Killed stale processes from previous run");
    }

    // Container processes that escaped their process group are not in
    // those PID files — nothing recorded them. They carry a kill token
    // naming the pod-runner that started them, so the ones whose runner
    // is provably gone can be reclaimed here and nowhere else.
    let orphans_killed = pod_runner::process::sweep::kill_dead_runner_orphans().await;
    if orphans_killed > 0 {
        warn!(
            orphans_killed,
            "Killed escaped container processes from previous run"
        );
    }

    // Create metrics
    let metrics = Metrics::new();

    let provider = Arc::new(DefaultExecutorProvider::from(&pr_config)) as Arc<dyn ExecutorProvider>;

    let memory_store = Arc::new(MemoryStore::new());
    let file_store = Arc::new(FileStore::new(FileStoreConfig::from(&pr_config)));
    let store = Arc::new(MultiplexStore::new(
        memory_store as Arc<dyn PodStore>,
        file_store as Arc<dyn PodStore>,
    )) as Arc<dyn PodStore>;

    let resolvers = ResolverSet::from_configs(
        &pr_config.secret_resolver,
        &pr_config.config_map_resolver,
        &pr_config.node_env_resolver,
        &pr_config.kubeconfig,
    )
    .await
    .expect("Failed to initialize resolvers");

    let (shutdown_tx, _) = watch::channel(());
    // Subscribed before `shutdown_tx` is moved into the serve future, so the
    // heartbeat task stops the moment a shutdown signal arrives.
    let heartbeat_shutdown = shutdown_tx.subscribe();
    let cleanup_config = CleanupConfig::from(&pr_config);
    let logic = PodLogic::new(
        &pr_config,
        cleanup_config,
        store,
        provider,
        resolvers,
        shutdown_tx.clone(),
    )
    .with_metrics(Arc::clone(&metrics))
    .register_task(Arc::new(DiskSizePodTask::with_interval(
        std::time::Duration::from_secs(pr_config.dir_size_scan_interval_secs),
    )))
    // The whole data dir: pod-run/, pod-state/ and pod-pid/.
    .runner_work_dir(pr_config.data_dir.clone())
    .register_runner_task(Arc::new(DiskSizeRunnerTask::with_interval(
        std::time::Duration::from_secs(pr_config.dir_size_scan_interval_secs),
    )));

    // Pick up the pod dirs the previous run left, before any pod is admitted:
    // their deadlines are on disk, and a restart must not lose them.
    logic.recover_pod_dirs();

    // Spawn metrics server
    let metrics_port = pr_config.metrics_port();
    if metrics_port > 0 {
        let metrics_shutdown = shutdown_tx.subscribe();
        let metrics_clone = Arc::clone(&metrics);
        tokio::spawn(async move {
            pod_runner::metrics::server::serve_metrics(
                metrics_port,
                metrics_clone,
                metrics_shutdown,
            )
            .await;
        });
    }

    let grace_secs = pr_config.default_grace_period_secs;
    let state = Arc::new(AppState { logic });
    let app = create_router(Arc::clone(&state), Some(Arc::clone(&metrics)));

    let listener = tokio::net::TcpListener::bind(&pr_config.bind_address)
        .await
        .unwrap();
    info!(addr = %pr_config.bind_address, tls = "mTLS", "Server listening");

    // Hand km the node sizing to print with the startup banner. The pod
    // ceiling in particular is invisible until it is hit, and then only as a
    // wave of ProviderFailed pods that look like a pod fault rather than a
    // configured limit.
    pod_runner::server::messages::report_sizing(
        &pr_config.work_dir,
        pr_config.capacity_cpu,
        pr_config.capacity_memory_gb,
        pr_config.capacity_pods,
    );
    // Kept aside for the shutdown message: the config is consumed by the
    // server setup below.
    let work_dir = pr_config.work_dir.clone();

    // Readiness heartbeat. Spawned before serving starts: its first
    // successful self-probe is what tells km this server is up, so it has
    // to be running by the time the listener starts accepting.
    if let Some(file) = cli.heartbeat_file.as_deref() {
        let file = std::path::PathBuf::from(file);
        let vk_prconfig = pr_config
            .config_dir
            .join(pod_runner::config::template::VK_PRCONFIG_FILENAME);
        let interval = std::time::Duration::from_secs(cli.heartbeat_secs.max(1));
        tokio::spawn(pod_runner::server::heartbeat::run(
            file,
            interval,
            vk_prconfig,
            heartbeat_shutdown,
        ));
    } else {
        info!("No --heartbeat-file given; readiness heartbeat disabled");
    }

    let server_config = pod_runner::server::tls::build_server_config(&pr_config)
        .expect("Failed to build TLS config");
    let shutdown = Box::pin(async move {
        wait_for_shutdown_signal().await;
        info!("Shutting down...");
        let _ = shutdown_tx.send(());
        // Every pod is going down, so sweep once for all of them instead
        // of once per pod. Doing it here rather than at the end of the
        // drain means an escaped server starts shutting down at the same
        // moment the containers do, and still has the drain window to
        // finish in — which for an embedded postgres is the difference
        // between a checkpoint and a recovery on next start.
        let signalled = pod_runner::process::sweep::terminate_orphans(
            &pod_runner::process::sweep::runner_prefix(),
        )
        .await;
        if signalled > 0 {
            warn!(
                signalled,
                "SIGTERM sent to container processes that escaped their process group"
            );
        }
    });
    pod_runner::server::tls::axum_serve_tls(listener, app, server_config, shutdown).await;

    // Wait for all running pods to finish cleanup before exiting.
    // Timeout is 2x the grace period to allow for SIGTERM + SIGKILL + the
    // monitor's post-completion work.
    let wait_timeout = std::time::Duration::from_secs(grace_secs * 2 + 10);
    state.logic.wait_for_pods(wait_timeout).await;

    // Whatever ignored the SIGTERM above, or escaped a container while the
    // drain ran. One scan for every pod this runner started; the per-pod
    // sweeps are skipped on this path precisely so this is the only one.
    let killed =
        pod_runner::process::sweep::kill_orphans(&pod_runner::process::sweep::runner_prefix())
            .await;
    if killed > 0 {
        warn!(
            killed,
            "SIGKILLed container processes that outlived their pods"
        );
    }

    info!("Server stopped");

    // Reported from here rather than mid-drain: by now every pod has been
    // through its termination grace, so the count is the final one.
    pod_runner::server::messages::write(
        &work_dir,
        pod_runner::server::messages::STOP_PHASE,
        pod_runner::server::messages::LEVEL_INFO,
        &format!("stopped; {killed} container processes outlived their pods and were killed"),
    );
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigint = signal(SignalKind::interrupt()).expect("install SIGINT handler");
    let mut sigterm = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    let mut sigquit = signal(SignalKind::quit()).expect("install SIGQUIT handler");
    tokio::select! {
        _ = sigint.recv() => {
            info!("Received SIGINT (Ctrl+C). Initiating graceful stop...");
        }
        _ = sigterm.recv() => {
            info!("Received SIGTERM. Initiating graceful stop...");
        }
        _ = sigquit.recv() => {
            info!("Received SIGQUIT. Initiating forceful stop...");
        }
    }
}

#[cfg(windows)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::windows::{ctrl_break, ctrl_c, ctrl_close, ctrl_logoff, ctrl_shutdown};
    let mut ctrlc = ctrl_c().expect("install Ctrl+C handler");
    let mut ctrlbreak = ctrl_break().expect("install Ctrl+Break handler");
    let mut ctrlclose = ctrl_close().expect("install Ctrl+Close handler");
    let mut ctrllogoff = ctrl_logoff().expect("install Ctrl+Logoff handler");
    let mut ctrlshutdown = ctrl_shutdown().expect("install Ctrl+Shutdown handler");
    tokio::select! {
        _ = ctrlc.recv() => {
            info!("Received Ctrl+C. Initiating graceful stop...");
        }
        _ = ctrlbreak.recv() => {
            info!("Received Ctrl+Break. Initiating graceful stop...");
        }
        _ = ctrlclose.recv() => {
            info!("Received Ctrl+Close. Initiating graceful stop...");
        }
        _ = ctrllogoff.recv() => {
            info!("Received Ctrl+Logoff. Initiating graceful stop...");
        }
        _ = ctrlshutdown.recv() => {
            info!("Received Ctrl+Shutdown. Initiating graceful stop...");
        }
    }
}
