// Copyright 2026 Tabsdata Inc.

use std::sync::Arc;

use clap::Parser;
use file_rotate::{
    compression::Compression, suffix::AppendCount, ContentLimit, FileRotate,
};
use tokio::sync::watch;
use tracing::info;

use pod_runner::config::cli::CliArgs;
use pod_runner::config::resolve::resolve_config;
use pod_runner::logic::{
    CleanupConfig, DefaultExecutorProvider, ExecutorProvider, PodLogic, ResolverSet,
};
use pod_runner::metrics::Metrics;
use pod_runner::server::app::{AppState, create_router};
use pod_runner::store::file::{FileStore, FileStoreConfig};
use pod_runner::store::memory::MemoryStore;
use pod_runner::store::multiplex::MultiplexStore;
use pod_runner::store::PodStore;

#[tokio::main]
async fn main() {
    // Parse CLI arguments and resolve configuration first so we know
    // the log directory. Logging is initialised below once we've
    // decided where to write.
    let cli = CliArgs::parse();
    let pr_config = resolve_config(&cli).expect("Failed to resolve configuration");

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
        None,
    );
    // Wrap in a Mutex so tracing's MakeWriter can clone a guard per
    // event, and use synchronous writes so log lines are visible
    // immediately (important for startup detection by external
    // supervisors / test harnesses).  Logging volume here is modest
    // (~80 lines/sec at peak load), so the sync overhead is fine.
    let writer = std::sync::Mutex::new(writer);

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_writer(writer)
        .with_ansi(false)
        .init();

    info!(
        log = %log_path.display(),
        max_size_mb = cli.logmaxsizemb,
        max_files = cli.logmaxfiles,
        "Logging initialized (size-based rotation)",
    );

    // Install rustls crypto provider (needed by kube client in resolvers)
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Kill any orphaned processes from a previous run
    let stale_killed = pod_runner::process::executor::cleanup_stale_pids(&pr_config.pid_dir);
    if stale_killed > 0 {
        info!(stale_killed, "Killed stale processes from previous run");
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
    let cleanup_config = CleanupConfig::from(&pr_config);
    let logic = PodLogic::new(
        &pr_config,
        cleanup_config.clone(),
        store,
        provider,
        resolvers,
        shutdown_tx.clone(),
    )
    .with_metrics(Arc::clone(&metrics));

    // Spawn purge daemon for expired work dirs and archives
    let _purge = pod_runner::logic::purge::spawn_purge_daemon(
        cleanup_config,
        shutdown_tx.subscribe(),
    );

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

    let server_config = pod_runner::server::tls::build_server_config(&pr_config)
        .expect("Failed to build TLS config");
    let shutdown = Box::pin(async move {
        wait_for_shutdown_signal().await;
        info!("Shutting down...");
        let _ = shutdown_tx.send(());
    });
    pod_runner::server::tls::axum_serve_tls(
        listener, app, server_config, shutdown,
    )
    .await;

    // Wait for all running pods to finish cleanup before exiting.
    // Timeout is 2x the grace period to allow for SIGTERM + SIGKILL + archive.
    let wait_timeout = std::time::Duration::from_secs(grace_secs * 2 + 10);
    state.logic.wait_for_pods(wait_timeout).await;
    info!("Server stopped");
}

#[cfg(unix)]
async fn wait_for_shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
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
    use tokio::signal::windows::{
        ctrl_break, ctrl_c, ctrl_close, ctrl_logoff, ctrl_shutdown,
    };
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
