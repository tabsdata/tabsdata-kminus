// Copyright 2026 Tabsdata Inc.

//! Probe runner — executes startup, liveness, and readiness probes.
//!
//! # Probe lifecycle
//!
//! Each container's probes run in a dedicated async task managed by a
//! [`JoinSet`]. The probe task loops on a timer, executing the probe
//! action and tracking consecutive successes/failures against thresholds.
//!
//! **Startup probe** gates all other probes. Until it passes (or is
//! absent), liveness and readiness probes do not run.
//!
//! **Liveness probe** failure (threshold exceeded) kills the container.
//!
//! **Readiness probe** success/failure toggles the container's ready
//! state and emits `ContainerReady` events.
//!
//! # Signal propagation
//!
//! Every probe execution is interruptible. The probe loop wraps each
//! `execute_probe` call in a `select!` with the container's stop signal
//! and the global shutdown signal. When interrupted:
//!
//! - **Exec probes**: the spawned process is explicitly killed via
//!   `ExecutorHandle::kill()`. The shutdown receiver is passed to the
//!   process executor so its I/O pump tasks also exit promptly.
//! - **HTTP/TCP probes**: the `tokio::time::timeout` future is dropped,
//!   which drops the TCP connection.
//!
//! # Probe actions
//!
//! - **Exec**: spawns a process via [`ProcessExecutor`], exit code 0 = success.
//!   Output goes to `ProbeLogStream` (appends across runs).
//! - **HTTP GET**: connects to host:port, sends `GET path`, 2xx/3xx = success.
//! - **TCP socket**: connects to host:port, connection success = success.
//!
//! All actions are wrapped in a timeout (`timeout_seconds`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, watch};
use tokio::task::JoinSet;
use tracing::{Instrument, debug, info_span, warn};

use crate::pod::container_log::ProbeLogStream;
use crate::pod::executor::{PodEvent, PodEventKind, PodState};
use crate::pod::lifecycle::ProbeKind;
use crate::process::executor::{DevNullOutput, MergedOutput, ProcessExecutor};
use crate::spec::constants::*;
use crate::spec::pod::{Container, Probe};

/// Map the string probe type (used in ProbeTracking) to the typed
/// [`ProbeKind`] expected by the lifecycle state machine.
fn probe_kind_from_str(s: &str) -> Option<ProbeKind> {
    match s {
        PROBE_STARTUP => Some(ProbeKind::Startup),
        PROBE_LIVENESS => Some(ProbeKind::Liveness),
        PROBE_READINESS => Some(ProbeKind::Readiness),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Probe log defaults
// ---------------------------------------------------------------------------

const PROBE_LOG_MAX_SIZE_BYTES: u64 = 10 * 1024 * 1024; // 10MB
const PROBE_LOG_MAX_FILES: usize = 3;

// ---------------------------------------------------------------------------
// ProbeAction — internal message from probe tasks to orchestrator
// ---------------------------------------------------------------------------

/// Result of a single probe execution, sent from probe tasks to the
/// orchestrator via mpsc channel.
#[derive(Debug)]
pub(crate) enum ProbeAction {
    /// Container readiness changed.
    ReadinessChanged { container: String, ready: bool },
    /// Container should be killed (startup or liveness failure).
    KillContainer { container: String, reason: String },
}

// ---------------------------------------------------------------------------
// ProbeTracking — per-probe local state
// ---------------------------------------------------------------------------

/// Per-probe state not tracked by the lifecycle state machine.
///
/// Consecutive success/failure counts live in the lifecycle's
/// [`crate::pod::lifecycle::ProbePhase`] — query via
/// `cl.probe_phase(kind)`. This struct holds only local flags that
/// gate the probe loop itself.
struct ProbeTracking {
    #[allow(dead_code)] // kept for debugging
    container: String,
    probe_type: String,
    /// Whether the container's startup probe has passed. All other
    /// probes are gated on this. Orthogonal to consecutive counts.
    startup_passed: bool,
    /// Total number of probe executions (success + failure). Used only
    /// to throttle the probe loop to one execution per `period_seconds`
    /// tick — consecutive counts for threshold logic live in the
    /// lifecycle's [`crate::pod::lifecycle::ProbePhase`].
    probes_run: u32,
}

// ---------------------------------------------------------------------------
// spawn_probe_tasks
// ---------------------------------------------------------------------------

/// Spawn probe tasks for all containers into the given `JoinSet`.
///
/// `stop_txs` maps container names to their stop signals. When a
/// container exits, the orchestrator sends `true` on the corresponding
/// stop channel, which causes that container's probe task to exit.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_probe_tasks(
    containers: &[Container],
    join_set: &mut JoinSet<()>,
    action_tx: tokio::sync::mpsc::Sender<ProbeAction>,
    event_tx: tokio::sync::mpsc::Sender<PodEvent>,
    state: Arc<Mutex<PodState>>,
    log_dir: Option<PathBuf>,
    shutdown: watch::Receiver<()>,
    stop_txs: &std::collections::HashMap<String, watch::Sender<bool>>,
) {
    for container in containers {
        let probes: Vec<(String, Probe)> = container
            .probes()
            .into_iter()
            .map(|(name, probe)| (name.to_string(), probe.clone()))
            .collect();

        if probes.is_empty() {
            continue;
        }

        let container_name = container.name.clone();
        let working_dir = PathBuf::from(&container.working_dir);
        let action_tx = action_tx.clone();
        let event_tx = event_tx.clone();
        let state = Arc::clone(&state);
        let log_dir = log_dir.clone();
        let shutdown = shutdown.clone();
        let stop = stop_txs
            .get(&container_name)
            .map(|tx| tx.subscribe())
            .unwrap_or_else(|| watch::channel(false).1);

        let span = info_span!("probes", container = container_name.as_str(),);

        join_set.spawn(
            async move {
                run_container_probes(
                    &container_name,
                    &working_dir,
                    &probes,
                    &action_tx,
                    &event_tx,
                    &state,
                    log_dir.as_deref(),
                    shutdown,
                    stop,
                )
                .await;
            }
            .instrument(span),
        );
    }
}

// ---------------------------------------------------------------------------
// run_container_probes — main probe loop
// ---------------------------------------------------------------------------

/// Run all probes for a single container in a loop until stopped.
///
/// Each probe execution is wrapped in a `select!` with stop/shutdown
/// so it can be interrupted mid-probe. On interruption, exec probe
/// processes are explicitly killed.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
async fn run_container_probes(
    container_name: &str,
    working_dir: &Path,
    probes: &[(String, Probe)],
    action_tx: &tokio::sync::mpsc::Sender<ProbeAction>,
    event_tx: &tokio::sync::mpsc::Sender<PodEvent>,
    state: &Mutex<PodState>,
    log_dir: Option<&std::path::Path>,
    mut shutdown: watch::Receiver<()>,
    mut stop: watch::Receiver<bool>,
) {
    let has_startup = probes.iter().any(|(t, _)| t == PROBE_STARTUP);
    let mut trackers: Vec<ProbeTracking> = probes
        .iter()
        .map(|(probe_type, _)| ProbeTracking {
            container: container_name.to_string(),
            probe_type: probe_type.clone(),
            startup_passed: !has_startup,
            probes_run: 0,
        })
        .collect();

    let started_at = tokio::time::Instant::now();

    loop {
        // Wait for the next tick, or exit on stop/shutdown.
        tokio::select! {
            _ = stop.changed() => {
                debug!("Probe loop stopping (container exited)");
                return;
            }
            _ = shutdown.changed() => {
                debug!("Probe loop stopping (shutdown)");
                return;
            }
            _ = tokio::time::sleep(Duration::from_millis(500)) => {}
        }

        let elapsed = started_at.elapsed();

        // Check startup status once per iteration (avoids borrow conflict)
        let startup_passed = trackers
            .iter()
            .find(|t| t.probe_type == PROBE_STARTUP)
            .map(|t| t.startup_passed)
            .unwrap_or(true);

        for (i, (probe_type, probe)) in probes.iter().enumerate() {
            let tracker = &mut trackers[i];

            if probe_type == PROBE_STARTUP && tracker.startup_passed {
                continue;
            }
            if probe_type != PROBE_STARTUP && !startup_passed {
                continue;
            }

            // Check initial delay
            if elapsed.as_secs() < probe.initial_delay_seconds as u64 {
                continue;
            }

            // Check period
            let period = Duration::from_secs(probe.period_seconds as u64);
            let ticks_elapsed = elapsed.as_millis() / period.as_millis().max(1);
            if tracker.probes_run > 0 && ticks_elapsed as u32 <= tracker.probes_run {
                continue;
            }

            // Create a span for this probe execution. Use debug_span so
            // it appears at debug level, matching the probe log level.
            // The span nests: pod → container → probes → probe{type=X}
            //
            // We use .instrument() on the async execution (not .enter())
            // because .enter() guards don't survive across .await points.
            // The exec probe spawns a ProcessExecutor whose debug_span("process")
            // will be a child of this probe span automatically.
            let probe_span = info_span!("probe", r#type = probe_type.as_str());

            let timeout = Duration::from_secs(probe.timeout_seconds as u64);

            // Execute the probe inside the span. The async block is
            // instrumented so all logs from execute_probe_interruptible
            // (and any child ProcessExecutor) inherit the probe context.
            let probe_result = async {
                execute_probe_interruptible(
                    probe,
                    container_name,
                    working_dir,
                    probe_type,
                    log_dir,
                    timeout,
                    &mut shutdown,
                    &mut stop,
                )
                .await
            }
            .instrument(probe_span.clone())
            .await;

            let success = match probe_result {
                Some(s) => s,
                None => {
                    // Interrupted by stop or shutdown — exit the probe loop.
                    // Use the span for this last log message.
                    let _enter = probe_span.enter();
                    debug!("Probe interrupted by stop/shutdown");
                    return;
                }
            };

            // Threshold evaluation and event emission — enter the span
            // synchronously for these (they have short-lived .await on
            // mutex locks, but the span context is good enough for the
            // log messages that bracket them).
            let _enter = probe_span.enter();

            // Record the probe result in the lifecycle state machine —
            // it is the authoritative source for consecutive success/
            // failure counts. Returns whether the count in the current
            // direction has reached the given threshold.
            let kind = probe_kind_from_str(&tracker.probe_type)
                .expect("probe_type is one of PROBE_STARTUP/LIVENESS/READINESS");
            let threshold = if success {
                probe.success_threshold
            } else {
                probe.failure_threshold
            };
            let threshold_reached = {
                let mut st = state.lock().await;
                match st.lifecycle.container_mut(container_name) {
                    Some(cl) => cl.on_probe_result(kind, success, threshold),
                    // No lifecycle entry — container terminated before
                    // the probe loop received the stop signal. Treat as
                    // threshold not reached to avoid false actions.
                    None => false,
                }
            };
            tracker.probes_run = tracker.probes_run.saturating_add(1);
            if success {
                debug!(threshold, threshold_reached, "Probe passed");
            } else {
                debug!(threshold, threshold_reached, "Probe failed");
            }

            // Emit probe result event
            {
                let st = state.lock().await;
                let snapshot = st.status();
                let _ = event_tx.try_send(PodEvent {
                    kind: PodEventKind::ProbeResult {
                        container: container_name.to_string(),
                        probe_type: probe_type.clone(),
                        success,
                    },
                    status: snapshot,
                });
            }

            // Evaluate thresholds via the lifecycle's response. A
            // `threshold_reached` in the current direction means:
            //   success  → success_threshold hit
            //   !success → failure_threshold hit
            // The _enter guard keeps the probe span active for log
            // messages in the threshold evaluation. It's dropped at the
            // end of the for-loop iteration (or on early return).
            match probe_type.as_str() {
                PROBE_STARTUP => {
                    if success && threshold_reached {
                        debug!("Startup probe passed");
                        tracker.startup_passed = true;
                        for t in trackers.iter_mut() {
                            t.startup_passed = true;
                        }
                    } else if !success && threshold_reached {
                        warn!("Startup probe failed — killing container");
                        let _ = action_tx.try_send(ProbeAction::KillContainer {
                            container: container_name.to_string(),
                            reason: "startup probe failed (threshold reached)".to_string(),
                        });
                        return;
                    }
                }
                PROBE_LIVENESS => {
                    if !success && threshold_reached {
                        warn!("Liveness probe failed — killing container");
                        let _ = action_tx.try_send(ProbeAction::KillContainer {
                            container: container_name.to_string(),
                            reason: "liveness probe failed (threshold reached)".to_string(),
                        });
                        return;
                    }
                }
                PROBE_READINESS => {
                    if success && threshold_reached {
                        let changed = {
                            let mut st = state.lock().await;
                            match st.lifecycle.container_mut(container_name) {
                                Some(cl) => cl.set_ready(true),
                                None => false,
                            }
                        };
                        if changed {
                            debug!("Readiness probe passed — container ready");
                            let _ = action_tx.try_send(ProbeAction::ReadinessChanged {
                                container: container_name.to_string(),
                                ready: true,
                            });
                            let snapshot = {
                                let st = state.lock().await;
                                st.status()
                            };
                            let _ = event_tx.try_send(PodEvent {
                                kind: PodEventKind::ContainerReady {
                                    name: container_name.to_string(),
                                },
                                status: snapshot,
                            });
                        }
                    } else if !success && threshold_reached {
                        let changed = {
                            let mut st = state.lock().await;
                            match st.lifecycle.container_mut(container_name) {
                                Some(cl) => cl.set_ready(false),
                                None => false,
                            }
                        };
                        if changed {
                            warn!("Readiness probe failed — container not ready");
                            let _ = action_tx.try_send(ProbeAction::ReadinessChanged {
                                container: container_name.to_string(),
                                ready: false,
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }
}

// ---------------------------------------------------------------------------
// execute_probe_interruptible — wraps probe execution with stop/shutdown
// ---------------------------------------------------------------------------

/// Execute a probe, interruptible by stop or shutdown signals.
///
/// Returns `Some(success)` on normal completion, `None` if interrupted.
/// On interruption, exec probe processes are explicitly killed.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
async fn execute_probe_interruptible(
    probe: &Probe,
    container_name: &str,
    working_dir: &Path,
    probe_type: &str,
    log_dir: Option<&std::path::Path>,
    timeout: Duration,
    shutdown: &mut watch::Receiver<()>,
    stop: &mut watch::Receiver<bool>,
) -> Option<bool> {
    // For exec probes, we need to be able to kill the spawned process
    // on interruption. We spawn the process first, then select! between
    // waiting for it and the signals.
    if let Some(ref exec_action) = probe.exec_action {
        return execute_exec_probe_interruptible(
            &exec_action.command,
            container_name,
            working_dir,
            probe_type,
            log_dir,
            timeout,
            shutdown,
            stop,
        )
        .await;
    }

    // For HTTP/TCP probes, the future is lightweight (just a TCP connection).
    // Dropping the future on interrupt is sufficient — the connection is closed.
    tokio::select! {
        result = execute_network_probe(probe, timeout) => Some(result),
        _ = stop.changed() => None,
        _ = shutdown.changed() => None,
    }
}

/// Execute a network probe (HTTP or TCP).
async fn execute_network_probe(probe: &Probe, timeout: Duration) -> bool {
    if let Some(ref http_get) = probe.http_get {
        return execute_http_probe(http_get, timeout).await;
    }
    if let Some(ref tcp_socket) = probe.tcp_socket {
        return execute_tcp_probe(tcp_socket, timeout).await;
    }
    // No action configured
    debug!("No probe action configured, returning success");
    true
}

// ---------------------------------------------------------------------------
// Exec probe — with explicit kill on interruption
// ---------------------------------------------------------------------------

/// Execute an exec probe with stop/shutdown interruption support.
///
/// Spawns the process, then select!s between:
/// - process completion (normal path)
/// - timeout (kills process)
/// - stop/shutdown signal (kills process immediately)
///
/// The shutdown receiver is passed to the process executor so its
/// I/O pump tasks also exit promptly on shutdown.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
async fn execute_exec_probe_interruptible(
    command: &[String],
    container_name: &str,
    working_dir: &Path,
    probe_type: &str,
    log_dir: Option<&std::path::Path>,
    timeout: Duration,
    shutdown: &mut watch::Receiver<()>,
    stop: &mut watch::Receiver<bool>,
) -> Option<bool> {
    if command.is_empty() {
        warn!("Exec probe has empty command");
        return Some(false);
    }

    let mut builder = ProcessExecutor::new(&command[0]);
    if command.len() > 1 {
        builder = builder.args(&command[1..]);
    }
    if !working_dir.as_os_str().is_empty() {
        builder = builder.work_dir(working_dir);
    }

    if let Some(dir) = log_dir {
        builder = builder.output(MergedOutput::new(ProbeLogStream::new(
            dir,
            container_name,
            probe_type,
            PROBE_LOG_MAX_SIZE_BYTES,
            PROBE_LOG_MAX_FILES,
        )));
    } else {
        builder = builder.output(DevNullOutput);
    }

    // Pass the real shutdown receiver so the process's I/O pump exits
    // on shutdown (not a leaked sender that never fires).
    let mut handle = match builder.spawn(shutdown.clone()).await {
        Ok(h) => h,
        Err(e) => {
            warn!("Failed to spawn exec probe: {}", e);
            return Some(false);
        }
    };

    // Wait for process exit, timeout, or stop/shutdown.
    // On any interruption, explicitly kill the probe process.
    tokio::select! {
        result = tokio::time::timeout(timeout, handle.wait()) => {
            match result {
                Ok(Ok(code)) => {
                    debug!(exit_code = code, "Exec probe completed");
                    Some(code == 0)
                }
                Ok(Err(e)) => {
                    warn!("Exec probe wait error: {}", e);
                    Some(false)
                }
                Err(_) => {
                    warn!("Exec probe timed out after {:?} — killing", timeout);
                    handle.kill(Duration::ZERO).await;
                    Some(false)
                }
            }
        }
        _ = stop.changed() => {
            debug!("Exec probe interrupted (container exited) — killing probe process");
            handle.kill(Duration::ZERO).await;
            None
        }
        _ = shutdown.changed() => {
            debug!("Exec probe interrupted (shutdown) — killing probe process");
            handle.kill(Duration::ZERO).await;
            None
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP probe
// ---------------------------------------------------------------------------

/// Execute an HTTP GET probe: connect, send GET, 2xx/3xx = success.
async fn execute_http_probe(http_get: &crate::spec::pod::HttpGetAction, timeout: Duration) -> bool {
    let host = if http_get.host.is_empty() {
        "127.0.0.1"
    } else {
        &http_get.host
    };
    let port = http_get.port.unwrap_or(80);
    let path = if http_get.path.is_empty() {
        "/"
    } else {
        &http_get.path
    };
    let addr = format!("{}:{}", host, port);

    match tokio::time::timeout(timeout, async {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(&addr).await?;
        let request = format!("GET {} HTTP/1.0\r\nHost: {}\r\n\r\n", path, host);
        stream.write_all(request.as_bytes()).await?;
        let mut buf = vec![0u8; 256];
        let n = stream.read(&mut buf).await?;
        let response = String::from_utf8_lossy(&buf[..n]);
        let code: u16 = response
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        Ok::<u16, std::io::Error>(code)
    })
    .await
    {
        Ok(Ok(code)) => {
            let success = (200..400).contains(&code);
            debug!(status_code = code, success, "HTTP probe completed");
            success
        }
        Ok(Err(e)) => {
            debug!("HTTP probe connection failed: {}", e);
            false
        }
        Err(_) => {
            debug!("HTTP probe timed out after {:?}", timeout);
            false
        }
    }
}

// ---------------------------------------------------------------------------
// TCP probe
// ---------------------------------------------------------------------------

/// Execute a TCP socket probe: connection success = success.
async fn execute_tcp_probe(
    tcp_socket: &crate::spec::pod::TcpSocketAction,
    timeout: Duration,
) -> bool {
    let host = if tcp_socket.host.is_empty() {
        "127.0.0.1"
    } else {
        &tcp_socket.host
    };
    let port = tcp_socket.port.unwrap_or(0);
    let addr = format!("{}:{}", host, port);

    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect(&addr)).await {
        Ok(Ok(_)) => {
            debug!("TCP probe connected to {}", addr);
            true
        }
        Ok(Err(e)) => {
            debug!("TCP probe connection failed: {}", e);
            false
        }
        Err(_) => {
            debug!("TCP probe timed out after {:?}", timeout);
            false
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::{ExecAction, HttpGetAction, TcpSocketAction};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    // -- helpers -------------------------------------------------------------

    fn shutdown_pair() -> (watch::Sender<()>, watch::Receiver<()>) {
        watch::channel(())
    }

    fn stop_pair() -> (watch::Sender<bool>, watch::Receiver<bool>) {
        watch::channel(false)
    }

    /// Bind a listener on an ephemeral port and return (listener, port).
    async fn bind_ephemeral() -> (TcpListener, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        (listener, port)
    }

    /// Accept one connection, read the request, write `response`, close.
    fn spawn_http_once(
        listener: TcpListener,
        response: &'static [u8],
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let _ = sock.write_all(response).await;
            let _ = sock.shutdown().await;
        })
    }

    // -- probe_kind_from_str -------------------------------------------------

    #[test]
    fn probe_kind_from_str_startup() {
        assert!(matches!(
            probe_kind_from_str(PROBE_STARTUP),
            Some(ProbeKind::Startup)
        ));
    }

    #[test]
    fn probe_kind_from_str_liveness() {
        assert!(matches!(
            probe_kind_from_str(PROBE_LIVENESS),
            Some(ProbeKind::Liveness)
        ));
    }

    #[test]
    fn probe_kind_from_str_readiness() {
        assert!(matches!(
            probe_kind_from_str(PROBE_READINESS),
            Some(ProbeKind::Readiness)
        ));
    }

    #[test]
    fn probe_kind_from_str_unknown() {
        assert!(probe_kind_from_str("bogus").is_none());
        assert!(probe_kind_from_str("").is_none());
    }

    // -- execute_tcp_probe ---------------------------------------------------

    #[tokio::test]
    async fn tcp_probe_connects_to_listening_port() {
        let (listener, port) = bind_ephemeral().await;
        // Keep the listener alive and accepting for the duration of the probe.
        let accept = tokio::spawn(async move { listener.accept().await });

        let action = TcpSocketAction {
            port: Some(port),
            host: "127.0.0.1".to_string(),
        };
        assert!(execute_tcp_probe(&action, Duration::from_secs(1)).await);
        accept.abort();
    }

    #[tokio::test]
    async fn tcp_probe_defaults_host_to_loopback_when_empty() {
        let (listener, port) = bind_ephemeral().await;
        let accept = tokio::spawn(async move { listener.accept().await });

        // Empty host → code uses 127.0.0.1.
        let action = TcpSocketAction {
            port: Some(port),
            host: String::new(),
        };
        assert!(execute_tcp_probe(&action, Duration::from_secs(1)).await);
        accept.abort();
    }

    #[tokio::test]
    async fn tcp_probe_fails_on_closed_port() {
        // Bind then drop to get an almost-certainly-free port.
        let (listener, port) = bind_ephemeral().await;
        drop(listener);

        let action = TcpSocketAction {
            port: Some(port),
            host: "127.0.0.1".to_string(),
        };
        assert!(!execute_tcp_probe(&action, Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn tcp_probe_times_out() {
        // Connecting to a blackhole IP: 10.255.255.1 is typically unroutable
        // locally, but behavior varies. Use a non-routable TEST-NET address
        // and a very short timeout. If the OS returns an immediate error
        // instead of hanging, the probe still returns false.
        let action = TcpSocketAction {
            port: Some(1),
            host: "192.0.2.1".to_string(), // TEST-NET-1 (RFC 5737)
        };
        let start = tokio::time::Instant::now();
        let ok = execute_tcp_probe(&action, Duration::from_millis(100)).await;
        // Either timed out (< ~200ms) or got an immediate error — both must be false.
        assert!(!ok);
        assert!(start.elapsed() < Duration::from_secs(2));
    }

    // -- execute_http_probe --------------------------------------------------

    #[tokio::test]
    async fn http_probe_2xx_is_success() {
        let (listener, port) = bind_ephemeral().await;
        let server = spawn_http_once(listener, b"HTTP/1.0 200 OK\r\n\r\nbody");

        let action = HttpGetAction {
            path: "/".to_string(),
            port: Some(port),
            host: "127.0.0.1".to_string(),
        };
        assert!(execute_http_probe(&action, Duration::from_secs(1)).await);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_3xx_is_success() {
        let (listener, port) = bind_ephemeral().await;
        let server = spawn_http_once(
            listener,
            b"HTTP/1.0 301 Moved Permanently\r\nLocation: /new\r\n\r\n",
        );

        let action = HttpGetAction {
            path: "/old".to_string(),
            port: Some(port),
            host: String::new(),
        };
        assert!(execute_http_probe(&action, Duration::from_secs(1)).await);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_4xx_is_failure() {
        let (listener, port) = bind_ephemeral().await;
        let server = spawn_http_once(listener, b"HTTP/1.0 404 Not Found\r\n\r\n");

        let action = HttpGetAction {
            path: "/".to_string(),
            port: Some(port),
            host: String::new(),
        };
        assert!(!execute_http_probe(&action, Duration::from_secs(1)).await);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_5xx_is_failure() {
        let (listener, port) = bind_ephemeral().await;
        let server = spawn_http_once(listener, b"HTTP/1.0 500 Internal Server Error\r\n\r\n");

        let action = HttpGetAction {
            path: "/".to_string(),
            port: Some(port),
            host: String::new(),
        };
        assert!(!execute_http_probe(&action, Duration::from_secs(1)).await);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn http_probe_connection_refused_is_failure() {
        let (listener, port) = bind_ephemeral().await;
        drop(listener);

        let action = HttpGetAction {
            path: "/".to_string(),
            port: Some(port),
            host: "127.0.0.1".to_string(),
        };
        assert!(!execute_http_probe(&action, Duration::from_secs(1)).await);
    }

    #[tokio::test]
    async fn http_probe_times_out_when_server_never_responds() {
        let (listener, port) = bind_ephemeral().await;
        // Accept but never write a response. Keep the socket alive so the
        // reader actually waits for data rather than seeing EOF immediately.
        let held = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            // Park for longer than the probe timeout.
            tokio::time::sleep(Duration::from_secs(2)).await;
            drop(sock);
        });

        let action = HttpGetAction {
            path: "/".to_string(),
            port: Some(port),
            host: "127.0.0.1".to_string(),
        };
        let start = tokio::time::Instant::now();
        let ok = execute_http_probe(&action, Duration::from_millis(200)).await;
        assert!(!ok);
        assert!(start.elapsed() < Duration::from_secs(1));
        held.abort();
    }

    #[tokio::test]
    async fn http_probe_defaults_path_to_slash_when_empty() {
        let (listener, port) = bind_ephemeral().await;
        let got_request = Arc::new(Mutex::new(Vec::<u8>::new()));
        let got_request_clone = Arc::clone(&got_request);
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0u8; 1024];
            let n = sock.read(&mut buf).await.unwrap();
            got_request_clone.lock().await.extend_from_slice(&buf[..n]);
            let _ = sock.write_all(b"HTTP/1.0 200 OK\r\n\r\n").await;
        });

        let action = HttpGetAction {
            path: String::new(),
            port: Some(port),
            host: String::new(),
        };
        assert!(execute_http_probe(&action, Duration::from_secs(1)).await);
        server.await.unwrap();

        let req = got_request.lock().await;
        let req_str = String::from_utf8_lossy(&req);
        // Empty path must be rewritten to "/".
        assert!(req_str.starts_with("GET / HTTP/1.0"), "got: {:?}", req_str);
    }

    #[tokio::test]
    async fn http_probe_malformed_response_is_failure() {
        let (listener, port) = bind_ephemeral().await;
        let server = spawn_http_once(listener, b"garbage not-an-http-response\r\n\r\n");

        let action = HttpGetAction {
            path: "/".to_string(),
            port: Some(port),
            host: "127.0.0.1".to_string(),
        };
        assert!(!execute_http_probe(&action, Duration::from_secs(1)).await);
        server.await.unwrap();
    }

    // -- execute_network_probe -----------------------------------------------

    #[tokio::test]
    async fn network_probe_routes_to_http() {
        let (listener, port) = bind_ephemeral().await;
        let server = spawn_http_once(listener, b"HTTP/1.0 200 OK\r\n\r\n");

        let probe = Probe {
            http_get: Some(HttpGetAction {
                path: "/".to_string(),
                port: Some(port),
                host: "127.0.0.1".to_string(),
            }),
            ..Default::default()
        };
        assert!(execute_network_probe(&probe, Duration::from_secs(1)).await);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn network_probe_routes_to_tcp() {
        let (listener, port) = bind_ephemeral().await;
        let accept = tokio::spawn(async move { listener.accept().await });

        let probe = Probe {
            tcp_socket: Some(TcpSocketAction {
                port: Some(port),
                host: "127.0.0.1".to_string(),
            }),
            ..Default::default()
        };
        assert!(execute_network_probe(&probe, Duration::from_secs(1)).await);
        accept.abort();
    }

    #[tokio::test]
    async fn network_probe_no_action_returns_success() {
        let probe = Probe::default();
        assert!(execute_network_probe(&probe, Duration::from_secs(1)).await);
    }

    // -- execute_exec_probe_interruptible ------------------------------------
    //
    // Exec probes shell out to `sh`, so the whole block is Unix-only.

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_exit_zero_is_success() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let result = execute_exec_probe_interruptible(
            &["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_secs(5),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(true));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_exit_nonzero_is_failure() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let result = execute_exec_probe_interruptible(
            &["sh".to_string(), "-c".to_string(), "exit 1".to_string()],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_secs(5),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(false));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_empty_command_is_failure() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let result = execute_exec_probe_interruptible(
            &[],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_secs(1),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(false));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_spawn_failure_is_failure() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        // A command that doesn't exist anywhere in PATH or at the given path.
        let result = execute_exec_probe_interruptible(
            &["/no/such/binary-xyz-12345".to_string()],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_secs(1),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(false));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_timeout_kills_and_fails() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let start = tokio::time::Instant::now();
        let result = execute_exec_probe_interruptible(
            &["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_millis(200),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        let elapsed = start.elapsed();
        assert_eq!(result, Some(false));
        assert!(
            elapsed < Duration::from_secs(3),
            "timeout did not fire promptly: {:?}",
            elapsed
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_stop_signal_returns_none() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (stop_tx, mut stop_rx) = stop_pair();

        // Fire the stop signal shortly after the probe starts.
        let signal = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = stop_tx.send(true);
        });

        let start = tokio::time::Instant::now();
        let result = execute_exec_probe_interruptible(
            &["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_secs(30),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        let elapsed = start.elapsed();
        signal.await.unwrap();
        assert!(
            result.is_none(),
            "expected None on stop interrupt, got {:?}",
            result
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "stop did not interrupt promptly: {:?}",
            elapsed
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_probe_shutdown_signal_returns_none() {
        let (sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let signal = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = sh_tx.send(());
        });

        let start = tokio::time::Instant::now();
        let result = execute_exec_probe_interruptible(
            &["sh".to_string(), "-c".to_string(), "sleep 10".to_string()],
            "c",
            Path::new(""),
            PROBE_LIVENESS,
            None,
            Duration::from_secs(30),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        let elapsed = start.elapsed();
        signal.await.unwrap();
        assert!(
            result.is_none(),
            "expected None on shutdown interrupt, got {:?}",
            result
        );
        assert!(
            elapsed < Duration::from_secs(3),
            "shutdown did not interrupt promptly: {:?}",
            elapsed
        );
    }

    // -- execute_probe_interruptible -----------------------------------------

    #[cfg(unix)]
    #[tokio::test]
    async fn interruptible_dispatches_to_exec() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let probe = Probe {
            exec_action: Some(ExecAction {
                command: vec!["sh".to_string(), "-c".to_string(), "exit 0".to_string()],
            }),
            ..Default::default()
        };
        let result = execute_probe_interruptible(
            &probe,
            "c",
            Path::new(""),
            PROBE_READINESS,
            None,
            Duration::from_secs(5),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(true));
    }

    #[tokio::test]
    async fn interruptible_dispatches_to_network_tcp() {
        let (listener, port) = bind_ephemeral().await;
        let accept = tokio::spawn(async move { listener.accept().await });

        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let probe = Probe {
            tcp_socket: Some(TcpSocketAction {
                port: Some(port),
                host: "127.0.0.1".to_string(),
            }),
            ..Default::default()
        };
        let result = execute_probe_interruptible(
            &probe,
            "c",
            Path::new(""),
            PROBE_READINESS,
            None,
            Duration::from_secs(1),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(true));
        accept.abort();
    }

    #[tokio::test]
    async fn interruptible_network_stop_returns_none() {
        // Connect to a blackhole TEST-NET address so the future hangs long
        // enough for the stop signal to win the select!.
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (stop_tx, mut stop_rx) = stop_pair();

        let signal = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = stop_tx.send(true);
        });

        let probe = Probe {
            tcp_socket: Some(TcpSocketAction {
                port: Some(1),
                host: "192.0.2.1".to_string(),
            }),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        let result = execute_probe_interruptible(
            &probe,
            "c",
            Path::new(""),
            PROBE_READINESS,
            None,
            Duration::from_secs(30),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        let elapsed = start.elapsed();
        signal.await.unwrap();
        assert!(
            result.is_none(),
            "expected None on stop interrupt, got {:?}",
            result
        );
        assert!(elapsed < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn interruptible_network_shutdown_returns_none() {
        let (sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let signal = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = sh_tx.send(());
        });

        let probe = Probe {
            tcp_socket: Some(TcpSocketAction {
                port: Some(1),
                host: "192.0.2.1".to_string(),
            }),
            ..Default::default()
        };
        let start = tokio::time::Instant::now();
        let result = execute_probe_interruptible(
            &probe,
            "c",
            Path::new(""),
            PROBE_READINESS,
            None,
            Duration::from_secs(30),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        let elapsed = start.elapsed();
        signal.await.unwrap();
        assert!(result.is_none());
        assert!(elapsed < Duration::from_secs(3));
    }

    #[tokio::test]
    async fn interruptible_no_action_returns_success() {
        let (_sh_tx, mut shutdown_rx) = shutdown_pair();
        let (_st_tx, mut stop_rx) = stop_pair();

        let probe = Probe::default(); // no exec, http, or tcp
        let result = execute_probe_interruptible(
            &probe,
            "c",
            Path::new(""),
            PROBE_READINESS,
            None,
            Duration::from_secs(1),
            &mut shutdown_rx,
            &mut stop_rx,
        )
        .await;
        assert_eq!(result, Some(true));
    }
}
