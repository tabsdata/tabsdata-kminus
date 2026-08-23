// Copyright 2026 Tabsdata Inc.

//! Readiness heartbeat: prove the server is serving, then publish that.
//!
//! Once per interval this task calls the pod-runner's own `/capacity` over
//! mTLS — byte for byte the request the virtual-kubelet's node controller
//! makes — and rewrites the heartbeat file with the outcome. km gates the
//! virtual-kubelet's start on the first `ready` beat and treats a stale
//! file as a dead pod-runner, so nothing is ever started against a
//! listener that is bound but not yet serving.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::client::config::VKPRConfig;
use crate::client::http::build_client;

/// Written once the self-probe succeeds; the only status km accepts.
const STATUS_READY: &str = "ready";

/// Written when the self-probe fails, with the cause in `detail`.
const STATUS_DEGRADED: &str = "degraded";

/// Endpoint the self-probe calls.
const ENDPOINT_CAPACITY: &str = "/capacity";

/// Per-probe timeout. Kept well under any sane interval so a wedged
/// server produces a failed beat rather than a skipped one.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

/// Suffix of the staging file a beat is written to before being renamed
/// into place. Matches the other components' `.tmp`.
const STAGING_SUFFIX: &str = ".tmp";

/// Retry spacing until the first success: the server is still coming up
/// and km's readiness gate is waiting on that first beat, so retry fast
/// instead of making every startup wait a full interval.
const STARTUP_RETRY: Duration = Duration::from_millis(250);

/// Spacing before the next self-probe.
fn probe_wait(ready_once: bool, interval: Duration) -> Duration {
    if ready_once { interval } else { STARTUP_RETRY }
}

/// What to publish after one self-probe attempt: the status, and the detail
/// that explains a failure. Split from the loop so the mapping is testable
/// without a server to probe.
fn beat_for(outcome: &Result<(), String>) -> (&'static str, &str) {
    match outcome {
        Ok(()) => (STATUS_READY, ""),
        Err(reason) => (STATUS_DEGRADED, reason.as_str()),
    }
}

/// Self-probe `/capacity` every `interval` and publish the result to
/// `file` until `shutdown` fires.
pub async fn run(
    file: PathBuf,
    interval: Duration,
    vk_prconfig: PathBuf,
    mut shutdown: watch::Receiver<()>,
) {
    info!(
        file = %file.display(),
        interval_secs = interval.as_secs(),
        endpoint = ENDPOINT_CAPACITY,
        "Readiness heartbeat started"
    );

    let mut client: Option<(reqwest::Client, String)> = None;
    let mut ready_once = false;

    loop {
        let outcome = tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            outcome = probe(&mut client, &vk_prconfig) => outcome,
        };

        let (status, detail) = beat_for(&outcome);
        match &outcome {
            Ok(()) => {
                if !ready_once {
                    info!("Self-probe succeeded; reporting ready");
                    ready_once = true;
                } else {
                    debug!("Self-probe succeeded");
                }
            }
            // Before the first success this is just startup; after it, the
            // server has stopped serving and km needs to know.
            Err(e) if ready_once => warn!("Self-probe failed, reporting degraded: {}", e),
            Err(e) => debug!("Self-probe not ready yet: {}", e),
        }
        write(&file, status, detail);

        let wait = probe_wait(ready_once, interval);
        tokio::select! {
            biased;
            _ = shutdown.changed() => break,
            _ = tokio::time::sleep(wait) => {}
        }
    }

    info!("Readiness heartbeat stopped");
}

/// One `/capacity` call, building the client on first use so a config not
/// yet readable at startup is retried rather than disabling the heartbeat.
async fn probe(
    client: &mut Option<(reqwest::Client, String)>,
    vk_prconfig: &Path,
) -> Result<(), String> {
    if client.is_none() {
        *client = Some(load_client(vk_prconfig)?);
    }
    let (http, url) = client.as_ref().expect("client built above");

    let response = http
        .get(format!("{url}{ENDPOINT_CAPACITY}"))
        .timeout(PROBE_TIMEOUT)
        .send()
        .await
        .map_err(|e| format!("GET {url}{ENDPOINT_CAPACITY} failed: {e}"))?;

    if response.status().is_success() {
        Ok(())
    } else {
        Err(format!(
            "{ENDPOINT_CAPACITY} returned {}",
            response.status()
        ))
    }
}

/// Build the mTLS client and base URL from the config the pod-runner
/// generates for the virtual-kubelet — the same credentials the caller
/// being simulated would use.
fn load_client(vk_prconfig: &Path) -> Result<(reqwest::Client, String), String> {
    let config = VKPRConfig::from_file(vk_prconfig)
        .map_err(|e| format!("cannot read {}: {e}", vk_prconfig.display()))?;
    let url = config.url.trim_end_matches('/').to_string();
    let http = build_client(&Some(config))?;
    Ok((http, url))
}

/// Publish one beat. Write failures are logged, never fatal: km reads the
/// resulting staleness and decides what to do about it.
fn write(file: &Path, status: &str, detail: &str) {
    if let Some(parent) = file.parent() {
        if let Err(e) = std::fs::create_dir_all(parent) {
            warn!(dir = %parent.display(), "Cannot create heartbeat directory: {}", e);
            return;
        }
    }
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default();
    let escaped = detail.replace('\\', "\\\\").replace('"', "\\\"");
    let body = format!(r#"{{"ts":{ts},"status":"{status}","detail":"{escaped}"}}"#);

    // Write then rename, so km never reads a half-written beat and counts it
    // as a miss. Rename replaces the target on Windows as well as POSIX.
    // Same directory, so the rename stays within one volume.
    let staging = match file.file_name() {
        Some(name) => {
            let mut staged = name.to_os_string();
            staged.push(STAGING_SUFFIX);
            file.with_file_name(staged)
        }
        None => {
            warn!(file = %file.display(), "Heartbeat path has no file name");
            return;
        }
    };
    if let Err(e) = std::fs::write(&staging, body) {
        warn!(file = %staging.display(), "Cannot write heartbeat: {}", e);
        return;
    }
    if let Err(e) = std::fs::rename(&staging, file) {
        warn!(file = %file.display(), "Cannot publish heartbeat: {}", e);
        let _ = std::fs::remove_file(&staging);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("pr-hb-{}-{}", std::process::id(), n))
    }

    #[test]
    fn write_creates_the_parent_directory() {
        let dir = tempdir();
        let file = dir.join("nested").join("heartbeat.json");
        write(&file, STATUS_READY, "");
        assert!(file.is_file());
        let body = std::fs::read_to_string(&file).unwrap();
        assert!(body.contains(r#""status":"ready""#));
        assert!(body.contains(r#""ts":"#));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_escapes_the_detail_so_the_json_stays_valid() {
        let dir = tempdir();
        let file = dir.join("heartbeat.json");
        write(&file, STATUS_DEGRADED, r#"GET "https://x" failed: c:\path"#);
        let body = std::fs::read_to_string(&file).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], STATUS_DEGRADED);
        assert_eq!(parsed["detail"], r#"GET "https://x" failed: c:\path"#);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_leaves_no_staging_file_behind() {
        let dir = tempdir();
        let file = dir.join("heartbeat.json");
        write(&file, STATUS_READY, "");
        assert!(file.is_file());
        assert!(
            !dir.join("heartbeat.json.tmp").exists(),
            "staging file must be renamed into place, not left behind"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn write_replaces_an_existing_beat() {
        let dir = tempdir();
        let file = dir.join("heartbeat.json");
        write(&file, STATUS_DEGRADED, "starting");
        write(&file, STATUS_READY, "");
        let body = std::fs::read_to_string(&file).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], STATUS_READY);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn beat_for_maps_the_outcome_to_a_status_and_reason() {
        assert_eq!(beat_for(&Ok(())), (STATUS_READY, ""));
        let failed = Err("/capacity returned 503".to_string());
        assert_eq!(
            beat_for(&failed),
            (STATUS_DEGRADED, "/capacity returned 503")
        );
    }

    #[test]
    fn probe_wait_is_fast_until_the_first_success() {
        let interval = Duration::from_secs(15);
        // km's readiness gate waits on the first beat, so it must not be an
        // interval away.
        assert_eq!(probe_wait(false, interval), STARTUP_RETRY);
        assert_eq!(probe_wait(true, interval), interval);
    }

    #[tokio::test]
    async fn probe_reports_a_missing_config_instead_of_panicking() {
        let mut client = None;
        let err = probe(&mut client, Path::new("/nonexistent/vk-prconfig.yaml"))
            .await
            .unwrap_err();
        assert!(err.contains("cannot read"));
        assert!(client.is_none());
    }
}
