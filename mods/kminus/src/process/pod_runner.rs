// Copyright 2026 Tabsdata Inc.

//! Pod-runner subprocess: startup, readiness, status gathering.

use std::fs;
use std::path::Path;
use std::process::{Child, Command, Stdio};

use tracing::info;

use crate::config::KmSysConfig;
use crate::loglevel::LogLevel;
use crate::process::control_plane;
use crate::system::heartbeat::{self, heartbeat_file};
use crate::system::pid::{apply_subprocess_flags, kill_child, remove_pid, write_pid};
use crate::system::proc::resolve_binary;

/// Binary name for the pod-runner subprocess.
pub const BIN: &str = "pod-runner";

/// Filename of the pod-runner's auto-generated server config (TLS
/// certs, listen address). Regenerated on every startup.
pub const PR_CONFIG_FILE: &str = "prconfig.yaml";

/// Filename of the client config that pod-runner generates for the
/// virtual-kubelet (mTLS materials + URL). Regenerated on startup.
pub const VK_PR_CONFIG_FILE: &str = "vk-prconfig.yaml";

/// Filename of the user-editable pod-runner system config (capacity,
/// resolvers, etc.). Auto-created with defaults on first startup;
/// preserved across restarts.
pub const PR_SYSCONFIG_FILE: &str = "prsysconfig.yaml";

// YAML keys (with trailing `:`) inside `prsysconfig.yaml` that the
// status banner parses by line-prefix matching. Kept as constants so
// any rename in the pod-runner YAML schema is reflected here.
const CAPACITY_BLOCK_KEY: &str = "capacity:";
const CAPACITY_CPU_KEY: &str = "cpu:";
const CAPACITY_MEMORY_GB_KEY: &str = "memoryGB:";
const CAPACITY_PODS_KEY: &str = "pods:";

/// Spawn the pod-runner and wait until it reports itself ready in its
/// heartbeat file — which it writes only after its own `/capacity` call
/// succeeds, so the listener is proven to be serving before the
/// virtual-kubelet is started against it. On failure, kills the
/// already-running control-plane and exits.
///
/// When `imagesdir` is `Some`, it is forwarded to the pod-runner as
/// `--imagesdir <value>`. Otherwise the pod-runner resolves its own
/// default (`imagesDir` from `prsysconfig.yaml`, then `./images`).
#[allow(clippy::too_many_arguments)]
pub fn start(
    workdir: &Path,
    configdir: &Path,
    logdir: &Path,
    pids: &Path,
    sys_cfg: &KmSysConfig,
    port_pr: &str,
    port_pr_metrics: &str,
    kubeconfig: &Path,
    imagesdir: Option<&str>,
    loglevel: LogLevel,
    cp: &mut Child,
) -> Child {
    info!("Starting {}...", BIN);
    let pr_bin = resolve_binary(BIN);
    let log_max_size = sys_cfg.log.max_size_mb.to_string();
    let log_max_files = sys_cfg.log.max_files.to_string();
    let loglevel = loglevel.to_string();
    let heartbeat = heartbeat_file(workdir, BIN);
    let heartbeat_secs = sys_cfg.heartbeat.interval().as_secs().to_string();
    heartbeat::clear(&heartbeat);
    let mut command = Command::new(&pr_bin);
    command
        .args([
            "--kubeconfig",
            kubeconfig.to_str().unwrap(),
            "--configdir",
            configdir.to_str().unwrap(),
            "--workdir",
            workdir.to_str().unwrap(),
            "--logdir",
            logdir.to_str().unwrap(),
            "--logmaxsizemb",
            &log_max_size,
            "--logmaxfiles",
            &log_max_files,
            "--loglevel",
            &loglevel,
            "--port",
            port_pr,
            "--metrics-port",
            port_pr_metrics,
            "--heartbeat-file",
            heartbeat.to_str().unwrap(),
            "--heartbeat-secs",
            &heartbeat_secs,
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(dir) = imagesdir {
        command.args(["--imagesdir", dir]);
    }
    apply_subprocess_flags(&mut command);
    let mut pr = command
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to start {}: {}", BIN, e));

    write_pid(pids, BIN, pr.id());

    if !heartbeat::wait_until_ready(BIN, &heartbeat, sys_cfg.heartbeat.startup_timeout()) {
        // Claimed before the children are killed below, so the process
        // watcher does not report those kills as a liveness failure.
        if !crate::exit::claim() {
            std::process::exit(1);
        }
        crate::exit::report(&crate::exit::ExitReason::Readiness {
            component: BIN,
            detail: format!(
                "did not report ready within {}s ({})",
                sys_cfg.heartbeat.startup_timeout().as_secs(),
                heartbeat.display()
            ),
        });
        kill_child(&mut pr, BIN);
        kill_child(cp, control_plane::BIN);
        remove_pid(pids, BIN);
        remove_pid(pids, control_plane::BIN);
        std::process::exit(1);
    }
    info!(pid = pr.id(), "{} ready", BIN);
    pr
}

/// Read the pod-runner's reported capacity (cpu, memoryGB, pods)
/// from `prsysconfig.yaml`. Any field that cannot be read is
/// returned as "?".
pub fn read_capacity(configdir: &Path) -> (String, String, String) {
    let syscfg_path = configdir.join(PR_SYSCONFIG_FILE);
    let content = match fs::read_to_string(&syscfg_path) {
        Ok(c) => c,
        Err(_) => return ("?".into(), "?".into(), "?".into()),
    };
    let mut cpu = "?".to_string();
    let mut mem = "?".to_string();
    let mut pods = "?".to_string();
    let mut in_capacity = false;
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed == CAPACITY_BLOCK_KEY {
            in_capacity = true;
            continue;
        }
        if in_capacity {
            if !trimmed.starts_with(CAPACITY_CPU_KEY)
                && !trimmed.starts_with(CAPACITY_MEMORY_GB_KEY)
                && !trimmed.starts_with(CAPACITY_PODS_KEY)
                && !trimmed.is_empty()
            {
                in_capacity = false;
                continue;
            }
            if let Some(v) = trimmed.strip_prefix(CAPACITY_CPU_KEY) {
                cpu = v.trim().to_string();
            }
            if let Some(v) = trimmed.strip_prefix(CAPACITY_MEMORY_GB_KEY) {
                mem = v.trim().to_string();
            }
            if let Some(v) = trimmed.strip_prefix(CAPACITY_PODS_KEY) {
                pods = v.trim().to_string();
            }
        }
    }
    (cpu, mem, pods)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::path::PathBuf;

    fn tempdir() -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = env::temp_dir().join(format!("km-pr-{}-{}", std::process::id(), n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn read_capacity_returns_unknowns_when_file_missing() {
        let dir = tempdir();
        let (cpu, mem, pods) = read_capacity(&dir);
        assert_eq!(cpu, "?");
        assert_eq!(mem, "?");
        assert_eq!(pods, "?");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_capacity_parses_capacity_block() {
        let dir = tempdir();
        let yaml = "nodename: foo\n\
                    capacity:\n  cpu: 100\n  memoryGB: 64\n  pods: 250\n\
                    metricsUrl: \"\"\n";
        fs::write(dir.join(PR_SYSCONFIG_FILE), yaml).unwrap();
        let (cpu, mem, pods) = read_capacity(&dir);
        assert_eq!(cpu, "100");
        assert_eq!(mem, "64");
        assert_eq!(pods, "250");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_capacity_partial_block() {
        let dir = tempdir();
        let yaml = "capacity:\n  pods: 999\n";
        fs::write(dir.join(PR_SYSCONFIG_FILE), yaml).unwrap();
        let (cpu, mem, pods) = read_capacity(&dir);
        assert_eq!(cpu, "?");
        assert_eq!(mem, "?");
        assert_eq!(pods, "999");
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn read_capacity_stops_at_next_block() {
        // Sibling top-level blocks must not contaminate capacity.
        let dir = tempdir();
        let yaml = "capacity:\n  cpu: 4\n  memoryGB: 8\n  pods: 100\n\
                    metricsUrl: \"http://x\"\n";
        fs::write(dir.join(PR_SYSCONFIG_FILE), yaml).unwrap();
        let (cpu, mem, pods) = read_capacity(&dir);
        assert_eq!(cpu, "4");
        assert_eq!(mem, "8");
        assert_eq!(pods, "100");
        fs::remove_dir_all(&dir).ok();
    }
}
