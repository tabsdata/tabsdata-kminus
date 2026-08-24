// Copyright 2026 Tabsdata Inc.

//! Virtual-kubelet subprocess: startup, readiness, status gathering.

use std::path::{Path, PathBuf};
use std::process::Command;

use tracing::info;

use kcommon::token::{KM_KILL_TOKEN_VAR, Run};
use kcommon::tracked::Tracked;

use crate::config::KmSysConfig;
use crate::system::components::Components;
use crate::system::heartbeat::{self, heartbeat_file};
use crate::system::pid::{apply_subprocess_flags, capture_stderr, pids_dir, write_pid};
use crate::system::proc::resolve_binary;

/// Binary name for the virtual-kubelet subprocess.
pub const BIN: &str = "virtual-kubelet";

/// Spawn the virtual-kubelet and wait until it reports itself ready in
/// its heartbeat file — which it writes only while its node is
/// registered with the API server and its status sync is running. Exits
/// the process on failure, rather than leaving km to report a ready
/// stack over a dead kubelet.
// Launcher-style: every parameter is an independent value the caller
// already has, so bundling them into a struct moves the list rather
// than shortening it.
#[allow(clippy::too_many_arguments)]
pub fn start(
    workdir: &Path,
    configdir: &Path,
    logdir: &Path,
    pids: &Path,
    sys_cfg: &KmSysConfig,
    port_vk: &str,
    vk_kubeconfig: &Path,
    vk_prconfig: &Path,
    run: &Run,
    components: &Components,
) {
    info!("Starting {}...", BIN);
    let vk_bin = resolve_binary(BIN);
    let log_max_size = sys_cfg.log.max_size_mb.to_string();
    let log_max_files = sys_cfg.log.max_files.to_string();
    let heartbeat = heartbeat_file(workdir, BIN);
    let heartbeat_secs = sys_cfg.heartbeat.interval().as_secs().to_string();
    heartbeat::clear(&heartbeat);
    let mut command = Command::new(&vk_bin);
    command
        .env("KUBECONFIG", vk_kubeconfig)
        .env("PRCONFIG", vk_prconfig)
        .args([
            "--workdir",
            workdir.to_str().unwrap(),
            "--configdir",
            configdir.to_str().unwrap(),
            "--logdir",
            logdir.to_str().unwrap(),
            "--logmaxsizemb",
            &log_max_size,
            "--logmaxfiles",
            &log_max_files,
            "--kubelet-port",
            port_vk,
            "--heartbeat-file",
            heartbeat.to_str().unwrap(),
            "--heartbeat-secs",
            &heartbeat_secs,
        ]);
    capture_stderr(&mut command, logdir, BIN);
    apply_subprocess_flags(&mut command);
    let vk = Tracked::spawn(command, KM_KILL_TOKEN_VAR, &run.scope(BIN))
        .unwrap_or_else(|e| panic!("Failed to start {}: {}", BIN, e));

    let pid = vk.pid();
    write_pid(pids, BIN, pid);
    components.register(BIN, vk);

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
        components.teardown(pids);
        std::process::exit(1);
    }
    info!(pid, "{} ready", BIN);
}

/// The virtual-kubelet's pids directory under `<workdir>/data/<BIN>/pids`.
pub fn pids_dir_in(workdir: &Path) -> PathBuf {
    pids_dir(workdir, BIN)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::system::pid::{DATA_DIR, PIDS_DIR};

    #[test]
    fn pids_dir_in_under_data_vk_pids() {
        let workdir = PathBuf::from("/tmp/foo");
        let p = pids_dir_in(&workdir);
        assert_eq!(
            p,
            PathBuf::from("/tmp/foo")
                .join(DATA_DIR)
                .join(BIN)
                .join(PIDS_DIR)
        );
    }
}
