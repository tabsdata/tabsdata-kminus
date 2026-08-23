// Copyright 2026 Tabsdata Inc.

//! Virtual-kubelet subprocess: startup, readiness, status gathering.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use tracing::info;

use crate::config::KmSysConfig;
use crate::process::{control_plane, pod_runner};
use crate::system::heartbeat::{self, heartbeat_file};
use crate::system::pid::{apply_subprocess_flags, kill_child, pids_dir, remove_pid, write_pid};
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
    cp: &mut Child,
    pr: &mut Child,
) -> Child {
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
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    apply_subprocess_flags(&mut command);
    let mut vk = command
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to start {}: {}", BIN, e));

    write_pid(pids, BIN, vk.id());

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
        kill_child(&mut vk, BIN);
        kill_child(pr, pod_runner::BIN);
        kill_child(cp, control_plane::BIN);
        for name in [BIN, pod_runner::BIN, control_plane::BIN] {
            remove_pid(pids, name);
        }
        std::process::exit(1);
    }
    info!(pid = vk.id(), "{} ready", BIN);
    vk
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
