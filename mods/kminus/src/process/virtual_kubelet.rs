// Copyright 2026 Tabsdata Inc.

//! Virtual-kubelet subprocess: startup, readiness, status gathering.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use tracing::{info, warn};

use crate::config::KmSysConfig;
use crate::system::file::wait_for_file;
use crate::system::pid::{apply_subprocess_flags, pids_dir, write_pid};

/// Binary name for the virtual-kubelet subprocess.
pub const BIN: &str = "virtual-kubelet";

/// Spawn the virtual-kubelet. The VK has no clean readiness signal,
/// so we wait for its pids dir to appear under `<workdir>/data/` and
/// then warm up for a few seconds.
pub fn start(
    bin_dir: &Path,
    workdir: &Path,
    configdir: &Path,
    logdir: &Path,
    pids: &Path,
    sys_cfg: &KmSysConfig,
    port_vk: &str,
    vk_kubeconfig: &Path,
    vk_prconfig: &Path,
) -> Child {
    info!("Starting {}...", BIN);
    let vk_bin = bin_dir.join(BIN);
    let log_max_size = sys_cfg.log.max_size_mb.to_string();
    let log_max_files = sys_cfg.log.max_files.to_string();
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
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    apply_subprocess_flags(&mut command);
    let vk = command
        .spawn()
        .unwrap_or_else(|e| panic!("Failed to start {}: {}", BIN, e));

    write_pid(pids, BIN, vk.id());

    let vk_pids_dir = pids_dir_in(workdir);
    if !wait_for_file(&vk_pids_dir, Duration::from_secs(15)) {
        warn!("{} may not have fully started, continuing...", BIN);
    }
    std::thread::sleep(Duration::from_secs(5));
    info!(pid = vk.id(), "{} started", BIN);
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
