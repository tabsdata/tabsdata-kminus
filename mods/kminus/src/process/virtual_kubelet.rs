// Copyright 2026 Tabsdata Inc.

//! Virtual-kubelet subprocess: startup, readiness, status gathering.

use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::config::KmSysConfig;
use crate::system::file::wait_for_file;
use crate::system::pid::{apply_subprocess_flags, pids_dir, write_pid};
use crate::system::proc::resolve_binary;

/// Binary name for the virtual-kubelet subprocess.
pub const BIN: &str = "virtual-kubelet";

/// How long to wait for the kubelet API port to start accepting connections.
const PORT_READY_TIMEOUT: Duration = Duration::from_secs(15);
/// Interval between readiness checks.
const READY_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Per-attempt TCP connect timeout for the kubelet port probe.
const PORT_CONNECT_TIMEOUT: Duration = Duration::from_millis(200);

/// Spawn the virtual-kubelet, then wait until its kubelet API port is
/// accepting connections (the process is up and serving) before returning.
pub fn start(
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
    let vk_bin = resolve_binary(BIN);
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
    wait_until_ready(port_vk, vk.id());
    info!(pid = vk.id(), "{} started", BIN);
    vk
}

/// Poll until the virtual-kubelet's kubelet API port accepts a TCP
/// connection — i.e. the process is up and serving — or the timeout elapses.
/// Replaces a fixed warm-up sleep, so startup returns as soon as the VK is
/// serving rather than after a worst-case delay.
fn wait_until_ready(port_vk: &str, pid: u32) {
    let start = Instant::now();
    let deadline = start + PORT_READY_TIMEOUT;
    loop {
        if kubelet_port_open(port_vk) {
            info!(
                pid,
                elapsed_ms = start.elapsed().as_millis() as u64,
                "{} kubelet port ready",
                BIN
            );
            return;
        }
        if Instant::now() >= deadline {
            warn!(
                pid,
                "{} kubelet port {} not accepting connections after {}s; continuing",
                BIN,
                port_vk,
                PORT_READY_TIMEOUT.as_secs()
            );
            return;
        }
        std::thread::sleep(READY_POLL_INTERVAL);
    }
}

/// Whether the kubelet API port accepts a TCP connection on loopback.
fn kubelet_port_open(port_vk: &str) -> bool {
    let Ok(port) = port_vk.parse::<u16>() else {
        return false;
    };
    TcpStream::connect_timeout(
        &SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
        PORT_CONNECT_TIMEOUT,
    )
    .is_ok()
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
