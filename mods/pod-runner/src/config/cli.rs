// Copyright 2026 Tabsdata Inc.

//! CLI argument parsing.

use clap::Parser;

use crate::config::loglevel::LogLevel;

/// Default config directory (under cwd when standalone).
pub const DEFAULT_CONFIGDIR: &str = "config";
/// Default working directory.
/// `data/pod-runner/` is created under this.
pub const DEFAULT_WORKDIR: &str = ".";
/// Default log directory (under cwd when standalone).
pub const DEFAULT_LOGDIR: &str = "log";
/// Default root for resolving relative container `image` paths (under
/// cwd when standalone). Used when neither `--imagesdir` nor an
/// `imagesDir` entry in `prsysconfig.yaml` is set.
pub const DEFAULT_IMAGESDIR: &str = "images";

/// Pod Runner — executes pods as local processes.
/// Default seconds between readiness heartbeats. Matches km's own
/// default so a standalone run behaves like a supervised one.
pub const DEFAULT_HEARTBEAT_SECS: u64 = 15;

#[derive(Parser, Debug)]
#[command(
    name = "pod-runner",
    version = crate::VERSION,
    about = "pod-runner — executes pods as local processes.",
    long_about = format!(
        "pod-runner — executes pods as local processes.\n\n{}\n{}",
        crate::COPYRIGHT,
        crate::LICENSE,
    ),
)]
pub struct CliArgs {
    /// Path to kubeconfig file. Falls back to KUBECONFIG env var.
    #[arg(long)]
    pub kubeconfig: Option<String>,

    /// Directory for config files (prconfig.yaml).
    /// If prconfig.yaml doesn't exist, auto-generates PKI and configs.
    /// Default: ./config
    #[arg(long)]
    pub configdir: Option<String>,

    /// Working directory. `data/pod-runner/` is created under this.
    /// Default: .
    #[arg(long)]
    pub workdir: Option<String>,

    /// Log directory. Contains pod-runner's own rotating log file
    /// (`pod-runner.log`) and per-pod container log subdirectories.
    /// Default: ./log
    #[arg(long)]
    pub logdir: Option<String>,

    /// Root directory for resolving relative container `image` paths
    /// in pod specs. Absolute `image` paths are used as-is.
    /// Precedence: this flag > `imagesDir` in `prsysconfig.yaml` >
    /// `./images` under the current directory.
    #[arg(long)]
    pub imagesdir: Option<String>,

    /// Max size of a single pod-runner log file (MB) before rotation.
    #[arg(long, default_value_t = 100)]
    pub logmaxsizemb: u32,

    /// Max number of rotated pod-runner log files to keep.
    #[arg(long, default_value_t = 3)]
    pub logmaxfiles: u32,

    /// Server port (only used when auto-generating prconfig.yaml).
    /// Default: 3000
    #[arg(long)]
    pub port: Option<u16>,

    /// Metrics server port. Overrides `metricsUrl` from prsysconfig.yaml.
    /// Set to 0 to disable metrics. Default: prsysconfig.yaml's metricsUrl
    /// (3001)
    #[arg(long)]
    pub metrics_port: Option<u16>,

    /// Level for pod-runner's own log file. Independent of `RUST_LOG`,
    /// which is ignored. km forwards its own `--loglevel` here.
    #[arg(long, value_enum, default_value_t = LogLevel::Debug)]
    pub loglevel: LogLevel,

    /// File to publish the readiness heartbeat into. Every interval the
    /// server calls its own `/capacity` and rewrites this file with the
    /// result; km gates startup and supervision on it. No heartbeat is
    /// published when unset (standalone runs).
    #[arg(long)]
    pub heartbeat_file: Option<String>,

    /// Seconds between readiness heartbeats.
    #[arg(long, default_value_t = DEFAULT_HEARTBEAT_SECS)]
    pub heartbeat_secs: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_no_args() {
        let args = CliArgs::try_parse_from(["prog"]).unwrap();
        assert!(args.kubeconfig.is_none());
        assert!(args.configdir.is_none());
        assert!(args.workdir.is_none());
        assert!(args.logdir.is_none());
        assert!(args.imagesdir.is_none());
        assert!(args.port.is_none());
        assert!(args.metrics_port.is_none());
        assert_eq!(args.loglevel, LogLevel::Debug);
    }

    #[test]
    fn parse_all_args() {
        let args = CliArgs::try_parse_from([
            "prog",
            "--kubeconfig",
            "/etc/kube/config",
            "--configdir",
            "/etc/pr",
            "--workdir",
            "/var/pr",
            "--logdir",
            "/var/log/pr",
            "--imagesdir",
            "/var/lib/kminus/images",
            "--port",
            "9000",
            "--metrics-port",
            "9001",
            "--loglevel",
            "warn",
        ])
        .unwrap();
        assert_eq!(args.kubeconfig.as_deref(), Some("/etc/kube/config"));
        assert_eq!(args.configdir.as_deref(), Some("/etc/pr"));
        assert_eq!(args.port, Some(9000));
        assert_eq!(args.metrics_port, Some(9001));
        assert_eq!(args.workdir.as_deref(), Some("/var/pr"));
        assert_eq!(args.logdir.as_deref(), Some("/var/log/pr"));
        assert_eq!(args.imagesdir.as_deref(), Some("/var/lib/kminus/images"));
        assert_eq!(args.loglevel, LogLevel::Warn);
    }

    #[test]
    fn parse_unknown_arg_fails() {
        let result = CliArgs::try_parse_from(["prog", "--unknown"]);
        assert!(result.is_err());
    }
}
