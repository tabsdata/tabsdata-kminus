// Copyright 2026 Tabsdata Inc.

//! km system configuration (`kmsysconfig.yaml`).
//!
//! Holds log rotation settings and port assignments for all components
//! of the kminus stack. Created on first startup with defaults seeded
//! from `--portstart`; user-editable thereafter.

use std::fs;
use std::path::Path;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::process::{control_plane, pod_runner, virtual_kubelet};
use crate::system::port::find_free_port;

/// Filename of km's own system configuration file.
pub const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";

/// Number of consecutive ports an instance occupies, counting up from
/// `--portstart`: api, kcm, pr, prMetrics, vk, etcd, etcdPeer — in that order.
/// etcd's two ports come last so that adding them left every existing
/// assignment where it was.
pub const KM_PORT_COUNT: u16 = 7;

/// Largest accepted `--portstart`. Anything higher would push the
/// instance's [`KM_PORT_COUNT`] consecutive ports past the top of the
/// port space.
pub const MAX_PORTSTART: u16 = u16::MAX - (KM_PORT_COUNT - 1);

/// Default heartbeat interval, in seconds: how often every component
/// re-verifies itself and rewrites its heartbeat file.
pub const DEFAULT_HEARTBEAT_SECS: u32 = 15;

/// Default number of consecutive missed heartbeats tolerated before a
/// component is declared dead.
pub const DEFAULT_MAX_MISSED_HEARTBEATS: u32 = 4;

/// Label suffix for the control-plane's API server port.
const API_SERVER_LABEL: &str = "API server";

/// Label suffix for the pod-runner's metrics port.
const METRICS_LABEL: &str = "metrics";

/// Label for etcd's client port.
const ETCD_LABEL: &str = "etcd";

/// Label for etcd's peer port, which a single-node etcd binds but never uses.
const ETCD_PEER_LABEL: &str = "etcd peer";

/// Port assignments persisted in `kmsysconfig.yaml`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KmPorts {
    #[serde(default)]
    pub api: u16,
    #[serde(default)]
    pub kcm: u16,
    #[serde(default)]
    pub pr: u16,
    #[serde(default, rename = "prMetrics")]
    pub pr_metrics: u16,
    #[serde(default)]
    pub vk: u16,
    /// etcd's client port. Absent from files written before km managed it,
    /// where it defaults to 0 — which already means "pick a free one at
    /// startup", so those instances keep working.
    #[serde(default)]
    pub etcd: u16,
    /// etcd's peer port. Bound but unused by a single-node etcd; managed only
    /// so that no port the stack binds is left unaccounted for.
    #[serde(default, rename = "etcdPeer")]
    pub etcd_peer: u16,
}

impl KmPorts {
    /// Ports assigned consecutively from `portstart`. `portstart=0`
    /// produces all-zero ports — random allocation is deferred to
    /// startup, see [`KmSysConfig::resolve_random_ports`]. Any positive
    /// value takes [`KM_PORT_COUNT`] consecutive ports, and errors when
    /// that range would not fit below the top of the port space.
    pub fn from_portstart(portstart: u16) -> Result<Self, String> {
        if portstart == 0 {
            return Ok(Self {
                api: 0,
                kcm: 0,
                pr: 0,
                pr_metrics: 0,
                vk: 0,
                etcd: 0,
                etcd_peer: 0,
            });
        }
        if portstart > MAX_PORTSTART {
            return Err(format!(
                "portstart {} leaves no room for the {} consecutive ports an \
                 instance needs: use 0 (random ports at every start) or at most {}",
                portstart, KM_PORT_COUNT, MAX_PORTSTART
            ));
        }
        Ok(Self {
            api: portstart,
            kcm: portstart + 1,
            pr: portstart + 2,
            pr_metrics: portstart + 3,
            vk: portstart + 4,
            etcd: portstart + 5,
            etcd_peer: portstart + 6,
        })
    }

    /// The ports paired with the component label used in availability
    /// messages, in `portstart` assignment order.
    pub fn labeled(&self) -> Vec<(u16, String)> {
        vec![
            (
                self.api,
                format!("{} {}", control_plane::BIN, API_SERVER_LABEL),
            ),
            (self.kcm, control_plane::KCM.to_string()),
            (self.pr, pod_runner::BIN.to_string()),
            (
                self.pr_metrics,
                format!("{} {}", pod_runner::BIN, METRICS_LABEL),
            ),
            (self.vk, virtual_kubelet::BIN.to_string()),
            (self.etcd, ETCD_LABEL.to_string()),
            (self.etcd_peer, ETCD_PEER_LABEL.to_string()),
        ]
    }
}

/// Log settings persisted in `kmsysconfig.yaml`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KmLog {
    #[serde(rename = "maxSizeMB")]
    pub max_size_mb: u32,
    #[serde(rename = "maxFiles")]
    pub max_files: u32,
}

/// Heartbeat settings persisted in `kmsysconfig.yaml`. Shared by km and,
/// forwarded on the command line, by every component it supervises.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KmHeartbeat {
    #[serde(default = "default_heartbeat_secs", rename = "intervalSecs")]
    pub interval_secs: u32,
    #[serde(default = "default_max_missed", rename = "maxMissed")]
    pub max_missed: u32,
}

fn default_heartbeat_secs() -> u32 {
    DEFAULT_HEARTBEAT_SECS
}

fn default_max_missed() -> u32 {
    DEFAULT_MAX_MISSED_HEARTBEATS
}

impl Default for KmHeartbeat {
    fn default() -> Self {
        Self {
            interval_secs: DEFAULT_HEARTBEAT_SECS,
            max_missed: DEFAULT_MAX_MISSED_HEARTBEATS,
        }
    }
}

impl KmHeartbeat {
    /// Interval between heartbeats. A configured 0 would spin every
    /// component and every monitor, so it falls back to the default.
    pub fn interval(&self) -> Duration {
        let secs = if self.interval_secs == 0 {
            DEFAULT_HEARTBEAT_SECS
        } else {
            self.interval_secs
        };
        Duration::from_secs(secs as u64)
    }

    /// Consecutive misses tolerated. 0 would fail a component before it
    /// could ever beat, so the floor is 1.
    pub fn max_missed(&self) -> u32 {
        self.max_missed.max(1)
    }

    /// How long a component gets to report ready at startup: the same
    /// budget the monitor gives it to stay ready afterwards.
    pub fn startup_timeout(&self) -> Duration {
        self.interval() * self.max_missed()
    }
}

/// Full km system configuration.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KmSysConfig {
    pub log: KmLog,
    pub ports: KmPorts,
    /// Absent in files written before heartbeats existed: defaulted.
    #[serde(default)]
    pub heartbeat: KmHeartbeat,
}

impl KmSysConfig {
    /// Default config seeded from a `portstart` value. Port assignment
    /// rules — and the accepted range — are [`KmPorts::from_portstart`].
    pub fn defaults(portstart: u16) -> Result<Self, String> {
        Ok(Self {
            log: KmLog {
                max_size_mb: 100,
                max_files: 3,
            },
            ports: KmPorts::from_portstart(portstart)?,
            heartbeat: KmHeartbeat::default(),
        })
    }

    /// Create `kmsysconfig.yaml` in `configdir` with defaults seeded from
    /// `portstart`. Errors with `AlreadyExists` if the file is already there.
    ///
    /// Any 0-valued port stays 0 in the file — it is replaced with a
    /// fresh random port at runtime via [`Self::resolve_random_ports`].
    pub fn create(configdir: &Path, portstart: u16) -> std::io::Result<Self> {
        let path = configdir.join(KM_SYSCONFIG_FILE);
        if path.exists() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("{} already exists", path.display()),
            ));
        }
        let cfg = Self::defaults(portstart)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        fs::create_dir_all(configdir)?;
        let header = "# km system configuration.\n\
            # Log rotation, ports and heartbeat settings for the kminus stack.\n\
            # heartbeat.intervalSecs: how often each component re-verifies\n\
            # itself; heartbeat.maxMissed: consecutive misses tolerated\n\
            # before the stack is torn down.\n\
            # A port value of 0 means: pick a free random port on startup\n\
            # (the file value stays 0; resolution happens in memory only).\n\n";
        let body = serde_yaml::to_string(&cfg).map_err(|e| std::io::Error::other(e.to_string()))?;
        fs::write(&path, format!("{}{}", header, body))?;
        Ok(cfg)
    }

    /// Load an existing `kmsysconfig.yaml` from `configdir`. Errors if
    /// the file is missing or unparseable.
    pub fn load(configdir: &Path) -> std::io::Result<Self> {
        let path = configdir.join(KM_SYSCONFIG_FILE);
        let data = fs::read_to_string(&path)?;
        let cfg: Self = serde_yaml::from_str(&data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("failed to parse {}: {e}", path.display()),
            )
        })?;
        Ok(cfg)
    }

    /// Replace any 0-valued ports with free random ports. Returns
    /// `true` if at least one port was randomly allocated.
    pub fn resolve_random_ports(&mut self) -> bool {
        let mut any_random = false;
        for p in [
            &mut self.ports.api,
            &mut self.ports.kcm,
            &mut self.ports.pr,
            &mut self.ports.pr_metrics,
            &mut self.ports.vk,
            &mut self.ports.etcd,
            &mut self.ports.etcd_peer,
        ] {
            if *p == 0 {
                *p = find_free_port();
                any_random = true;
            }
        }
        any_random
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn tempdir() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!(
            "km-cfg-{}-{}-{}",
            std::process::id(),
            format!("{:?}", std::thread::current().id()).replace(['(', ')'], ""),
            n
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn defaults_with_portstart_assigns_consecutive_ports() {
        let cfg = KmSysConfig::defaults(10300).unwrap();
        assert_eq!(cfg.ports.api, 10300);
        assert_eq!(cfg.ports.kcm, 10301);
        assert_eq!(cfg.ports.pr, 10302);
        assert_eq!(cfg.ports.pr_metrics, 10303);
        assert_eq!(cfg.ports.vk, 10304);
        assert_eq!(cfg.ports.etcd, 10305);
        assert_eq!(cfg.ports.etcd_peer, 10306);
        assert_eq!(cfg.log.max_size_mb, 100);
        assert_eq!(cfg.log.max_files, 3);
    }

    #[test]
    fn defaults_with_zero_portstart_uses_zeros() {
        let cfg = KmSysConfig::defaults(0).unwrap();
        assert_eq!(cfg.ports.api, 0);
        assert_eq!(cfg.ports.kcm, 0);
        assert_eq!(cfg.ports.pr, 0);
        assert_eq!(cfg.ports.pr_metrics, 0);
        assert_eq!(cfg.ports.vk, 0);
        assert_eq!(cfg.ports.etcd, 0);
        assert_eq!(cfg.ports.etcd_peer, 0);
    }

    #[test]
    fn from_portstart_accepts_the_highest_fitting_range() {
        let ports = KmPorts::from_portstart(MAX_PORTSTART).unwrap();
        assert_eq!(ports.api, MAX_PORTSTART);
        // The last port assigned must land exactly on the top of the space,
        // whichever port that is as the set grows.
        assert_eq!(ports.labeled().last().unwrap().0, u16::MAX);
    }

    #[test]
    fn from_portstart_rejects_a_range_past_the_port_space() {
        let res = KmPorts::from_portstart(MAX_PORTSTART + 1);
        assert!(res.is_err());
        let msg = res.unwrap_err();
        assert!(msg.contains(&MAX_PORTSTART.to_string()));
    }

    #[test]
    fn labeled_covers_every_port_in_assignment_order() {
        let ports = KmPorts::from_portstart(12000).unwrap();
        let labeled = ports.labeled();
        assert_eq!(labeled.len(), KM_PORT_COUNT as usize);
        let numbers: Vec<u16> = labeled.iter().map(|(p, _)| *p).collect();
        assert_eq!(
            numbers,
            vec![12000, 12001, 12002, 12003, 12004, 12005, 12006]
        );
        assert!(labeled[0].1.contains(control_plane::BIN));
        assert!(labeled[0].1.contains(API_SERVER_LABEL));
        assert_eq!(labeled[1].1, control_plane::KCM);
        assert_eq!(labeled[2].1, pod_runner::BIN);
        assert!(labeled[3].1.contains(METRICS_LABEL));
        assert_eq!(labeled[4].1, virtual_kubelet::BIN);
        assert_eq!(labeled[5].1, ETCD_LABEL);
        assert_eq!(labeled[6].1, ETCD_PEER_LABEL);
    }

    #[test]
    fn create_writes_when_missing() {
        let dir = tempdir();
        let cfg = KmSysConfig::create(&dir, 11000).unwrap();
        assert_eq!(cfg.ports.api, 11000);
        assert!(dir.join(KM_SYSCONFIG_FILE).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_errors_when_file_exists() {
        let dir = tempdir();
        fs::write(dir.join(KM_SYSCONFIG_FILE), "log:\n  maxSizeMB: 1\n  maxFiles: 1\nports:\n  api: 1\n  kcm: 2\n  pr: 3\n  prMetrics: 4\n  vk: 5\n").unwrap();
        let res = KmSysConfig::create(&dir, 11000);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::AlreadyExists);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn create_rejects_out_of_range_portstart() {
        let dir = tempdir();
        let res = KmSysConfig::create(&dir, MAX_PORTSTART + 1);
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
        assert!(!dir.join(KM_SYSCONFIG_FILE).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn heartbeat_defaults_are_the_documented_ones() {
        let hb = KmHeartbeat::default();
        assert_eq!(hb.interval_secs, DEFAULT_HEARTBEAT_SECS);
        assert_eq!(hb.max_missed, DEFAULT_MAX_MISSED_HEARTBEATS);
        assert_eq!(hb.interval(), Duration::from_secs(15));
        assert_eq!(hb.max_missed(), 4);
        assert_eq!(hb.startup_timeout(), Duration::from_secs(60));
    }

    #[test]
    fn heartbeat_rejects_degenerate_values() {
        let hb = KmHeartbeat {
            interval_secs: 0,
            max_missed: 0,
        };
        // A 0 interval would spin; 0 misses would fail before the first beat.
        assert_eq!(
            hb.interval(),
            Duration::from_secs(DEFAULT_HEARTBEAT_SECS as u64)
        );
        assert_eq!(hb.max_missed(), 1);
    }

    #[test]
    fn load_defaults_heartbeat_when_the_section_is_absent() {
        let dir = tempdir();
        let yaml = "log:\n  maxSizeMB: 50\n  maxFiles: 7\n\
                    ports:\n  api: 9000\n  kcm: 9001\n  pr: 9002\n  prMetrics: 9003\n  vk: 9004\n";
        fs::write(dir.join(KM_SYSCONFIG_FILE), yaml).unwrap();

        let cfg = KmSysConfig::load(&dir).unwrap();
        assert_eq!(cfg.heartbeat, KmHeartbeat::default());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_reads_configured_heartbeat_values() {
        let dir = tempdir();
        let yaml = "log:\n  maxSizeMB: 50\n  maxFiles: 7\n\
                    ports:\n  api: 9000\n  kcm: 9001\n  pr: 9002\n  prMetrics: 9003\n  vk: 9004\n\
                    heartbeat:\n  intervalSecs: 5\n  maxMissed: 2\n";
        fs::write(dir.join(KM_SYSCONFIG_FILE), yaml).unwrap();

        let cfg = KmSysConfig::load(&dir).unwrap();
        assert_eq!(cfg.heartbeat.interval(), Duration::from_secs(5));
        assert_eq!(cfg.heartbeat.max_missed(), 2);
        assert_eq!(cfg.heartbeat.startup_timeout(), Duration::from_secs(10));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_reads_existing_file() {
        let dir = tempdir();
        let yaml = "log:\n  maxSizeMB: 50\n  maxFiles: 7\n\
                    ports:\n  api: 9000\n  kcm: 9001\n  pr: 9002\n  prMetrics: 9003\n  vk: 9004\n";
        fs::write(dir.join(KM_SYSCONFIG_FILE), yaml).unwrap();

        let cfg = KmSysConfig::load(&dir).unwrap();
        // Written before km managed etcd's ports: defaulted, not an error.
        assert_eq!(cfg.ports.etcd, 0);
        assert_eq!(cfg.ports.etcd_peer, 0);
        assert_eq!(cfg.log.max_size_mb, 50);
        assert_eq!(cfg.log.max_files, 7);
        assert_eq!(cfg.ports.api, 9000);
        assert_eq!(cfg.ports.vk, 9004);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_errors_when_file_missing() {
        let dir = tempdir();
        let res = KmSysConfig::load(&dir);
        assert!(res.is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_rejects_invalid_yaml() {
        let dir = tempdir();
        fs::write(dir.join(KM_SYSCONFIG_FILE), "not: [valid: yaml").unwrap();
        let res = KmSysConfig::load(&dir);
        assert!(res.is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_random_ports_replaces_zeros() {
        let mut cfg = KmSysConfig::defaults(0).unwrap();
        let any_random = cfg.resolve_random_ports();
        assert!(any_random);
        // All ports must be non-zero and unique.
        let ports: HashSet<u16> = [
            cfg.ports.api,
            cfg.ports.kcm,
            cfg.ports.pr,
            cfg.ports.pr_metrics,
            cfg.ports.vk,
            cfg.ports.etcd,
            cfg.ports.etcd_peer,
        ]
        .into_iter()
        .collect();
        assert_eq!(ports.len(), KM_PORT_COUNT as usize);
        assert!(ports.iter().all(|&p| p != 0));
    }

    #[test]
    fn resolve_random_ports_returns_false_when_all_set() {
        let mut cfg = KmSysConfig::defaults(10300).unwrap();
        let any_random = cfg.resolve_random_ports();
        assert!(!any_random);
        assert_eq!(cfg.ports.api, 10300);
    }

    #[test]
    fn resolve_random_ports_replaces_only_zeros() {
        let mut cfg = KmSysConfig::defaults(10300).unwrap();
        cfg.ports.kcm = 0; // only this one should be replaced
        let any_random = cfg.resolve_random_ports();
        assert!(any_random);
        assert_eq!(cfg.ports.api, 10300);
        assert_ne!(cfg.ports.kcm, 0);
        assert_eq!(cfg.ports.pr, 10302);
    }

    #[test]
    fn yaml_round_trip_preserves_field_names() {
        let cfg = KmSysConfig::defaults(10300).unwrap();
        let yaml = serde_yaml::to_string(&cfg).unwrap();
        // camelCase aliases must be present in the output.
        assert!(yaml.contains("maxSizeMB:"));
        assert!(yaml.contains("maxFiles:"));
        assert!(yaml.contains("prMetrics:"));
        // Round-trip back.
        let cfg2: KmSysConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(cfg, cfg2);
    }
}
