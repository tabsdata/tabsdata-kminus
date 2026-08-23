// Copyright 2026 Tabsdata Inc.

//! km system configuration (`kmsysconfig.yaml`).
//!
//! Holds log rotation settings and port assignments for all components
//! of the kminus stack. Created on first startup with defaults seeded
//! from `--portstart`; user-editable thereafter.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::process::{control_plane, pod_runner, virtual_kubelet};
use crate::system::port::find_free_port;

/// Filename of km's own system configuration file.
pub const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";

/// Number of consecutive ports an instance occupies, counting up from
/// `--portstart`: api, kcm, pr, prMetrics, vk — in that order.
pub const KM_PORT_COUNT: u16 = 5;

/// Largest accepted `--portstart`. Anything higher would push the
/// instance's [`KM_PORT_COUNT`] consecutive ports past the top of the
/// port space.
pub const MAX_PORTSTART: u16 = u16::MAX - (KM_PORT_COUNT - 1);

/// Label suffix for the control-plane's API server port.
const API_SERVER_LABEL: &str = "API server";

/// Label suffix for the pod-runner's metrics port.
const METRICS_LABEL: &str = "metrics";

/// Port assignments persisted in `kmsysconfig.yaml`.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
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

/// Full km system configuration.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct KmSysConfig {
    pub log: KmLog,
    pub ports: KmPorts,
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
            # Log rotation settings and ports for the kminus stack.\n\
            # A port value of 0 means: pick a free random port on startup\n\
            # (the file value stays 0; resolution happens in memory only).\n\n";
        let body = serde_yaml::to_string(&cfg)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e.to_string()))?;
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
    }

    #[test]
    fn from_portstart_accepts_the_highest_fitting_range() {
        let ports = KmPorts::from_portstart(MAX_PORTSTART).unwrap();
        assert_eq!(ports.api, MAX_PORTSTART);
        assert_eq!(ports.vk, u16::MAX);
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
        assert_eq!(numbers, vec![12000, 12001, 12002, 12003, 12004]);
        assert!(labeled[0].1.contains(control_plane::BIN));
        assert!(labeled[0].1.contains(API_SERVER_LABEL));
        assert_eq!(labeled[1].1, control_plane::KCM);
        assert_eq!(labeled[2].1, pod_runner::BIN);
        assert!(labeled[3].1.contains(METRICS_LABEL));
        assert_eq!(labeled[4].1, virtual_kubelet::BIN);
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
    fn load_reads_existing_file() {
        let dir = tempdir();
        let yaml = "log:\n  maxSizeMB: 50\n  maxFiles: 7\n\
                    ports:\n  api: 9000\n  kcm: 9001\n  pr: 9002\n  prMetrics: 9003\n  vk: 9004\n";
        fs::write(dir.join(KM_SYSCONFIG_FILE), yaml).unwrap();

        let cfg = KmSysConfig::load(&dir).unwrap();
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
        ]
        .into_iter()
        .collect();
        assert_eq!(ports.len(), 5);
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
