// Copyright 2026 Tabsdata Inc.

//! km system configuration (`kmsysconfig.yaml`).
//!
//! Holds log rotation settings and port assignments for all components
//! of the kminus stack. Created on first startup with defaults seeded
//! from `--portstart`; user-editable thereafter.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::utils::find_free_port;

/// Filename of km's own system configuration file.
pub const KM_SYSCONFIG_FILE: &str = "kmsysconfig.yaml";

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
    /// Default config seeded from a `portstart` value. `portstart=0`
    /// produces all-zero ports (random allocation deferred to runtime);
    /// any positive value assigns ports consecutively.
    pub fn defaults(portstart: u16) -> Self {
        let ports = if portstart == 0 {
            KmPorts {
                api: 0,
                kcm: 0,
                pr: 0,
                pr_metrics: 0,
                vk: 0,
            }
        } else {
            KmPorts {
                api: portstart,
                kcm: portstart + 1,
                pr: portstart + 2,
                pr_metrics: portstart + 3,
                vk: portstart + 4,
            }
        };
        Self {
            log: KmLog {
                max_size_mb: 100,
                max_files: 3,
            },
            ports,
        }
    }

    /// Load `kmsysconfig.yaml` from `configdir`, or create it with
    /// defaults seeded from `portstart` if missing.
    ///
    /// Any 0-valued port stays 0 in the file — it is replaced with a
    /// fresh random port at runtime via [`Self::resolve_random_ports`].
    pub fn load_or_create(configdir: &Path, portstart: u16) -> std::io::Result<Self> {
        let path = configdir.join(KM_SYSCONFIG_FILE);
        if path.exists() {
            let data = fs::read_to_string(&path)?;
            let cfg: Self = serde_yaml::from_str(&data).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("failed to parse {}: {e}", path.display()),
                )
            })?;
            return Ok(cfg);
        }
        let cfg = Self::defaults(portstart);
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
        let dir = std::env::temp_dir()
            .join(format!("km-cfg-{}-{}-{}", std::process::id(),
                          format!("{:?}", std::thread::current().id()).replace(['(', ')'], ""), n));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn defaults_with_portstart_assigns_consecutive_ports() {
        let cfg = KmSysConfig::defaults(10300);
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
        let cfg = KmSysConfig::defaults(0);
        assert_eq!(cfg.ports.api, 0);
        assert_eq!(cfg.ports.kcm, 0);
        assert_eq!(cfg.ports.pr, 0);
        assert_eq!(cfg.ports.pr_metrics, 0);
        assert_eq!(cfg.ports.vk, 0);
    }

    #[test]
    fn load_or_create_creates_when_missing() {
        let dir = tempdir();
        let cfg = KmSysConfig::load_or_create(&dir, 11000).unwrap();
        assert_eq!(cfg.ports.api, 11000);
        assert!(dir.join(KM_SYSCONFIG_FILE).exists());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_or_create_reads_existing_file() {
        let dir = tempdir();
        // Pre-write a file with non-default values.
        let yaml = "log:\n  maxSizeMB: 50\n  maxFiles: 7\n\
                    ports:\n  api: 9000\n  kcm: 9001\n  pr: 9002\n  prMetrics: 9003\n  vk: 9004\n";
        fs::write(dir.join(KM_SYSCONFIG_FILE), yaml).unwrap();

        let cfg = KmSysConfig::load_or_create(&dir, 60000).unwrap();
        assert_eq!(cfg.log.max_size_mb, 50);
        assert_eq!(cfg.log.max_files, 7);
        assert_eq!(cfg.ports.api, 9000);
        assert_eq!(cfg.ports.vk, 9004);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_or_create_rejects_invalid_yaml() {
        let dir = tempdir();
        fs::write(dir.join(KM_SYSCONFIG_FILE), "not: [valid: yaml").unwrap();
        let res = KmSysConfig::load_or_create(&dir, 10300);
        assert!(res.is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_random_ports_replaces_zeros() {
        let mut cfg = KmSysConfig::defaults(0);
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
        let mut cfg = KmSysConfig::defaults(10300);
        let any_random = cfg.resolve_random_ports();
        assert!(!any_random);
        assert_eq!(cfg.ports.api, 10300);
    }

    #[test]
    fn resolve_random_ports_replaces_only_zeros() {
        let mut cfg = KmSysConfig::defaults(10300);
        cfg.ports.kcm = 0; // only this one should be replaced
        let any_random = cfg.resolve_random_ports();
        assert!(any_random);
        assert_eq!(cfg.ports.api, 10300);
        assert_ne!(cfg.ports.kcm, 0);
        assert_eq!(cfg.ports.pr, 10302);
    }

    #[test]
    fn yaml_round_trip_preserves_field_names() {
        let cfg = KmSysConfig::defaults(10300);
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
