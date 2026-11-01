// Copyright 2026 Tabsdata Inc.

//! Cleanup TTLs — how long a finished pod's data dirs and pod dir live.
//!
//! Two independent clocks govern a finished pod's on-disk footprint:
//!
//! - **reap** — deletes the pod's data dir (`pod-run/<uid>/`: `work/`,
//!   `volumes/`), the bulk of the footprint. Runs from **pod completion**.
//! - **delete** — deletes the pod incarnation's state dir
//!   (`pod-state/<bucket>/<pod_name>/<stamp>/`: `log/`, `store/`), its only
//!   record. Runs from the moment the pod is **removed from the pod
//!   runner**, because until then the runner still answers queries about
//!   it.
//!
//! Each clock has a TTL per outcome (success/failure), so four values in
//! total. Every value is configured at the pod runner level and can be
//! overridden per pod with an annotation.
//!
//! A TTL is `<number><unit>` with unit `s`, `m` or `h`, or the literal
//! `never`. There is deliberately no `d` unit: `24h` says the same thing
//! without a second way to spell it.
//!
//! The values resolved for a pod are written at pod start, recording where
//! each one came from, to a [`CLEANUP_TTLS_FILE`] in each dir they govern:
//! [`ReapTtls`] in the data dir, [`DeleteTtls`] in the state dir. Nothing
//! on the reaping path re-derives them from config: each dir carries its
//! own retention policy, so it stays correct across a config edit or a
//! runner restart.

use std::collections::HashMap;
use std::fs;
use std::io;
use std::path::Path;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::spec::constants::{
    ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, ANN_POD_DATA_REAP_TTL_ON_POD_SUCCESS,
    ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
    ANN_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE, CLEANUP_TTLS_FILE,
    DEFAULT_POD_DATA_REAP_TTL_ON_POD_FAILURE, DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS,
    DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
    DEFAULT_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE, TTL_NEVER,
};

/// Seconds in a minute — TTL unit `m`.
const SECS_PER_MINUTE: u64 = 60;
/// Seconds in an hour — TTL unit `h`.
const SECS_PER_HOUR: u64 = 3600;

// ---------------------------------------------------------------------------
// Ttl
// ---------------------------------------------------------------------------

/// A cleanup TTL: a finite delay, or never.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ttl {
    /// No deadline — the guarded files are never removed.
    Never,
    /// Expires this long after the clock origin.
    After(Duration),
}

/// A TTL value that failed to parse.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid TTL {value:?}: {reason} (expected <number>[s|m|h] or \"{TTL_NEVER}\")")]
pub struct TtlParseError {
    /// The value as given.
    pub value: String,
    /// Why it was rejected.
    pub reason: &'static str,
}

impl Ttl {
    /// Parse a TTL. Accepts `never` or `<number><unit>` with unit `s`,
    /// `m` or `h` — lowercase, no sign, no fraction, no whitespace.
    pub fn parse(value: &str) -> Result<Self, TtlParseError> {
        let err = |reason: &'static str| TtlParseError {
            value: value.to_string(),
            reason,
        };

        if value == TTL_NEVER {
            return Ok(Ttl::Never);
        }

        // Split on bytes, not chars: a multi-byte tail can never be a
        // valid unit, and this keeps the digit check ASCII-only.
        let (unit, digits) = value.as_bytes().split_last().ok_or_else(|| err("empty"))?;
        if digits.is_empty() {
            return Err(err("missing number"));
        }
        if !digits.iter().all(u8::is_ascii_digit) {
            return Err(err("number must be digits only"));
        }
        let secs_per_unit = match unit {
            b's' => 1,
            b'm' => SECS_PER_MINUTE,
            b'h' => SECS_PER_HOUR,
            _ => return Err(err("unit must be s, m or h")),
        };

        // `digits` is ASCII by the check above, so the utf8 conversion
        // cannot fail.
        let count: u64 = std::str::from_utf8(digits)
            .map_err(|_| err("number must be digits only"))?
            .parse()
            .map_err(|_| err("number too large"))?;
        let secs = count
            .checked_mul(secs_per_unit)
            .ok_or_else(|| err("number too large"))?;

        Ok(Ttl::After(Duration::from_secs(secs)))
    }

    /// Resolved seconds, or `None` for [`Ttl::Never`].
    pub fn secs(&self) -> Option<u64> {
        match self {
            Ttl::Never => None,
            Ttl::After(d) => Some(d.as_secs()),
        }
    }

    /// The deadline measured from `origin`, or `None` when there is no
    /// deadline to schedule — either the TTL is `never`, or the sum would
    /// overflow, which takes a TTL far past any accepted value.
    pub fn deadline(&self, origin: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self {
            Ttl::Never => None,
            Ttl::After(d) => origin.checked_add_signed(chrono::Duration::seconds(
                // A TTL wide enough to overflow i64 seconds is absurd, but
                // saturating here keeps the cast honest.
                d.as_secs().min(i64::MAX as u64) as i64,
            )),
        }
    }
}

/// A TTL together with the string it was configured as, so
/// [`CLEANUP_TTLS_FILE`] can report `1h` rather than `3600s`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlSpec {
    raw: String,
    ttl: Ttl,
}

impl TtlSpec {
    /// Parse a configured TTL string.
    pub fn parse(raw: &str) -> Result<Self, TtlParseError> {
        Ok(Self {
            raw: raw.to_string(),
            ttl: Ttl::parse(raw)?,
        })
    }

    /// The value as configured.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// The parsed TTL.
    pub fn ttl(&self) -> Ttl {
        self.ttl
    }
}

// ---------------------------------------------------------------------------
// TtlConfig — pod runner level
// ---------------------------------------------------------------------------

/// The four cleanup TTLs as configured at the pod runner level.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TtlConfig {
    /// Data dir of a succeeded pod, from completion.
    pub reap_on_success: TtlSpec,
    /// Data dir of a failed pod, from completion.
    pub reap_on_failure: TtlSpec,
    /// State dir of a succeeded pod, from removal from the runner.
    pub delete_on_success: TtlSpec,
    /// State dir of a failed pod, from removal from the runner.
    pub delete_on_failure: TtlSpec,
}

impl Default for TtlConfig {
    fn default() -> Self {
        // The defaults are constants in this crate, so they parse.
        Self {
            reap_on_success: TtlSpec::parse(DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS).unwrap(),
            reap_on_failure: TtlSpec::parse(DEFAULT_POD_DATA_REAP_TTL_ON_POD_FAILURE).unwrap(),
            delete_on_success: TtlSpec::parse(DEFAULT_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE)
                .unwrap(),
            delete_on_failure: TtlSpec::parse(DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE)
                .unwrap(),
        }
    }
}

// ---------------------------------------------------------------------------
// ReapTtls / DeleteTtls — resolved per pod
// ---------------------------------------------------------------------------

/// Where a resolved TTL came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TtlSource {
    /// The pod runner configuration.
    Config,
    /// A per-pod annotation.
    Annotation,
}

/// One resolved TTL, as written to a [`CLEANUP_TTLS_FILE`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtlSetting {
    /// The value as configured, e.g. `1h` or `never`.
    pub raw: String,
    /// Resolved seconds; `None` for `never`.
    pub secs: Option<u64>,
    /// Whether config or an annotation supplied it.
    pub source: TtlSource,
}

impl TtlSetting {
    /// The TTL this setting resolved to.
    pub fn ttl(&self) -> Ttl {
        match self.secs {
            None => Ttl::Never,
            Some(secs) => Ttl::After(Duration::from_secs(secs)),
        }
    }
}

/// The reap TTLs resolved for one pod — the [`CLEANUP_TTLS_FILE`] in its
/// data dir.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReapTtls {
    /// When this file was written (RFC3339).
    pub written_at: String,
    /// Data dir of a succeeded pod, from completion.
    pub reap_ttl_on_success: TtlSetting,
    /// Data dir of a failed pod, from completion.
    pub reap_ttl_on_failure: TtlSetting,
}

impl ReapTtls {
    /// The reap TTL for the outcome the pod reached.
    pub fn ttl(&self, succeeded: bool) -> Ttl {
        if succeeded {
            self.reap_ttl_on_success.ttl()
        } else {
            self.reap_ttl_on_failure.ttl()
        }
    }
}

/// The delete TTLs resolved for one pod — the [`CLEANUP_TTLS_FILE`] in its
/// state dir.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteTtls {
    /// When this file was written (RFC3339).
    pub written_at: String,
    /// State dir of a succeeded pod, from removal from the runner.
    pub delete_ttl_on_success: TtlSetting,
    /// State dir of a failed pod, from removal from the runner.
    pub delete_ttl_on_failure: TtlSetting,
}

impl DeleteTtls {
    /// The delete TTL for the outcome the pod reached.
    pub fn ttl(&self, succeeded: bool) -> Ttl {
        if succeeded {
            self.delete_ttl_on_success.ttl()
        } else {
            self.delete_ttl_on_failure.ttl()
        }
    }
}

/// Resolve a pod's reap TTLs: each annotation overrides its config
/// counterpart, and an annotation that doesn't parse is ignored with a
/// warning rather than failing the pod.
pub fn resolve_reap_ttls(
    config: &TtlConfig,
    annotations: &HashMap<String, String>,
    written_at: &str,
) -> ReapTtls {
    ReapTtls {
        written_at: written_at.to_string(),
        reap_ttl_on_success: resolve_one(
            &config.reap_on_success,
            annotations,
            ANN_POD_DATA_REAP_TTL_ON_POD_SUCCESS,
        ),
        reap_ttl_on_failure: resolve_one(
            &config.reap_on_failure,
            annotations,
            ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE,
        ),
    }
}

/// Resolve a pod's delete TTLs, as [`resolve_reap_ttls`] does its reap ones.
pub fn resolve_delete_ttls(
    config: &TtlConfig,
    annotations: &HashMap<String, String>,
    written_at: &str,
) -> DeleteTtls {
    DeleteTtls {
        written_at: written_at.to_string(),
        delete_ttl_on_success: resolve_one(
            &config.delete_on_success,
            annotations,
            ANN_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE,
        ),
        delete_ttl_on_failure: resolve_one(
            &config.delete_on_failure,
            annotations,
            ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
        ),
    }
}

fn resolve_one(config: &TtlSpec, annotations: &HashMap<String, String>, key: &str) -> TtlSetting {
    let from_config = || TtlSetting {
        raw: config.raw().to_string(),
        secs: config.ttl().secs(),
        source: TtlSource::Config,
    };

    let Some(raw) = annotations.get(key) else {
        return from_config();
    };
    match Ttl::parse(raw) {
        Ok(ttl) => TtlSetting {
            raw: raw.clone(),
            secs: ttl.secs(),
            source: TtlSource::Annotation,
        },
        Err(e) => {
            warn!(key, error = %e, "Invalid TTL annotation, using configured value");
            from_config()
        }
    }
}

// ---------------------------------------------------------------------------
// On-disk access
// ---------------------------------------------------------------------------

/// Write a [`CLEANUP_TTLS_FILE`] — [`ReapTtls`] or [`DeleteTtls`] — into
/// `dir`, creating the dir if needed.
pub fn write_cleanup_ttls<T: Serialize>(dir: &Path, ttls: &T) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let json = serde_json::to_vec_pretty(ttls).map_err(io::Error::other)?;
    fs::write(dir.join(CLEANUP_TTLS_FILE), json)
}

/// Read the [`CLEANUP_TTLS_FILE`] from `dir`. Returns `None` when it is
/// absent or unreadable — the caller falls back to live config, since a
/// dir with no policy of its own is better reaped on the current settings
/// than left forever.
pub fn read_cleanup_ttls<T: DeserializeOwned>(dir: &Path) -> Option<T> {
    let path = dir.join(CLEANUP_TTLS_FILE);
    if !path.exists() {
        return None;
    }
    match fs::read(&path).map(|raw| serde_json::from_slice::<T>(&raw)) {
        Ok(Ok(ttls)) => Some(ttls),
        Ok(Err(e)) => {
            warn!(path = ?path, error = %e, "Malformed cleanup TTLs file");
            None
        }
        Err(e) => {
            warn!(path = ?path, error = %e, "Failed to read cleanup TTLs file");
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> std::path::PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pod-runner-ttl-{}-{}", name, std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn ann(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // -- Ttl::parse --

    #[test]
    fn parse_never() {
        assert_eq!(Ttl::parse(TTL_NEVER).unwrap(), Ttl::Never);
    }

    #[test]
    fn parse_seconds() {
        assert_eq!(Ttl::parse("0s").unwrap(), Ttl::After(Duration::ZERO));
        assert_eq!(
            Ttl::parse("30s").unwrap(),
            Ttl::After(Duration::from_secs(30))
        );
    }

    #[test]
    fn parse_minutes() {
        assert_eq!(
            Ttl::parse("5m").unwrap(),
            Ttl::After(Duration::from_secs(300))
        );
        assert_eq!(
            Ttl::parse("90m").unwrap(),
            Ttl::After(Duration::from_secs(5400))
        );
    }

    #[test]
    fn parse_hours() {
        assert_eq!(
            Ttl::parse("1h").unwrap(),
            Ttl::After(Duration::from_secs(3600))
        );
        assert_eq!(
            Ttl::parse("24h").unwrap(),
            Ttl::After(Duration::from_secs(86_400))
        );
        assert_eq!(
            Ttl::parse("8760h").unwrap(),
            Ttl::After(Duration::from_secs(31_536_000))
        );
    }

    #[test]
    fn parse_rejects_day_unit() {
        // No `d`: 24h is the only way to say a day.
        assert!(Ttl::parse("1d").is_err());
    }

    #[test]
    fn parse_rejects_bare_number() {
        assert!(Ttl::parse("1").is_err());
        assert!(Ttl::parse("0").is_err());
    }

    #[test]
    fn parse_rejects_empty_and_unitless() {
        assert!(Ttl::parse("").is_err());
        assert!(Ttl::parse("s").is_err());
        assert!(Ttl::parse("h").is_err());
    }

    #[test]
    fn parse_rejects_sign_fraction_and_space() {
        assert!(Ttl::parse("-1h").is_err());
        assert!(Ttl::parse("1.5h").is_err());
        assert!(Ttl::parse("1 h").is_err());
        assert!(Ttl::parse(" 1h").is_err());
        assert!(Ttl::parse("1s ").is_err());
    }

    #[test]
    fn parse_rejects_uppercase_unit() {
        // An obvious typo is better rejected than silently accepted.
        assert!(Ttl::parse("1H").is_err());
        assert!(Ttl::parse("NEVER").is_err());
    }

    #[test]
    fn parse_rejects_overflow() {
        assert!(Ttl::parse("99999999999999999999s").is_err());
        // Fits u64 as a count, overflows when scaled to seconds.
        assert!(Ttl::parse("9223372036854775807h").is_err());
    }

    #[test]
    fn parse_rejects_multibyte_tail() {
        assert!(Ttl::parse("1é").is_err());
    }

    #[test]
    fn parse_error_names_the_value() {
        let e = Ttl::parse("1d").unwrap_err();
        assert_eq!(e.value, "1d");
        assert!(e.to_string().contains("1d"), "{}", e);
    }

    // -- deadline / secs --

    #[test]
    fn secs_of_never_is_none() {
        assert_eq!(Ttl::Never.secs(), None);
        assert_eq!(Ttl::After(Duration::from_secs(5)).secs(), Some(5));
    }

    #[test]
    fn deadline_adds_ttl_to_origin() {
        let origin: DateTime<Utc> = "2026-08-04T09:00:00Z".parse().unwrap();
        let deadline = Ttl::parse("1h").unwrap().deadline(origin).unwrap();
        assert_eq!(deadline.to_rfc3339(), "2026-08-04T10:00:00+00:00");
    }

    #[test]
    fn deadline_of_zero_is_the_origin() {
        let origin: DateTime<Utc> = "2026-08-04T09:00:00Z".parse().unwrap();
        assert_eq!(Ttl::parse("0s").unwrap().deadline(origin), Some(origin));
    }

    #[test]
    fn deadline_of_never_is_none() {
        let origin: DateTime<Utc> = "2026-08-04T09:00:00Z".parse().unwrap();
        assert_eq!(Ttl::Never.deadline(origin), None);
    }

    // -- TtlSpec / TtlConfig --

    #[test]
    fn spec_keeps_the_configured_spelling() {
        let spec = TtlSpec::parse("1h").unwrap();
        assert_eq!(spec.raw(), "1h");
        assert_eq!(spec.ttl().secs(), Some(3600));
    }

    #[test]
    fn config_defaults_match_the_documented_values() {
        let c = TtlConfig::default();
        assert_eq!(c.reap_on_success.raw(), "0s");
        assert_eq!(c.reap_on_failure.raw(), "1h");
        assert_eq!(c.delete_on_success.raw(), "1h");
        assert_eq!(c.delete_on_failure.raw(), "24h");
    }

    // -- resolve_reap_ttls / resolve_delete_ttls --

    #[test]
    fn resolve_uses_config_when_no_annotations() {
        let reap = resolve_reap_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        let delete = resolve_delete_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        assert_eq!(reap.written_at, "now");
        assert_eq!(delete.written_at, "now");
        for s in [
            &reap.reap_ttl_on_success,
            &reap.reap_ttl_on_failure,
            &delete.delete_ttl_on_success,
            &delete.delete_ttl_on_failure,
        ] {
            assert_eq!(s.source, TtlSource::Config);
        }
        assert_eq!(reap.reap_ttl_on_success.secs, Some(0));
        assert_eq!(delete.delete_ttl_on_failure.secs, Some(86_400));
    }

    #[test]
    fn resolve_reap_on_success_annotation() {
        let reap = resolve_reap_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DATA_REAP_TTL_ON_POD_SUCCESS, "5m")]),
            "now",
        );
        assert_eq!(reap.reap_ttl_on_success.raw, "5m");
        assert_eq!(reap.reap_ttl_on_success.secs, Some(300));
        assert_eq!(reap.reap_ttl_on_success.source, TtlSource::Annotation);
        assert_eq!(reap.reap_ttl_on_failure.source, TtlSource::Config);
    }

    #[test]
    fn resolve_reap_on_failure_annotation() {
        let reap = resolve_reap_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, "2h")]),
            "now",
        );
        assert_eq!(reap.reap_ttl_on_failure.secs, Some(7200));
        assert_eq!(reap.reap_ttl_on_failure.source, TtlSource::Annotation);
    }

    #[test]
    fn resolve_delete_on_success_annotation() {
        let delete = resolve_delete_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE, "0s")]),
            "now",
        );
        assert_eq!(delete.delete_ttl_on_success.secs, Some(0));
        assert_eq!(delete.delete_ttl_on_success.source, TtlSource::Annotation);
        assert_eq!(delete.delete_ttl_on_failure.source, TtlSource::Config);
    }

    #[test]
    fn resolve_delete_on_failure_annotation() {
        let delete = resolve_delete_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, TTL_NEVER)]),
            "now",
        );
        assert_eq!(delete.delete_ttl_on_failure.raw, TTL_NEVER);
        assert_eq!(delete.delete_ttl_on_failure.secs, None);
        assert_eq!(delete.delete_ttl_on_failure.ttl(), Ttl::Never);
        assert_eq!(delete.delete_ttl_on_failure.source, TtlSource::Annotation);
    }

    #[test]
    fn each_half_ignores_the_other_halfs_annotations() {
        let annotations = ann(&[
            (ANN_POD_DATA_REAP_TTL_ON_POD_SUCCESS, "5m"),
            (ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, "5m"),
        ]);
        let reap = resolve_reap_ttls(&TtlConfig::default(), &annotations, "now");
        let delete = resolve_delete_ttls(&TtlConfig::default(), &annotations, "now");
        assert_eq!(reap.reap_ttl_on_failure.source, TtlSource::Config);
        assert_eq!(delete.delete_ttl_on_success.source, TtlSource::Config);
    }

    #[test]
    fn resolve_ignores_invalid_annotation() {
        let reap = resolve_reap_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, "1d")]),
            "now",
        );
        assert_eq!(reap.reap_ttl_on_failure.raw, "1h", "fell back to config");
        assert_eq!(reap.reap_ttl_on_failure.source, TtlSource::Config);
    }

    #[test]
    fn resolve_ignores_unrelated_annotations() {
        let reap = resolve_reap_ttls(
            &TtlConfig::default(),
            &ann(&[("prSomethingElse", "5m")]),
            "now",
        );
        assert_eq!(reap.reap_ttl_on_success.source, TtlSource::Config);
    }

    // -- outcome selection --

    #[test]
    fn outcome_picks_the_matching_ttl() {
        let reap = resolve_reap_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        let delete = resolve_delete_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        assert_eq!(reap.ttl(true).secs(), Some(0));
        assert_eq!(reap.ttl(false).secs(), Some(3600));
        assert_eq!(delete.ttl(true).secs(), Some(3600));
        assert_eq!(delete.ttl(false).secs(), Some(86_400));
    }

    // -- on-disk --

    #[test]
    fn write_then_read_round_trips() {
        let dir = tmp("round-trip");
        let reap = resolve_reap_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DATA_REAP_TTL_ON_POD_FAILURE, "15m")]),
            "2026-08-04T09:00:00+00:00",
        );
        write_cleanup_ttls(&dir, &reap).unwrap();
        assert_eq!(read_cleanup_ttls::<ReapTtls>(&dir), Some(reap));

        let delete = resolve_delete_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        write_cleanup_ttls(&dir, &delete).unwrap();
        assert_eq!(read_cleanup_ttls::<DeleteTtls>(&dir), Some(delete));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_file_of_the_other_half_does_not_read_back() {
        let dir = tmp("other-half");
        let reap = resolve_reap_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        write_cleanup_ttls(&dir, &reap).unwrap();
        assert_eq!(read_cleanup_ttls::<DeleteTtls>(&dir), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn write_creates_the_dir() {
        let dir = tmp("creates-dir").join("nested");
        let reap = resolve_reap_ttls(&TtlConfig::default(), &HashMap::new(), "now");

        write_cleanup_ttls(&dir, &reap).unwrap();
        assert!(dir.join(CLEANUP_TTLS_FILE).exists());

        let _ = fs::remove_dir_all(dir.parent().unwrap());
    }

    #[test]
    fn read_missing_file_is_none() {
        let dir = tmp("missing");
        fs::create_dir_all(&dir).unwrap();
        assert_eq!(read_cleanup_ttls::<ReapTtls>(&dir), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_malformed_file_is_none() {
        let dir = tmp("malformed");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join(CLEANUP_TTLS_FILE), "{not json").unwrap();
        assert_eq!(read_cleanup_ttls::<ReapTtls>(&dir), None);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn json_uses_camel_case_keys() {
        let reap = resolve_reap_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        let json = serde_json::to_string(&reap).unwrap();
        assert!(json.contains("\"writtenAt\""), "{}", json);
        assert!(json.contains("\"reapTtlOnSuccess\""), "{}", json);
        assert!(!json.contains("deleteTtl"), "{}", json);
        assert!(json.contains("\"source\":\"config\""), "{}", json);

        let delete = resolve_delete_ttls(&TtlConfig::default(), &HashMap::new(), "now");
        let json = serde_json::to_string(&delete).unwrap();
        assert!(json.contains("\"deleteTtlOnFailure\""), "{}", json);
        assert!(!json.contains("reapTtl"), "{}", json);
    }

    #[test]
    fn never_serializes_as_null_secs() {
        let delete = resolve_delete_ttls(
            &TtlConfig::default(),
            &ann(&[(ANN_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE, TTL_NEVER)]),
            "now",
        );
        let json = serde_json::to_string(&delete).unwrap();
        assert!(json.contains("\"raw\":\"never\",\"secs\":null"), "{}", json);
    }
}
