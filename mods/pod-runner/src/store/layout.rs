// Copyright 2026 Tabsdata Inc.

//! On-disk pod layout — where a pod's data and its record live.
//!
//! A pod's footprint is split in two, because the halves have different
//! lifetimes and different readers:
//!
//! - **pod-run** — `<data>/pod-run/<pod_id>/`: what the containers produce
//!   (`work/`, `volumes/`). The bulk of the disk, dead weight once the pod
//!   is finished, so it goes on the reap deadline. Keyed by the full pod id,
//!   uid included, so a new pod never touches the data an earlier one is
//!   still keeping for inspection.
//! - **pod-state** — `<data>/pod-state/<bucket>/<log_pod_id>/<incarnation>/`:
//!   the record of what one pod did and why it ended (`log/`, `store/`).
//!   Small, and read after the fact by log pod id alone, whether or not the
//!   runner still knows the pod — so it goes on the delete deadline, and its
//!   path is computed rather than looked up.
//!
//! The **log pod id** is the value of the pod's log id label
//! ([`DEFAULT_POD_LOG_ID_LABEL`], configurable) when it carries one that is
//! a valid Kubernetes name, and the pod name otherwise. A workload that sets
//! the label on its pod template — a CronJob, a Job with retries, a
//! Deployment — files all its pods under one id, so all of them are read
//! back together. The uid is never part of the state path; a pod runner
//! serves a single namespace, so names do not collide across namespaces.
//!
//! Every pod filed under an id gets an **incarnation** dir of its own,
//! `<stamp>_<pod_name>`: its creation time
//! ([`PodMetadata::incarnation_stamp`]) and its name. The stamp sorts
//! incarnations chronologically; the name keeps pods created in the same
//! second — replicas, parallel Job pods — apart. Kubernetes names never
//! contain `_`, so the two halves split back unambiguously. A pod replayed
//! after a restart keeps its creation time and name, so it lands back in
//! its own incarnation dir.
//!
//! `<bucket>` is [`state_bucket`] of the log pod id. Pod-state dirs outlive
//! the pods that wrote them, so they accumulate far more entries than
//! pod-run dirs do; the bucket keeps any one directory from growing
//! unbounded.
//!
//! [`PodMetadata::incarnation_stamp`]: crate::spec::pod::PodMetadata::incarnation_stamp
//! [`DEFAULT_POD_LOG_ID_LABEL`]: crate::spec::constants::DEFAULT_POD_LOG_ID_LABEL

use std::path::{Path, PathBuf};

use crate::server::config::{POD_LOG_SUBDIR, POD_RUN_DIR, POD_STATE_DIR, POD_STORE_SUBDIR};
use crate::spec::constants::DEFAULT_POD_LOG_ID_LABEL;
use crate::spec::pod::PodSpec;

use super::PodId;

/// Number of pod-state buckets. Part of the on-disk format: changing it
/// moves every pod's state dir, so existing records stop being found.
pub const POD_STATE_BUCKETS: u64 = 256;

/// Stamp used for a spec that carries no usable creation time. Admission
/// sets one on every pod, so this only names dirs for specs that never went
/// through it.
pub const UNSTAMPED: &str = "00000000T000000Z";

/// Separates the stamp from the pod name in an incarnation dir name. Not a
/// character Kubernetes allows in a name, so the split is unambiguous.
pub const INCARNATION_SEPARATOR: char = '_';

/// Longest Kubernetes object name (DNS-1123 subdomain).
const KUBE_NAME_MAX_LEN: usize = 253;

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// The pod-state bucket for a log pod id: FNV-1a 64 of its bytes,
/// xor-folded to 8 bits, modulo [`POD_STATE_BUCKETS`], as two lowercase hex
/// digits.
///
/// FNV-1a rather than `std`'s hasher, whose output is explicitly not
/// stable across Rust releases — a bucket is a path on disk and must
/// resolve the same way in every build that ever reads it. The fold is
/// not optional: FNV's multiply only carries upward, so its low byte
/// depends on nothing but the low byte of each input, and taken alone it
/// crowds similar ids into a fraction of the buckets.
pub fn state_bucket(log_pod_id: &str) -> String {
    let hash = log_pod_id.bytes().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
    });
    let folded = hash ^ (hash >> 32);
    let folded = folded ^ (folded >> 16);
    let folded = folded ^ (folded >> 8);
    format!("{:02x}", folded % POD_STATE_BUCKETS)
}

/// Whether `name` is a valid Kubernetes object name (DNS-1123 subdomain):
/// at most 253 lowercase alphanumerics, `-` and `.`, starting and ending
/// with an alphanumeric. Such a name is always a safe single path
/// component.
pub fn is_kube_name(name: &str) -> bool {
    let alnum = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit();
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= KUBE_NAME_MAX_LEN
        && alnum(bytes[0])
        && alnum(bytes[bytes.len() - 1])
        && bytes.iter().all(|&b| alnum(b) || b == b'-' || b == b'.')
}

/// Split an incarnation dir name into its stamp and pod name.
pub fn parse_incarnation(dir_name: &str) -> Option<(&str, &str)> {
    dir_name.split_once(INCARNATION_SEPARATOR)
}

/// The two roots of the pod layout, and the per-pod paths under them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodLayout {
    /// Pod data root: `<data>/pod-run/`.
    pub run_root: PathBuf,
    /// Pod record root: `<data>/pod-state/`.
    pub state_root: PathBuf,
    /// Pod label whose value is the log pod id. Empty disables it.
    pub log_id_label: String,
}

impl PodLayout {
    /// The layout rooted at the runner's data dir, with the default log id
    /// label.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            run_root: data_dir.join(POD_RUN_DIR),
            state_root: data_dir.join(POD_STATE_DIR),
            log_id_label: DEFAULT_POD_LOG_ID_LABEL.to_string(),
        }
    }

    /// This layout with another log id label; empty disables it.
    pub fn with_log_id_label(mut self, label: impl Into<String>) -> Self {
        self.log_id_label = label.into();
        self
    }

    /// The pod's data dir: `<data>/pod-run/<pod_id>/`.
    pub fn run_dir(&self, id: &PodId) -> PathBuf {
        self.run_root.join(id.full())
    }

    /// The log pod id `spec` is filed under: its log id label's value when
    /// that is a valid Kubernetes name, its pod name otherwise.
    pub fn log_pod_id(&self, spec: &PodSpec) -> String {
        if !self.log_id_label.is_empty()
            && let Some(value) = spec.metadata.labels.get(&self.log_id_label)
            && is_kube_name(value)
        {
            return value.clone();
        }
        spec.metadata.name.clone()
    }

    /// The dir every pod filed under a log pod id shares:
    /// `<data>/pod-state/<bucket>/<log_pod_id>/`.
    pub fn log_pod_dir(&self, log_pod_id: &str) -> PathBuf {
        self.state_root
            .join(state_bucket(log_pod_id))
            .join(log_pod_id)
    }

    /// One incarnation's record dir: `<log_pod_dir>/<incarnation>/`.
    pub fn state_dir_at(&self, log_pod_id: &str, incarnation: &str) -> PathBuf {
        self.log_pod_dir(log_pod_id).join(incarnation)
    }

    /// The incarnation dir name of the pod `spec` describes:
    /// `<stamp>_<pod_name>`.
    pub fn incarnation(spec: &PodSpec) -> String {
        let stamp = spec
            .metadata
            .incarnation_stamp()
            .unwrap_or_else(|| UNSTAMPED.to_string());
        format!("{stamp}{INCARNATION_SEPARATOR}{}", spec.metadata.name)
    }

    /// The record dir of the pod `spec` describes.
    pub fn state_dir(&self, spec: &PodSpec) -> PathBuf {
        self.state_dir_at(&self.log_pod_id(spec), &Self::incarnation(spec))
    }

    /// The pod's container log dir: `<state_dir>/log/`.
    pub fn log_dir(&self, spec: &PodSpec) -> PathBuf {
        self.state_dir(spec).join(POD_LOG_SUBDIR)
    }

    /// The pod's store dir: `<state_dir>/store/`.
    pub fn store_dir(&self, spec: &PodSpec) -> PathBuf {
        self.state_dir(spec).join(POD_STORE_SUBDIR)
    }

    /// Every incarnation's record dir on disk, across all buckets and log
    /// pod ids.
    pub fn list_state_dirs(&self) -> Vec<PathBuf> {
        subdirs(&self.state_root)
            .into_iter()
            .flat_map(|bucket| subdirs(&bucket))
            .flat_map(|id| subdirs(&id))
            .collect()
    }

    /// Every pod-run dir on disk.
    pub fn list_run_dirs(&self) -> Vec<PathBuf> {
        subdirs(&self.run_root)
    }
}

impl Default for PodLayout {
    fn default() -> Self {
        Self::new(Path::new(""))
    }
}

/// The subdirectories of `dir`; empty when it is absent or unreadable.
fn subdirs(dir: &Path) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::PodMetadata;

    fn tmp(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("pod-runner-layout-{}-{}", name, std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn spec(name: &str, uid: &str, created: Option<&str>) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                namespace: "default".into(),
                name: name.into(),
                uid: uid.into(),
                creation_timestamp: created.map(String::from),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn labeled(name: &str, created: &str, log_id: &str) -> PodSpec {
        let mut s = spec(name, &format!("uid-{name}"), Some(created));
        s.metadata
            .labels
            .insert(DEFAULT_POD_LOG_ID_LABEL.into(), log_id.into());
        s
    }

    #[test]
    fn bucket_is_two_lowercase_hex_digits() {
        for name in ["", "a", "app", "tabsdata-worker-7c9e6679"] {
            let b = state_bucket(name);
            assert_eq!(b.len(), 2, "{name:?} -> {b:?}");
            assert!(
                b.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
                "{name:?} -> {b:?}"
            );
        }
    }

    #[test]
    fn bucket_is_pinned() {
        // The bucket is a path on disk: these values must never change, or
        // records written by an earlier build stop being found.
        assert_eq!(state_bucket(""), "e1");
        assert_eq!(state_bucket("a"), "bb");
        assert_eq!(state_bucket("default_app_uid-1"), "0b");
    }

    #[test]
    fn buckets_spread() {
        let buckets: std::collections::HashSet<String> = (0..1000)
            .map(|i| state_bucket(&format!("pod-{i}")))
            .collect();
        // 1000 names over 256 buckets leave very few empty.
        assert!(buckets.len() > 200, "only {} buckets used", buckets.len());
    }

    #[test]
    fn kube_names() {
        for good in [
            "a",
            "app",
            "nightly-etl",
            "web.v2",
            "a1",
            "1a",
            &"a".repeat(253),
        ] {
            assert!(is_kube_name(good), "{good:?}");
        }
        for bad in [
            "",
            "App",
            "-app",
            "app-",
            ".app",
            "app.",
            "a_b",
            "a/b",
            "..",
            "a b",
            &"a".repeat(254),
        ] {
            assert!(!is_kube_name(bad), "{bad:?}");
        }
    }

    #[test]
    fn paths() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let s = spec("app", "uid-1", Some("2026-10-19T14:30:05Z"));
        let id = PodId::from_spec(&s);
        let id_dir = layout.state_root.join(state_bucket("app")).join("app");

        assert_eq!(layout.run_root, PathBuf::from("/w/data").join(POD_RUN_DIR));
        assert_eq!(
            layout.state_root,
            PathBuf::from("/w/data").join(POD_STATE_DIR)
        );
        assert_eq!(
            layout.run_dir(&id),
            layout.run_root.join("default_app_uid-1")
        );
        assert_eq!(layout.log_pod_dir("app"), id_dir);
        assert_eq!(PodLayout::incarnation(&s), "20261019T143005Z_app");
        assert_eq!(layout.state_dir(&s), id_dir.join("20261019T143005Z_app"));
        assert_eq!(
            layout.log_dir(&s),
            layout.state_dir(&s).join(POD_LOG_SUBDIR)
        );
        assert_eq!(
            layout.store_dir(&s),
            layout.state_dir(&s).join(POD_STORE_SUBDIR)
        );
    }

    #[test]
    fn incarnation_names_split_back() {
        assert_eq!(
            parse_incarnation("20261019T143005Z_web-7d9f-x7k2p"),
            Some(("20261019T143005Z", "web-7d9f-x7k2p"))
        );
        assert_eq!(parse_incarnation("20261019T143005Z"), None);
    }

    #[test]
    fn without_the_label_a_pod_is_filed_under_its_name() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let first = spec("app", "uid-1", Some("2026-10-19T14:30:05Z"));
        let second = spec("app", "uid-2", Some("2026-10-19T15:00:00Z"));

        assert_eq!(layout.log_pod_id(&first), "app");
        assert_eq!(
            layout.state_dir(&first).parent(),
            layout.state_dir(&second).parent(),
            "incarnations of one name share its dir; the uid is not in the path"
        );
        assert_ne!(layout.state_dir(&first), layout.state_dir(&second));
        assert_ne!(
            layout.run_dir(&PodId::from_spec(&first)),
            layout.run_dir(&PodId::from_spec(&second)),
            "the data dir stays per uid"
        );
    }

    #[test]
    fn a_valid_label_files_every_pod_under_its_value() {
        let layout = PodLayout::new(Path::new("/w/data"));
        // Two runs of a CronJob: generated pod names, one log pod id.
        let run1 = labeled("nightly-29001230-x7k2p", "2026-10-19T00:00:03Z", "nightly");
        let run2 = labeled("nightly-29002670-q4m8z", "2026-10-20T00:00:02Z", "nightly");

        assert_eq!(layout.log_pod_id(&run1), "nightly");
        let dir = layout.log_pod_dir("nightly");
        assert_eq!(
            layout.state_dir(&run1),
            dir.join("20261019T000003Z_nightly-29001230-x7k2p")
        );
        assert_eq!(
            layout.state_dir(&run2),
            dir.join("20261020T000002Z_nightly-29002670-q4m8z")
        );
    }

    #[test]
    fn pods_created_in_the_same_second_get_their_own_dirs() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let a = labeled("web-7d9f-aaaaa", "2026-10-19T14:30:05Z", "web");
        let b = labeled("web-7d9f-bbbbb", "2026-10-19T14:30:05Z", "web");
        assert_eq!(layout.state_dir(&a).parent(), layout.state_dir(&b).parent());
        assert_ne!(layout.state_dir(&a), layout.state_dir(&b));
    }

    #[test]
    fn an_invalid_label_value_falls_back_to_the_pod_name() {
        let layout = PodLayout::new(Path::new("/w/data"));
        for bad in ["", "Nightly", "night_ly", "../x", "a/b"] {
            assert_eq!(
                layout.log_pod_id(&labeled("app", "2026-10-19T14:30:05Z", bad)),
                "app",
                "{bad:?}"
            );
        }
    }

    #[test]
    fn the_label_is_configurable_and_can_be_disabled() {
        let s = labeled("app", "2026-10-19T14:30:05Z", "nightly");
        let disabled = PodLayout::new(Path::new("/w")).with_log_id_label("");
        assert_eq!(disabled.log_pod_id(&s), "app");

        let mut other = spec("app", "uid-1", Some("2026-10-19T14:30:05Z"));
        other
            .metadata
            .labels
            .insert("tabsdata.com/worker-id".into(), "w-1".into());
        let custom = PodLayout::new(Path::new("/w")).with_log_id_label("tabsdata.com/worker-id");
        assert_eq!(custom.log_pod_id(&other), "w-1");
        assert_eq!(
            custom.log_pod_id(&s),
            "app",
            "the default label is not read"
        );
    }

    #[test]
    fn a_replay_lands_in_its_own_state_dir() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let original = labeled("app-x7k2p", "2026-10-19T14:30:05Z", "app");
        assert_eq!(
            layout.state_dir(&original),
            layout.state_dir(&original.clone())
        );
    }

    #[test]
    fn an_unstamped_spec_gets_the_placeholder_stamp() {
        let layout = PodLayout::new(Path::new("/w/data"));
        assert_eq!(
            layout.state_dir(&spec("app", "uid-1", None)),
            layout.state_dir_at("app", &format!("{UNSTAMPED}_app"))
        );
    }

    #[test]
    fn lists_incarnations_across_buckets_and_ids() {
        let base = tmp("list");
        let layout = PodLayout::new(&base);
        let specs = [
            spec("a", "u1", Some("2026-10-19T14:00:00Z")),
            spec("a", "u2", Some("2026-10-19T15:00:00Z")),
            labeled("b-x", "2026-10-19T14:00:00Z", "b"),
        ];
        for s in &specs {
            std::fs::create_dir_all(layout.state_dir(s)).unwrap();
            std::fs::create_dir_all(layout.run_dir(&PodId::from_spec(s))).unwrap();
        }
        // Stray files are not pod dirs.
        std::fs::write(layout.run_root.join("stray"), b"").unwrap();
        std::fs::write(layout.log_pod_dir("a").join("_stray"), b"").unwrap();

        let mut state = layout.list_state_dirs();
        state.sort();
        let mut expected: Vec<PathBuf> = specs.iter().map(|s| layout.state_dir(s)).collect();
        expected.sort();
        assert_eq!(state, expected);
        assert_eq!(layout.list_run_dirs().len(), specs.len());

        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn missing_roots_list_nothing() {
        let layout = PodLayout::new(Path::new("/nonexistent/pod-runner-layout"));
        assert!(layout.list_state_dirs().is_empty());
        assert!(layout.list_run_dirs().is_empty());
    }
}
