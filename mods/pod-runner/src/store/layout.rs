// Copyright 2026 Tabsdata Inc.

//! On-disk pod layout — where a pod's data and its record live.
//!
//! A pod's footprint is split in two, because the halves have different
//! lifetimes and different readers:
//!
//! - **pod-run** — `<data>/pod-run/<pod_id>/`: what the containers produce
//!   (`work/`, `volumes/`). The bulk of the disk, dead weight once the pod
//!   is finished, so it goes on the reap deadline. Keyed by the full pod id,
//!   uid included, so a new incarnation never touches the data an earlier
//!   one is still keeping for inspection.
//! - **pod-state** — `<data>/pod-state/<bucket>/<pod_name>/<stamp>/`: the
//!   record of what one incarnation of the pod did and why it ended (`log/`,
//!   `store/`). Small, and read after the fact by pod name alone, whether or
//!   not the runner still knows the pod — so it goes on the delete deadline,
//!   and its path is computed rather than looked up.
//!
//! A pod runner serves a single namespace, so the pod name identifies a pod
//! across its incarnations; the uid the API server assigns each one is
//! deliberately not part of the state path. Incarnations of one name share
//! its `<pod_name>/` dir, each in a `<stamp>/` of its own — the pod's
//! creation time ([`PodMetadata::incarnation_stamp`]). A pod replayed after
//! a restart keeps its creation time, so it lands back in its own stamp dir.
//!
//! `<bucket>` is [`state_bucket`] of the pod name. Pod-state dirs outlive
//! the pods that wrote them, so they accumulate far more entries than
//! pod-run dirs do; the bucket keeps any one directory from growing
//! unbounded.
//!
//! [`PodMetadata::incarnation_stamp`]: crate::spec::pod::PodMetadata::incarnation_stamp

use std::path::{Path, PathBuf};

use crate::server::config::{POD_LOG_SUBDIR, POD_RUN_DIR, POD_STATE_DIR, POD_STORE_SUBDIR};
use crate::spec::pod::PodSpec;

use super::PodId;

/// Number of pod-state buckets. Part of the on-disk format: changing it
/// moves every pod's state dir, so existing records stop being found.
pub const POD_STATE_BUCKETS: u64 = 256;

/// Stamp used for a spec that carries no usable creation time. Admission
/// sets one on every pod, so this only names dirs for specs that never went
/// through it.
pub const UNSTAMPED: &str = "00000000T000000Z";

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// The pod-state bucket for a pod name: FNV-1a 64 of its bytes, xor-folded
/// to 8 bits, modulo [`POD_STATE_BUCKETS`], as two lowercase hex digits.
///
/// FNV-1a rather than `std`'s hasher, whose output is explicitly not
/// stable across Rust releases — a bucket is a path on disk and must
/// resolve the same way in every build that ever reads it. The fold is
/// not optional: FNV's multiply only carries upward, so its low byte
/// depends on nothing but the low byte of each input, and taken alone it
/// crowds similar names into a fraction of the buckets.
pub fn state_bucket(pod_name: &str) -> String {
    let hash = pod_name.bytes().fold(FNV_OFFSET_BASIS, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME)
    });
    let folded = hash ^ (hash >> 32);
    let folded = folded ^ (folded >> 16);
    let folded = folded ^ (folded >> 8);
    format!("{:02x}", folded % POD_STATE_BUCKETS)
}

/// The two roots of the pod layout, and the per-pod paths under them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PodLayout {
    /// Pod data root: `<data>/pod-run/`.
    pub run_root: PathBuf,
    /// Pod record root: `<data>/pod-state/`.
    pub state_root: PathBuf,
}

impl PodLayout {
    /// The layout rooted at the runner's data dir.
    pub fn new(data_dir: &Path) -> Self {
        Self {
            run_root: data_dir.join(POD_RUN_DIR),
            state_root: data_dir.join(POD_STATE_DIR),
        }
    }

    /// The pod's data dir: `<data>/pod-run/<pod_id>/`.
    pub fn run_dir(&self, id: &PodId) -> PathBuf {
        self.run_root.join(id.full())
    }

    /// The dir every incarnation of a pod name shares:
    /// `<data>/pod-state/<bucket>/<pod_name>/`.
    pub fn name_dir(&self, pod_name: &str) -> PathBuf {
        self.state_root.join(state_bucket(pod_name)).join(pod_name)
    }

    /// One incarnation's record dir: `<name_dir>/<stamp>/`.
    pub fn state_dir_at(&self, pod_name: &str, stamp: &str) -> PathBuf {
        self.name_dir(pod_name).join(stamp)
    }

    /// The record dir of the incarnation `spec` describes.
    pub fn state_dir(&self, spec: &PodSpec) -> PathBuf {
        let stamp = spec
            .metadata
            .incarnation_stamp()
            .unwrap_or_else(|| UNSTAMPED.to_string());
        self.state_dir_at(&spec.metadata.name, &stamp)
    }

    /// The incarnation's container log dir: `<state_dir>/log/`.
    pub fn log_dir(&self, spec: &PodSpec) -> PathBuf {
        self.state_dir(spec).join(POD_LOG_SUBDIR)
    }

    /// The incarnation's store dir: `<state_dir>/store/`.
    pub fn store_dir(&self, spec: &PodSpec) -> PathBuf {
        self.state_dir(spec).join(POD_STORE_SUBDIR)
    }

    /// Every incarnation's record dir on disk, across all buckets and
    /// names.
    pub fn list_state_dirs(&self) -> Vec<PathBuf> {
        subdirs(&self.state_root)
            .into_iter()
            .flat_map(|bucket| subdirs(&bucket))
            .flat_map(|name| subdirs(&name))
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
    fn paths() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let s = spec("app", "uid-1", Some("2026-10-19T14:30:05Z"));
        let id = PodId::from_spec(&s);
        let name_dir = layout.state_root.join(state_bucket("app")).join("app");

        assert_eq!(layout.run_root, PathBuf::from("/w/data").join(POD_RUN_DIR));
        assert_eq!(
            layout.state_root,
            PathBuf::from("/w/data").join(POD_STATE_DIR)
        );
        assert_eq!(
            layout.run_dir(&id),
            layout.run_root.join("default_app_uid-1")
        );
        assert_eq!(layout.name_dir("app"), name_dir);
        assert_eq!(layout.state_dir(&s), name_dir.join("20261019T143005Z"));
        assert_eq!(
            layout.state_dir(&s),
            layout.state_dir_at("app", "20261019T143005Z")
        );
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
    fn incarnations_share_the_name_dir_not_the_state_dir() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let first = spec("app", "uid-1", Some("2026-10-19T14:30:05Z"));
        let second = spec("app", "uid-2", Some("2026-10-19T15:00:00Z"));

        assert_eq!(
            layout.state_dir(&first).parent(),
            layout.state_dir(&second).parent(),
            "the uid is not part of the state path"
        );
        assert_ne!(layout.state_dir(&first), layout.state_dir(&second));
        assert_ne!(
            layout.run_dir(&PodId::from_spec(&first)),
            layout.run_dir(&PodId::from_spec(&second)),
            "the data dir stays per uid"
        );
    }

    #[test]
    fn a_replay_lands_in_its_own_state_dir() {
        let layout = PodLayout::new(Path::new("/w/data"));
        let original = spec("app", "uid-1", Some("2026-10-19T14:30:05Z"));
        let replayed = original.clone();
        assert_eq!(layout.state_dir(&original), layout.state_dir(&replayed));
    }

    #[test]
    fn an_unstamped_spec_gets_the_placeholder_stamp() {
        let layout = PodLayout::new(Path::new("/w/data"));
        assert_eq!(
            layout.state_dir(&spec("app", "uid-1", None)),
            layout.state_dir_at("app", UNSTAMPED)
        );
    }

    #[test]
    fn lists_incarnations_across_buckets_and_names() {
        let base = tmp("list");
        let layout = PodLayout::new(&base);
        let specs = [
            spec("a", "u1", Some("2026-10-19T14:00:00Z")),
            spec("a", "u2", Some("2026-10-19T15:00:00Z")),
            spec("b", "u3", Some("2026-10-19T14:00:00Z")),
        ];
        for s in &specs {
            std::fs::create_dir_all(layout.state_dir(s)).unwrap();
            std::fs::create_dir_all(layout.run_dir(&PodId::from_spec(s))).unwrap();
        }
        // Stray files are not pod dirs.
        std::fs::write(layout.run_root.join("stray"), b"").unwrap();
        std::fs::write(layout.name_dir("a").join("_stray"), b"").unwrap();

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
