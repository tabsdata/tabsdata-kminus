// Copyright 2026 Tabsdata Inc.

//! Volume resolution — set up volume mounts for containers.
//!
//! Mount paths have already been rewritten to absolute physical paths
//! by [`pod_prepare`](crate::pod::prepare::pod_prepare).
//!
//! Volumes are pod-level: each volume referenced by a `volumeMount` is
//! materialized exactly once under `<pod_dir>/volumes/<volume_name>`, and
//! every container mount path becomes a symlink to that volume directory.
//! This makes volumes shared across containers (e.g. an `emptyDir`
//! written by one container is visible to the others).
//!
//! - `hostPath`: `volumes/<name>` is a symlink to the host path.
//! - `emptyDir`: `volumes/<name>` is an empty directory.
//! - `configMap`: `volumes/<name>` is a directory with one file per
//!   configmap key (filename = key, contents = value). With
//!   `configMap.items` set, only the listed keys are written, each at its
//!   relative `path`. The configmap data must be pre-resolved into
//!   [`ResolvedK8sValues::config_map_volumes`]. The convention is to mount
//!   configmaps at `/configmaps/<configmap-name>` (not enforced).
//! - `projected`: `volumes/<name>` is a directory merging multiple
//!   `configMap` sources flat (respecting each item's `path`). Matching
//!   k8s, keys from later sources overwrite earlier ones on a path
//!   collision — but unlike k8s (which is silent) pod-runner logs a
//!   warning. Only `configMap` sources are supported; other source types
//!   are skipped with a warning. `volumeMount.subPath` is not supported.
//!
//! A mount path that overlaps its own volume directory is never replaced:
//! identical paths are left alone (the mount is already satisfied) and a
//! containment overlap is a [`PrepareError::MountPathOverlap`]. Admission
//! keeps mount paths inside `<pod_dir>/work/<container>` — this is the
//! backstop for a `hostPath` source that reaches into that subtree.
//!
//! Read-only mounts are NOT enforced (mounts are plain symlinks); the
//! pod executor writes a warning at the beginning of the container log
//! for each read-only mount instead (see
//! [`crate::pod::container_log::read_only_mount_warning`]).

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Component, Path, PathBuf};

use tracing::warn;

use crate::pod::prepare::PrepareError;
use crate::pod::resolver::k8s_values::ResolvedK8sValues;
use crate::spec::pod::{KeyToPath, PodSpec, Volume, VolumeMount};
use crate::utils::links::{remove_symlink, symlink_dir};

/// Directory under the pod dir where volumes are materialized:
/// `<pod_dir>/volumes/<volume_name>`.
pub const VOLUMES_DIR: &str = "volumes";

/// Information about a resolved mount.
#[derive(Debug)]
pub struct MountInfo {
    pub volume_name: String,
    pub mount_path: String,
    /// Materialized volume directory (`<pod_dir>/volumes/<volume_name>`)
    /// the mount path symlinks to.
    pub volume_path: String,
    /// For `hostPath` volumes, the host directory; empty otherwise.
    pub host_path: String,
    pub read_only: bool,
}

/// Resolved volumes for all containers.
#[derive(Debug)]
pub struct ResolvedVolumes {
    pub containers: HashMap<String, Vec<MountInfo>>,
}

/// Materialize pod volumes under `<pod_dir>/volumes/` and symlink every
/// container mount path to its volume directory.
///
/// The pod must have been prepared first (paths rewritten) and configmap
/// data referenced by `configMap` volumes must already be present in
/// `resolved.config_map_volumes`. Each volume is materialized once,
/// regardless of how many containers mount it.
pub fn resolve_volumes(
    pod: &PodSpec,
    resolved: &ResolvedK8sValues,
    pod_dir: &Path,
) -> Result<ResolvedVolumes, PrepareError> {
    let volume_map = pod.spec.volume_map();
    let volumes_dir = pod_dir.join(VOLUMES_DIR);
    let mut result = ResolvedVolumes {
        containers: HashMap::new(),
    };
    // Volume name -> materialized volume dir (None for unsupported volume
    // types, which are skipped with a warning).
    let mut materialized: HashMap<String, Option<PathBuf>> = HashMap::new();

    for c in pod.spec.all_containers() {
        let mut mounts = Vec::new();

        for vm in &c.volume_mounts {
            let vol =
                volume_map
                    .get(vm.name.as_str())
                    .ok_or_else(|| PrepareError::UnknownVolume {
                        container: c.name.clone(),
                        volume: vm.name.clone(),
                    })?;

            let vol_dir = match materialized.get(vm.name.as_str()) {
                Some(dir) => dir.clone(),
                None => {
                    let dir = materialize_volume(vol, &volumes_dir, resolved)?;
                    materialized.insert(vm.name.clone(), dir.clone());
                    dir
                }
            };
            let Some(vol_dir) = vol_dir else {
                warn!(
                    container = %c.name, volume = %vm.name,
                    "volume has an unsupported type (only hostPath, emptyDir, \
                     configMap and projected are supported); skipping mount"
                );
                continue;
            };

            link_mount(&c.name, vm, &vol_dir)?;
            mounts.push(MountInfo {
                volume_name: vm.name.clone(),
                mount_path: vm.mount_path.clone(),
                volume_path: vol_dir.to_string_lossy().to_string(),
                host_path: vol
                    .host_path
                    .as_ref()
                    .map(|hp| hp.path.clone())
                    .unwrap_or_default(),
                read_only: vm.read_only,
            });
        }

        if !mounts.is_empty() {
            result.containers.insert(c.name.clone(), mounts);
        }
    }

    Ok(result)
}

/// Materialize a volume at `<volumes_dir>/<volume_name>`.
///
/// Returns the volume directory, or `None` for unsupported volume types.
fn materialize_volume(
    vol: &Volume,
    volumes_dir: &Path,
    resolved: &ResolvedK8sValues,
) -> Result<Option<PathBuf>, PrepareError> {
    let vol_dir = volumes_dir.join(&vol.name);

    if vol.empty_dir.is_some() {
        fs::create_dir_all(&vol_dir).map_err(|source| PrepareError::Io {
            context: format!(
                "Failed to create emptyDir '{}' at {}",
                vol.name,
                vol_dir.display()
            ),
            source,
        })?;
    } else if let Some(ref cm) = vol.config_map {
        let data = resolved.config_map_volumes.get(&cm.name).ok_or_else(|| {
            PrepareError::VolumeConfigMapMissing {
                volume: vol.name.clone(),
                name: cm.name.clone(),
            }
        })?;
        let plan = plan_projection(&[CmSource {
            name: &cm.name,
            items: &cm.items,
            data,
        }]);
        materialize_plan(&vol_dir, &vol.name, &plan, cm.default_mode)?;
    } else if let Some(ref proj) = vol.projected {
        // Gather configMap sources in declared order. Unsupported
        // source types (secret, downwardAPI, serviceAccountToken) are
        // skipped with a warning rather than silently dropped.
        let mut sources = Vec::new();
        for src in &proj.sources {
            match src.config_map {
                Some(ref cm) => {
                    let data = resolved.config_map_volumes.get(&cm.name).ok_or_else(|| {
                        PrepareError::VolumeConfigMapMissing {
                            volume: vol.name.clone(),
                            name: cm.name.clone(),
                        }
                    })?;
                    sources.push(CmSource {
                        name: &cm.name,
                        items: &cm.items,
                        data,
                    });
                }
                None => warn!(
                    volume = %vol.name,
                    "projected volume has an unsupported source type \
                     (only configMap is supported); skipping it"
                ),
            }
        }
        let plan = plan_projection(&sources);
        materialize_plan(&vol_dir, &vol.name, &plan, proj.default_mode)?;
    } else if let Some(ref hp) = vol.host_path {
        let host = &hp.path;
        if !Path::new(host).is_dir() {
            return Err(PrepareError::HostPathMissing {
                volume: vol.name.clone(),
                path: host.clone(),
            });
        }

        fs::create_dir_all(volumes_dir).map_err(|source| PrepareError::Io {
            context: format!("Failed to create volumes dir {}", volumes_dir.display()),
            source,
        })?;
        // Replace a stale symlink (e.g. from an interrupted previous run).
        if vol_dir.is_symlink() {
            remove_symlink(&vol_dir);
        }
        symlink_dir(Path::new(host), &vol_dir).map_err(|source| PrepareError::Io {
            context: format!("Failed to symlink '{}' -> '{}'", vol_dir.display(), host),
            source,
        })?;
    } else {
        return Ok(None);
    }

    Ok(Some(vol_dir))
}

/// Symlink a container mount path to its materialized volume directory.
///
/// The mount path's parent directories are created as needed. A stale
/// symlink or an empty directory already at the mount path (e.g. created
/// by an image template) is replaced; any other existing content is a
/// [`PrepareError::MountPathConflict`].
///
/// A mount path that overlaps the volume directory is never replaced — see
/// [`mount_overlap`]. Replacing it would delete the volume's own source, or
/// leave a symlink pointing at itself through the volume.
fn link_mount(container: &str, vm: &VolumeMount, vol_dir: &Path) -> Result<(), PrepareError> {
    let mount = Path::new(&vm.mount_path);

    match mount_overlap(mount, vol_dir) {
        // The mount path already *is* the volume (a hostPath volume whose
        // source is the mount path itself). The mount is satisfied as it
        // stands: replacing the directory with a symlink to the volume
        // would aim it back at itself and make the path unresolvable.
        MountOverlap::Same => {
            warn!(
                container = %container, volume = %vm.name, path = %vm.mount_path,
                "mount path and volume directory are the same object; the mount is \
                 already satisfied, leaving the existing directory in place"
            );
            return Ok(());
        }
        MountOverlap::Nested => {
            return Err(PrepareError::MountPathOverlap {
                container: container.to_string(),
                volume: vm.name.clone(),
                path: vm.mount_path.clone(),
                volume_path: vol_dir.to_string_lossy().to_string(),
            });
        }
        MountOverlap::Distinct => {}
    }

    if let Some(parent) = mount.parent() {
        fs::create_dir_all(parent).map_err(|source| PrepareError::Io {
            context: format!("Failed to create parent for mount '{}'", vm.name),
            source,
        })?;
    }

    if mount.is_symlink() {
        remove_symlink(mount);
    } else if mount.is_dir() {
        // Only an empty directory may be replaced by the mount symlink.
        fs::remove_dir(mount).map_err(|_| PrepareError::MountPathConflict {
            container: container.to_string(),
            volume: vm.name.clone(),
            path: vm.mount_path.clone(),
        })?;
    } else if mount.exists() {
        return Err(PrepareError::MountPathConflict {
            container: container.to_string(),
            volume: vm.name.clone(),
            path: vm.mount_path.clone(),
        });
    }

    symlink_dir(vol_dir, mount).map_err(|source| PrepareError::Io {
        context: format!(
            "Failed to symlink '{}' -> '{}'",
            vm.mount_path,
            vol_dir.display()
        ),
        source,
    })
}

/// How a container mount path relates to its materialized volume directory.
#[derive(Debug, PartialEq, Eq)]
enum MountOverlap {
    /// Separate objects — the mount can be linked to the volume.
    Distinct,
    /// The same object: the mount path and the volume directory resolve to
    /// one directory on disk.
    Same,
    /// One is reached through the other.
    Nested,
}

/// Classify a mount path against its volume directory.
///
/// A mount is materialized by replacing whatever sits at the mount path with
/// a symlink to the volume directory. That is only safe while the two are
/// separate objects. When they are the same — a `hostPath` volume whose
/// source is the mount path itself — the replacement destroys the source and
/// leaves two symlinks aiming at each other, which no OS can resolve (on
/// Windows: `ERROR_CANT_RESOLVE_FILENAME`). When one contains the other, the
/// replacement either deletes the volume's source or nests the volume inside
/// itself.
///
/// Both sides are compared in canonical form, so the check sees through
/// symlinks — the volume directory of a `hostPath` volume *is* a symlink, and
/// its host source is what matters.
fn mount_overlap(mount: &Path, vol_dir: &Path) -> MountOverlap {
    // Without a canonical form for both sides there is nothing to compare.
    // A mount path that cannot be resolved even partially (e.g. a symlink
    // cycle already in the way) is left to the existing handling below.
    let (Some(mount_real), Some(vol_real)) = (
        resolve_existing_prefix(mount),
        resolve_existing_prefix(vol_dir),
    ) else {
        return MountOverlap::Distinct;
    };

    if mount_real == vol_real {
        MountOverlap::Same
    } else if mount_real.starts_with(&vol_real) || vol_real.starts_with(&mount_real) {
        MountOverlap::Nested
    } else {
        MountOverlap::Distinct
    }
}

/// Canonical form of `path`, resolving as much of it as exists on disk.
///
/// [`fs::canonicalize`] requires the whole path to exist, but a mount path
/// usually does not — it is about to be created. Resolve the deepest existing
/// ancestor and re-append the components below it, so containment can be
/// compared for a path that isn't there yet. Returns `None` when no ancestor
/// resolves at all.
fn resolve_existing_prefix(path: &Path) -> Option<PathBuf> {
    let mut below = Vec::new();
    let mut current = path;
    loop {
        if let Ok(real) = fs::canonicalize(current) {
            let mut resolved = real;
            resolved.extend(below.iter().rev());
            return Some(resolved);
        }
        below.push(current.file_name()?);
        current = current.parent()?;
    }
}

// ---------------------------------------------------------------------------
// ConfigMap / projected materialization
// ---------------------------------------------------------------------------

/// A configMap source contributing files to a configMap or projected volume:
/// the configmap name (for diagnostics), the item selection (empty = all
/// keys), and the resolved key→value data.
struct CmSource<'a> {
    name: &'a str,
    items: &'a [KeyToPath],
    data: &'a HashMap<String, String>,
}

/// A path written by more than one source/key — the later write wins.
#[derive(Debug, PartialEq, Eq)]
struct Collision {
    path: String,
    previous: String,
    winner: String,
}

/// The flat file layout for a (configMap or projected) volume, plus the
/// anomalies worth warning about.
#[derive(Debug, Default)]
struct ProjectionPlan {
    /// Final relative-path → contents. Later sources win on collision.
    files: BTreeMap<String, String>,
    /// Path collisions, in the order they occurred.
    collisions: Vec<Collision>,
    /// `(configmap, key)` pairs named by an item but absent from the data.
    missing_keys: Vec<(String, String)>,
    /// `(configmap, key, path)` for item paths rejected as unsafe.
    unsafe_paths: Vec<(String, String, String)>,
}

/// Whether a relative path is safe to write under the mount directory:
/// non-empty, not absolute, and free of `..` components.
fn is_safe_rel_path(p: &str) -> bool {
    if p.is_empty() {
        return false;
    }
    let path = Path::new(p);
    !path.is_absolute() && !path.components().any(|c| matches!(c, Component::ParentDir))
}

/// Compute the flat file layout produced by merging `sources` into one
/// directory. Pure (no I/O) so the merge/collision rules are unit-testable.
///
/// Sources are processed in order; within a source, when `items` is empty
/// every key is projected at a file named after the key (sorted for
/// determinism), otherwise only the listed keys are projected at their
/// `path`. A path written by a later source/key overrides the earlier one
/// and is recorded as a collision (matching k8s' last-wins behavior).
fn plan_projection(sources: &[CmSource]) -> ProjectionPlan {
    let mut plan = ProjectionPlan::default();
    // rel_path -> "configmap/key" that last wrote it, for collision reports.
    let mut provenance: HashMap<String, String> = HashMap::new();

    for src in sources {
        let pairs: Vec<(String, String)> = if src.items.is_empty() {
            let mut keys: Vec<String> = src.data.keys().cloned().collect();
            keys.sort();
            keys.into_iter().map(|k| (k.clone(), k)).collect()
        } else {
            src.items
                .iter()
                .map(|it| (it.key.clone(), it.path.clone()))
                .collect()
        };

        for (key, rel_path) in pairs {
            if !is_safe_rel_path(&rel_path) {
                plan.unsafe_paths
                    .push((src.name.to_string(), key, rel_path));
                continue;
            }
            let Some(value) = src.data.get(&key) else {
                plan.missing_keys.push((src.name.to_string(), key));
                continue;
            };
            let winner = format!("{}/{}", src.name, key);
            if let Some(previous) = provenance.get(&rel_path) {
                plan.collisions.push(Collision {
                    path: rel_path.clone(),
                    previous: previous.clone(),
                    winner: winner.clone(),
                });
            }
            plan.files.insert(rel_path.clone(), value.clone());
            provenance.insert(rel_path, winner);
        }
    }
    plan
}

/// Create the volume directory and write every planned file (creating parent
/// directories for nested item paths). Logs a warning for each collision,
/// missing key, and unsafe path.
fn materialize_plan(
    vol_dir: &Path,
    volume_name: &str,
    plan: &ProjectionPlan,
    default_mode: Option<u32>,
) -> Result<(), PrepareError> {
    fs::create_dir_all(vol_dir).map_err(|source| PrepareError::Io {
        context: format!(
            "Failed to create volume dir '{volume_name}' at {}",
            vol_dir.display()
        ),
        source,
    })?;

    for (cm, key, path) in &plan.unsafe_paths {
        warn!(
            volume = %volume_name, config_map = %cm, key = %key, path = %path,
            "projected volume item path is unsafe (absolute or contains '..'); skipping"
        );
    }
    for (cm, key) in &plan.missing_keys {
        warn!(
            volume = %volume_name, config_map = %cm, key = %key,
            "projected volume item references a key absent from the configMap; skipping"
        );
    }
    for c in &plan.collisions {
        warn!(
            volume = %volume_name, path = %c.path, winner = %c.winner, overwrote = %c.previous,
            "projected volume path collision: '{}' from {} overwrites the value from {} (last source wins)",
            c.path, c.winner, c.previous
        );
    }

    for (rel_path, contents) in &plan.files {
        let file_path = vol_dir.join(rel_path);
        if let Some(parent) = file_path.parent() {
            fs::create_dir_all(parent).map_err(|source| PrepareError::Io {
                context: format!("Failed to create dir for '{}'", file_path.display()),
                source,
            })?;
        }
        fs::write(&file_path, contents).map_err(|source| PrepareError::Io {
            context: format!("Failed to write volume file '{}'", file_path.display()),
            source,
        })?;
        apply_default_mode(&file_path, default_mode)?;
    }
    Ok(())
}

/// Apply a volume's `defaultMode` to a just-written file. Kubernetes expresses
/// the mode as Unix permission bits, so on Windows there is nothing to apply.
#[cfg(unix)]
fn apply_default_mode(file_path: &Path, default_mode: Option<u32>) -> Result<(), PrepareError> {
    use std::os::unix::fs::PermissionsExt;

    let Some(mode) = default_mode else {
        return Ok(());
    };
    fs::set_permissions(file_path, fs::Permissions::from_mode(mode)).map_err(|source| {
        PrepareError::Io {
            context: format!(
                "Failed to set mode {mode:04o} on volume file '{}'",
                file_path.display()
            ),
            source,
        }
    })
}

#[cfg(windows)]
fn apply_default_mode(_file_path: &Path, _default_mode: Option<u32>) -> Result<(), PrepareError> {
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::*;
    use std::fs;

    fn setup_dir(name: &str) -> std::path::PathBuf {
        crate::testutils::unique_test_dir(&format!("pod/resolver/volumes/{name}"))
    }

    fn make_pod(containers: Vec<Container>, volumes: Vec<Volume>) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                name: "test".to_string(),
                namespace: "default".to_string(),
                uid: "uid-1".to_string(),
                ..Default::default()
            },
            spec: PodSpecBody {
                containers,
                volumes,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn empty_dir_creates_directory_under_volumes_and_symlinks_mount() {
        let base = setup_dir("empty-dir");
        let mount_path = base.join("work").join("app").join("data");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "data".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "data".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            }],
        );

        let result = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();

        // The volume is an empty directory under <pod_dir>/volumes/.
        let vol_dir = base.join(VOLUMES_DIR).join("data");
        assert!(vol_dir.is_dir());
        assert!(!vol_dir.is_symlink());
        assert!(fs::read_dir(&vol_dir).unwrap().next().is_none());

        // The container mount is a symlink to the volume dir.
        assert!(mount_path.is_symlink());
        assert_eq!(fs::read_link(&mount_path).unwrap(), vol_dir);

        // Writing through the mount lands in the volume dir.
        fs::write(mount_path.join("f.txt"), "x").unwrap();
        assert_eq!(fs::read_to_string(vol_dir.join("f.txt")).unwrap(), "x");

        assert_eq!(result.containers["app"].len(), 1);
        assert_eq!(result.containers["app"][0].volume_name, "data");
        assert_eq!(
            result.containers["app"][0].volume_path,
            vol_dir.to_string_lossy()
        );
    }

    #[test]
    fn empty_dir_is_shared_between_containers() {
        let base = setup_dir("empty-dir-shared");
        let mount_a = base.join("work").join("a").join("shared");
        let mount_b = base.join("work").join("b").join("data");

        let container = |name: &str, mount: &Path| Container {
            name: name.to_string(),
            volume_mounts: vec![VolumeMount {
                name: "shared".to_string(),
                mount_path: mount.to_string_lossy().to_string(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let pod = make_pod(
            vec![container("a", &mount_a), container("b", &mount_b)],
            vec![Volume {
                name: "shared".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            }],
        );

        resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();

        // A write through one container's mount is visible through the other.
        fs::write(mount_a.join("marker.txt"), "from-a").unwrap();
        assert_eq!(
            fs::read_to_string(mount_b.join("marker.txt")).unwrap(),
            "from-a"
        );
    }

    #[test]
    fn host_path_volume_is_symlink_to_host() {
        let base = setup_dir("host-path");
        let host_dir = base.join("host-data");
        fs::create_dir_all(&host_dir).unwrap();
        fs::write(host_dir.join("file.txt"), "hello").unwrap();

        let mount_path = base.join("work").join("app").join("mounted");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "vol".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    read_only: true,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "vol".to_string(),
                host_path: Some(HostPathVolume {
                    path: host_dir.to_string_lossy().to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );

        let result = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();

        // The volume is a symlink to the host path.
        let vol_dir = base.join(VOLUMES_DIR).join("vol");
        assert!(vol_dir.is_symlink());
        assert_eq!(fs::read_link(&vol_dir).unwrap(), host_dir);

        // The container mount is a symlink to the volume dir.
        assert!(mount_path.is_symlink());
        assert_eq!(fs::read_link(&mount_path).unwrap(), vol_dir);
        assert_eq!(
            fs::read_to_string(mount_path.join("file.txt")).unwrap(),
            "hello"
        );

        assert_eq!(
            result.containers["app"][0].host_path,
            host_dir.to_string_lossy()
        );
        assert_eq!(
            result.containers["app"][0].volume_path,
            vol_dir.to_string_lossy()
        );
        assert!(result.containers["app"][0].read_only);
    }

    #[test]
    fn host_path_missing_errors() {
        let base = setup_dir("host-missing");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "vol".to_string(),
                    mount_path: base.join("mount").to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "vol".to_string(),
                host_path: Some(HostPathVolume {
                    path: "/nonexistent/host/path".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();
        assert!(matches!(err, PrepareError::HostPathMissing { .. }));
    }

    #[test]
    fn unknown_volume_errors() {
        let base = setup_dir("unknown-volume");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "missing".to_string(),
                    mount_path: "/tmp/x".to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();
        assert!(
            matches!(err, PrepareError::UnknownVolume { ref volume, .. } if volume == "missing")
        );
    }

    #[test]
    fn mount_path_with_nonempty_dir_errors() {
        let base = setup_dir("mount-conflict");
        let mount_path = base.join("work").join("app").join("data");
        fs::create_dir_all(&mount_path).unwrap();
        fs::write(mount_path.join("existing.txt"), "from-image").unwrap();

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "data".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "data".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            }],
        );

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();
        assert!(
            matches!(err, PrepareError::MountPathConflict { ref volume, .. } if volume == "data")
        );
    }

    #[test]
    fn mount_path_with_empty_dir_is_replaced_by_symlink() {
        let base = setup_dir("mount-empty-dir");
        let mount_path = base.join("work").join("app").join("data");
        fs::create_dir_all(&mount_path).unwrap();

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "data".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "data".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            }],
        );

        resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();
        assert!(mount_path.is_symlink());
        assert_eq!(
            fs::read_link(&mount_path).unwrap(),
            base.join(VOLUMES_DIR).join("data")
        );
    }

    #[test]
    fn stale_mount_symlink_is_replaced() {
        let base = setup_dir("mount-stale-symlink");
        let mount_path = base.join("work").join("app").join("data");
        let stale_target = base.join("stale-target");
        fs::create_dir_all(&stale_target).unwrap();
        fs::create_dir_all(mount_path.parent().unwrap()).unwrap();
        symlink_dir(&stale_target, &mount_path).unwrap();

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "data".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "data".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            }],
        );

        resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();
        assert_eq!(
            fs::read_link(&mount_path).unwrap(),
            base.join(VOLUMES_DIR).join("data")
        );
    }

    // -- mount path / volume directory overlap --

    fn host_path_pod(mount_path: &Path, host_dir: &Path) -> PodSpec {
        make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "runtimes".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "runtimes".to_string(),
                host_path: Some(HostPathVolume {
                    path: host_dir.to_string_lossy().to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        )
    }

    #[test]
    fn mount_path_equal_to_host_source_is_left_intact() {
        // The pymanager regression: a hostPath volume mounted at its own
        // source. Replacing the mount would delete the host directory and
        // leave it symlinked to a volume that symlinks back — unresolvable.
        let base = setup_dir("mount-equals-source");
        let host_dir = base.join("instance").join("runtimes");
        fs::create_dir_all(&host_dir).unwrap();
        fs::write(host_dir.join("keep.txt"), "precious").unwrap();

        let pod = host_path_pod(&host_dir, &host_dir);
        resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();

        // The host directory survives as a real directory, contents intact.
        assert!(host_dir.is_dir());
        assert!(!host_dir.is_symlink());
        assert_eq!(
            fs::read_to_string(host_dir.join("keep.txt")).unwrap(),
            "precious"
        );
        // And it is still reachable through the volume directory.
        let vol_dir = base.join(VOLUMES_DIR).join("runtimes");
        assert!(vol_dir.is_symlink());
        assert_eq!(
            fs::read_to_string(vol_dir.join("keep.txt")).unwrap(),
            "precious"
        );
    }

    #[test]
    fn mount_path_equal_to_empty_host_source_is_left_intact() {
        // Same shape, but with an empty source — the case that actually
        // slipped through, since `remove_dir` only succeeds when empty.
        let base = setup_dir("mount-equals-empty-source");
        let host_dir = base.join("instance").join("runtimes");
        fs::create_dir_all(&host_dir).unwrap();

        let pod = host_path_pod(&host_dir, &host_dir);
        resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();

        assert!(host_dir.is_dir());
        assert!(!host_dir.is_symlink());
        // Writing through the mount path lands in a real directory.
        fs::write(host_dir.join("built.txt"), "x").unwrap();
        assert_eq!(
            fs::read_to_string(base.join(VOLUMES_DIR).join("runtimes").join("built.txt")).unwrap(),
            "x"
        );
    }

    #[test]
    fn mount_path_containing_host_source_errors() {
        // The host source lives under the mount path: replacing the mount
        // would delete the volume's own source.
        let base = setup_dir("mount-contains-source");
        let mount_path = base.join("work").join("app").join("mounted");
        let host_dir = mount_path.join("inner");
        fs::create_dir_all(&host_dir).unwrap();

        let pod = host_path_pod(&mount_path, &host_dir);
        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();

        assert!(
            matches!(err, PrepareError::MountPathOverlap { ref volume, .. } if volume == "runtimes"),
            "got: {err}"
        );
        // Nothing was removed on the way to the error.
        assert!(host_dir.is_dir());
        assert!(mount_path.is_dir());
        assert!(!mount_path.is_symlink());
    }

    #[test]
    fn mount_path_under_host_source_errors() {
        // The reverse containment, and the one a pod def can express today:
        // a hostPath source that is an ancestor of the pod dir (`hostPath: /`
        // is a common k8s pattern for node agents). Linking the mount would
        // aim it at a directory that contains it — every write through the
        // mount escapes into the host tree, and the path recurses without
        // bound.
        let base = setup_dir("mount-under-source");
        let mount_path = base.join("work").join("app").join("mnt");
        fs::create_dir_all(&mount_path).unwrap();

        let pod = host_path_pod(&mount_path, &base);
        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();

        assert!(
            matches!(err, PrepareError::MountPathOverlap { .. }),
            "got: {err}"
        );
        // The mount path is still a plain directory, not a door to the host.
        assert!(mount_path.is_dir());
        assert!(!mount_path.is_symlink());
    }

    #[test]
    fn mount_path_inside_volume_dir_errors() {
        // The mount path points inside the volume directory itself, which
        // would nest the volume in itself. The mount path does not exist
        // yet, so this only trips if partially-resolved paths are compared.
        let base = setup_dir("mount-inside-volume");
        let mount_path = base.join(VOLUMES_DIR).join("data").join("sub");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "data".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "data".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            }],
        );

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();
        assert!(
            matches!(err, PrepareError::MountPathOverlap { .. }),
            "got: {err}"
        );
        assert!(!mount_path.exists());
    }

    #[test]
    fn mount_overlap_classifies_distinct_paths() {
        // The ordinary layout: a mount under the container work dir and a
        // volume under <pod_dir>/volumes are unrelated.
        let base = setup_dir("mount-overlap-distinct");
        let vol_dir = base.join(VOLUMES_DIR).join("data");
        let mount = base.join("work").join("app").join("data");
        fs::create_dir_all(&vol_dir).unwrap();
        fs::create_dir_all(mount.parent().unwrap()).unwrap();

        assert_eq!(mount_overlap(&mount, &vol_dir), MountOverlap::Distinct);
        assert_eq!(mount_overlap(&vol_dir, &vol_dir), MountOverlap::Same);
        assert_eq!(
            mount_overlap(&vol_dir.join("nested"), &vol_dir),
            MountOverlap::Nested
        );
    }

    #[test]
    fn mount_overlap_sees_through_symlinks() {
        // A hostPath volume dir is a symlink; the comparison must resolve it
        // rather than compare link paths.
        let base = setup_dir("mount-overlap-symlink");
        let host_dir = base.join("host-data");
        let vol_dir = base.join(VOLUMES_DIR).join("vol");
        fs::create_dir_all(&host_dir).unwrap();
        fs::create_dir_all(vol_dir.parent().unwrap()).unwrap();
        symlink_dir(&host_dir, &vol_dir).unwrap();

        assert_eq!(mount_overlap(&host_dir, &vol_dir), MountOverlap::Same);
        assert_eq!(
            mount_overlap(&host_dir.join("under"), &vol_dir),
            MountOverlap::Nested
        );
        assert_eq!(
            mount_overlap(&base.join("elsewhere"), &vol_dir),
            MountOverlap::Distinct
        );
    }

    #[test]
    fn config_map_volume_creates_files_per_key() {
        let base = setup_dir("cm-vol");
        let mount_path = base.join("work").join("app").join("etc").join("app");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "config".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    read_only: true,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "config".to_string(),
                config_map: Some(ConfigMapVolume {
                    name: "app-config".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );

        let mut resolved = ResolvedK8sValues::default();
        resolved.config_map_volumes.insert(
            "app-config".to_string(),
            [
                ("app.conf".to_string(), "setting=value".to_string()),
                ("logging.yaml".to_string(), "level: info".to_string()),
            ]
            .into_iter()
            .collect(),
        );

        let result = resolve_volumes(&pod, &resolved, &base).unwrap();

        // Files are materialized under <pod_dir>/volumes/<volume_name>.
        let vol_dir = base.join(VOLUMES_DIR).join("config");
        assert!(vol_dir.is_dir());

        // The container mount is a symlink to the volume dir.
        assert!(mount_path.is_symlink());
        assert_eq!(fs::read_link(&mount_path).unwrap(), vol_dir);
        assert_eq!(
            fs::read_to_string(mount_path.join("app.conf")).unwrap(),
            "setting=value"
        );
        assert_eq!(
            fs::read_to_string(mount_path.join("logging.yaml")).unwrap(),
            "level: info"
        );
        assert_eq!(result.containers["app"][0].volume_name, "config");
        assert!(result.containers["app"][0].read_only);
    }

    #[cfg(unix)]
    #[test]
    fn config_map_volume_default_mode_is_applied() {
        use std::os::unix::fs::PermissionsExt;

        let base = setup_dir("cm-vol-mode");
        let mount_path = base.join("mount");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "assets".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "assets".to_string(),
                config_map: Some(ConfigMapVolume {
                    name: "app-assets".to_string(),
                    default_mode: Some(0o755),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );

        let mut resolved = ResolvedK8sValues::default();
        resolved.config_map_volumes.insert(
            "app-assets".to_string(),
            [("build.sh".to_string(), "#!/usr/bin/env bash\n".to_string())]
                .into_iter()
                .collect(),
        );

        resolve_volumes(&pod, &resolved, &base).unwrap();

        let script = base.join(VOLUMES_DIR).join("assets").join("build.sh");
        let mode = fs::metadata(&script).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o755);
    }

    #[cfg(unix)]
    #[test]
    fn config_map_volume_without_default_mode_keeps_os_default() {
        use std::os::unix::fs::PermissionsExt;

        let base = setup_dir("cm-vol-no-mode");
        let mount_path = base.join("mount");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "assets".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "assets".to_string(),
                config_map: Some(ConfigMapVolume {
                    name: "app-assets".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );

        let mut resolved = ResolvedK8sValues::default();
        resolved.config_map_volumes.insert(
            "app-assets".to_string(),
            [("plain.txt".to_string(), "text".to_string())]
                .into_iter()
                .collect(),
        );

        resolve_volumes(&pod, &resolved, &base).unwrap();

        let file = base.join(VOLUMES_DIR).join("assets").join("plain.txt");
        let mode = fs::metadata(&file).unwrap().permissions().mode();
        assert_eq!(mode & 0o111, 0);
    }

    #[test]
    fn config_map_volume_missing_data_errors() {
        let base = setup_dir("cm-vol-missing");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "config".to_string(),
                    mount_path: base.join("mount").to_string_lossy().to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "config".to_string(),
                config_map: Some(ConfigMapVolume {
                    name: "app-config".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        );

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();
        assert!(matches!(err, PrepareError::VolumeConfigMapMissing { .. }));
    }

    #[test]
    fn no_volumes_is_ok() {
        let base = setup_dir("no-volumes");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                ..Default::default()
            }],
            vec![],
        );

        let result = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap();
        assert!(result.containers.is_empty());
        assert!(!base.join(VOLUMES_DIR).exists());
    }

    // -- projected / configMap items: pure planner --

    fn cm(name: &str, items: Vec<(&str, &str)>) -> ConfigMapVolume {
        ConfigMapVolume {
            name: name.to_string(),
            items: items
                .into_iter()
                .map(|(k, p)| KeyToPath {
                    key: k.to_string(),
                    path: p.to_string(),
                })
                .collect(),
            ..Default::default()
        }
    }

    fn data(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn plan_projection_all_keys_when_no_items() {
        let d = data(&[("a", "1"), ("b", "2")]);
        let plan = plan_projection(&[CmSource {
            name: "cm",
            items: &[],
            data: &d,
        }]);
        assert_eq!(plan.files.get("a").map(String::as_str), Some("1"));
        assert_eq!(plan.files.get("b").map(String::as_str), Some("2"));
        assert!(plan.collisions.is_empty());
    }

    #[test]
    fn plan_projection_items_select_and_rename() {
        let d = data(&[("apiserver.yaml", "A"), ("ignored", "X")]);
        let items = vec![KeyToPath {
            key: "apiserver.yaml".to_string(),
            path: "apiserver/apiserver.yaml".to_string(),
        }];
        let plan = plan_projection(&[CmSource {
            name: "cm",
            items: &items,
            data: &d,
        }]);
        assert_eq!(plan.files.len(), 1);
        assert_eq!(
            plan.files
                .get("apiserver/apiserver.yaml")
                .map(String::as_str),
            Some("A")
        );
    }

    #[test]
    fn plan_projection_last_source_wins_on_collision() {
        let d1 = data(&[("k", "first")]);
        let d2 = data(&[("k", "second")]);
        let i1 = vec![KeyToPath {
            key: "k".into(),
            path: "shared.yaml".into(),
        }];
        let i2 = vec![KeyToPath {
            key: "k".into(),
            path: "shared.yaml".into(),
        }];
        let plan = plan_projection(&[
            CmSource {
                name: "cm1",
                items: &i1,
                data: &d1,
            },
            CmSource {
                name: "cm2",
                items: &i2,
                data: &d2,
            },
        ]);
        assert_eq!(
            plan.files.get("shared.yaml").map(String::as_str),
            Some("second")
        );
        assert_eq!(plan.collisions.len(), 1);
        assert_eq!(plan.collisions[0].path, "shared.yaml");
        assert_eq!(plan.collisions[0].previous, "cm1/k");
        assert_eq!(plan.collisions[0].winner, "cm2/k");
    }

    #[test]
    fn plan_projection_missing_key_and_unsafe_path() {
        let d = data(&[("present", "v")]);
        let items = vec![
            KeyToPath {
                key: "absent".into(),
                path: "a.yaml".into(),
            },
            KeyToPath {
                key: "present".into(),
                path: "../escape.yaml".into(),
            },
        ];
        let plan = plan_projection(&[CmSource {
            name: "cm",
            items: &items,
            data: &d,
        }]);
        assert!(plan.files.is_empty());
        assert_eq!(
            plan.missing_keys,
            vec![("cm".to_string(), "absent".to_string())]
        );
        assert_eq!(
            plan.unsafe_paths,
            vec![(
                "cm".to_string(),
                "present".to_string(),
                "../escape.yaml".to_string()
            )]
        );
    }

    // -- projected / configMap items: end-to-end materialization --

    fn projected_pod(mount_path: &Path, sources: Vec<VolumeProjection>) -> PodSpec {
        make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "cfg".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    read_only: true,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "cfg".to_string(),
                projected: Some(ProjectedVolume {
                    sources,
                    ..Default::default()
                }),
                ..Default::default()
            }],
        )
    }

    #[test]
    fn projected_volume_merges_sources_flat() {
        let base = setup_dir("projected");
        let mount_path = base.join("work").join("app").join("etc");
        let pod = projected_pod(
            &mount_path,
            vec![
                VolumeProjection {
                    config_map: Some(cm(
                        "cm-a",
                        vec![("apiserver.yaml", "apiserver/apiserver.yaml")],
                    )),
                    ..Default::default()
                },
                VolumeProjection {
                    config_map: Some(cm(
                        "cm-b",
                        vec![("apiserver.log.yaml", "apiserver/apiserver.log.yaml")],
                    )),
                    ..Default::default()
                },
            ],
        );

        let mut resolved = ResolvedK8sValues::default();
        resolved
            .config_map_volumes
            .insert("cm-a".to_string(), data(&[("apiserver.yaml", "A")]));
        resolved
            .config_map_volumes
            .insert("cm-b".to_string(), data(&[("apiserver.log.yaml", "B")]));

        let result = resolve_volumes(&pod, &resolved, &base).unwrap();
        assert!(mount_path.is_symlink());
        assert_eq!(
            fs::read_link(&mount_path).unwrap(),
            base.join(VOLUMES_DIR).join("cfg")
        );
        assert_eq!(
            fs::read_to_string(mount_path.join("apiserver/apiserver.yaml")).unwrap(),
            "A"
        );
        assert_eq!(
            fs::read_to_string(mount_path.join("apiserver/apiserver.log.yaml")).unwrap(),
            "B"
        );
        assert!(result.containers["app"][0].read_only);
    }

    #[test]
    fn projected_volume_collision_last_source_wins_on_disk() {
        let base = setup_dir("projected-collision");
        let mount_path = base.join("work").join("app").join("etc");
        let pod = projected_pod(
            &mount_path,
            vec![
                VolumeProjection {
                    config_map: Some(cm("cm-a", vec![("v", "shared.yaml")])),
                    ..Default::default()
                },
                VolumeProjection {
                    config_map: Some(cm("cm-b", vec![("v", "shared.yaml")])),
                    ..Default::default()
                },
            ],
        );
        let mut resolved = ResolvedK8sValues::default();
        resolved
            .config_map_volumes
            .insert("cm-a".to_string(), data(&[("v", "from-a")]));
        resolved
            .config_map_volumes
            .insert("cm-b".to_string(), data(&[("v", "from-b")]));

        resolve_volumes(&pod, &resolved, &base).unwrap();
        // Last source (cm-b) wins; pod-runner logs a warning (see materialize_plan).
        assert_eq!(
            fs::read_to_string(mount_path.join("shared.yaml")).unwrap(),
            "from-b"
        );
    }

    #[test]
    fn projected_volume_missing_configmap_data_errors() {
        let base = setup_dir("projected-missing");
        let mount_path = base.join("work").join("app").join("etc");
        let pod = projected_pod(
            &mount_path,
            vec![VolumeProjection {
                config_map: Some(cm("absent-cm", vec![("k", "k.yaml")])),
                ..Default::default()
            }],
        );
        let err = resolve_volumes(&pod, &ResolvedK8sValues::default(), &base).unwrap_err();
        assert!(matches!(err, PrepareError::VolumeConfigMapMissing { .. }));
    }

    #[test]
    fn config_map_volume_items_select_keys() {
        let base = setup_dir("cm-items");
        let mount_path = base.join("work").join("app").join("etc");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "cfg".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    read_only: true,
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "cfg".to_string(),
                config_map: Some(cm("app-config", vec![("a.conf", "nested/a.conf")])),
                ..Default::default()
            }],
        );
        let mut resolved = ResolvedK8sValues::default();
        resolved.config_map_volumes.insert(
            "app-config".to_string(),
            data(&[("a.conf", "AA"), ("b.conf", "BB")]),
        );

        resolve_volumes(&pod, &resolved, &base).unwrap();
        assert_eq!(
            fs::read_to_string(mount_path.join("nested/a.conf")).unwrap(),
            "AA"
        );
        // b.conf was not selected by items.
        assert!(!mount_path.join("b.conf").exists());
    }
}
