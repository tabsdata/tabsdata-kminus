// Copyright 2026 Tabsdata Inc.

//! Volume resolution — set up volume mounts for containers.
//!
//! Mount paths have already been rewritten to absolute physical paths
//! by [`pod_prepare`](crate::pod::prepare::pod_prepare).
//!
//! - `emptyDir`: creates an empty directory at the mount path.
//! - `hostPath`: creates a symlink from the mount path to the host path.
//! - `configMap`: creates a directory at the mount path and writes one file
//!   per configmap key (filename = key, contents = value). With
//!   `configMap.items` set, only the listed keys are written, each at its
//!   relative `path`. The configmap data must be pre-resolved into
//!   [`ResolvedK8sValues::config_map_volumes`]. The convention is to mount
//!   configmaps at `/configmaps/<configmap-name>` (not enforced).
//! - `projected`: merges multiple `configMap` sources flat into the single
//!   mount directory (respecting each item's `path`). Matching k8s, keys from
//!   later sources overwrite earlier ones on a path collision — but unlike
//!   k8s (which is silent) pod-runner logs a warning. Only `configMap`
//!   sources are supported; other source types are skipped with a warning.
//!   `volumeMount.subPath` is not supported.

use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Component, Path};

use tracing::warn;

use crate::pod::prepare::PrepareError;
use crate::pod::resolver::k8s_values::ResolvedK8sValues;
use crate::spec::pod::{KeyToPath, PodSpec};

/// Information about a resolved mount.
#[derive(Debug)]
pub struct MountInfo {
    pub volume_name: String,
    pub mount_path: String,
    pub host_path: String,
    pub read_only: bool,
}

/// Resolved volumes for all containers.
#[derive(Debug)]
pub struct ResolvedVolumes {
    pub containers: HashMap<String, Vec<MountInfo>>,
}

/// Create symlinks/directories/configmap files for all volume mounts.
///
/// The pod must have been prepared first (paths rewritten) and configmap
/// data referenced by `configMap` volumes must already be present in
/// `resolved.config_map_volumes`.
pub fn resolve_volumes(
    pod: &PodSpec,
    resolved: &ResolvedK8sValues,
) -> Result<ResolvedVolumes, PrepareError> {
    let volume_map = pod.spec.volume_map();
    let mut result = ResolvedVolumes {
        containers: HashMap::new(),
    };

    for c in pod.spec.all_containers() {
        let mut mounts = Vec::new();

        for vm in &c.volume_mounts {
            let vol = volume_map.get(vm.name.as_str()).ok_or_else(|| {
                PrepareError::UnknownVolume {
                    container: c.name.clone(),
                    volume: vm.name.clone(),
                }
            })?;

            if vol.empty_dir.is_some() {
                fs::create_dir_all(&vm.mount_path).map_err(|source| PrepareError::Io {
                    context: format!(
                        "Failed to create emptyDir '{}' at {}",
                        vm.name, vm.mount_path
                    ),
                    source,
                })?;
                mounts.push(MountInfo {
                    volume_name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                    host_path: String::new(),
                    read_only: false,
                });
            } else if let Some(ref cm) = vol.config_map {
                let data = resolved.config_map_volumes.get(&cm.name).ok_or_else(|| {
                    PrepareError::VolumeConfigMapMissing {
                        volume: vm.name.clone(),
                        name: cm.name.clone(),
                    }
                })?;
                let plan = plan_projection(&[CmSource {
                    name: &cm.name,
                    items: &cm.items,
                    data,
                }]);
                materialize_plan(&vm.mount_path, &vm.name, &plan)?;
                mounts.push(MountInfo {
                    volume_name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                    host_path: String::new(),
                    read_only: vm.read_only,
                });
            } else if let Some(ref proj) = vol.projected {
                // Gather configMap sources in declared order. Unsupported
                // source types (secret, downwardAPI, serviceAccountToken) are
                // skipped with a warning rather than silently dropped.
                let mut sources = Vec::new();
                for src in &proj.sources {
                    match src.config_map {
                        Some(ref cm) => {
                            let data =
                                resolved.config_map_volumes.get(&cm.name).ok_or_else(|| {
                                    PrepareError::VolumeConfigMapMissing {
                                        volume: vm.name.clone(),
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
                            volume = %vm.name,
                            "projected volume has an unsupported source type \
                             (only configMap is supported); skipping it"
                        ),
                    }
                }
                let plan = plan_projection(&sources);
                materialize_plan(&vm.mount_path, &vm.name, &plan)?;
                mounts.push(MountInfo {
                    volume_name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                    host_path: String::new(),
                    read_only: vm.read_only,
                });
            } else if let Some(ref hp) = vol.host_path {
                let host = &hp.path;
                if !Path::new(host).is_dir() {
                    return Err(PrepareError::HostPathMissing {
                        volume: vm.name.clone(),
                        path: host.clone(),
                    });
                }

                // Create parent dir for the symlink
                if let Some(parent) = Path::new(&vm.mount_path).parent() {
                    fs::create_dir_all(parent).map_err(|source| PrepareError::Io {
                        context: format!("Failed to create parent for mount '{}'", vm.name),
                        source,
                    })?;
                }

                // Remove existing symlink
                let mount = Path::new(&vm.mount_path);
                if mount.is_symlink() {
                    let _ = fs::remove_file(mount);
                }

                #[cfg(unix)]
                std::os::unix::fs::symlink(host, &vm.mount_path).map_err(|source| {
                    PrepareError::Io {
                        context: format!("Failed to symlink '{}' -> '{}'", vm.mount_path, host),
                        source,
                    }
                })?;

                #[cfg(windows)]
                std::os::windows::fs::symlink_dir(host, &vm.mount_path).map_err(|source| {
                    PrepareError::Io {
                        context: format!("Failed to symlink '{}' -> '{}'", vm.mount_path, host),
                        source,
                    }
                })?;

                mounts.push(MountInfo {
                    volume_name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                    host_path: host.clone(),
                    read_only: vm.read_only,
                });
            }
        }

        if !mounts.is_empty() {
            result.containers.insert(c.name.clone(), mounts);
        }
    }

    Ok(result)
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

/// Create the mount directory and write every planned file (creating parent
/// directories for nested item paths). Logs a warning for each collision,
/// missing key, and unsafe path.
fn materialize_plan(
    mount_path: &str,
    volume_name: &str,
    plan: &ProjectionPlan,
) -> Result<(), PrepareError> {
    fs::create_dir_all(mount_path).map_err(|source| PrepareError::Io {
        context: format!("Failed to create volume dir '{volume_name}' at {mount_path}"),
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
        let file_path = Path::new(mount_path).join(rel_path);
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
    }
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
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-volumes-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
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
    fn empty_dir_creates_directory() {
        let base = setup_dir("empty-dir");
        let mount_path = base.join("app").join("data");

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

        let result = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap();
        assert!(mount_path.is_dir());
        assert_eq!(result.containers["app"].len(), 1);
        assert_eq!(result.containers["app"][0].volume_name, "data");
    }

    #[test]
    fn host_path_creates_symlink() {
        let base = setup_dir("host-path");
        let host_dir = base.join("host-data");
        fs::create_dir_all(&host_dir).unwrap();
        fs::write(host_dir.join("file.txt"), "hello").unwrap();

        let mount_path = base.join("app").join("mounted");

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

        let result = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap();
        assert!(mount_path.is_symlink());
        assert_eq!(
            fs::read_to_string(mount_path.join("file.txt")).unwrap(),
            "hello"
        );
        assert_eq!(result.containers["app"][0].host_path, host_dir.to_string_lossy());
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

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap_err();
        assert!(matches!(err, PrepareError::HostPathMissing { .. }));
    }

    #[test]
    fn unknown_volume_errors() {
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

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap_err();
        assert!(matches!(err, PrepareError::UnknownVolume { ref volume, .. } if volume == "missing"));
    }

    #[test]
    fn config_map_volume_creates_files_per_key() {
        let base = setup_dir("cm-vol");
        let mount_path = base.join("app").join("etc").join("app");

        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "config".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    read_only: true,
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

        let result = resolve_volumes(&pod, &resolved).unwrap();
        assert!(mount_path.is_dir());
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

        let err = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap_err();
        assert!(matches!(err, PrepareError::VolumeConfigMapMissing { .. }));
    }

    #[test]
    fn no_volumes_is_ok() {
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                ..Default::default()
            }],
            vec![],
        );

        let result = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap();
        assert!(result.containers.is_empty());
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
            plan.files.get("apiserver/apiserver.yaml").map(String::as_str),
            Some("A")
        );
    }

    #[test]
    fn plan_projection_last_source_wins_on_collision() {
        let d1 = data(&[("k", "first")]);
        let d2 = data(&[("k", "second")]);
        let i1 = vec![KeyToPath { key: "k".into(), path: "shared.yaml".into() }];
        let i2 = vec![KeyToPath { key: "k".into(), path: "shared.yaml".into() }];
        let plan = plan_projection(&[
            CmSource { name: "cm1", items: &i1, data: &d1 },
            CmSource { name: "cm2", items: &i2, data: &d2 },
        ]);
        assert_eq!(plan.files.get("shared.yaml").map(String::as_str), Some("second"));
        assert_eq!(plan.collisions.len(), 1);
        assert_eq!(plan.collisions[0].path, "shared.yaml");
        assert_eq!(plan.collisions[0].previous, "cm1/k");
        assert_eq!(plan.collisions[0].winner, "cm2/k");
    }

    #[test]
    fn plan_projection_missing_key_and_unsafe_path() {
        let d = data(&[("present", "v")]);
        let items = vec![
            KeyToPath { key: "absent".into(), path: "a.yaml".into() },
            KeyToPath { key: "present".into(), path: "../escape.yaml".into() },
        ];
        let plan = plan_projection(&[CmSource { name: "cm", items: &items, data: &d }]);
        assert!(plan.files.is_empty());
        assert_eq!(plan.missing_keys, vec![("cm".to_string(), "absent".to_string())]);
        assert_eq!(
            plan.unsafe_paths,
            vec![("cm".to_string(), "present".to_string(), "../escape.yaml".to_string())]
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
                }],
                ..Default::default()
            }],
            vec![Volume {
                name: "cfg".to_string(),
                projected: Some(ProjectedVolume { sources }),
                ..Default::default()
            }],
        )
    }

    #[test]
    fn projected_volume_merges_sources_flat() {
        let base = setup_dir("projected");
        let mount_path = base.join("app").join("etc");
        let pod = projected_pod(
            &mount_path,
            vec![
                VolumeProjection {
                    config_map: Some(cm("cm-a", vec![("apiserver.yaml", "apiserver/apiserver.yaml")])),
                    ..Default::default()
                },
                VolumeProjection {
                    config_map: Some(cm("cm-b", vec![("apiserver.log.yaml", "apiserver/apiserver.log.yaml")])),
                    ..Default::default()
                },
            ],
        );

        let mut resolved = ResolvedK8sValues::default();
        resolved.config_map_volumes.insert("cm-a".to_string(), data(&[("apiserver.yaml", "A")]));
        resolved.config_map_volumes.insert("cm-b".to_string(), data(&[("apiserver.log.yaml", "B")]));

        let result = resolve_volumes(&pod, &resolved).unwrap();
        assert_eq!(fs::read_to_string(mount_path.join("apiserver/apiserver.yaml")).unwrap(), "A");
        assert_eq!(fs::read_to_string(mount_path.join("apiserver/apiserver.log.yaml")).unwrap(), "B");
        assert!(result.containers["app"][0].read_only);
    }

    #[test]
    fn projected_volume_collision_last_source_wins_on_disk() {
        let base = setup_dir("projected-collision");
        let mount_path = base.join("app").join("etc");
        let pod = projected_pod(
            &mount_path,
            vec![
                VolumeProjection { config_map: Some(cm("cm-a", vec![("v", "shared.yaml")])), ..Default::default() },
                VolumeProjection { config_map: Some(cm("cm-b", vec![("v", "shared.yaml")])), ..Default::default() },
            ],
        );
        let mut resolved = ResolvedK8sValues::default();
        resolved.config_map_volumes.insert("cm-a".to_string(), data(&[("v", "from-a")]));
        resolved.config_map_volumes.insert("cm-b".to_string(), data(&[("v", "from-b")]));

        resolve_volumes(&pod, &resolved).unwrap();
        // Last source (cm-b) wins; pod-runner logs a warning (see materialize_plan).
        assert_eq!(fs::read_to_string(mount_path.join("shared.yaml")).unwrap(), "from-b");
    }

    #[test]
    fn projected_volume_missing_configmap_data_errors() {
        let base = setup_dir("projected-missing");
        let mount_path = base.join("app").join("etc");
        let pod = projected_pod(
            &mount_path,
            vec![VolumeProjection {
                config_map: Some(cm("absent-cm", vec![("k", "k.yaml")])),
                ..Default::default()
            }],
        );
        let err = resolve_volumes(&pod, &ResolvedK8sValues::default()).unwrap_err();
        assert!(matches!(err, PrepareError::VolumeConfigMapMissing { .. }));
    }

    #[test]
    fn config_map_volume_items_select_keys() {
        let base = setup_dir("cm-items");
        let mount_path = base.join("app").join("etc");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                volume_mounts: vec![VolumeMount {
                    name: "cfg".to_string(),
                    mount_path: mount_path.to_string_lossy().to_string(),
                    read_only: true,
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
        resolved
            .config_map_volumes
            .insert("app-config".to_string(), data(&[("a.conf", "AA"), ("b.conf", "BB")]));

        resolve_volumes(&pod, &resolved).unwrap();
        assert_eq!(fs::read_to_string(mount_path.join("nested/a.conf")).unwrap(), "AA");
        // b.conf was not selected by items.
        assert!(!mount_path.join("b.conf").exists());
    }
}
