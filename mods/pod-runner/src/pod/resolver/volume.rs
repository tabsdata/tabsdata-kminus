// Copyright 2026 Tabsdata Inc.

//! Volume resolution — set up volume mounts for containers.
//!
//! Mount paths have already been rewritten to absolute physical paths
//! by [`pod_prepare`](crate::pod::prepare::pod_prepare).
//!
//! - `emptyDir`: creates an empty directory at the mount path.
//! - `hostPath`: creates a symlink from the mount path to the host path.
//! - `configMap`: creates a directory at the mount path and writes one file
//!   per configmap key (filename = key, contents = value). The configmap data
//!   must be pre-resolved into [`ResolvedK8sValues::config_map_volumes`].
//!   kminus mounts the entire configmap as a directory — `configMap.items`
//!   and `volumeMount.subPath` are not supported. The convention is to mount
//!   configmaps at `/configmaps/<configmap-name>` (not enforced).

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::pod::prepare::PrepareError;
use crate::pod::resolver::k8s_values::ResolvedK8sValues;
use crate::spec::pod::PodSpec;

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

                fs::create_dir_all(&vm.mount_path).map_err(|source| PrepareError::Io {
                    context: format!(
                        "Failed to create configMap dir '{}' at {}",
                        vm.name, vm.mount_path
                    ),
                    source,
                })?;

                for (key, value) in data {
                    let file_path = Path::new(&vm.mount_path).join(key);
                    fs::write(&file_path, value).map_err(|source| PrepareError::Io {
                        context: format!(
                            "Failed to write configMap file '{}'",
                            file_path.display()
                        ),
                        source,
                    })?;
                }

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
}
