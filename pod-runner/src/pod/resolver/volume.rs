// Copyright 2026 Tabsdata Inc.

//! Volume resolution — set up volume mounts for containers.
//!
//! Mount paths have already been rewritten to absolute physical paths
//! by [`pod_prepare`](crate::pod::prepare::pod_prepare).
//!
//! - `emptyDir`: creates an empty directory at the mount path.
//! - `hostPath`: creates a symlink from the mount path to the host path.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

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

/// Create symlinks/directories for all volume mounts.
///
/// The pod must have been prepared first (paths rewritten).
pub fn resolve_volumes(pod: &PodSpec) -> Result<ResolvedVolumes, String> {
    let volume_map = pod.spec.volume_map();
    let mut result = ResolvedVolumes {
        containers: HashMap::new(),
    };

    for c in pod.spec.all_containers() {
        let mut mounts = Vec::new();

        for vm in &c.volume_mounts {
            let vol = volume_map.get(vm.name.as_str()).ok_or_else(|| {
                format!(
                    "Container '{}' references unknown volume '{}'",
                    c.name, vm.name
                )
            })?;

            if vol.empty_dir.is_some() {
                fs::create_dir_all(&vm.mount_path).map_err(|e| {
                    format!(
                        "Failed to create emptyDir '{}' at {}: {}",
                        vm.name, vm.mount_path, e
                    )
                })?;
                mounts.push(MountInfo {
                    volume_name: vm.name.clone(),
                    mount_path: vm.mount_path.clone(),
                    host_path: String::new(),
                    read_only: false,
                });
            } else if let Some(ref hp) = vol.host_path {
                let host = &hp.path;
                if !Path::new(host).is_dir() {
                    return Err(format!(
                        "Volume '{}' hostPath does not exist: {}",
                        vm.name, host
                    ));
                }

                // Create parent dir for the symlink
                if let Some(parent) = Path::new(&vm.mount_path).parent() {
                    fs::create_dir_all(parent).map_err(|e| {
                        format!("Failed to create parent for mount '{}': {}", vm.name, e)
                    })?;
                }

                // Remove existing symlink
                let mount = Path::new(&vm.mount_path);
                if mount.is_symlink() {
                    let _ = fs::remove_file(mount);
                }

                #[cfg(unix)]
                std::os::unix::fs::symlink(host, &vm.mount_path).map_err(|e| {
                    format!(
                        "Failed to symlink '{}' -> '{}': {}",
                        vm.mount_path, host, e
                    )
                })?;

                #[cfg(windows)]
                std::os::windows::fs::symlink_dir(host, &vm.mount_path).map_err(|e| {
                    format!(
                        "Failed to symlink '{}' -> '{}': {}",
                        vm.mount_path, host, e
                    )
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

        let result = resolve_volumes(&pod).unwrap();
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

        let result = resolve_volumes(&pod).unwrap();
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

        let err = resolve_volumes(&pod).unwrap_err();
        assert!(err.contains("hostPath does not exist"));
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

        let err = resolve_volumes(&pod).unwrap_err();
        assert!(err.contains("unknown volume 'missing'"));
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

        let result = resolve_volumes(&pod).unwrap();
        assert!(result.containers.is_empty());
    }
}
