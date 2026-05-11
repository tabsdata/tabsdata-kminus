// Copyright 2026 Tabsdata Inc.

//! Pod preparation — transform an original spec into a running spec.
//!
//! The preparation pipeline has four steps:
//!
//! 1. **[`rewrite_pod_paths`]** — rewrite `workingDir` and `volumeMount.mountPath`
//!    to physical paths under the pod working directory.
//! 2. **[`resolve_k8s_values`]** — resolve environment variables and
//!    fetch configmap data referenced by `configMap` volumes.
//! 3. **[`create_container_dirs`]** — create container working directories and
//!    copy image template directories.
//! 4. **[`setup_volumes`]** — create emptyDir directories, hostPath symlinks,
//!    and configMap volume directories (one file per key).
//!
//! [`prepare_pod`] runs all four steps in order and returns the running spec.

use std::fs;
use std::path::Path;

use tracing::debug;

use crate::pod::resolver::k8s_values::{resolve_k8s_values, ResolvedK8sValues};
use crate::pod::resolver::volume::resolve_volumes;
use crate::pod::resolver::ResolverSet;
use crate::spec::pod::{EnvVar, PodSpec};

/// Environment variable injected into every container with the container's
/// working directory. Allows shell scripts to reference paths relative to
/// the pod root without hardcoding absolute paths.
const ENV_PR_POD_ROOT: &str = "PR_POD_ROOT";

/// Annotation key added to metadata to indicate the pod has been prepared.
const PREPARED_ANNOTATION: &str = "pr.tabsdata.com/prepared";

/// Image value that means "no template to copy".
const IMAGE_SKIP: &str = "/dev/null";

// ---------------------------------------------------------------------------
// Full pipeline
// ---------------------------------------------------------------------------

/// Prepare a pod spec for native execution.
///
/// Runs the full pipeline: path rewriting, env resolution, directory
/// creation (with image copying), and volume setup.
pub async fn prepare_pod(
    spec: &PodSpec,
    pod_dir: &str,
    namespace: &str,
    resolvers: &ResolverSet,
) -> Result<PodSpec, String> {
    let mut prepared = rewrite_pod_paths(spec, pod_dir)?;
    let resolved = resolve_k8s_values(&prepared, namespace, resolvers).await?;
    apply_resolved_envs(&mut prepared, &resolved);
    inject_pr_pod_root(&mut prepared);
    create_container_dirs(&prepared, pod_dir)?;
    setup_volumes(&prepared, &resolved)?;
    Ok(prepared)
}

// ---------------------------------------------------------------------------
// Step 1: rewrite paths
// ---------------------------------------------------------------------------

/// Rewrite `workingDir` and `volumeMount.mountPath` for native execution.
///
/// For each container (init + main):
/// - `workingDir` is prefixed with `<pod_dir>/work/<container_name>`,
///   or set to it if absent.
/// - `volumeMount.mountPath` values are prefixed with the container root.
///
/// Adds the `pr.tabsdata.com/prepared` annotation.
pub fn rewrite_pod_paths(pod: &PodSpec, pod_dir: &str) -> Result<PodSpec, String> {
    if is_prepared(pod) {
        return Err(
            "Pod has already been prepared (pr.tabsdata.com/prepared annotation is set)"
                .to_string(),
        );
    }

    let mut prepared = pod.clone();

    for c in prepared.spec.all_containers_mut() {
        let container_root = Path::new(pod_dir).join("work").join(&c.name);
        let container_root_str = container_root.to_string_lossy();

        if c.working_dir.is_empty() {
            c.working_dir = container_root_str.to_string();
        } else {
            let rel = c.working_dir.trim_start_matches('/');
            c.working_dir = container_root.join(rel).to_string_lossy().to_string();
        }

        for vm in &mut c.volume_mounts {
            let rel = vm.mount_path.trim_start_matches('/');
            vm.mount_path = container_root.join(rel).to_string_lossy().to_string();
        }
    }

    prepared
        .metadata
        .annotations
        .insert(PREPARED_ANNOTATION.to_string(), "true".to_string());
    Ok(prepared)
}

/// Return `true` if the pod has been prepared.
pub fn is_prepared(pod: &PodSpec) -> bool {
    pod.metadata
        .annotations
        .get(PREPARED_ANNOTATION)
        .map(|v| v == "true")
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Step 2: resolve k8s-sourced values (envs + configmap volume data)
// ---------------------------------------------------------------------------

/// Resolve environment variables and configmap volume data from secrets,
/// config maps, and node envs.
pub async fn resolve_pod_k8s_values(
    spec: &PodSpec,
    namespace: &str,
    resolvers: &ResolverSet,
) -> Result<ResolvedK8sValues, String> {
    resolve_k8s_values(spec, namespace, resolvers).await
}

/// Apply resolved environment variables to the spec, replacing
/// `valueFrom` references with literal values.
pub fn apply_resolved_envs(spec: &mut PodSpec, resolved: &ResolvedK8sValues) {
    for c in spec.spec.all_containers_mut() {
        if let Some(ce) = resolved.containers.get(&c.name) {
            c.env = ce
                .env
                .iter()
                .map(|(k, v)| EnvVar {
                    name: k.clone(),
                    value: Some(v.clone()),
                    value_from: None,
                })
                .collect();
            c.env_from.clear();
        }
    }
}

/// Inject `PR_POD_ROOT` env var into every container, set to that
/// container's `working_dir`. This lets shell scripts reference pod-relative
/// paths without hardcoding absolute paths.
fn inject_pr_pod_root(spec: &mut PodSpec) {
    for c in spec.spec.all_containers_mut() {
        if c.working_dir.is_empty() {
            continue;
        }
        // Remove any existing PR_POD_ROOT (shouldn't happen, but be safe)
        c.env.retain(|e| e.name != ENV_PR_POD_ROOT);
        c.env.push(EnvVar {
            name: ENV_PR_POD_ROOT.to_string(),
            value: Some(c.working_dir.clone()),
            value_from: None,
        });
    }
}

// ---------------------------------------------------------------------------
// Step 3: create container dirs (+ image copy)
// ---------------------------------------------------------------------------

/// Create container working directories and copy image templates.
///
/// For each container:
/// - Creates the working directory (`<pod_dir>/work/<name>`).
/// - If the container's `image` field is a local directory path (not
///   empty, not `/dev/null`), copies the directory tree into the
///   container's work dir.
pub fn create_container_dirs(spec: &PodSpec, pod_dir: &str) -> Result<(), String> {
    for c in spec.spec.all_containers() {
        // Create working dir
        if !c.working_dir.is_empty() {
            fs::create_dir_all(&c.working_dir).map_err(|e| {
                format!(
                    "Failed to create working dir for '{}': {}",
                    c.name, e
                )
            })?;
        }

        // Copy image template directory
        if !c.image.is_empty() && c.image != IMAGE_SKIP {
            let src = Path::new(&c.image);
            if src.is_dir() {
                let dest = Path::new(pod_dir).join("work").join(&c.name);
                debug!(container = c.name.as_str(), src = ?src, dest = ?dest, "Copying image template");
                copy_dir_recursive(src, &dest)?;
            }
            // If the image path doesn't exist as a directory, skip silently —
            // it may be a container image reference handled elsewhere.
        }
    }
    Ok(())
}

/// Recursively copy a directory tree, creating dest dirs as needed.
fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<(), String> {
    fs::create_dir_all(dest)
        .map_err(|e| format!("Failed to create dir {}: {}", dest.display(), e))?;

    let entries = fs::read_dir(src)
        .map_err(|e| format!("Failed to read dir {}: {}", src.display(), e))?;

    for entry in entries {
        let entry =
            entry.map_err(|e| format!("Failed to read entry in {}: {}", src.display(), e))?;
        let src_path = entry.path();
        let dest_path = dest.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dest_path)?;
        } else {
            fs::copy(&src_path, &dest_path).map_err(|e| {
                format!(
                    "Failed to copy {} -> {}: {}",
                    src_path.display(),
                    dest_path.display(),
                    e
                )
            })?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Step 4: set up volumes
// ---------------------------------------------------------------------------

/// Create emptyDir directories, hostPath symlinks, and configMap volume
/// directories (one file per key).
pub fn setup_volumes(spec: &PodSpec, resolved: &ResolvedK8sValues) -> Result<(), String> {
    resolve_volumes(spec, resolved).map(|_| ())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::{
        ConfigMapVolume, Container, EmptyDirVolume, HostPathVolume, PodMetadata, PodSpecBody,
        Volume, VolumeMount,
    };

    fn make_container(name: &str, working_dir: &str, mounts: Vec<(&str, &str)>) -> Container {
        Container {
            name: name.to_string(),
            working_dir: working_dir.to_string(),
            volume_mounts: mounts
                .into_iter()
                .map(|(n, p)| VolumeMount {
                    name: n.to_string(),
                    mount_path: p.to_string(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn make_pod(containers: Vec<Container>, init_containers: Vec<Container>) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                name: "test-pod".to_string(),
                namespace: "default".to_string(),
                uid: "uid-1".to_string(),
                ..Default::default()
            },
            spec: PodSpecBody {
                containers,
                init_containers,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-prepare-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    // -- rewrite_pod_paths --

    #[test]
    fn rewrites_working_dir() {
        let pod = make_pod(
            vec![make_container("app", "/usr/src/app", vec![])],
            vec![],
        );
        let prepared = rewrite_pod_paths(&pod, "/data/pods/mypod").unwrap();
        assert_eq!(
            prepared.spec.containers[0].working_dir,
            "/data/pods/mypod/work/app/usr/src/app"
        );
    }

    #[test]
    fn empty_working_dir_defaults_to_container_root() {
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, "/data/pods/mypod").unwrap();
        assert_eq!(
            prepared.spec.containers[0].working_dir,
            "/data/pods/mypod/work/app"
        );
    }

    #[test]
    fn rewrites_volume_mount_paths() {
        let pod = make_pod(
            vec![make_container(
                "app",
                "/app",
                vec![("data", "/mnt/data"), ("config", "/etc/config")],
            )],
            vec![],
        );
        let prepared = rewrite_pod_paths(&pod, "/pods/p1").unwrap();
        assert_eq!(
            prepared.spec.containers[0].volume_mounts[0].mount_path,
            "/pods/p1/work/app/mnt/data"
        );
        assert_eq!(
            prepared.spec.containers[0].volume_mounts[1].mount_path,
            "/pods/p1/work/app/etc/config"
        );
    }

    #[test]
    fn rewrites_init_containers() {
        let pod = make_pod(
            vec![make_container("app", "/app", vec![])],
            vec![make_container("init", "/setup", vec![])],
        );
        let prepared = rewrite_pod_paths(&pod, "/pods/p1").unwrap();
        assert_eq!(
            prepared.spec.init_containers[0].working_dir,
            "/pods/p1/work/init/setup"
        );
    }

    #[test]
    fn adds_prepared_annotation() {
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, "/pods/p1").unwrap();
        assert!(is_prepared(&prepared));
        assert!(!is_prepared(&pod));
    }

    #[test]
    fn double_prepare_fails() {
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, "/pods/p1").unwrap();
        let result = rewrite_pod_paths(&prepared, "/pods/p1");
        assert!(result.is_err());
    }

    #[test]
    fn does_not_modify_original() {
        let pod = make_pod(vec![make_container("app", "/app", vec![])], vec![]);
        let _ = rewrite_pod_paths(&pod, "/pods/p1").unwrap();
        assert_eq!(pod.spec.containers[0].working_dir, "/app");
        assert!(!is_prepared(&pod));
    }

    #[test]
    fn multiple_containers() {
        let pod = make_pod(
            vec![
                make_container("web", "/var/www", vec![]),
                make_container("worker", "/opt/worker", vec![]),
            ],
            vec![],
        );
        let prepared = rewrite_pod_paths(&pod, "/pods/p1").unwrap();
        assert_eq!(
            prepared.spec.containers[0].working_dir,
            "/pods/p1/work/web/var/www"
        );
        assert_eq!(
            prepared.spec.containers[1].working_dir,
            "/pods/p1/work/worker/opt/worker"
        );
    }

    // -- create_container_dirs --

    #[test]
    fn creates_working_dirs() {
        let dir = temp_dir("create-dirs");
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, &dir.to_string_lossy()).unwrap();

        create_container_dirs(&prepared, &dir.to_string_lossy()).unwrap();
        assert!(dir.join("work/app").is_dir());
    }

    #[test]
    fn copies_image_template() {
        let dir = temp_dir("image-copy");
        let image_dir = dir.join("image-template");
        fs::create_dir_all(&image_dir).unwrap();
        fs::write(image_dir.join("app.conf"), "setting=value").unwrap();
        fs::create_dir_all(image_dir.join("subdir")).unwrap();
        fs::write(image_dir.join("subdir/nested.txt"), "nested").unwrap();

        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = image_dir.to_string_lossy().to_string();

        let prepared = rewrite_pod_paths(&pod, &dir.to_string_lossy()).unwrap();
        create_container_dirs(&prepared, &dir.to_string_lossy()).unwrap();

        let work = dir.join("work/app");
        assert!(work.join("app.conf").exists());
        assert_eq!(fs::read_to_string(work.join("app.conf")).unwrap(), "setting=value");
        assert!(work.join("subdir/nested.txt").exists());
    }

    #[test]
    fn skips_dev_null_image() {
        let dir = temp_dir("skip-devnull");
        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = "/dev/null".to_string();

        let prepared = rewrite_pod_paths(&pod, &dir.to_string_lossy()).unwrap();
        create_container_dirs(&prepared, &dir.to_string_lossy()).unwrap();
        // Should not error, just create the working dir
        assert!(dir.join("work/app").is_dir());
    }

    // -- setup_volumes --

    #[test]
    fn setup_empty_dir() {
        let dir = temp_dir("setup-empty");
        let mut container = make_container("app", "", vec![("data", "/mnt/data")]);
        container.volume_mounts[0].mount_path =
            dir.join("work/app/mnt/data").to_string_lossy().to_string();

        let pod = PodSpec {
            spec: PodSpecBody {
                containers: vec![container],
                volumes: vec![Volume {
                    name: "data".to_string(),
                    empty_dir: Some(EmptyDirVolume {}),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..make_pod(vec![], vec![])
        };

        setup_volumes(&pod, &ResolvedK8sValues::default()).unwrap();
        assert!(dir.join("work/app/mnt/data").is_dir());
    }

    #[test]
    fn setup_host_path() {
        let dir = temp_dir("setup-host");
        let host_dir = dir.join("host-data");
        fs::create_dir_all(&host_dir).unwrap();
        fs::write(host_dir.join("test.txt"), "hello").unwrap();

        let mount_path = dir.join("work/app/mounted");
        let mut container = make_container("app", "", vec![("vol", "/mounted")]);
        container.volume_mounts[0].mount_path = mount_path.to_string_lossy().to_string();

        let pod = PodSpec {
            spec: PodSpecBody {
                containers: vec![container],
                volumes: vec![Volume {
                    name: "vol".to_string(),
                    host_path: Some(HostPathVolume {
                        path: host_dir.to_string_lossy().to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            },
            ..make_pod(vec![], vec![])
        };

        setup_volumes(&pod, &ResolvedK8sValues::default()).unwrap();
        assert!(mount_path.is_symlink());
        assert_eq!(
            fs::read_to_string(mount_path.join("test.txt")).unwrap(),
            "hello"
        );
    }

    // -- prepare_pod (full pipeline) --

    #[tokio::test]
    async fn full_pipeline() {
        let dir = temp_dir("full-pipeline");
        let pod = make_pod(vec![make_container("app", "/app", vec![])], vec![]);

        let prepared = prepare_pod(
            &pod,
            &dir.to_string_lossy(),
            "default",
            &ResolverSet::default(),
        )
        .await
        .unwrap();

        assert!(is_prepared(&prepared));
        assert!(dir.join("work/app/app").is_dir());
        assert_eq!(
            prepared.spec.containers[0].working_dir,
            format!("{}/work/app/app", dir.display())
        );
    }

    #[tokio::test]
    async fn full_pipeline_with_config_map_volume() {
        use crate::pod::resolver::config_map::FileConfigMapResolver;

        let dir = temp_dir("full-pipeline-cm-vol");
        let cm_dir = dir.join("configmaps");
        fs::create_dir_all(&cm_dir).unwrap();
        fs::write(
            cm_dir.join("app-config.yaml"),
            "data:\n  app.conf: \"setting=value\"\n  logging.yaml: \"level: info\"\n",
        )
        .unwrap();

        let mut pod = make_pod(
            vec![make_container("app", "/app", vec![("config", "/etc/app")])],
            vec![],
        );
        pod.spec.volumes.push(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolume {
                name: "app-config".to_string(),
            }),
            ..Default::default()
        });

        let resolvers = ResolverSet {
            config_map_resolver: Some(Box::new(FileConfigMapResolver::new(cm_dir).unwrap())),
            ..Default::default()
        };

        let prepared = prepare_pod(&pod, &dir.to_string_lossy(), "default", &resolvers)
            .await
            .unwrap();

        let mount = dir.join("work/app/etc/app");
        assert_eq!(
            prepared.spec.containers[0].volume_mounts[0].mount_path,
            mount.to_string_lossy().to_string()
        );
        assert!(mount.is_dir());
        assert_eq!(
            fs::read_to_string(mount.join("app.conf")).unwrap(),
            "setting=value"
        );
        assert_eq!(
            fs::read_to_string(mount.join("logging.yaml")).unwrap(),
            "level: info"
        );
    }

    // -- inject_pr_pod_root --

    #[test]
    fn inject_pr_pod_root_sets_env() {
        let mut spec = make_pod(
            vec![
                Container {
                    name: "app".to_string(),
                    working_dir: "/work/app".to_string(),
                    ..Default::default()
                },
                Container {
                    name: "sidecar".to_string(),
                    working_dir: "/work/sidecar".to_string(),
                    ..Default::default()
                },
            ],
            vec![],
        );

        inject_pr_pod_root(&mut spec);

        let app_env: Vec<_> = spec.spec.containers[0]
            .env
            .iter()
            .filter(|e| e.name == ENV_PR_POD_ROOT)
            .collect();
        assert_eq!(app_env.len(), 1);
        assert_eq!(app_env[0].value.as_deref(), Some("/work/app"));

        let sidecar_env: Vec<_> = spec.spec.containers[1]
            .env
            .iter()
            .filter(|e| e.name == ENV_PR_POD_ROOT)
            .collect();
        assert_eq!(sidecar_env.len(), 1);
        assert_eq!(sidecar_env[0].value.as_deref(), Some("/work/sidecar"));
    }

    #[test]
    fn inject_pr_pod_root_skips_empty_working_dir() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: String::new(),
                ..Default::default()
            }],
            vec![],
        );

        inject_pr_pod_root(&mut spec);

        assert!(
            spec.spec.containers[0]
                .env
                .iter()
                .all(|e| e.name != ENV_PR_POD_ROOT),
            "PR_POD_ROOT should not be set when working_dir is empty"
        );
    }

    #[tokio::test]
    async fn full_pipeline_injects_pr_pod_root() {
        let dir = temp_dir("pr-pod-root");
        let pod = make_pod(vec![make_container("app", "/app", vec![])], vec![]);

        let prepared = prepare_pod(
            &pod,
            &dir.to_string_lossy(),
            "default",
            &ResolverSet::default(),
        )
        .await
        .unwrap();

        let pr_root: Vec<_> = prepared.spec.containers[0]
            .env
            .iter()
            .filter(|e| e.name == ENV_PR_POD_ROOT)
            .collect();
        assert_eq!(pr_root.len(), 1);
        assert_eq!(
            pr_root[0].value.as_deref().unwrap(),
            prepared.spec.containers[0].working_dir,
        );
    }

    // -- apply_resolved_envs --

    #[test]
    fn apply_resolved_envs_replaces_refs() {
        use crate::pod::resolver::k8s_values::{ContainerEnv, ResolvedK8sValues};
        use std::collections::{HashMap, HashSet};

        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                env: vec![EnvVar {
                    name: "FROM_REF".to_string(),
                    value: None,
                    value_from: Some(crate::spec::pod::EnvVarSource {
                        secret_key_ref: Some(crate::spec::pod::KeyRef {
                            name: "sec".to_string(),
                            key: "k".to_string(),
                        }),
                        ..Default::default()
                    }),
                }],
                env_from: vec![crate::spec::pod::EnvFromSource {
                    secret_ref: Some(crate::spec::pod::ObjectRef {
                        name: "bulk".to_string(),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );

        let resolved = ResolvedK8sValues {
            config_map_volumes: HashMap::new(),
            containers: [(
                "app".to_string(),
                ContainerEnv {
                    name: "app".to_string(),
                    env: [
                        ("FROM_REF".to_string(), "resolved-value".to_string()),
                        ("BULK_KEY".to_string(), "bulk-value".to_string()),
                    ]
                    .into_iter()
                    .collect(),
                    secret_keys: HashSet::new(),
                },
            )]
            .into_iter()
            .collect(),
        };

        apply_resolved_envs(&mut spec, &resolved);

        // env_from should be cleared
        assert!(spec.spec.containers[0].env_from.is_empty());
        // env should have literal values only (no value_from)
        let env = &spec.spec.containers[0].env;
        assert_eq!(env.len(), 2);
        assert!(env.iter().all(|e| e.value.is_some() && e.value_from.is_none()));
        let env_map: HashMap<&str, &str> = env
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap()))
            .collect();
        assert_eq!(env_map["FROM_REF"], "resolved-value");
        assert_eq!(env_map["BULK_KEY"], "bulk-value");
    }
}
