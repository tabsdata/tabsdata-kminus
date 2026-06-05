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
//! 4. **[`setup_volumes`]** — materialize pod volumes under
//!    `<pod_dir>/volumes/` (emptyDir directories, hostPath symlinks,
//!    configMap volume directories with one file per key) and symlink
//!    each container mount path to its volume directory.
//!
//! [`prepare_pod`] runs all four steps in order and returns the running spec.
//!
//! # Waiting for missing configmaps / secrets
//!
//! Step 2 calls into [`crate::pod::resolver`], which polls the configured
//! [`crate::pod::resolver::secret::SecretResolver`] /
//! [`crate::pod::resolver::config_map::ConfigMapResolver`] with an
//! exponential backoff (see [`crate::pod::resolver::backoff`]) until every
//! referenced entry exists, the `timeout` expires, or `cancel` fires.
//! `timeout` is derived from the pod's `activeDeadlineSeconds`; when it is
//! `None` the wait is indefinite (only `cancel` can stop it).
//!
//! # Known limitations
//!
//! 1. **Existence is checked at the entry (configmap/secret) level, not the
//!    key level.** The preparer waits for the named configmap/secret to
//!    appear, but if the named key is absent inside an existing
//!    configmap/secret the env entry is silently skipped (see the env
//!    resolution logic in [`crate::pod::resolver::k8s_values`]).
//! 2. **`optional: true` on configmap/secret references is not supported.**
//!    Pods using it are rejected at admission time — see
//!    [`crate::pod::admission`].
//! 3. **Pod-level wait barrier (stricter than k8s).** The preparer
//!    collects the *union* of every configmap/secret referenced by every
//!    init/main container and every `configMap` volume, and blocks the
//!    whole pod in `Pending` until they all resolve. K8s instead waits
//!    per container: an init or main container whose own references
//!    resolve can proceed while siblings keep waiting. Observable only
//!    when references straddle multiple containers and resources land
//!    out of order.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::sync::watch;
use tracing::debug;

use crate::pod::resolver::config_map::ConfigMapResolverError;
use crate::pod::resolver::k8s_values::{resolve_k8s_values, ResolvedK8sValues};
use crate::pod::resolver::secret::SecretResolverError;
use crate::pod::resolver::volume::resolve_volumes;
use crate::pod::resolver::ResolverSet;
use crate::spec::pod::{EnvVar, PodSpec};

// ---------------------------------------------------------------------------
// PrepareError
// ---------------------------------------------------------------------------

/// Errors raised by the pod preparation pipeline.
#[derive(Debug, thiserror::Error)]
pub enum PrepareError {
    /// The pod was already prepared (annotation is set).
    #[error("Pod has already been prepared (pr.tabsdata.com/prepared annotation is set)")]
    AlreadyPrepared,
    /// Pod references secrets but the resolver set has no secret resolver.
    #[error("Pod references secrets but no SecretResolver configured")]
    NoSecretResolver,
    /// Pod references configmaps but the resolver set has no configmap resolver.
    #[error("Pod references configmaps but no ConfigMapResolver configured")]
    NoConfigMapResolver,
    /// Secret resolution failed.
    #[error("{0}")]
    SecretResolution(#[from] SecretResolverError),
    /// ConfigMap resolution failed.
    #[error("{0}")]
    ConfigMapResolution(#[from] ConfigMapResolverError),
    /// A configmap referenced by a `configMap` volume was not resolved.
    #[error("ConfigMap '{0}' referenced by a volume was not resolved")]
    ConfigMapVolumeNotResolved(String),
    /// A `volumeMount` references a volume not declared on the pod.
    #[error("Container '{container}' references unknown volume '{volume}'")]
    UnknownVolume { container: String, volume: String },
    /// A `configMap` volume references a configmap that wasn't resolved.
    #[error("Volume '{volume}' references unresolved configMap '{name}'")]
    VolumeConfigMapMissing { volume: String, name: String },
    /// A `hostPath` volume points at a non-existent directory.
    #[error("Volume '{volume}' hostPath does not exist: {path}")]
    HostPathMissing { volume: String, path: String },
    /// A container mount path is occupied by content that cannot be
    /// replaced by the mount symlink (a file or a non-empty directory).
    #[error("Container '{container}' mount path for volume '{volume}' is occupied: {path}")]
    MountPathConflict {
        container: String,
        volume: String,
        path: String,
    },
    /// Image archive file is missing for a container.
    #[error("Image archive for container '{container}' not found at {path}")]
    ImageArchiveMissing { container: String, path: String },
    /// Image template directory is missing for a container.
    #[error("Image directory for container '{container}' not found at {path}")]
    ImageDirMissing { container: String, path: String },
    /// Filesystem I/O error with caller-supplied context.
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

impl PrepareError {
    /// Build an [`Io`](Self::Io) error with the given context.
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io {
            context: context.into(),
            source,
        }
    }

    /// True when the error originates from a resolver timeout.
    pub fn is_timeout(&self) -> bool {
        use crate::pod::resolver::config_map::ConfigMapResolverErrorKind;
        use crate::pod::resolver::secret::SecretResolverErrorKind;
        match self {
            Self::SecretResolution(e) => e.kind == SecretResolverErrorKind::Timeout,
            Self::ConfigMapResolution(e) => e.kind == ConfigMapResolverErrorKind::Timeout,
            _ => false,
        }
    }
}

/// Environment variable injected into every container with the container's
/// working directory. Allows shell scripts to reference paths relative to
/// the container working directory without hardcoding absolute paths.
const ENV_KM_CONTAINER_ROOT: &str = "KM_CONTAINER_ROOT";

/// Annotation key added to metadata to indicate the pod has been prepared.
const PREPARED_ANNOTATION: &str = "pr.tabsdata.com/prepared";

/// Image value that means "no template to copy".
const IMAGE_SKIP: &str = "/dev/null";

/// Suffixes that identify a gzip-compressed tar archive image.
const IMAGE_SUFFIX_TGZ: &str = ".tgz";
const IMAGE_SUFFIX_TAR_GZ: &str = ".tar.gz";

/// Image source kinds supported by the pod runner.
#[derive(Debug, PartialEq, Eq)]
enum ImageKind {
    /// Directory tree — copied recursively into the container work dir.
    Directory,
    /// `.tgz` / `.tar.gz` archive — extracted into the container work dir.
    TarGz,
}

// ---------------------------------------------------------------------------
// Full pipeline
// ---------------------------------------------------------------------------

/// Prepare a pod spec for native execution.
///
/// Runs the full pipeline: path rewriting, env resolution, directory
/// creation (with image copying), and volume setup.
///
/// `images_dir` is the root used to resolve relative container `image`
/// paths. Absolute `image` paths are used as-is. An empty path disables
/// the root (the container `image` is used verbatim).
pub async fn prepare_pod(
    spec: &PodSpec,
    pod_dir: &Path,
    namespace: &str,
    resolvers: &ResolverSet,
    images_dir: &Path,
    timeout: Option<Duration>,
    cancel: &mut watch::Receiver<()>,
) -> Result<PodSpec, PrepareError> {
    let mut prepared = rewrite_pod_paths(spec, pod_dir)?;
    let resolved =
        resolve_k8s_values(&prepared, namespace, resolvers, timeout, cancel).await?;
    apply_envs_and_substitute(&mut prepared, &resolved);
    create_container_dirs(&prepared, pod_dir, images_dir)?;
    setup_volumes(&prepared, &resolved, pod_dir)?;
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
pub fn rewrite_pod_paths(pod: &PodSpec, pod_dir: &Path) -> Result<PodSpec, PrepareError> {
    if is_prepared(pod) {
        return Err(PrepareError::AlreadyPrepared);
    }

    let mut prepared = pod.clone();

    for c in prepared.spec.all_containers_mut() {
        let container_root = pod_dir.join("work").join(&c.name);
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
    timeout: Option<Duration>,
    cancel: &mut watch::Receiver<()>,
) -> Result<ResolvedK8sValues, PrepareError> {
    resolve_k8s_values(spec, namespace, resolvers, timeout, cancel).await
}

/// Apply resolved env values to the spec, inject `KM_CONTAINER_ROOT`, and
/// perform K8s-style `$(VAR)` substitution.
///
/// Implements the kubelet's expansion semantics
/// (`k8s.io/.../third_party/forked/golang/expansion`):
///
/// - `envFrom`-derived entries are loaded literally; their values are NOT
///   subject to `$(VAR)` substitution.
/// - Individual `env` entries are processed in declaration order. Each
///   value is substituted using only variables defined *earlier*
///   (`envFrom` entries + `KM_CONTAINER_ROOT` + previously-declared
///   individual entries).
/// - `command` and `args` are substituted using the full env map after
///   all entries are processed.
/// - Substitution rules: `$(VAR)` → value when defined; `$$` → `$`;
///   `$(UNDEFINED)` and unclosed `$(VAR` are left literal.
///
/// `KM_CONTAINER_ROOT` is pod-runner specific (not a K8s feature) and is
/// treated as if it were an `envFrom` entry: visible to all individual env
/// substitutions. A user-defined `KM_CONTAINER_ROOT` entry is dropped.
pub fn apply_envs_and_substitute(spec: &mut PodSpec, resolved: &ResolvedK8sValues) {
    for c in spec.spec.all_containers_mut() {
        let individual_names: HashSet<String> =
            c.env.iter().map(|e| e.name.clone()).collect();

        let mut new_env: Vec<EnvVar> = Vec::new();
        let mut available: HashMap<String, String> = HashMap::new();

        // 1. KM_CONTAINER_ROOT (auto-injected, treated like an envFrom entry).
        if !c.working_dir.is_empty() {
            available.insert(ENV_KM_CONTAINER_ROOT.to_string(), c.working_dir.clone());
            new_env.push(EnvVar {
                name: ENV_KM_CONTAINER_ROOT.to_string(),
                value: Some(c.working_dir.clone()),
                value_from: None,
            });
        }

        if let Some(ce) = resolved.containers.get(&c.name) {
            // 2. envFrom-derived entries (in ce.env but not in original individual env).
            //    Loaded literally — NOT subject to $(VAR) substitution.
            for (k, v) in &ce.env {
                if individual_names.contains(k) || k == ENV_KM_CONTAINER_ROOT {
                    continue;
                }
                available.insert(k.clone(), v.clone());
                new_env.push(EnvVar {
                    name: k.clone(),
                    value: Some(v.clone()),
                    value_from: None,
                });
            }

            // 3. Individual env entries in declaration order. Substitute $(VAR)
            //    using only vars defined earlier.
            for ev in &c.env {
                if ev.name == ENV_KM_CONTAINER_ROOT {
                    continue;
                }
                let Some(raw_v) = ce.env.get(&ev.name) else {
                    continue;
                };
                let substituted = substitute_dollar(raw_v, &available);
                available.insert(ev.name.clone(), substituted.clone());
                new_env.push(EnvVar {
                    name: ev.name.clone(),
                    value: Some(substituted),
                    value_from: None,
                });
            }
        }

        c.env = new_env;
        c.env_from.clear();

        // 4. Substitute command/args using the full env map.
        for s in &mut c.command {
            *s = substitute_dollar(s, &available);
        }
        for s in &mut c.args {
            *s = substitute_dollar(s, &available);
        }
    }
}

/// K8s-style single-pass `$(VAR)` substitution.
///
/// Rules: `$(VAR)` → `env[VAR]` when defined, else left literal; `$$` → `$`;
/// unclosed `$(VAR` left literal; bare `$X` left as-is. The string between
/// `(` and `)` is used verbatim as the lookup key (no identifier-format
/// validation, matching kubelet behavior).
fn substitute_dollar(s: &str, env: &HashMap<String, String>) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'$' {
            out.push(bytes[i] as char);
            i += 1;
            continue;
        }
        if i + 1 >= bytes.len() {
            out.push('$');
            i += 1;
            continue;
        }
        match bytes[i + 1] {
            b'$' => {
                out.push('$');
                i += 2;
            }
            b'(' => {
                if let Some(rel_end) = bytes[i + 2..].iter().position(|&b| b == b')') {
                    let end = i + 2 + rel_end;
                    let name = &s[i + 2..end];
                    if let Some(val) = env.get(name) {
                        out.push_str(val);
                    } else {
                        out.push_str(&s[i..=end]);
                    }
                    i = end + 1;
                } else {
                    out.push('$');
                    i += 1;
                }
            }
            _ => {
                out.push('$');
                i += 1;
            }
        }
    }
    out
}

// ---------------------------------------------------------------------------
// Step 3: create container dirs (+ image copy)
// ---------------------------------------------------------------------------

/// Create container working directories and populate them from the
/// container `image`.
///
/// For each container:
/// - Creates the working directory (`<pod_dir>/work/<name>`).
/// - When the container's `image` field is not empty and not `/dev/null`:
///   - Resolves the image path: absolute paths are used as-is; relative
///     paths are resolved against `images_dir`. A relative path with an
///     empty `images_dir` is used verbatim.
///   - Detects the image kind by file name suffix (`.tgz` / `.tar.gz`
///     means a gzip tar archive; otherwise a directory tree).
///   - Copies a directory recursively, or extracts a tar.gz archive,
///     into the container's work dir.
/// - Errors out if the resolved image path does not exist on disk, or
///   if the resolved path does not match its expected kind (e.g. a
///   `.tgz` path that is actually a directory, or a non-archive path
///   that does not exist as a directory).
pub fn create_container_dirs(
    spec: &PodSpec,
    pod_dir: &Path,
    images_dir: &Path,
) -> Result<(), PrepareError> {
    for c in spec.spec.all_containers() {
        // Create working dir
        if !c.working_dir.is_empty() {
            fs::create_dir_all(&c.working_dir).map_err(|source| PrepareError::Io {
                context: format!("Failed to create working dir for '{}'", c.name),
                source,
            })?;
        }

        // Populate from image (directory copy or archive extract)
        if !c.image.is_empty() && c.image != IMAGE_SKIP {
            let src = resolve_image_path(&c.image, images_dir);
            let dest = pod_dir.join("work").join(&c.name);
            let kind = image_kind(&src);

            match kind {
                ImageKind::TarGz => {
                    if !src.is_file() {
                        return Err(PrepareError::ImageArchiveMissing {
                            container: c.name.clone(),
                            path: src.display().to_string(),
                        });
                    }
                    debug!(container = c.name.as_str(), src = ?src, dest = ?dest, "Extracting image archive");
                    extract_tar_gz(&src, &dest)?;
                }
                ImageKind::Directory => {
                    if !src.is_dir() {
                        return Err(PrepareError::ImageDirMissing {
                            container: c.name.clone(),
                            path: src.display().to_string(),
                        });
                    }
                    debug!(container = c.name.as_str(), src = ?src, dest = ?dest, "Copying image template");
                    copy_dir_recursive(&src, &dest)?;
                }
            }
        }
    }
    Ok(())
}

/// Resolve a container `image` field into a filesystem path.
///
/// Absolute paths are returned as-is. Relative paths are joined against
/// `images_dir`; an empty `images_dir` leaves the path unchanged.
fn resolve_image_path(image: &str, images_dir: &Path) -> PathBuf {
    let p = Path::new(image);
    if p.is_absolute() || images_dir.as_os_str().is_empty() {
        p.to_path_buf()
    } else {
        images_dir.join(p)
    }
}

/// Classify an image path by file-name suffix.
fn image_kind(path: &Path) -> ImageKind {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    if name.ends_with(IMAGE_SUFFIX_TAR_GZ) || name.ends_with(IMAGE_SUFFIX_TGZ) {
        ImageKind::TarGz
    } else {
        ImageKind::Directory
    }
}

/// Extract a `.tar.gz` archive into `dest`.
fn extract_tar_gz(src: &Path, dest: &Path) -> Result<(), PrepareError> {
    fs::create_dir_all(dest).map_err(|source| PrepareError::Io {
        context: format!("Failed to create dir {}", dest.display()),
        source,
    })?;

    let file = fs::File::open(src).map_err(|source| PrepareError::Io {
        context: format!("Failed to open archive {}", src.display()),
        source,
    })?;
    let dec = flate2::read::GzDecoder::new(file);
    let mut archive = tar::Archive::new(dec);
    archive.unpack(dest).map_err(|source| PrepareError::Io {
        context: format!("Failed to extract {} -> {}", src.display(), dest.display()),
        source,
    })?;
    Ok(())
}

/// Recursively copy a directory tree, creating dest dirs as needed.
fn copy_dir_recursive(src: &Path, dest: &Path) -> Result<(), PrepareError> {
    fs::create_dir_all(dest).map_err(|source| PrepareError::Io {
        context: format!("Failed to create dir {}", dest.display()),
        source,
    })?;

    let entries = fs::read_dir(src).map_err(|source| PrepareError::Io {
        context: format!("Failed to read dir {}", src.display()),
        source,
    })?;

    for entry in entries {
        let entry = entry.map_err(|source| PrepareError::Io {
            context: format!("Failed to read entry in {}", src.display()),
            source,
        })?;
        let src_path = entry.path();
        let dest_path = dest.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dest_path)?;
        } else {
            fs::copy(&src_path, &dest_path).map_err(|source| PrepareError::Io {
                context: format!(
                    "Failed to copy {} -> {}",
                    src_path.display(),
                    dest_path.display(),
                ),
                source,
            })?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Step 4: set up volumes
// ---------------------------------------------------------------------------

/// Materialize pod volumes under `<pod_dir>/volumes/` (emptyDir
/// directories, hostPath symlinks, configMap volume directories with one
/// file per key) and symlink each container mount path to its volume
/// directory.
pub fn setup_volumes(
    spec: &PodSpec,
    resolved: &ResolvedK8sValues,
    pod_dir: &Path,
) -> Result<(), PrepareError> {
    resolve_volumes(spec, resolved, pod_dir).map(|_| ())
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
        let prepared = rewrite_pod_paths(&pod, Path::new("/data/pods/mypod")).unwrap();
        assert_eq!(
            prepared.spec.containers[0].working_dir,
            "/data/pods/mypod/work/app/usr/src/app"
        );
    }

    #[test]
    fn empty_working_dir_defaults_to_container_root() {
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, Path::new("/data/pods/mypod")).unwrap();
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
        let prepared = rewrite_pod_paths(&pod, Path::new("/pods/p1")).unwrap();
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
        let prepared = rewrite_pod_paths(&pod, Path::new("/pods/p1")).unwrap();
        assert_eq!(
            prepared.spec.init_containers[0].working_dir,
            "/pods/p1/work/init/setup"
        );
    }

    #[test]
    fn adds_prepared_annotation() {
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, Path::new("/pods/p1")).unwrap();
        assert!(is_prepared(&prepared));
        assert!(!is_prepared(&pod));
    }

    #[test]
    fn double_prepare_fails() {
        let pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        let prepared = rewrite_pod_paths(&pod, Path::new("/pods/p1")).unwrap();
        let result = rewrite_pod_paths(&prepared, Path::new("/pods/p1"));
        assert!(result.is_err());
    }

    #[test]
    fn does_not_modify_original() {
        let pod = make_pod(vec![make_container("app", "/app", vec![])], vec![]);
        let _ = rewrite_pod_paths(&pod, Path::new("/pods/p1")).unwrap();
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
        let prepared = rewrite_pod_paths(&pod, Path::new("/pods/p1")).unwrap();
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
        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();

        create_container_dirs(&prepared, &dir, Path::new("")).unwrap();
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

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        create_container_dirs(&prepared, &dir, Path::new("")).unwrap();

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

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        create_container_dirs(&prepared, &dir, Path::new("")).unwrap();
        // Should not error, just create the working dir
        assert!(dir.join("work/app").is_dir());
    }

    #[test]
    fn extracts_tar_gz_image() {
        use flate2::write::GzEncoder;
        use flate2::Compression;
        use std::io::Write;

        let dir = temp_dir("image-targz");
        fs::create_dir_all(&dir).unwrap();
        let archive_path = dir.join("app.tgz");

        // Build a small tar.gz with two files
        let file = fs::File::create(&archive_path).unwrap();
        let enc = GzEncoder::new(file, Compression::default());
        let mut builder = tar::Builder::new(enc);
        let mut hdr = tar::Header::new_gnu();
        let payload = b"setting=value";
        hdr.set_size(payload.len() as u64);
        hdr.set_mode(0o644);
        hdr.set_cksum();
        builder.append_data(&mut hdr, "app.conf", &payload[..]).unwrap();
        let nested = b"nested";
        let mut hdr2 = tar::Header::new_gnu();
        hdr2.set_size(nested.len() as u64);
        hdr2.set_mode(0o644);
        hdr2.set_cksum();
        builder
            .append_data(&mut hdr2, "subdir/nested.txt", &nested[..])
            .unwrap();
        let enc = builder.into_inner().unwrap();
        enc.finish().unwrap().flush().unwrap();

        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = archive_path.to_string_lossy().to_string();

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        create_container_dirs(&prepared, &dir, Path::new("")).unwrap();

        let work = dir.join("work/app");
        assert_eq!(
            fs::read_to_string(work.join("app.conf")).unwrap(),
            "setting=value"
        );
        assert_eq!(
            fs::read_to_string(work.join("subdir/nested.txt")).unwrap(),
            "nested"
        );
    }

    #[test]
    fn extracts_tar_gz_with_tar_gz_suffix() {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let dir = temp_dir("image-tar-gz-suffix");
        fs::create_dir_all(&dir).unwrap();
        let archive_path = dir.join("app.tar.gz");

        let file = fs::File::create(&archive_path).unwrap();
        let enc = GzEncoder::new(file, Compression::default());
        let mut builder = tar::Builder::new(enc);
        let payload = b"hello";
        let mut hdr = tar::Header::new_gnu();
        hdr.set_size(payload.len() as u64);
        hdr.set_mode(0o644);
        hdr.set_cksum();
        builder.append_data(&mut hdr, "greet.txt", &payload[..]).unwrap();
        builder.into_inner().unwrap().finish().unwrap();

        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = archive_path.to_string_lossy().to_string();

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        create_container_dirs(&prepared, &dir, Path::new("")).unwrap();

        assert_eq!(
            fs::read_to_string(dir.join("work/app/greet.txt")).unwrap(),
            "hello"
        );
    }

    #[test]
    fn resolves_relative_image_against_images_dir() {
        let dir = temp_dir("image-relative");
        let images_dir = dir.join("images");
        let image = images_dir.join("my-app");
        fs::create_dir_all(&image).unwrap();
        fs::write(image.join("payload.txt"), "rel").unwrap();

        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = "my-app".to_string();

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        create_container_dirs(&prepared, &dir, &images_dir).unwrap();

        assert_eq!(
            fs::read_to_string(dir.join("work/app/payload.txt")).unwrap(),
            "rel"
        );
    }

    #[test]
    fn absolute_image_ignores_images_dir() {
        let dir = temp_dir("image-abs-ignores-root");
        let images_dir = dir.join("images-root");
        fs::create_dir_all(&images_dir).unwrap();
        let abs_image = dir.join("elsewhere");
        fs::create_dir_all(&abs_image).unwrap();
        fs::write(abs_image.join("abs.txt"), "abs").unwrap();

        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = abs_image.to_string_lossy().to_string();

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        create_container_dirs(&prepared, &dir, &images_dir).unwrap();

        assert_eq!(
            fs::read_to_string(dir.join("work/app/abs.txt")).unwrap(),
            "abs"
        );
    }

    #[test]
    fn missing_image_directory_errors() {
        let dir = temp_dir("image-missing-dir");
        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = dir.join("does-not-exist").to_string_lossy().to_string();

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        let result = create_container_dirs(&prepared, &dir, Path::new(""));
        let err = result.unwrap_err();
        assert!(matches!(err, PrepareError::ImageDirMissing { .. }), "got: {err}");
    }

    #[test]
    fn missing_image_archive_errors() {
        let dir = temp_dir("image-missing-archive");
        let mut pod = make_pod(vec![make_container("app", "", vec![])], vec![]);
        pod.spec.containers[0].image = dir.join("missing.tgz").to_string_lossy().to_string();

        let prepared = rewrite_pod_paths(&pod, &dir).unwrap();
        let result = create_container_dirs(&prepared, &dir, Path::new(""));
        let err = result.unwrap_err();
        assert!(matches!(err, PrepareError::ImageArchiveMissing { .. }), "got: {err}");
    }

    // -- resolve_image_path / image_kind --

    #[test]
    fn resolve_image_path_keeps_absolute() {
        let resolved = resolve_image_path("/abs/img", Path::new("/root"));
        assert_eq!(resolved, PathBuf::from("/abs/img"));
    }

    #[test]
    fn resolve_image_path_joins_relative() {
        let resolved = resolve_image_path("img", Path::new("/root"));
        assert_eq!(resolved, PathBuf::from("/root/img"));
    }

    #[test]
    fn resolve_image_path_empty_root_keeps_relative_verbatim() {
        let resolved = resolve_image_path("img", Path::new(""));
        assert_eq!(resolved, PathBuf::from("img"));
    }

    #[test]
    fn image_kind_detects_archive_suffixes() {
        assert_eq!(image_kind(Path::new("foo.tgz")), ImageKind::TarGz);
        assert_eq!(image_kind(Path::new("foo.tar.gz")), ImageKind::TarGz);
        assert_eq!(image_kind(Path::new("FOO.TGZ")), ImageKind::TarGz);
        assert_eq!(image_kind(Path::new("/a/b/c.tar.gz")), ImageKind::TarGz);
        assert_eq!(image_kind(Path::new("dir")), ImageKind::Directory);
        assert_eq!(image_kind(Path::new("foo.zip")), ImageKind::Directory);
    }

    // -- setup_volumes --

    #[test]
    fn setup_empty_dir() {
        let dir = temp_dir("setup-empty");
        let mount_path = dir.join("work/app/mnt/data");
        let mut container = make_container("app", "", vec![("data", "/mnt/data")]);
        container.volume_mounts[0].mount_path = mount_path.to_string_lossy().to_string();

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

        setup_volumes(&pod, &ResolvedK8sValues::default(), &dir).unwrap();
        // The volume is a directory under <pod_dir>/volumes/; the mount
        // path is a symlink to it.
        assert!(dir.join("volumes/data").is_dir());
        assert!(mount_path.is_symlink());
        assert_eq!(fs::read_link(&mount_path).unwrap(), dir.join("volumes/data"));
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

        setup_volumes(&pod, &ResolvedK8sValues::default(), &dir).unwrap();
        // The volume is a symlink to the host path; the mount path is a
        // symlink to the volume.
        let vol_dir = dir.join("volumes/vol");
        assert!(vol_dir.is_symlink());
        assert_eq!(fs::read_link(&vol_dir).unwrap(), host_dir);
        assert!(mount_path.is_symlink());
        assert_eq!(fs::read_link(&mount_path).unwrap(), vol_dir);
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
        let (_tx, mut cancel) = watch::channel(());

        let prepared = prepare_pod(
            &pod,
            &dir,
            "default",
            &ResolverSet::default(),
            Path::new(""),
            None,
            &mut cancel,
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
                ..Default::default()
            }),
            ..Default::default()
        });

        let resolvers = ResolverSet {
            config_map_resolver: Some(Box::new(FileConfigMapResolver::new(cm_dir).unwrap())),
            ..Default::default()
        };

        let (_tx, mut cancel) = watch::channel(());
        let prepared = prepare_pod(
            &pod,
            &dir,
            "default",
            &resolvers,
            Path::new(""),
            None,
            &mut cancel,
        )
        .await
        .unwrap();

        let mount = dir.join("work/app/etc/app");
        assert_eq!(
            prepared.spec.containers[0].volume_mounts[0].mount_path,
            mount.to_string_lossy().to_string()
        );
        // The mount is a symlink to the volume dir; reads through it see
        // the materialized configmap files.
        assert!(mount.is_symlink());
        assert_eq!(fs::read_link(&mount).unwrap(), dir.join("volumes/config"));
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

    // -- apply_envs_and_substitute --

    use crate::pod::resolver::k8s_values::{ContainerEnv, ResolvedK8sValues};

    fn resolved_for(name: &str, env: Vec<(&str, &str)>) -> ResolvedK8sValues {
        ResolvedK8sValues {
            config_map_volumes: HashMap::new(),
            containers: [(
                name.to_string(),
                ContainerEnv {
                    name: name.to_string(),
                    env: env
                        .into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                        .collect(),
                    secret_keys: HashSet::new(),
                },
            )]
            .into_iter()
            .collect(),
        }
    }

    fn env_to_map(env: &[EnvVar]) -> HashMap<&str, &str> {
        env.iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap()))
            .collect()
    }

    #[test]
    fn injects_km_container_root_from_working_dir() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: "/work/app".to_string(),
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for("app", vec![]);

        apply_envs_and_substitute(&mut spec, &resolved);

        let map = env_to_map(&spec.spec.containers[0].env);
        assert_eq!(map[ENV_KM_CONTAINER_ROOT], "/work/app");
    }

    #[test]
    fn skips_km_container_root_when_working_dir_empty() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: String::new(),
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for("app", vec![]);

        apply_envs_and_substitute(&mut spec, &resolved);

        assert!(
            spec.spec.containers[0]
                .env
                .iter()
                .all(|e| e.name != ENV_KM_CONTAINER_ROOT),
        );
    }

    #[test]
    fn user_cannot_override_km_container_root() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: "/work/app".to_string(),
                env: vec![EnvVar {
                    name: ENV_KM_CONTAINER_ROOT.to_string(),
                    value: Some("/bogus".to_string()),
                    value_from: None,
                }],
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for("app", vec![(ENV_KM_CONTAINER_ROOT, "/bogus")]);

        apply_envs_and_substitute(&mut spec, &resolved);

        let entries: Vec<_> = spec.spec.containers[0]
            .env
            .iter()
            .filter(|e| e.name == ENV_KM_CONTAINER_ROOT)
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].value.as_deref(), Some("/work/app"));
    }

    #[test]
    fn resolves_value_from_refs_into_literal() {
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
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                }],
                env_from: vec![crate::spec::pod::EnvFromSource {
                    secret_ref: Some(crate::spec::pod::ObjectRef {
                        name: "bulk".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for(
            "app",
            vec![("FROM_REF", "resolved-value"), ("BULK_KEY", "bulk-value")],
        );

        apply_envs_and_substitute(&mut spec, &resolved);

        let map = env_to_map(&spec.spec.containers[0].env);
        assert!(spec.spec.containers[0].env_from.is_empty());
        assert_eq!(map["FROM_REF"], "resolved-value");
        assert_eq!(map["BULK_KEY"], "bulk-value");
        assert!(
            spec.spec.containers[0]
                .env
                .iter()
                .all(|e| e.value.is_some() && e.value_from.is_none())
        );
    }

    #[test]
    fn k8s_order_resolves_only_earlier_env_entries() {
        // Per K8s expansion semantics, B=$(A) declared BEFORE A=val
        // must leave B as the literal "$(A)".
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                env: vec![
                    EnvVar {
                        name: "B".to_string(),
                        value: Some("$(A)".to_string()),
                        value_from: None,
                    },
                    EnvVar {
                        name: "A".to_string(),
                        value: Some("val".to_string()),
                        value_from: None,
                    },
                ],
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for("app", vec![("A", "val"), ("B", "$(A)")]);

        apply_envs_and_substitute(&mut spec, &resolved);

        let map = env_to_map(&spec.spec.containers[0].env);
        assert_eq!(map["A"], "val");
        assert_eq!(map["B"], "$(A)");
    }

    #[test]
    fn k8s_order_chains_resolve_when_declared_in_order() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                env: vec![
                    EnvVar {
                        name: "A".to_string(),
                        value: Some("hello".to_string()),
                        value_from: None,
                    },
                    EnvVar {
                        name: "B".to_string(),
                        value: Some("$(A)".to_string()),
                        value_from: None,
                    },
                    EnvVar {
                        name: "C".to_string(),
                        value: Some("$(B)/world".to_string()),
                        value_from: None,
                    },
                ],
                command: vec!["sh".to_string(), "-c".to_string()],
                args: vec!["echo $(C)".to_string()],
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for(
            "app",
            vec![("A", "hello"), ("B", "$(A)"), ("C", "$(B)/world")],
        );

        apply_envs_and_substitute(&mut spec, &resolved);

        let map = env_to_map(&spec.spec.containers[0].env);
        assert_eq!(map["A"], "hello");
        assert_eq!(map["B"], "hello");
        assert_eq!(map["C"], "hello/world");
        assert_eq!(spec.spec.containers[0].args[0], "echo hello/world");
    }

    #[test]
    fn env_from_values_are_not_substituted() {
        // envFrom-derived values must be loaded literally, even if they
        // contain $(VAR) that would otherwise resolve.
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: "/work/app".to_string(),
                env_from: vec![crate::spec::pod::EnvFromSource {
                    config_map_ref: Some(crate::spec::pod::ObjectRef {
                        name: "cm".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        // Simulate the configmap producing a value containing $(KM_CONTAINER_ROOT).
        let resolved = resolved_for("app", vec![("CM_KEY", "$(KM_CONTAINER_ROOT)/x")]);

        apply_envs_and_substitute(&mut spec, &resolved);

        let map = env_to_map(&spec.spec.containers[0].env);
        assert_eq!(map["CM_KEY"], "$(KM_CONTAINER_ROOT)/x");
        assert_eq!(map[ENV_KM_CONTAINER_ROOT], "/work/app");
    }

    #[test]
    fn individual_env_can_reference_env_from_and_km_container_root() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: "/work/app".to_string(),
                env: vec![EnvVar {
                    name: "TARGET".to_string(),
                    value: Some("$(KM_CONTAINER_ROOT)/$(SUBDIR)".to_string()),
                    value_from: None,
                }],
                env_from: vec![crate::spec::pod::EnvFromSource {
                    config_map_ref: Some(crate::spec::pod::ObjectRef {
                        name: "cm".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            vec![],
        );
        let resolved = resolved_for(
            "app",
            vec![("SUBDIR", "data"), ("TARGET", "$(KM_CONTAINER_ROOT)/$(SUBDIR)")],
        );

        apply_envs_and_substitute(&mut spec, &resolved);

        let map = env_to_map(&spec.spec.containers[0].env);
        assert_eq!(map["TARGET"], "/work/app/data");
    }

    #[test]
    fn command_and_args_substituted_with_full_env() {
        let mut spec = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: "/work/app".to_string(),
                env: vec![EnvVar {
                    name: "FILE".to_string(),
                    value: Some("$(KM_CONTAINER_ROOT)/data.txt".to_string()),
                    value_from: None,
                }],
                command: vec!["cat".to_string(), "$(FILE)".to_string()],
                args: vec!["$(KM_CONTAINER_ROOT)/x".to_string()],
                ..Default::default()
            }],
            vec![],
        );
        let resolved =
            resolved_for("app", vec![("FILE", "$(KM_CONTAINER_ROOT)/data.txt")]);

        apply_envs_and_substitute(&mut spec, &resolved);

        assert_eq!(spec.spec.containers[0].command[1], "/work/app/data.txt");
        assert_eq!(spec.spec.containers[0].args[0], "/work/app/x");
    }

    // -- substitute_dollar (pure function) --

    #[test]
    fn substitute_dollar_resolves_known_vars() {
        let env: HashMap<String, String> =
            [("ROOT".to_string(), "/work/app".to_string())].into_iter().collect();
        assert_eq!(substitute_dollar("$(ROOT)/foo", &env), "/work/app/foo");
        assert_eq!(substitute_dollar("a$(ROOT)b", &env), "a/work/appb");
    }

    #[test]
    fn substitute_dollar_leaves_unknown_vars_literal() {
        let env: HashMap<String, String> = HashMap::new();
        assert_eq!(substitute_dollar("$(UNDEF)/x", &env), "$(UNDEF)/x");
    }

    #[test]
    fn substitute_dollar_escapes_double_dollar() {
        let env: HashMap<String, String> =
            [("V".to_string(), "value".to_string())].into_iter().collect();
        // $$ -> $; then "(V)" is plain text, so $(V) literal stays
        assert_eq!(substitute_dollar("$$(V)", &env), "$(V)");
        // No false re-expansion: $$ -> $ only
        assert_eq!(substitute_dollar("$$X", &env), "$X");
    }

    #[test]
    fn substitute_dollar_keeps_bare_dollar() {
        let env: HashMap<String, String> = HashMap::new();
        assert_eq!(substitute_dollar("$VAR", &env), "$VAR");
        assert_eq!(substitute_dollar("end$", &env), "end$");
        // Unclosed $( is left literal
        assert_eq!(substitute_dollar("$(unclosed", &env), "$(unclosed");
    }

    #[tokio::test]
    async fn full_pipeline_substitutes_km_container_root() {
        let dir = temp_dir("substitute-km");
        let pod = make_pod(
            vec![Container {
                name: "app".to_string(),
                working_dir: String::new(),
                env: vec![EnvVar {
                    name: "ROOT".to_string(),
                    value: Some("$(KM_CONTAINER_ROOT)".to_string()),
                    value_from: None,
                }],
                command: vec!["sh".to_string(), "-c".to_string()],
                args: vec!["echo $(ROOT)/marker".to_string()],
                ..Default::default()
            }],
            vec![],
        );

        let (_tx, mut cancel) = watch::channel(());
        let prepared = prepare_pod(
            &pod,
            &dir,
            "default",
            &ResolverSet::default(),
            Path::new(""),
            None,
            &mut cancel,
        )
        .await
        .unwrap();

        let working_dir = prepared.spec.containers[0].working_dir.clone();
        let env: HashMap<&str, &str> = prepared.spec.containers[0]
            .env
            .iter()
            .map(|e| (e.name.as_str(), e.value.as_deref().unwrap()))
            .collect();
        assert_eq!(env["ROOT"], working_dir);
        assert_eq!(
            prepared.spec.containers[0].args[0],
            format!("echo {}/marker", working_dir),
        );
    }
}
