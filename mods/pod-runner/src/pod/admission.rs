// Copyright 2026 Tabsdata Inc.

//! Pod admission — validate a pod spec before it is stored and prepared.
//!
//! Admission runs before storage and before [`crate::pod::prepare::prepare_pod`].
//! It enforces pod-runner-specific constraints on top of structural YAML
//! validation. A rejected pod never enters the store and the API returns
//! the error verbatim to the caller.
//!
//! # Rules
//!
//! - `optional: true` is rejected on every configmap/secret reference
//!   (`env.valueFrom.secretKeyRef`, `env.valueFrom.configMapKeyRef`,
//!   `envFrom.secretRef`, `envFrom.configMapRef`). Pod-runner has no
//!   facility to silently skip a missing reference — see
//!   [Known limitations](#known-limitations).
//! - Volumes must use a supported source: `hostPath`, `emptyDir`,
//!   `configMap`, or `projected`. Any other source (e.g. `secret`,
//!   `persistentVolumeClaim`, `nfs`) is rejected rather than silently
//!   producing no mount.
//! - Probes must use `exec`, `httpGet`, or `tcpSocket`. A `gRPC` probe is
//!   rejected (it would otherwise be a no-op that always "succeeds").
//! - `volumeMount.subPath` / `subPathExpr` is rejected — a mount always
//!   exposes the whole volume directory.
//! - `volumeMount.mountPath` must be a container-absolute path: `/`-rooted,
//!   free of `':'` (the k8s `VolumeMount` contract) and of `..` segments. A
//!   host path — notably a Windows drive-absolute one — is rejected; see
//!   [`check_mount_paths`] for why it is destructive rather than merely
//!   misplaced.
//!
//! # Known limitations
//!
//! 1. **Existence is checked at the entry (configmap/secret) level, not the
//!    key level.** The preparer waits for the named configmap/secret to
//!    become available, but if the named key is absent inside an existing
//!    configmap/secret the env entry is silently skipped (see
//!    [`crate::pod::resolver::k8s_values`]).
//! 2. **`optional` is not supported.** Pods using `optional: true` on any
//!    configmap/secret reference are rejected here at admission. `optional:
//!    false` is accepted (it's the implicit default).
//! 3. **Pod-level wait barrier (stricter than k8s).** Pod-runner waits for
//!    the *union* of every configmap/secret referenced by every init and
//!    main container (and every `configMap` volume) to be available before
//!    starting any container. K8s, by contrast, waits per-container: an
//!    init or main container whose own references resolve can proceed
//!    while siblings keep waiting. Observable only when references
//!    straddle multiple containers and resources land out of order.

use crate::spec::pod::{Container, PodSpec, Probe, Volume};

/// Error message prefix when admission rejects a pod for using
/// `optional: true` on a configmap or secret reference.
pub const ERR_OPTIONAL_NOT_SUPPORTED: &str =
    "pod-runner does not support 'optional: true' on configmap/secret references";

/// Error message prefix when a volume uses a source pod-runner can't mount.
pub const ERR_UNSUPPORTED_VOLUME: &str =
    "pod-runner only supports hostPath, emptyDir, configMap and projected volumes";

/// Error message prefix when a probe uses an unsupported action (gRPC).
pub const ERR_UNSUPPORTED_PROBE: &str =
    "pod-runner only supports exec, httpGet and tcpSocket probes";

/// Error message prefix when a volumeMount uses `subPath`/`subPathExpr`.
pub const ERR_SUBPATH_NOT_SUPPORTED: &str =
    "pod-runner does not support volumeMount subPath/subPathExpr";

/// Error message prefix when a volumeMount's `mountPath` is not a
/// container-absolute path (`/`-rooted, no `':'`, no `..` segment). The
/// specific reason is reported in the error's location.
pub const ERR_INVALID_MOUNT_PATH: &str =
    "pod-runner requires volumeMount mountPath to be container-absolute";

/// Error message prefix when a pod's `metadata.uid` cannot name its data
/// dir.
pub const ERR_INVALID_UID: &str =
    "pod-runner requires metadata.uid to be ASCII letters, digits and '-'";

/// Volume mount fields that pod-runner cannot honor.
const UNSUPPORTED_MOUNT_FIELDS: &[&str] = &["subPath", "subPathExpr"];

/// Root a `mountPath` must start with — mount paths are container-absolute
/// POSIX paths, never host paths.
const MOUNT_PATH_ROOT: char = '/';

/// Character the k8s `VolumeMount` contract forbids in a `mountPath`.
/// Rejecting it also rules out Windows drive-absolute paths (`C:\dir`).
const MOUNT_PATH_FORBIDDEN_CHAR: char = ':';

/// Segment that would let a mount escape the container root.
const MOUNT_PATH_PARENT_SEGMENT: &str = "..";

/// Separators recognized when splitting a `mountPath` into segments. Both
/// are honored on every platform so a spec is admitted or rejected
/// identically regardless of the host OS.
const MOUNT_PATH_SEPARATORS: [char; 2] = ['/', '\\'];

/// Why a `mountPath` was rejected.
const MOUNT_PATH_REASON_EMPTY: &str = "empty";
const MOUNT_PATH_REASON_NOT_ROOTED: &str = "not '/'-rooted";
const MOUNT_PATH_REASON_FORBIDDEN_CHAR: &str = "contains ':'";
const MOUNT_PATH_REASON_PARENT_SEGMENT: &str = "contains a '..' segment";

/// Errors raised when admitting a pod spec.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// The pod sets `optional: true` on a configmap/secret reference.
    #[error("{ERR_OPTIONAL_NOT_SUPPORTED} ({location})")]
    OptionalNotSupported { location: String },
    /// A volume uses a source other than hostPath/emptyDir/configMap/projected.
    #[error("{ERR_UNSUPPORTED_VOLUME} ({location})")]
    UnsupportedVolume { location: String },
    /// A probe uses an unsupported action (gRPC).
    #[error("{ERR_UNSUPPORTED_PROBE} ({location})")]
    UnsupportedProbe { location: String },
    /// A volumeMount sets `subPath` or `subPathExpr`.
    #[error("{ERR_SUBPATH_NOT_SUPPORTED} ({location})")]
    SubPathNotSupported { location: String },
    /// A volumeMount's `mountPath` is not a container-absolute path.
    #[error("{ERR_INVALID_MOUNT_PATH} ({location})")]
    InvalidMountPath { location: String },
    /// The pod's `metadata.uid` is empty or not a plain path component.
    #[error("{ERR_INVALID_UID} (metadata.uid: '{uid}')")]
    InvalidUid { uid: String },
}

/// Validate a pod spec. Returns `Ok(())` if the spec is accepted, or an
/// [`AdmissionError`] describing the first rule violation.
///
/// Rejecting at admission means a pod whose spec uses a feature pod-runner
/// would otherwise silently ignore — an unmountable volume source, a gRPC
/// probe that would never run, a `subPath` mount — fails fast with a clear
/// error instead of running with surprising behavior.
///
/// Called from the logic layer before the pod is stored. A rejected pod
/// never enters the store, so the caller is responsible for surfacing the
/// error to the API client.
pub fn admit(spec: &PodSpec) -> Result<(), AdmissionError> {
    check_uid(&spec.metadata.uid)?;
    for v in &spec.spec.volumes {
        check_supported_volume(v)?;
    }
    for c in spec
        .spec
        .init_containers
        .iter()
        .chain(spec.spec.containers.iter())
    {
        check_no_optional_refs(c)?;
        check_supported_probes(c)?;
        check_no_subpath_mounts(c)?;
        check_mount_paths(c)?;
    }
    Ok(())
}

/// Reject a uid that cannot name the pod's data dir, `pod-run/<uid>/`: an
/// empty one, or one with a character that is not an ASCII letter, a digit
/// or `-`. Kubernetes' own uids are UUIDs. Without `_`, no uid can be
/// mistaken for the `<namespace>_<name>_<uid>` links beside the data dirs.
fn check_uid(uid: &str) -> Result<(), AdmissionError> {
    if uid.is_empty() || !uid.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
        return Err(AdmissionError::InvalidUid {
            uid: uid.to_string(),
        });
    }
    Ok(())
}

/// Reject a volume whose source isn't one of the supported kinds
/// (hostPath, emptyDir, configMap, projected).
fn check_supported_volume(v: &Volume) -> Result<(), AdmissionError> {
    if v.host_path.is_some()
        || v.empty_dir.is_some()
        || v.config_map.is_some()
        || v.projected.is_some()
    {
        return Ok(());
    }
    // No supported source set. Name the unsupported source key(s) if the
    // YAML carried any (e.g. `secret`, `persistentVolumeClaim`, `nfs`),
    // otherwise report the volume as sourceless.
    let detail = if v.extra.is_empty() {
        "no supported source".to_string()
    } else {
        let mut keys: Vec<&str> = v.extra.keys().map(String::as_str).collect();
        keys.sort_unstable();
        format!("unsupported source: {}", keys.join(", "))
    };
    Err(AdmissionError::UnsupportedVolume {
        location: format!("volume '{}', {detail}", v.name),
    })
}

/// Reject probes that use an unsupported action. Only gRPC is unsupported;
/// exec, httpGet and tcpSocket all execute.
fn check_supported_probes(c: &Container) -> Result<(), AdmissionError> {
    for (kind, probe) in [
        ("startupProbe", &c.startup_probe),
        ("livenessProbe", &c.liveness_probe),
        ("readinessProbe", &c.readiness_probe),
    ] {
        if let Some(p) = probe {
            check_supported_probe(c, kind, p)?;
        }
    }
    Ok(())
}

fn check_supported_probe(c: &Container, kind: &str, p: &Probe) -> Result<(), AdmissionError> {
    if p.grpc.is_some() {
        return Err(AdmissionError::UnsupportedProbe {
            location: format!("container '{}', {kind}: gRPC", c.name),
        });
    }
    Ok(())
}

/// Reject any volumeMount that sets `subPath` / `subPathExpr` — pod-runner
/// always exposes the whole volume directory at the mount path.
fn check_no_subpath_mounts(c: &Container) -> Result<(), AdmissionError> {
    for vm in &c.volume_mounts {
        for field in UNSUPPORTED_MOUNT_FIELDS {
            if vm.extra.contains_key(*field) {
                return Err(AdmissionError::SubPathNotSupported {
                    location: format!(
                        "container '{}', volumeMount '{}' sets {field}",
                        c.name, vm.name
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Reject any `mountPath` that is not a container-absolute path.
///
/// `mountPath` addresses a location *inside* the container, so it is
/// `/`-rooted and — per the k8s `VolumeMount` contract — must not contain
/// `':'`. Rejecting a host path here is not pedantry, it prevents data loss:
/// [`crate::pod::prepare::rewrite_pod_paths`] rebases a mount under
/// `<pod_dir>/work/<container>` by stripping the leading `/` and joining, and
/// `Path::join` discards the base when handed an absolute path. On Windows a
/// drive-absolute `mountPath` (`C:\dir`) therefore survives the rewrite
/// untouched, and [`crate::pod::resolver::volume`] then removes that host
/// directory and replaces it with a symlink to the volume. When the volume's
/// own source is that same directory (a `hostPath` whose path equals the
/// `mountPath`), the two links point at each other and every access through
/// the host path fails to resolve. A `..` segment escapes the container root
/// the same way, on every platform.
///
/// Separators are checked platform-independently so a spec is admitted or
/// rejected identically on Linux, macOS and Windows.
fn check_mount_paths(c: &Container) -> Result<(), AdmissionError> {
    for vm in &c.volume_mounts {
        let reason = if vm.mount_path.is_empty() {
            Some(MOUNT_PATH_REASON_EMPTY)
        } else if !vm.mount_path.starts_with(MOUNT_PATH_ROOT) {
            Some(MOUNT_PATH_REASON_NOT_ROOTED)
        } else if vm.mount_path.contains(MOUNT_PATH_FORBIDDEN_CHAR) {
            Some(MOUNT_PATH_REASON_FORBIDDEN_CHAR)
        } else if vm
            .mount_path
            .split(MOUNT_PATH_SEPARATORS)
            .any(|segment| segment == MOUNT_PATH_PARENT_SEGMENT)
        {
            Some(MOUNT_PATH_REASON_PARENT_SEGMENT)
        } else {
            None
        };
        if let Some(reason) = reason {
            return Err(AdmissionError::InvalidMountPath {
                location: format!(
                    "container '{}', volumeMount '{}' mountPath '{}': {reason}",
                    c.name, vm.name, vm.mount_path
                ),
            });
        }
    }
    Ok(())
}

/// Reject any configmap/secret reference on `c` that sets `optional: true`.
fn check_no_optional_refs(c: &Container) -> Result<(), AdmissionError> {
    for ev in &c.env {
        let Some(ref vf) = ev.value_from else {
            continue;
        };
        if let Some(ref r) = vf.secret_key_ref
            && r.optional == Some(true)
        {
            return Err(AdmissionError::OptionalNotSupported {
                location: format!(
                    "container '{}', env '{}', secretKeyRef '{}/{}'",
                    c.name, ev.name, r.name, r.key
                ),
            });
        }
        if let Some(ref r) = vf.config_map_key_ref
            && r.optional == Some(true)
        {
            return Err(AdmissionError::OptionalNotSupported {
                location: format!(
                    "container '{}', env '{}', configMapKeyRef '{}/{}'",
                    c.name, ev.name, r.name, r.key
                ),
            });
        }
    }
    for ef in &c.env_from {
        if let Some(ref r) = ef.secret_ref
            && r.optional == Some(true)
        {
            return Err(AdmissionError::OptionalNotSupported {
                location: format!("container '{}', envFrom secretRef '{}'", c.name, r.name),
            });
        }
        if let Some(ref r) = ef.config_map_ref
            && r.optional == Some(true)
        {
            return Err(AdmissionError::OptionalNotSupported {
                location: format!("container '{}', envFrom configMapRef '{}'", c.name, r.name),
            });
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::pod::{
        ConfigMapVolume, EmptyDirVolume, EnvFromSource, EnvVar, EnvVarSource, ExecAction,
        HostPathVolume, HttpGetAction, KeyRef, ObjectRef, PodMetadata, PodSpecBody,
        ProjectedVolume, TcpSocketAction, VolumeMount,
    };
    use std::collections::HashMap;

    fn pod_with_container(c: Container) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                name: "p".to_string(),
                namespace: "default".to_string(),
                uid: "uid-1".to_string(),
                ..Default::default()
            },
            spec: PodSpecBody {
                containers: vec![c],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn pod_with_init(c: Container) -> PodSpec {
        let mut p = pod_with_container(Container {
            name: "main".to_string(),
            ..Default::default()
        });
        p.spec.init_containers.push(c);
        p
    }

    #[test]
    fn accepts_minimal_pod() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            ..Default::default()
        });
        assert!(admit(&pod).is_ok());
    }

    #[test]
    fn accepts_secret_key_ref_without_optional() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PASS".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(KeyRef {
                        name: "creds".to_string(),
                        key: "pass".to_string(),
                        optional: None,
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        });
        assert!(admit(&pod).is_ok());
    }

    #[test]
    fn accepts_optional_false() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PASS".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(KeyRef {
                        name: "creds".to_string(),
                        key: "pass".to_string(),
                        optional: Some(false),
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        });
        assert!(admit(&pod).is_ok());
    }

    #[test]
    fn rejects_optional_secret_key_ref() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PASS".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(KeyRef {
                        name: "creds".to_string(),
                        key: "pass".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_OPTIONAL_NOT_SUPPORTED));
        assert!(err.contains("creds"));
        assert!(err.contains("pass"));
    }

    #[test]
    fn rejects_optional_config_map_key_ref() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PORT".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    config_map_key_ref: Some(KeyRef {
                        name: "cfg".to_string(),
                        key: "port".to_string(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_OPTIONAL_NOT_SUPPORTED));
        assert!(err.contains("cfg"));
    }

    #[test]
    fn rejects_optional_env_from_secret_ref() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            env_from: vec![EnvFromSource {
                secret_ref: Some(ObjectRef {
                    name: "bulk".to_string(),
                    optional: Some(true),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_OPTIONAL_NOT_SUPPORTED));
        assert!(err.contains("bulk"));
    }

    #[test]
    fn rejects_optional_env_from_config_map_ref() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            env_from: vec![EnvFromSource {
                config_map_ref: Some(ObjectRef {
                    name: "settings".to_string(),
                    optional: Some(true),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_OPTIONAL_NOT_SUPPORTED));
        assert!(err.contains("settings"));
    }

    #[test]
    fn rejects_optional_in_init_container() {
        let pod = pod_with_init(Container {
            name: "setup".to_string(),
            env_from: vec![EnvFromSource {
                secret_ref: Some(ObjectRef {
                    name: "init-creds".to_string(),
                    optional: Some(true),
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains("init-creds"));
        assert!(err.contains("setup"));
    }

    #[test]
    fn accepts_pod_with_active_deadline_seconds() {
        let mut pod = pod_with_container(Container {
            name: "app".to_string(),
            ..Default::default()
        });
        pod.spec.active_deadline_seconds = Some(300);
        assert!(admit(&pod).is_ok());
    }

    // -- supported volume sources --

    fn pod_with_volume(v: Volume) -> PodSpec {
        let mut p = pod_with_container(Container {
            name: "app".to_string(),
            ..Default::default()
        });
        p.spec.volumes.push(v);
        p
    }

    #[test]
    fn accepts_supported_volume_sources() {
        let supported = vec![
            Volume {
                name: "hp".to_string(),
                host_path: Some(HostPathVolume::default()),
                ..Default::default()
            },
            Volume {
                name: "ed".to_string(),
                empty_dir: Some(EmptyDirVolume {}),
                ..Default::default()
            },
            Volume {
                name: "cm".to_string(),
                config_map: Some(ConfigMapVolume::default()),
                ..Default::default()
            },
            Volume {
                name: "pj".to_string(),
                projected: Some(ProjectedVolume::default()),
                ..Default::default()
            },
        ];
        for v in supported {
            assert!(admit(&pod_with_volume(v)).is_ok());
        }
    }

    #[test]
    fn rejects_unsupported_volume_source() {
        // A `secret` volume lands in `extra` (pod-runner can't file-mount secrets).
        let mut extra = HashMap::new();
        extra.insert("secret".to_string(), serde_json::json!({"secretName": "s"}));
        let pod = pod_with_volume(Volume {
            name: "sec".to_string(),
            extra,
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_UNSUPPORTED_VOLUME), "got: {err}");
        assert!(err.contains("sec"));
        assert!(err.contains("secret"));
    }

    #[test]
    fn rejects_sourceless_volume() {
        let pod = pod_with_volume(Volume {
            name: "empty".to_string(),
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_UNSUPPORTED_VOLUME));
        assert!(err.contains("no supported source"));
    }

    #[test]
    fn unsupported_volume_deserializes_into_extra() {
        // The whole pipeline: a secret volume in YAML reaches admission via
        // `extra` and is rejected (rather than silently producing no mount).
        let yaml = r#"
metadata:
  uid: uid-1
spec:
  volumes:
    - name: sec
      secret:
        secretName: my-secret
  containers:
    - name: app
      image: /dev/null
"#;
        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(matches!(
            admit(&pod),
            Err(AdmissionError::UnsupportedVolume { .. })
        ));
    }

    // -- probes --

    fn exec_probe() -> Probe {
        Probe {
            exec_action: Some(ExecAction {
                command: vec!["true".to_string()],
            }),
            ..Default::default()
        }
    }

    #[test]
    fn accepts_supported_probes() {
        for probe in [
            exec_probe(),
            Probe {
                http_get: Some(HttpGetAction::default()),
                ..Default::default()
            },
            Probe {
                tcp_socket: Some(TcpSocketAction::default()),
                ..Default::default()
            },
        ] {
            let pod = pod_with_container(Container {
                name: "app".to_string(),
                liveness_probe: Some(probe),
                ..Default::default()
            });
            assert!(admit(&pod).is_ok());
        }
    }

    #[test]
    fn rejects_grpc_probe() {
        // gRPC probes would silently "always pass" — reject them. Cover all
        // three probe slots.
        let grpc = || Probe {
            grpc: Some(serde_json::json!({"port": 9000})),
            ..Default::default()
        };
        let cases = [
            (
                "startupProbe",
                Container {
                    name: "app".to_string(),
                    startup_probe: Some(grpc()),
                    ..Default::default()
                },
            ),
            (
                "livenessProbe",
                Container {
                    name: "app".to_string(),
                    liveness_probe: Some(grpc()),
                    ..Default::default()
                },
            ),
            (
                "readinessProbe",
                Container {
                    name: "app".to_string(),
                    readiness_probe: Some(grpc()),
                    ..Default::default()
                },
            ),
        ];
        for (kind, c) in cases {
            let err = admit(&pod_with_container(c)).unwrap_err().to_string();
            assert!(err.contains(ERR_UNSUPPORTED_PROBE), "{kind}: {err}");
            assert!(err.contains(kind), "{kind}: {err}");
        }
    }

    // -- volumeMount subPath --

    #[test]
    fn rejects_subpath_mount() {
        let mut extra = HashMap::new();
        extra.insert("subPath".to_string(), serde_json::json!("nested/file"));
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            volume_mounts: vec![VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                extra,
                ..Default::default()
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_SUBPATH_NOT_SUPPORTED), "got: {err}");
        assert!(err.contains("data"));
    }

    #[test]
    fn subpath_deserializes_into_extra_and_is_rejected() {
        let yaml = r#"
metadata:
  uid: uid-1
spec:
  containers:
    - name: app
      image: /dev/null
      volumeMounts:
        - name: data
          mountPath: /data
          subPath: nested/file
"#;
        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(matches!(
            admit(&pod),
            Err(AdmissionError::SubPathNotSupported { .. })
        ));
    }

    #[test]
    fn accepts_mount_without_subpath() {
        let pod = pod_with_container(Container {
            name: "app".to_string(),
            volume_mounts: vec![VolumeMount {
                name: "data".to_string(),
                mount_path: "/data".to_string(),
                read_only: true,
                ..Default::default()
            }],
            ..Default::default()
        });
        assert!(admit(&pod).is_ok());
    }

    // -- volumeMount mountPath --

    fn pod_with_mount_path(mount_path: &str) -> PodSpec {
        pod_with_container(Container {
            name: "app".to_string(),
            volume_mounts: vec![VolumeMount {
                name: "runtimes".to_string(),
                mount_path: mount_path.to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
    }

    #[test]
    fn accepts_container_absolute_mount_paths() {
        // A leading '..' in a *segment name* (not a segment) is fine, and so
        // is a path whose segments merely start with dots.
        for mount_path in [
            "/data",
            "/tabsdata/runtimes",
            "/a/b/c",
            "/.tools",
            "/..data",
        ] {
            assert!(
                admit(&pod_with_mount_path(mount_path)).is_ok(),
                "expected '{mount_path}' to be admitted"
            );
        }
    }

    #[test]
    fn rejects_non_rooted_mount_path() {
        for mount_path in ["", "data", "tabsdata/runtimes", "\\tabsdata\\runtimes"] {
            let err = admit(&pod_with_mount_path(mount_path))
                .unwrap_err()
                .to_string();
            assert!(err.contains(ERR_INVALID_MOUNT_PATH), "{mount_path}: {err}");
            assert!(err.contains("runtimes"), "{mount_path}: {err}");
        }
    }

    #[test]
    fn rejects_windows_host_mount_path() {
        // The pymanager/venvloader regression: a kminus pod def whose
        // mountPath expanded to the instance's runtimes host path. Left
        // unchecked, the volume resolver deletes that directory and replaces
        // it with a symlink pointing back at the volume.
        let err = admit(&pod_with_mount_path(
            "C:\\Users\\dimas\\.tabsdata\\instances\\tabsdata\\runtimes",
        ))
        .unwrap_err()
        .to_string();
        assert!(err.contains(ERR_INVALID_MOUNT_PATH), "got: {err}");
        assert!(err.contains(MOUNT_PATH_REASON_NOT_ROOTED), "got: {err}");
    }

    #[test]
    fn rejects_mount_path_with_colon() {
        // '/'-rooted but still carrying a drive/stream separator.
        let err = admit(&pod_with_mount_path("/C:/runtimes"))
            .unwrap_err()
            .to_string();
        assert!(err.contains(ERR_INVALID_MOUNT_PATH), "got: {err}");
        assert!(err.contains(MOUNT_PATH_REASON_FORBIDDEN_CHAR), "got: {err}");
    }

    #[test]
    fn rejects_mount_path_escaping_container_root() {
        // Both separators are honored on every platform, so a backslash-only
        // escape is caught on Unix too.
        for mount_path in ["/../escape", "/a/../../escape", "/a\\..\\escape"] {
            let err = admit(&pod_with_mount_path(mount_path))
                .unwrap_err()
                .to_string();
            assert!(err.contains(ERR_INVALID_MOUNT_PATH), "{mount_path}: {err}");
            assert!(
                err.contains(MOUNT_PATH_REASON_PARENT_SEGMENT),
                "{mount_path}: {err}"
            );
        }
    }

    #[test]
    fn rejects_invalid_mount_path_in_init_container() {
        let pod = pod_with_init(Container {
            name: "setup".to_string(),
            volume_mounts: vec![VolumeMount {
                name: "runtimes".to_string(),
                mount_path: "C:\\runtimes".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        });
        let err = admit(&pod).unwrap_err().to_string();
        assert!(err.contains(ERR_INVALID_MOUNT_PATH), "got: {err}");
        assert!(err.contains("setup"), "got: {err}");
    }

    #[test]
    fn invalid_mount_path_from_yaml_is_rejected() {
        // The full pipeline, as the pod def reached the runner.
        let yaml = r#"
metadata:
  uid: uid-1
spec:
  volumes:
    - name: runtimes
      hostPath:
        path: 'C:\Users\dimas\.tabsdata\instances\tabsdata\runtimes'
  containers:
    - name: app
      image: /dev/null
      volumeMounts:
        - name: runtimes
          mountPath: 'C:\Users\dimas\.tabsdata\instances\tabsdata\runtimes'
"#;
        let pod: PodSpec = serde_yaml_ng::from_str(yaml).unwrap();
        assert!(matches!(
            admit(&pod),
            Err(AdmissionError::InvalidMountPath { .. })
        ));
    }

    #[test]
    fn uids_that_cannot_name_a_data_dir_are_rejected() {
        for bad in ["", ".", "..", "a/b", r"a\b", "a_b", "a b", "a:b", "ü"] {
            let mut pod = pod_with_container(Container {
                name: "app".to_string(),
                ..Default::default()
            });
            pod.metadata.uid = bad.to_string();
            let err = admit(&pod).unwrap_err();
            assert!(matches!(err, AdmissionError::InvalidUid { .. }), "{bad:?}");
            assert!(err.to_string().contains(ERR_INVALID_UID), "{bad:?}");
        }
    }

    #[test]
    fn kubernetes_uids_are_admitted() {
        for good in ["d3744bc5-73c9-4f49-ae29-c9c7855b1c16", "uid-1", "A9"] {
            let mut pod = pod_with_container(Container {
                name: "app".to_string(),
                ..Default::default()
            });
            pod.metadata.uid = good.to_string();
            assert!(admit(&pod).is_ok(), "{good:?}");
        }
    }
}
