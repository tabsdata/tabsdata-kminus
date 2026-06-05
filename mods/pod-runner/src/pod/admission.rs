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

use crate::spec::pod::{Container, PodSpec};

/// Error message prefix when admission rejects a pod for using
/// `optional: true` on a configmap or secret reference.
pub const ERR_OPTIONAL_NOT_SUPPORTED: &str =
    "pod-runner does not support 'optional: true' on configmap/secret references";

/// Errors raised when admitting a pod spec.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    /// The pod sets `optional: true` on a configmap/secret reference.
    #[error("{ERR_OPTIONAL_NOT_SUPPORTED} ({location})")]
    OptionalNotSupported { location: String },
}

/// Validate a pod spec. Returns `Ok(())` if the spec is accepted, or an
/// [`AdmissionError`] describing the first rule violation.
///
/// Called from the logic layer before the pod is stored. A rejected pod
/// never enters the store, so the caller is responsible for surfacing the
/// error to the API client.
pub fn admit(spec: &PodSpec) -> Result<(), AdmissionError> {
    for c in spec.spec.init_containers.iter().chain(spec.spec.containers.iter()) {
        check_no_optional_refs(c)?;
    }
    Ok(())
}

/// Reject any configmap/secret reference on `c` that sets `optional: true`.
fn check_no_optional_refs(c: &Container) -> Result<(), AdmissionError> {
    for ev in &c.env {
        let Some(ref vf) = ev.value_from else { continue };
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
                location: format!(
                    "container '{}', envFrom secretRef '{}'",
                    c.name, r.name
                ),
            });
        }
        if let Some(ref r) = ef.config_map_ref
            && r.optional == Some(true)
        {
            return Err(AdmissionError::OptionalNotSupported {
                location: format!(
                    "container '{}', envFrom configMapRef '{}'",
                    c.name, r.name
                ),
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
        EnvFromSource, EnvVar, EnvVarSource, KeyRef, ObjectRef, PodMetadata, PodSpecBody,
    };

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
}
