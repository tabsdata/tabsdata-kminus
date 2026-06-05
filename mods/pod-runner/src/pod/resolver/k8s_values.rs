// Copyright 2026 Tabsdata Inc.

//! Resolution of Kubernetes-sourced pod values: env vars and configmap volumes.
//!
//! Resolves:
//! - Literal `env` values, `valueFrom.secretKeyRef`,
//!   `valueFrom.configMapKeyRef`, `envFrom.secretRef`, `envFrom.configMapRef`,
//!   and node-level env vars into a flat `HashMap<String, String>` per
//!   container.
//! - The data of each configmap referenced by a `configMap` volume, kept aside
//!   for [`crate::pod::resolver::volume`] to materialize as files at the
//!   container's mount path.
//!
//! # Env priority (lowest to highest)
//!
//! 1. Node-level envs
//! 2. `envFrom` entries (in order)
//! 3. Individual `env` entries

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::sync::watch;

use crate::pod::prepare::PrepareError;
use crate::spec::pod::PodSpec;

use super::ResolverSet;

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

/// Resolved environment for a single container.
#[derive(Debug)]
pub struct ContainerEnv {
    pub name: String,
    pub env: HashMap<String, String>,
    /// Env var names that came from secrets (for security tracking).
    pub secret_keys: HashSet<String>,
}

/// Resolved Kubernetes-sourced values for a pod: per-container envs plus the
/// data of every configmap referenced by a `configMap` volume.
#[derive(Debug, Default)]
pub struct ResolvedK8sValues {
    /// Per-container resolved environment.
    pub containers: HashMap<String, ContainerEnv>,
    /// `configmap_name -> {key: value}` for configmaps mounted as volumes.
    pub config_map_volumes: HashMap<String, HashMap<String, String>>,
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Resolve env references and configmap-volume data for every container.
///
/// `timeout` bounds how long the resolvers will wait for any missing
/// secret or configmap to appear. `None` means the wait is indefinite.
/// `cancel` is a watch receiver that, when fired, aborts the wait.
pub async fn resolve_k8s_values(
    pod: &PodSpec,
    namespace: &str,
    resolvers: &ResolverSet,
    timeout: Option<Duration>,
    cancel: &mut watch::Receiver<()>,
) -> Result<ResolvedK8sValues, PrepareError> {
    // Collect all referenced secret and configmap names
    let mut secret_names: Vec<String> = Vec::new();
    let mut configmap_names: Vec<String> = Vec::new();
    let mut configmap_volume_names: Vec<String> = Vec::new();

    for c in pod.spec.all_containers() {
        for ev in &c.env {
            if let Some(ref vf) = ev.value_from {
                if let Some(ref r) = vf.secret_key_ref {
                    secret_names.push(r.name.clone());
                } else if let Some(ref r) = vf.config_map_key_ref {
                    configmap_names.push(r.name.clone());
                }
            }
        }
        for ef in &c.env_from {
            if let Some(ref r) = ef.secret_ref {
                secret_names.push(r.name.clone());
            } else if let Some(ref r) = ef.config_map_ref {
                configmap_names.push(r.name.clone());
            }
        }
    }

    for v in &pod.spec.volumes {
        if let Some(ref cm) = v.config_map {
            configmap_names.push(cm.name.clone());
            configmap_volume_names.push(cm.name.clone());
        }
    }

    // Dedup
    secret_names.dedup();
    configmap_names.dedup();
    configmap_volume_names.dedup();

    // Batch resolve secrets
    let resolved_secrets = if !secret_names.is_empty() {
        let resolver = resolvers
            .secret_resolver
            .as_ref()
            .ok_or(PrepareError::NoSecretResolver)?;
        resolver
            .secrets(namespace, &secret_names, timeout, cancel)
            .await?
    } else {
        HashMap::new()
    };

    // Batch resolve configmaps
    let resolved_configmaps = if !configmap_names.is_empty() {
        let resolver = resolvers
            .config_map_resolver
            .as_ref()
            .ok_or(PrepareError::NoConfigMapResolver)?;
        resolver
            .config_maps(namespace, &configmap_names, timeout, cancel)
            .await?
    } else {
        HashMap::new()
    };

    // Build per-container env
    let mut result = ResolvedK8sValues {
        containers: HashMap::new(),
        config_map_volumes: HashMap::new(),
    };

    // Stash configmap data referenced by volumes for the volume resolver.
    for name in &configmap_volume_names {
        let data = resolved_configmaps
            .get(name)
            .cloned()
            .ok_or_else(|| PrepareError::ConfigMapVolumeNotResolved(name.clone()))?;
        result.config_map_volumes.insert(name.clone(), data);
    }

    for c in pod.spec.all_containers() {
        let mut env: HashMap<String, String> = HashMap::new();
        let mut secret_keys: HashSet<String> = HashSet::new();

        // 1. Node-level envs (lowest priority)
        env.extend(resolvers.node_envs.iter().map(|(k, v)| (k.clone(), v.clone())));

        // 2. envFrom (bulk loading, in order)
        for ef in &c.env_from {
            if let Some(ref cm_ref) = ef.config_map_ref {
                if let Some(cm_data) = resolved_configmaps.get(&cm_ref.name) {
                    env.extend(cm_data.iter().map(|(k, v)| (k.clone(), v.clone())));
                }
            } else if let Some(ref sec_ref) = ef.secret_ref
                && let Some(sec_data) = resolved_secrets.get(&sec_ref.name) {
                    env.extend(sec_data.iter().map(|(k, v)| (k.clone(), v.clone())));
                    secret_keys.extend(sec_data.keys().cloned());
                }
        }

        // 3. Individual env entries (highest priority)
        for ev in &c.env {
            if let Some(ref value) = ev.value {
                env.insert(ev.name.clone(), value.clone());
            } else if let Some(ref vf) = ev.value_from {
                if let Some(ref sec_ref) = vf.secret_key_ref {
                    if let Some(sec_data) = resolved_secrets.get(&sec_ref.name)
                        && let Some(val) = sec_data.get(&sec_ref.key) {
                            env.insert(ev.name.clone(), val.clone());
                            secret_keys.insert(ev.name.clone());
                        }
                } else if let Some(ref cm_ref) = vf.config_map_key_ref
                    && let Some(cm_data) = resolved_configmaps.get(&cm_ref.name)
                        && let Some(val) = cm_data.get(&cm_ref.key) {
                            env.insert(ev.name.clone(), val.clone());
                        }
            }
        }

        result.containers.insert(
            c.name.clone(),
            ContainerEnv {
                name: c.name.clone(),
                env,
                secret_keys,
            },
        );
    }

    Ok(result)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pod::resolver::config_map::FileConfigMapResolver;
    use crate::pod::resolver::secret::FileSecretResolver;
    use crate::spec::pod::*;
    use std::fs;

    fn setup_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-env-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn make_pod_with_envs(containers: Vec<Container>) -> PodSpec {
        PodSpec {
            metadata: PodMetadata {
                namespace: "default".to_string(),
                name: "test".to_string(),
                uid: "uid-1".to_string(),
                ..Default::default()
            },
            spec: PodSpecBody {
                containers,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    /// Resolve with no timeout and a fresh (never-fired) cancel signal —
    /// the common case for tests where the resolver should succeed on the
    /// first try.
    async fn resolve_no_wait(
        pod: &PodSpec,
        namespace: &str,
        resolvers: &ResolverSet,
    ) -> Result<ResolvedK8sValues, PrepareError> {
        let (_tx, mut rx) = watch::channel(());
        resolve_k8s_values(pod, namespace, resolvers, None, &mut rx).await
    }

    #[tokio::test]
    async fn literal_env_values() {
        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env: vec![
                EnvVar { name: "FOO".to_string(), value: Some("bar".to_string()), value_from: None },
                EnvVar { name: "BAZ".to_string(), value: Some("qux".to_string()), value_from: None },
            ],
            ..Default::default()
        }]);

        let result = resolve_no_wait(&pod, "default", &ResolverSet::default()).await.unwrap();
        let app = &result.containers["app"];
        assert_eq!(app.env["FOO"], "bar");
        assert_eq!(app.env["BAZ"], "qux");
        assert!(app.secret_keys.is_empty());
    }

    #[tokio::test]
    async fn node_envs_lowest_priority() {
        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "KEY".to_string(),
                value: Some("from-spec".to_string()),
                value_from: None,
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            node_envs: [("KEY".to_string(), "from-node".to_string())]
                .into_iter()
                .collect(),
            ..Default::default()
        };

        let result = resolve_no_wait(&pod, "default", &resolvers).await.unwrap();
        // Individual env overrides node env
        assert_eq!(result.containers["app"].env["KEY"], "from-spec");
    }

    #[tokio::test]
    async fn secret_key_ref() {
        let dir = setup_dir("secret-ref");
        fs::write(
            dir.join("my-secret.yaml"),
            "stringData:\n  password: s3cret\n",
        )
        .unwrap();

        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "DB_PASS".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(KeyRef {
                        name: "my-secret".to_string(),
                        key: "password".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            secret_resolver: Some(Box::new(FileSecretResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_no_wait(&pod, "default", &resolvers).await.unwrap();
        assert_eq!(result.containers["app"].env["DB_PASS"], "s3cret");
        assert!(result.containers["app"].secret_keys.contains("DB_PASS"));
    }

    #[tokio::test]
    async fn configmap_key_ref() {
        let dir = setup_dir("cm-ref");
        fs::write(dir.join("app-config.yaml"), "data:\n  port: '8080'\n").unwrap();

        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PORT".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    config_map_key_ref: Some(KeyRef {
                        name: "app-config".to_string(),
                        key: "port".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            config_map_resolver: Some(Box::new(FileConfigMapResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_no_wait(&pod, "default", &resolvers).await.unwrap();
        assert_eq!(result.containers["app"].env["PORT"], "8080");
        assert!(!result.containers["app"].secret_keys.contains("PORT"));
    }

    #[tokio::test]
    async fn env_from_secret_ref() {
        let dir = setup_dir("env-from-sec");
        fs::write(
            dir.join("creds.yaml"),
            "stringData:\n  USER: admin\n  PASS: secret\n",
        )
        .unwrap();

        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env_from: vec![EnvFromSource {
                secret_ref: Some(ObjectRef { name: "creds".to_string(), ..Default::default() }),
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            secret_resolver: Some(Box::new(FileSecretResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_no_wait(&pod, "default", &resolvers).await.unwrap();
        let app = &result.containers["app"];
        assert_eq!(app.env["USER"], "admin");
        assert_eq!(app.env["PASS"], "secret");
        assert!(app.secret_keys.contains("USER"));
        assert!(app.secret_keys.contains("PASS"));
    }

    #[tokio::test]
    async fn env_from_configmap_ref() {
        let dir = setup_dir("env-from-cm");
        fs::write(dir.join("settings.yaml"), "data:\n  LOG_LEVEL: debug\n").unwrap();

        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env_from: vec![EnvFromSource {
                config_map_ref: Some(ObjectRef { name: "settings".to_string(), ..Default::default() }),
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            config_map_resolver: Some(Box::new(FileConfigMapResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_no_wait(&pod, "default", &resolvers).await.unwrap();
        assert_eq!(result.containers["app"].env["LOG_LEVEL"], "debug");
    }

    #[tokio::test]
    async fn missing_resolver_errors() {
        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "X".to_string(),
                value: None,
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(KeyRef {
                        name: "sec".to_string(),
                        key: "k".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        }]);

        let err = resolve_no_wait(&pod, "default", &ResolverSet::default()).await.unwrap_err();
        assert!(matches!(err, PrepareError::NoSecretResolver));
    }

    #[tokio::test]
    async fn config_map_volume_data_resolved() {
        let dir = setup_dir("cm-vol");
        fs::write(
            dir.join("app-config.yaml"),
            "data:\n  app.conf: \"setting=value\"\n  log.yaml: \"level: info\"\n",
        )
        .unwrap();

        let mut pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            volume_mounts: vec![VolumeMount {
                name: "config".to_string(),
                mount_path: "/etc/app".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        }]);
        pod.spec.volumes.push(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolume {
                name: "app-config".to_string(),
            }),
            ..Default::default()
        });

        let resolvers = ResolverSet {
            config_map_resolver: Some(Box::new(FileConfigMapResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_no_wait(&pod, "default", &resolvers).await.unwrap();
        let cm = &result.config_map_volumes["app-config"];
        assert_eq!(cm["app.conf"], "setting=value");
        assert_eq!(cm["log.yaml"], "level: info");
    }

    #[tokio::test]
    async fn config_map_volume_missing_resolver_errors() {
        let mut pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            ..Default::default()
        }]);
        pod.spec.volumes.push(Volume {
            name: "config".to_string(),
            config_map: Some(ConfigMapVolume {
                name: "missing".to_string(),
            }),
            ..Default::default()
        });

        let err = resolve_no_wait(&pod, "default", &ResolverSet::default()).await.unwrap_err();
        assert!(matches!(err, PrepareError::NoConfigMapResolver));
    }

    #[tokio::test]
    async fn no_env_refs_needs_no_resolvers() {
        let pod = make_pod_with_envs(vec![Container {
            name: "app".to_string(),
            env: vec![EnvVar {
                name: "PLAIN".to_string(),
                value: Some("val".to_string()),
                value_from: None,
            }],
            ..Default::default()
        }]);

        let result = resolve_no_wait(&pod, "default", &ResolverSet::default()).await.unwrap();
        assert_eq!(result.containers["app"].env["PLAIN"], "val");
    }
}
