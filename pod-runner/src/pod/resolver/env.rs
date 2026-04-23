// Copyright 2026 Tabsdata Inc.

//! Environment resolution — collect and merge env vars for all containers.
//!
//! Resolves literal `env` values, `valueFrom.secretKeyRef`,
//! `valueFrom.configMapKeyRef`, `envFrom.secretRef`,
//! `envFrom.configMapRef`, and node-level env vars into a flat
//! `HashMap<String, String>` per container.
//!
//! # Priority (lowest to highest)
//!
//! 1. Node-level envs
//! 2. `envFrom` entries (in order)
//! 3. Individual `env` entries

use std::collections::{HashMap, HashSet};

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

/// Resolved environment for all containers in a pod.
#[derive(Debug)]
pub struct ResolvedEnvs {
    pub containers: HashMap<String, ContainerEnv>,
}

// ---------------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------------

/// Resolve all environment variable references for every container.
pub async fn resolve_envs(
    pod: &PodSpec,
    namespace: &str,
    resolvers: &ResolverSet,
) -> Result<ResolvedEnvs, String> {
    // Collect all referenced secret and configmap names
    let mut secret_names: Vec<String> = Vec::new();
    let mut configmap_names: Vec<String> = Vec::new();

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

    // Dedup
    secret_names.dedup();
    configmap_names.dedup();

    // Batch resolve secrets
    let resolved_secrets = if !secret_names.is_empty() {
        let resolver = resolvers
            .secret_resolver
            .as_ref()
            .ok_or("Pod references secrets but no SecretResolver configured")?;
        resolver
            .secrets(namespace, &secret_names)
            .await
            .map_err(|e| e.to_string())?
    } else {
        HashMap::new()
    };

    // Batch resolve configmaps
    let resolved_configmaps = if !configmap_names.is_empty() {
        let resolver = resolvers
            .config_map_resolver
            .as_ref()
            .ok_or("Pod references configmaps but no ConfigMapResolver configured")?;
        resolver
            .config_maps(namespace, &configmap_names)
            .await
            .map_err(|e| e.to_string())?
    } else {
        HashMap::new()
    };

    // Build per-container env
    let mut result = ResolvedEnvs {
        containers: HashMap::new(),
    };

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
            } else if let Some(ref sec_ref) = ef.secret_ref {
                if let Some(sec_data) = resolved_secrets.get(&sec_ref.name) {
                    env.extend(sec_data.iter().map(|(k, v)| (k.clone(), v.clone())));
                    secret_keys.extend(sec_data.keys().cloned());
                }
            }
        }

        // 3. Individual env entries (highest priority)
        for ev in &c.env {
            if let Some(ref value) = ev.value {
                env.insert(ev.name.clone(), value.clone());
            } else if let Some(ref vf) = ev.value_from {
                if let Some(ref sec_ref) = vf.secret_key_ref {
                    if let Some(sec_data) = resolved_secrets.get(&sec_ref.name) {
                        if let Some(val) = sec_data.get(&sec_ref.key) {
                            env.insert(ev.name.clone(), val.clone());
                            secret_keys.insert(ev.name.clone());
                        }
                    }
                } else if let Some(ref cm_ref) = vf.config_map_key_ref {
                    if let Some(cm_data) = resolved_configmaps.get(&cm_ref.name) {
                        if let Some(val) = cm_data.get(&cm_ref.key) {
                            env.insert(ev.name.clone(), val.clone());
                        }
                    }
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

        let result = resolve_envs(&pod, "default", &ResolverSet::default()).await.unwrap();
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

        let result = resolve_envs(&pod, "default", &resolvers).await.unwrap();
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

        let result = resolve_envs(&pod, "default", &resolvers).await.unwrap();
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

        let result = resolve_envs(&pod, "default", &resolvers).await.unwrap();
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
                secret_ref: Some(ObjectRef { name: "creds".to_string() }),
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            secret_resolver: Some(Box::new(FileSecretResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_envs(&pod, "default", &resolvers).await.unwrap();
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
                config_map_ref: Some(ObjectRef { name: "settings".to_string() }),
                ..Default::default()
            }],
            ..Default::default()
        }]);

        let resolvers = ResolverSet {
            config_map_resolver: Some(Box::new(FileConfigMapResolver::new(dir).unwrap())),
            ..Default::default()
        };

        let result = resolve_envs(&pod, "default", &resolvers).await.unwrap();
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
                    }),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        }]);

        let err = resolve_envs(&pod, "default", &ResolverSet::default()).await.unwrap_err();
        assert!(err.contains("no SecretResolver"));
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

        let result = resolve_envs(&pod, "default", &ResolverSet::default()).await.unwrap();
        assert_eq!(result.containers["app"].env["PLAIN"], "val");
    }
}
