// Copyright 2026 Tabsdata Inc.

//! Resolvers for secrets, config maps, node-level envs, and volumes.
//!
//! Resolver traits define how external data is fetched. Implementations
//! include file-based (reads YAML from disk) and kube-based (reads from
//! K8s API via kubeconfig).

pub mod config_map;
pub mod env;
pub mod secret;
pub mod volume;

use std::collections::HashMap;
use std::path::PathBuf;

/// Configuration for a resolver. The `type` field selects the
/// implementation, and `configs` holds implementation-specific settings.
#[derive(Debug, Clone, Default)]
pub struct ResolverConfig {
    /// Resolver type: `""` (disabled), `"file"`, `"kube"`, `"podrunner"`.
    pub resolver_type: String,
    /// Implementation-specific settings (e.g. `dir`, `file`, `kubeconfig`).
    pub configs: HashMap<String, String>,
}

/// A set of resolvers used during pod preparation.
pub struct ResolverSet {
    pub secret_resolver: Option<Box<dyn secret::SecretResolver>>,
    pub config_map_resolver: Option<Box<dyn config_map::ConfigMapResolver>>,
    /// Node-level environment variables (lowest priority).
    pub node_envs: HashMap<String, String>,
}

impl Default for ResolverSet {
    fn default() -> Self {
        Self {
            secret_resolver: None,
            config_map_resolver: None,
            node_envs: HashMap::new(),
        }
    }
}

impl ResolverSet {
    /// Build a ResolverSet from configuration.
    pub async fn from_configs(
        secret_config: &ResolverConfig,
        config_map_config: &ResolverConfig,
        node_env_config: &ResolverConfig,
        kubeconfig: &str,
    ) -> Result<Self, String> {
        let secret_resolver: Option<Box<dyn secret::SecretResolver>> =
            match secret_config.resolver_type.as_str() {
                "" | "none" => None,
                "file" => {
                    let dir = secret_config
                        .configs
                        .get("dir")
                        .ok_or("FileSecretResolver requires 'dir' config")?;
                    Some(Box::new(secret::FileSecretResolver::new(PathBuf::from(dir))?))
                }
                "kube" => {
                    let kc = secret_config.configs.get("kubeconfig")
                        .map(|s| s.as_str())
                        .unwrap_or(kubeconfig);
                    Some(Box::new(
                        secret::KubeSecretResolver::new(kc).await?,
                    ))
                }
                t => return Err(format!("Unknown secret resolver type: '{}'", t)),
            };

        let config_map_resolver: Option<Box<dyn config_map::ConfigMapResolver>> =
            match config_map_config.resolver_type.as_str() {
                "" | "none" => None,
                "file" => {
                    let dir = config_map_config
                        .configs
                        .get("dir")
                        .ok_or("FileConfigMapResolver requires 'dir' config")?;
                    Some(Box::new(config_map::FileConfigMapResolver::new(
                        PathBuf::from(dir),
                    )?))
                }
                "kube" => {
                    let kc = config_map_config.configs.get("kubeconfig")
                        .map(|s| s.as_str())
                        .unwrap_or(kubeconfig);
                    Some(Box::new(
                        config_map::KubeConfigMapResolver::new(kc).await?,
                    ))
                }
                t => return Err(format!("Unknown config map resolver type: '{}'", t)),
            };

        let node_envs = match node_env_config.resolver_type.as_str() {
            "" | "none" => HashMap::new(),
            "podrunner" => std::env::vars().collect(),
            "file" => {
                let file = node_env_config
                    .configs
                    .get("file")
                    .ok_or("File node env resolver requires 'file' config")?;
                let content = std::fs::read_to_string(file)
                    .map_err(|e| format!("Failed to read node env file '{}': {}", file, e))?;
                serde_json::from_str::<HashMap<String, String>>(&content).or_else(|_| {
                    serde_yaml_ng::from_str::<HashMap<String, String>>(&content)
                        .map_err(|e| format!("Failed to parse node env file '{}': {}", file, e))
                })?
            }
            t => return Err(format!("Unknown node env resolver type: '{}'", t)),
        };

        Ok(Self {
            secret_resolver,
            config_map_resolver,
            node_envs,
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-resolver-set-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn from_configs_disabled() {
        let rs = ResolverSet::from_configs(
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            "",
        )
        .await
        .unwrap();
        assert!(rs.secret_resolver.is_none());
        assert!(rs.config_map_resolver.is_none());
        assert!(rs.node_envs.is_empty());
    }

    #[tokio::test]
    async fn from_configs_file_secret() {
        let dir = temp_dir("file-secret");
        let config = ResolverConfig {
            resolver_type: "file".to_string(),
            configs: [("dir".to_string(), dir.to_string_lossy().to_string())]
                .into_iter()
                .collect(),
        };
        let rs = ResolverSet::from_configs(
            &config,
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            "",
        )
        .await
        .unwrap();
        assert!(rs.secret_resolver.is_some());
    }

    #[tokio::test]
    async fn from_configs_file_configmap() {
        let dir = temp_dir("file-cm");
        let config = ResolverConfig {
            resolver_type: "file".to_string(),
            configs: [("dir".to_string(), dir.to_string_lossy().to_string())]
                .into_iter()
                .collect(),
        };
        let rs = ResolverSet::from_configs(
            &ResolverConfig::default(),
            &config,
            &ResolverConfig::default(),
            "",
        )
        .await
        .unwrap();
        assert!(rs.config_map_resolver.is_some());
    }

    #[tokio::test]
    async fn from_configs_podrunner_node_envs() {
        let config = ResolverConfig {
            resolver_type: "podrunner".to_string(),
            ..Default::default()
        };
        let rs = ResolverSet::from_configs(
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            &config,
            "",
        )
        .await
        .unwrap();
        assert!(!rs.node_envs.is_empty());
    }

    #[tokio::test]
    async fn from_configs_file_node_envs() {
        let dir = temp_dir("file-node");
        let file = dir.join("node-envs.yaml");
        fs::write(&file, "MY_VAR: my_value\n").unwrap();

        let config = ResolverConfig {
            resolver_type: "file".to_string(),
            configs: [("file".to_string(), file.to_string_lossy().to_string())]
                .into_iter()
                .collect(),
        };
        let rs = ResolverSet::from_configs(
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            &config,
            "",
        )
        .await
        .unwrap();
        assert_eq!(rs.node_envs.get("MY_VAR").unwrap(), "my_value");
    }

    #[tokio::test]
    async fn from_configs_unknown_type_errors() {
        let config = ResolverConfig {
            resolver_type: "bogus".to_string(),
            ..Default::default()
        };
        let result = ResolverSet::from_configs(
            &config,
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            "",
        )
        .await;
        let err = result.err().expect("should fail");
        assert!(err.contains("Unknown secret resolver type"));
    }

    #[tokio::test]
    async fn from_configs_missing_dir_errors() {
        let config = ResolverConfig {
            resolver_type: "file".to_string(),
            configs: HashMap::new(),
        };
        let result = ResolverSet::from_configs(
            &config,
            &ResolverConfig::default(),
            &ResolverConfig::default(),
            "",
        )
        .await;
        let err = result.err().expect("should fail");
        assert!(err.contains("requires 'dir' config"));
    }
}
