// Copyright 2026 Tabsdata Inc.

//! ConfigMap resolver — fetch config map key/value data by name.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;

/// Fetch config maps by name. Returns `{name: {key: value}}` for each
/// requested name.
#[async_trait]
pub trait ConfigMapResolver: Send + Sync {
    async fn config_maps(
        &self,
        namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError>;
}

/// Error returned by a config map resolver.
#[derive(Debug)]
pub struct ConfigMapResolverError {
    pub name: String,
    pub message: String,
}

impl std::fmt::Display for ConfigMapResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "configmap '{}': {}", self.name, self.message)
    }
}

impl std::error::Error for ConfigMapResolverError {}

// ---------------------------------------------------------------------------
// File-based implementation
// ---------------------------------------------------------------------------

/// Reads config maps from `<name>.yaml` files in a directory.
pub struct FileConfigMapResolver {
    dir: PathBuf,
}

impl FileConfigMapResolver {
    pub fn new(dir: PathBuf) -> Result<Self, String> {
        if !dir.is_dir() {
            return Err(format!(
                "ConfigMap directory does not exist: {}",
                dir.display()
            ));
        }
        Ok(Self { dir })
    }
}

#[async_trait]
impl ConfigMapResolver for FileConfigMapResolver {
    async fn config_maps(
        &self,
        _namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError> {
        let mut result = HashMap::new();
        for name in names {
            let path = self.dir.join(format!("{}.yaml", name));
            let content =
                std::fs::read_to_string(&path).map_err(|e| ConfigMapResolverError {
                    name: name.clone(),
                    message: format!("failed to read {}: {}", path.display(), e),
                })?;
            let doc: serde_json::Value =
                serde_yaml_ng::from_str(&content).map_err(|e| ConfigMapResolverError {
                    name: name.clone(),
                    message: format!("invalid YAML in {}: {}", path.display(), e),
                })?;

            let mut data = HashMap::new();
            if let Some(obj) = doc.get("data").and_then(|d| d.as_object()) {
                for (key, val) in obj {
                    if let Some(s) = val.as_str() {
                        data.insert(key.clone(), s.to_string());
                    }
                }
            }
            result.insert(name.clone(), data);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Kube-based implementation
// ---------------------------------------------------------------------------

/// Reads config maps from the Kubernetes API using a kubeconfig file.
pub struct KubeConfigMapResolver {
    client: kube::Client,
}

impl KubeConfigMapResolver {
    pub async fn new(kubeconfig: &str) -> Result<Self, String> {
        let kubeconfig_data = kube::config::Kubeconfig::read_from(kubeconfig)
            .map_err(|e| format!("Failed to read kubeconfig '{}': {}", kubeconfig, e))?;
        let config = kube::Config::from_custom_kubeconfig(
            kubeconfig_data,
            &kube::config::KubeConfigOptions::default(),
        )
        .await
        .map_err(|e| format!("Failed to load kubeconfig '{}': {}", kubeconfig, e))?;

        let client = kube::Client::try_from(config)
            .map_err(|e| format!("Failed to create kube client: {}", e))?;

        Ok(Self { client })
    }
}

#[async_trait]
impl ConfigMapResolver for KubeConfigMapResolver {
    async fn config_maps(
        &self,
        namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError> {
        use k8s_openapi::api::core::v1::ConfigMap;
        use kube::api::Api;

        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        let mut result = HashMap::new();

        for name in names {
            let cm = api.get(name).await.map_err(|e| ConfigMapResolverError {
                name: name.clone(),
                message: format!("{}", e),
            })?;

            let data: HashMap<String, String> = cm.data.unwrap_or_default().into_iter().collect();
            result.insert(name.clone(), data);
        }
        Ok(result)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn setup_cm_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-configmaps-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn file_resolver_reads_data() {
        let dir = setup_cm_dir("basic");
        fs::write(
            dir.join("app-config.yaml"),
            "data:\n  DB_HOST: \"localhost\"\n  DB_PORT: \"5432\"\n",
        )
        .unwrap();

        let resolver = FileConfigMapResolver::new(dir).unwrap();
        let result = resolver
            .config_maps("default", &["app-config".to_string()])
            .await
            .unwrap();
        assert_eq!(result["app-config"]["DB_HOST"], "localhost");
        assert_eq!(result["app-config"]["DB_PORT"], "5432");
    }

    #[tokio::test]
    async fn file_resolver_empty_data() {
        let dir = setup_cm_dir("empty");
        fs::write(dir.join("empty.yaml"), "data: {}\n").unwrap();

        let resolver = FileConfigMapResolver::new(dir).unwrap();
        let result = resolver
            .config_maps("default", &["empty".to_string()])
            .await
            .unwrap();
        assert!(result["empty"].is_empty());
    }

    #[tokio::test]
    async fn file_resolver_missing_file() {
        let dir = setup_cm_dir("missing");
        let resolver = FileConfigMapResolver::new(dir).unwrap();
        let err = resolver
            .config_maps("default", &["nope".to_string()])
            .await
            .unwrap_err();
        assert!(err.message.contains("failed to read"));
    }

    #[test]
    fn file_resolver_invalid_dir() {
        let result = FileConfigMapResolver::new(PathBuf::from("/nonexistent"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn file_resolver_multiple() {
        let dir = setup_cm_dir("multi");
        fs::write(dir.join("a.yaml"), "data:\n  k: va\n").unwrap();
        fs::write(dir.join("b.yaml"), "data:\n  k: vb\n").unwrap();

        let resolver = FileConfigMapResolver::new(dir).unwrap();
        let result = resolver
            .config_maps("default", &["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        assert_eq!(result["a"]["k"], "va");
        assert_eq!(result["b"]["k"], "vb");
    }
}
