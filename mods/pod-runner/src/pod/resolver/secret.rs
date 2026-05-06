// Copyright 2026 Tabsdata Inc.

//! Secret resolver — fetch secret key/value data by name.

use std::collections::HashMap;
use std::path::PathBuf;

use async_trait::async_trait;

/// Fetch secrets by name. Returns `{name: {key: value}}` for each
/// requested name.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    async fn secrets(
        &self,
        namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError>;
}

/// Error returned by a secret resolver.
#[derive(Debug)]
pub struct SecretResolverError {
    pub name: String,
    pub message: String,
}

impl std::fmt::Display for SecretResolverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "secret '{}': {}", self.name, self.message)
    }
}

impl std::error::Error for SecretResolverError {}

// ---------------------------------------------------------------------------
// File-based implementation
// ---------------------------------------------------------------------------

/// Reads secrets from `<name>.yaml` files in a directory.
///
/// YAML format matches K8s Secret resources: `data` (base64-encoded)
/// and `stringData` (plain text) fields are merged.
pub struct FileSecretResolver {
    dir: PathBuf,
}

impl FileSecretResolver {
    pub fn new(dir: PathBuf) -> Result<Self, String> {
        if !dir.is_dir() {
            return Err(format!("Secret directory does not exist: {}", dir.display()));
        }
        Ok(Self { dir })
    }
}

#[async_trait]
impl SecretResolver for FileSecretResolver {
    async fn secrets(
        &self,
        _namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError> {
        let mut result = HashMap::new();
        for name in names {
            let path = self.dir.join(format!("{}.yaml", name));
            let content = std::fs::read_to_string(&path).map_err(|e| SecretResolverError {
                name: name.clone(),
                message: format!("failed to read {}: {}", path.display(), e),
            })?;
            let doc: serde_json::Value =
                serde_yaml_ng::from_str(&content).map_err(|e| SecretResolverError {
                    name: name.clone(),
                    message: format!("invalid YAML in {}: {}", path.display(), e),
                })?;
            result.insert(name.clone(), extract_secret_data(&doc, name)?);
        }
        Ok(result)
    }
}

/// Extract and merge `data` (base64) and `stringData` (plain) fields.
fn extract_secret_data(
    doc: &serde_json::Value,
    name: &str,
) -> Result<HashMap<String, String>, SecretResolverError> {
    let mut values = HashMap::new();

    if let Some(data) = doc.get("data").and_then(|d| d.as_object()) {
        for (key, val) in data {
            if let Some(b64) = val.as_str() {
                use base64::Engine;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| SecretResolverError {
                        name: name.to_string(),
                        message: format!("base64 decode failed for key '{}': {}", key, e),
                    })?;
                let s = String::from_utf8(decoded).map_err(|e| SecretResolverError {
                    name: name.to_string(),
                    message: format!("UTF-8 decode failed for key '{}': {}", key, e),
                })?;
                values.insert(key.clone(), s);
            }
        }
    }

    if let Some(string_data) = doc.get("stringData").and_then(|d| d.as_object()) {
        for (key, val) in string_data {
            if let Some(s) = val.as_str() {
                values.insert(key.clone(), s.to_string());
            }
        }
    }

    Ok(values)
}

// ---------------------------------------------------------------------------
// Kube-based implementation
// ---------------------------------------------------------------------------

/// Reads secrets from the Kubernetes API using a kubeconfig file.
pub struct KubeSecretResolver {
    client: kube::Client,
}

impl KubeSecretResolver {
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
impl SecretResolver for KubeSecretResolver {
    async fn secrets(
        &self,
        namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError> {
        use k8s_openapi::api::core::v1::Secret;
        use kube::api::Api;

        let api: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let mut result = HashMap::new();

        for name in names {
            let secret = api.get(name).await.map_err(|e| SecretResolverError {
                name: name.clone(),
                message: format!("{}", e),
            })?;

            let mut values = HashMap::new();
            if let Some(data) = secret.data {
                for (key, val) in data {
                    let s = String::from_utf8(val.0).map_err(|e| SecretResolverError {
                        name: name.clone(),
                        message: format!("UTF-8 decode for '{}': {}", key, e),
                    })?;
                    values.insert(key, s);
                }
            }
            if let Some(string_data) = secret.string_data {
                values.extend(string_data);
            }
            result.insert(name.clone(), values);
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

    fn setup_secret_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-secrets-{}-{}",
            name,
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[tokio::test]
    async fn file_resolver_reads_base64_data() {
        let dir = setup_secret_dir("b64");
        fs::write(
            dir.join("my-secret.yaml"),
            "data:\n  username: YWRtaW4=\n  password: cGFzc3dvcmQ=\n",
        )
        .unwrap();

        let resolver = FileSecretResolver::new(dir).unwrap();
        let result = resolver
            .secrets("default", &["my-secret".to_string()])
            .await
            .unwrap();
        assert_eq!(result["my-secret"]["username"], "admin");
        assert_eq!(result["my-secret"]["password"], "password");
    }

    #[tokio::test]
    async fn file_resolver_reads_string_data() {
        let dir = setup_secret_dir("str");
        fs::write(dir.join("plain.yaml"), "stringData:\n  api_key: \"sk-12345\"\n").unwrap();

        let resolver = FileSecretResolver::new(dir).unwrap();
        let result = resolver
            .secrets("default", &["plain".to_string()])
            .await
            .unwrap();
        assert_eq!(result["plain"]["api_key"], "sk-12345");
    }

    #[tokio::test]
    async fn file_resolver_merges_data_and_string_data() {
        let dir = setup_secret_dir("merge");
        fs::write(
            dir.join("mixed.yaml"),
            "data:\n  encoded: aGVsbG8=\nstringData:\n  plain: world\n",
        )
        .unwrap();

        let resolver = FileSecretResolver::new(dir).unwrap();
        let result = resolver
            .secrets("default", &["mixed".to_string()])
            .await
            .unwrap();
        assert_eq!(result["mixed"]["encoded"], "hello");
        assert_eq!(result["mixed"]["plain"], "world");
    }

    #[tokio::test]
    async fn file_resolver_missing_file() {
        let dir = setup_secret_dir("missing");
        let resolver = FileSecretResolver::new(dir).unwrap();
        let err = resolver
            .secrets("default", &["nonexistent".to_string()])
            .await
            .unwrap_err();
        assert!(err.message.contains("failed to read"));
    }

    #[test]
    fn file_resolver_invalid_dir() {
        let result = FileSecretResolver::new(PathBuf::from("/nonexistent/path"));
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn file_resolver_multiple_secrets() {
        let dir = setup_secret_dir("multi");
        fs::write(dir.join("a.yaml"), "stringData:\n  key: val-a\n").unwrap();
        fs::write(dir.join("b.yaml"), "stringData:\n  key: val-b\n").unwrap();

        let resolver = FileSecretResolver::new(dir).unwrap();
        let result = resolver
            .secrets("default", &["a".to_string(), "b".to_string()])
            .await
            .unwrap();
        assert_eq!(result["a"]["key"], "val-a");
        assert_eq!(result["b"]["key"], "val-b");
    }
}
