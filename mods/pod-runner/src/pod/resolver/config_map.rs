// Copyright 2026 Tabsdata Inc.

//! ConfigMap resolver — fetch config map key/value data by name.
//!
//! The trait's [`ConfigMapResolver::config_maps`] method accepts an
//! optional timeout (sourced from the pod's `activeDeadlineSeconds`) and
//! a cancel signal. If any requested config map does not (yet) exist the
//! resolver retries with an exponential backoff (see
//! [`crate::pod::resolver::backoff`]) until one of the following happens:
//!
//! - the config map becomes available — `Ok(...)` is returned;
//! - the timeout expires — [`ConfigMapResolverErrorKind::Timeout`] is
//!   returned;
//! - the cancel signal fires — [`ConfigMapResolverErrorKind::Cancelled`]
//!   is returned;
//! - a non-retryable error occurs (e.g. YAML parse, unreachable kube API)
//!   — [`ConfigMapResolverErrorKind::Other`] is returned without retrying.
//!
//! Existence is checked at the **entry** (configmap object) level. The
//! resolver does not inspect individual keys — a missing key inside an
//! existing configmap is silently skipped by the env-resolution code, not
//! waited for here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use super::backoff::poll_with_backoff;
// Re-exports preserved so existing call sites
// (`ConfigMapResolverError::not_found`,
// `ConfigMapResolverErrorKind::Timeout`) continue to resolve through
// this module.
pub use super::error::{ConfigMapResolverError, ConfigMapResolverErrorKind};

/// Op-name passed to the backoff logger.
const OP_NAME: &str = "configmap_resolver.fetch";

/// Fetch config maps by name. Returns `{name: {key: value}}` for each
/// requested name.
///
/// `timeout` bounds the total wait when any requested configmap is
/// missing. `None` means no bound (the wait is indefinite). `cancel` is
/// observed throughout the wait and aborts it when it fires.
#[async_trait]
pub trait ConfigMapResolver: Send + Sync {
    async fn config_maps(
        &self,
        namespace: &str,
        names: &[String],
        timeout: Option<Duration>,
        cancel: &mut watch::Receiver<()>,
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError>;
}

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

    /// Single-pass fetch. Returns
    /// [`ConfigMapResolverErrorKind::NotFound`] when any requested
    /// `<name>.yaml` is absent; other I/O or parse errors are classified
    /// as [`ConfigMapResolverErrorKind::Other`].
    fn try_fetch(
        &self,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError> {
        let mut result = HashMap::new();
        for name in names {
            let path = self.dir.join(format!("{}.yaml", name));
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(ConfigMapResolverError::not_found(
                        name,
                        format!("file not found: {}", path.display()),
                    ));
                }
                Err(e) => {
                    return Err(ConfigMapResolverError::other(
                        name,
                        format!("failed to read {}: {}", path.display(), e),
                    ));
                }
            };
            let doc: serde_json::Value = serde_yaml_ng::from_str(&content).map_err(|e| {
                ConfigMapResolverError::other(
                    name,
                    format!("invalid YAML in {}: {}", path.display(), e),
                )
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

#[async_trait]
impl ConfigMapResolver for FileConfigMapResolver {
    async fn config_maps(
        &self,
        _namespace: &str,
        names: &[String],
        timeout: Option<Duration>,
        cancel: &mut watch::Receiver<()>,
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError> {
        poll_with_backoff(
            OP_NAME,
            timeout,
            cancel,
            ConfigMapResolverError::is_not_found,
            || async { self.try_fetch(names) },
        )
        .await
        .map_err(Into::into)
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

    /// Single-pass fetch. A kube API 404 produces
    /// [`ConfigMapResolverErrorKind::NotFound`]; everything else is
    /// classified as [`ConfigMapResolverErrorKind::Other`] (no retry).
    async fn try_fetch(
        &self,
        namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError> {
        use k8s_openapi::api::core::v1::ConfigMap;
        use kube::api::Api;

        let api: Api<ConfigMap> = Api::namespaced(self.client.clone(), namespace);
        let mut result = HashMap::new();

        for name in names {
            let cm = match api.get(name).await {
                Ok(c) => c,
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Err(ConfigMapResolverError::not_found(name, e.message));
                }
                Err(e) => {
                    return Err(ConfigMapResolverError::other(name, format!("{}", e)));
                }
            };

            let data: HashMap<String, String> = cm.data.unwrap_or_default().into_iter().collect();
            result.insert(name.clone(), data);
        }
        Ok(result)
    }
}

#[async_trait]
impl ConfigMapResolver for KubeConfigMapResolver {
    async fn config_maps(
        &self,
        namespace: &str,
        names: &[String],
        timeout: Option<Duration>,
        cancel: &mut watch::Receiver<()>,
    ) -> Result<HashMap<String, HashMap<String, String>>, ConfigMapResolverError> {
        poll_with_backoff(
            OP_NAME,
            timeout,
            cancel,
            ConfigMapResolverError::is_not_found,
            || self.try_fetch(namespace, names),
        )
        .await
        .map_err(Into::into)
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

    fn cancel_channel() -> (watch::Sender<()>, watch::Receiver<()>) {
        watch::channel(())
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
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .config_maps("default", &["app-config".to_string()], None, &mut rx)
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
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .config_maps("default", &["empty".to_string()], None, &mut rx)
            .await
            .unwrap();
        assert!(result["empty"].is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn file_resolver_missing_file_times_out() {
        let dir = setup_cm_dir("missing-timeout");
        let resolver = FileConfigMapResolver::new(dir).unwrap();
        let (_tx, mut rx) = cancel_channel();

        let err = resolver
            .config_maps(
                "default",
                &["nope".to_string()],
                Some(Duration::from_secs(1)),
                &mut rx,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, ConfigMapResolverErrorKind::Timeout);
        assert!(err.message.contains("timed out"), "got: {}", err.message);
    }

    #[tokio::test(start_paused = true)]
    async fn file_resolver_missing_then_appears_succeeds() {
        let dir = setup_cm_dir("missing-then-appears");
        let resolver = FileConfigMapResolver::new(dir.clone()).unwrap();
        let (_tx, mut rx) = cancel_channel();
        let cm_path = dir.join("late.yaml");

        let writer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            fs::write(&cm_path, "data:\n  key: appeared\n").unwrap();
        });

        let result = resolver
            .config_maps(
                "default",
                &["late".to_string()],
                Some(Duration::from_secs(60)),
                &mut rx,
            )
            .await
            .unwrap();
        writer.await.unwrap();
        assert_eq!(result["late"]["key"], "appeared");
    }

    #[tokio::test(start_paused = true)]
    async fn file_resolver_cancelled_returns_cancelled_kind() {
        let dir = setup_cm_dir("cancelled");
        let resolver = FileConfigMapResolver::new(dir).unwrap();
        let (tx, mut rx) = cancel_channel();

        let canceller = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = tx.send(());
        });

        let err = resolver
            .config_maps("default", &["never-comes".to_string()], None, &mut rx)
            .await
            .unwrap_err();
        canceller.await.unwrap();
        assert_eq!(err.kind, ConfigMapResolverErrorKind::Cancelled);
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
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .config_maps(
                "default",
                &["a".to_string(), "b".to_string()],
                None,
                &mut rx,
            )
            .await
            .unwrap();
        assert_eq!(result["a"]["k"], "va");
        assert_eq!(result["b"]["k"], "vb");
    }

    #[test]
    fn error_kind_classification() {
        let nf = ConfigMapResolverError::not_found("c", "missing");
        assert!(nf.is_not_found());
        assert_eq!(nf.kind, ConfigMapResolverErrorKind::NotFound);

        let other = ConfigMapResolverError::other("c", "boom");
        assert!(!other.is_not_found());
        assert_eq!(other.kind, ConfigMapResolverErrorKind::Other);
    }
}
