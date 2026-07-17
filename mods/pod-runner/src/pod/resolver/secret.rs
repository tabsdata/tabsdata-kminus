// Copyright 2026 Tabsdata Inc.

//! Secret resolver — fetch secret key/value data by name.
//!
//! The trait's [`SecretResolver::secrets`] method accepts an optional
//! timeout (sourced from the pod's `activeDeadlineSeconds`) and a cancel
//! signal. If any requested secret does not (yet) exist the resolver
//! retries with an exponential backoff (see
//! [`crate::pod::resolver::backoff`]) until one of the following happens:
//!
//! - the secret becomes available — `Ok(...)` is returned;
//! - the timeout expires — [`SecretResolverErrorKind::Timeout`] is
//!   returned;
//! - the cancel signal fires — [`SecretResolverErrorKind::Cancelled`] is
//!   returned;
//! - a non-retryable error occurs (e.g. YAML parse, unreachable kube API)
//!   — [`SecretResolverErrorKind::Other`] is returned without retrying.
//!
//! Existence is checked at the **entry** (secret object) level. The
//! resolver does not inspect individual keys — a missing key inside an
//! existing secret is silently skipped by the env-resolution code, not
//! waited for here.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use super::backoff::poll_with_backoff;
// Re-exports preserved so existing call sites
// (`SecretResolverError::not_found`, `SecretResolverErrorKind::Timeout`)
// continue to resolve through this module.
pub use super::error::{SecretResolverError, SecretResolverErrorKind};

/// Op-name passed to the backoff logger.
const OP_NAME: &str = "secret_resolver.fetch";

/// Fetch secrets by name. Returns `{name: {key: value}}` for each
/// requested name.
///
/// `timeout` bounds the total wait when any requested secret is missing.
/// `None` means no bound (the wait is indefinite). `cancel` is observed
/// throughout the wait and aborts it when it fires.
#[async_trait]
pub trait SecretResolver: Send + Sync {
    async fn secrets(
        &self,
        namespace: &str,
        names: &[String],
        timeout: Option<Duration>,
        cancel: &mut watch::Receiver<()>,
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError>;
}

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
            return Err(format!(
                "Secret directory does not exist: {}",
                dir.display()
            ));
        }
        Ok(Self { dir })
    }

    /// Single-pass fetch. Returns [`SecretResolverErrorKind::NotFound`]
    /// when any requested `<name>.yaml` is absent; other I/O or parse
    /// errors are classified as [`SecretResolverErrorKind::Other`].
    fn try_fetch(
        &self,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError> {
        let mut result = HashMap::new();
        for name in names {
            let path = self.dir.join(format!("{}.yaml", name));
            let content = match std::fs::read_to_string(&path) {
                Ok(c) => c,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    return Err(SecretResolverError::not_found(
                        name,
                        format!("file not found: {}", path.display()),
                    ));
                }
                Err(e) => {
                    return Err(SecretResolverError::other(
                        name,
                        format!("failed to read {}: {}", path.display(), e),
                    ));
                }
            };
            let doc: serde_json::Value = serde_yaml_ng::from_str(&content).map_err(|e| {
                SecretResolverError::other(
                    name,
                    format!("invalid YAML in {}: {}", path.display(), e),
                )
            })?;
            result.insert(name.clone(), extract_secret_data(&doc, name)?);
        }
        Ok(result)
    }
}

#[async_trait]
impl SecretResolver for FileSecretResolver {
    async fn secrets(
        &self,
        _namespace: &str,
        names: &[String],
        timeout: Option<Duration>,
        cancel: &mut watch::Receiver<()>,
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError> {
        poll_with_backoff(
            OP_NAME,
            timeout,
            cancel,
            SecretResolverError::is_not_found,
            || async { self.try_fetch(names) },
        )
        .await
        .map_err(Into::into)
    }
}

/// Extract and merge `data` (base64) and `stringData` (plain) fields.
///
/// A key whose value is YAML `null` (`KEY:` with nothing after it) is
/// treated as an empty value and preserved as `key -> ""`. This keeps
/// parity with [`KubeSecretResolver`], where `k8s_openapi::ByteString`
/// decodes a null/empty value to empty bytes: an empty secret value is
/// valid and must survive as a set-but-empty env var rather than being
/// silently dropped.
fn extract_secret_data(
    doc: &serde_json::Value,
    name: &str,
) -> Result<HashMap<String, String>, SecretResolverError> {
    let mut values = HashMap::new();

    if let Some(data) = doc.get("data").and_then(|d| d.as_object()) {
        for (key, val) in data {
            if val.is_null() {
                values.insert(key.clone(), String::new());
            } else if let Some(b64) = val.as_str() {
                use base64::Engine;
                let decoded = base64::engine::general_purpose::STANDARD
                    .decode(b64)
                    .map_err(|e| {
                        SecretResolverError::other(
                            name,
                            format!("base64 decode failed for key '{}': {}", key, e),
                        )
                    })?;
                let s = String::from_utf8(decoded).map_err(|e| {
                    SecretResolverError::other(
                        name,
                        format!("UTF-8 decode failed for key '{}': {}", key, e),
                    )
                })?;
                values.insert(key.clone(), s);
            }
        }
    }

    if let Some(string_data) = doc.get("stringData").and_then(|d| d.as_object()) {
        for (key, val) in string_data {
            if val.is_null() {
                values.insert(key.clone(), String::new());
            } else if let Some(s) = val.as_str() {
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

    /// Single-pass fetch. A kube API 404 produces
    /// [`SecretResolverErrorKind::NotFound`]; everything else is
    /// classified as [`SecretResolverErrorKind::Other`] (no retry).
    async fn try_fetch(
        &self,
        namespace: &str,
        names: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError> {
        use k8s_openapi::api::core::v1::Secret;
        use kube::api::Api;

        let api: Api<Secret> = Api::namespaced(self.client.clone(), namespace);
        let mut result = HashMap::new();

        for name in names {
            let secret = match api.get(name).await {
                Ok(s) => s,
                Err(kube::Error::Api(e)) if e.code == 404 => {
                    return Err(SecretResolverError::not_found(name, e.message));
                }
                Err(e) => {
                    return Err(SecretResolverError::other(name, format!("{}", e)));
                }
            };

            result.insert(name.clone(), secret_to_values(name, secret)?);
        }
        Ok(result)
    }
}

/// Flatten a fetched [`Secret`]'s `data` (raw bytes) and `stringData`
/// (plain strings) into a `{key: value}` map.
///
/// Every stored key is preserved, including empty-valued ones: an empty
/// `data` value round-trips through `k8s_openapi::ByteString` as empty
/// bytes and becomes `key -> ""`, and `stringData` empties are kept as-is.
/// An empty secret value is valid (e.g. a password-less DB user) and must
/// survive as a set-but-empty env var, not be dropped. `stringData`
/// overrides `data` on key collision (matching the API server on write).
fn secret_to_values(
    name: &str,
    secret: k8s_openapi::api::core::v1::Secret,
) -> Result<HashMap<String, String>, SecretResolverError> {
    let mut values = HashMap::new();
    if let Some(data) = secret.data {
        for (key, val) in data {
            let s = String::from_utf8(val.0).map_err(|e| {
                SecretResolverError::other(name, format!("UTF-8 decode for '{}': {}", key, e))
            })?;
            values.insert(key, s);
        }
    }
    if let Some(string_data) = secret.string_data {
        values.extend(string_data);
    }
    Ok(values)
}

#[async_trait]
impl SecretResolver for KubeSecretResolver {
    async fn secrets(
        &self,
        namespace: &str,
        names: &[String],
        timeout: Option<Duration>,
        cancel: &mut watch::Receiver<()>,
    ) -> Result<HashMap<String, HashMap<String, String>>, SecretResolverError> {
        poll_with_backoff(
            OP_NAME,
            timeout,
            cancel,
            SecretResolverError::is_not_found,
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
    use serde_json::json;
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

    fn cancel_channel() -> (watch::Sender<()>, watch::Receiver<()>) {
        watch::channel(())
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
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .secrets("default", &["my-secret".to_string()], None, &mut rx)
            .await
            .unwrap();
        assert_eq!(result["my-secret"]["username"], "admin");
        assert_eq!(result["my-secret"]["password"], "password");
    }

    #[tokio::test]
    async fn file_resolver_reads_string_data() {
        let dir = setup_secret_dir("str");
        fs::write(
            dir.join("plain.yaml"),
            "stringData:\n  api_key: \"sk-12345\"\n",
        )
        .unwrap();

        let resolver = FileSecretResolver::new(dir).unwrap();
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .secrets("default", &["plain".to_string()], None, &mut rx)
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
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .secrets("default", &["mixed".to_string()], None, &mut rx)
            .await
            .unwrap();
        assert_eq!(result["mixed"]["encoded"], "hello");
        assert_eq!(result["mixed"]["plain"], "world");
    }

    #[tokio::test(start_paused = true)]
    async fn file_resolver_missing_file_times_out() {
        let dir = setup_secret_dir("missing-timeout");
        let resolver = FileSecretResolver::new(dir).unwrap();
        let (_tx, mut rx) = cancel_channel();

        let err = resolver
            .secrets(
                "default",
                &["nonexistent".to_string()],
                Some(Duration::from_secs(1)),
                &mut rx,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, SecretResolverErrorKind::Timeout);
        assert!(err.message.contains("timed out"), "got: {}", err.message);
        assert!(
            err.message.contains("file not found"),
            "got: {}",
            err.message
        );
    }

    #[tokio::test(start_paused = true)]
    async fn file_resolver_missing_then_appears_succeeds() {
        let dir = setup_secret_dir("missing-then-appears");
        let resolver = FileSecretResolver::new(dir.clone()).unwrap();
        let (_tx, mut rx) = cancel_channel();
        let secret_path = dir.join("late.yaml");

        let writer = tokio::spawn(async move {
            // Wait long enough for the resolver to fail at least once and
            // begin its first backoff (500ms), then materialize the file.
            tokio::time::sleep(Duration::from_secs(2)).await;
            fs::write(&secret_path, "stringData:\n  key: appeared\n").unwrap();
        });

        let result = resolver
            .secrets(
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
        let dir = setup_secret_dir("cancelled");
        let resolver = FileSecretResolver::new(dir).unwrap();
        let (tx, mut rx) = cancel_channel();

        let canceller = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = tx.send(());
        });

        let err = resolver
            .secrets(
                "default",
                &["never-comes".to_string()],
                None, // indefinite wait, only cancel can stop it
                &mut rx,
            )
            .await
            .unwrap_err();
        canceller.await.unwrap();
        assert_eq!(err.kind, SecretResolverErrorKind::Cancelled);
    }

    #[tokio::test(start_paused = true)]
    async fn file_resolver_invalid_yaml_returns_other_no_retry() {
        let dir = setup_secret_dir("bad-yaml");
        fs::write(dir.join("bad.yaml"), "not: valid: yaml: at: all: [\n").unwrap();
        let resolver = FileSecretResolver::new(dir).unwrap();
        let (_tx, mut rx) = cancel_channel();

        let err = resolver
            .secrets(
                "default",
                &["bad".to_string()],
                Some(Duration::from_secs(60)),
                &mut rx,
            )
            .await
            .unwrap_err();
        assert_eq!(err.kind, SecretResolverErrorKind::Other);
        assert!(err.message.contains("invalid YAML"), "got: {}", err.message);
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
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .secrets(
                "default",
                &["a".to_string(), "b".to_string()],
                None,
                &mut rx,
            )
            .await
            .unwrap();
        assert_eq!(result["a"]["key"], "val-a");
        assert_eq!(result["b"]["key"], "val-b");
    }

    #[test]
    fn error_kind_classification() {
        let nf = SecretResolverError::not_found("s", "missing");
        assert!(nf.is_not_found());
        assert_eq!(nf.kind, SecretResolverErrorKind::NotFound);

        let other = SecretResolverError::other("s", "boom");
        assert!(!other.is_not_found());
        assert_eq!(other.kind, SecretResolverErrorKind::Other);
    }

    // -- empty-value preservation (kube path) --------------------------------
    //
    // Regression coverage for the "empty secret value dropped from a
    // worker's env" report: an empty `data` value must survive as
    // `key -> ""` (a set-but-empty env var), never be absent. These drive
    // the exact JSON the API server returns for such a secret through the
    // same `Secret` deserialize + `secret_to_values` path the kube resolver
    // uses, so no live cluster is needed.

    #[test]
    fn kube_empty_data_value_is_preserved() {
        // API server returns base64 for `data`; an empty value is the empty
        // base64 string. Mirrors `stringData: {FOO_PASSWORD: ""}`.
        let secret: k8s_openapi::api::core::v1::Secret = serde_json::from_value(json!({
            "apiVersion": "v1",
            "kind": "Secret",
            "metadata": {"name": "td-empty-probe", "namespace": "default"},
            "type": "Opaque",
            "data": {"FOO_USER": "c29tZXVzZXI=", "FOO_PASSWORD": ""},
        }))
        .unwrap();

        let values = secret_to_values("td-empty-probe", secret).unwrap();
        assert_eq!(values.get("FOO_USER").map(String::as_str), Some("someuser"));
        // Present with an empty value — NOT dropped.
        assert_eq!(values.get("FOO_PASSWORD").map(String::as_str), Some(""));
    }

    #[test]
    fn kube_null_data_value_decodes_to_empty() {
        // A null `data` value round-trips through ByteString to empty bytes.
        let secret: k8s_openapi::api::core::v1::Secret = serde_json::from_value(json!({
            "metadata": {"name": "s", "namespace": "default"},
            "data": {"FOO_PASSWORD": null},
        }))
        .unwrap();

        let values = secret_to_values("s", secret).unwrap();
        assert_eq!(values.get("FOO_PASSWORD").map(String::as_str), Some(""));
    }

    #[test]
    fn kube_empty_string_data_value_is_preserved() {
        let secret: k8s_openapi::api::core::v1::Secret = serde_json::from_value(json!({
            "metadata": {"name": "s", "namespace": "default"},
            "stringData": {"USER": "admin", "PASSWORD": ""},
        }))
        .unwrap();

        let values = secret_to_values("s", secret).unwrap();
        assert_eq!(values.get("USER").map(String::as_str), Some("admin"));
        assert_eq!(values.get("PASSWORD").map(String::as_str), Some(""));
    }

    // -- empty-value preservation (file path) --------------------------------

    #[tokio::test]
    async fn file_resolver_preserves_empty_values() {
        let dir = setup_secret_dir("empty-values");
        // Empty base64 `data`, empty-string `stringData`, and a bare
        // `null` (`KEY:` with no value) all represent an empty secret value.
        fs::write(
            dir.join("creds.yaml"),
            "data:\n  D_EMPTY: \"\"\nstringData:\n  S_EMPTY: \"\"\n  S_NULL:\n",
        )
        .unwrap();

        let resolver = FileSecretResolver::new(dir).unwrap();
        let (_tx, mut rx) = cancel_channel();
        let result = resolver
            .secrets("default", &["creds".to_string()], None, &mut rx)
            .await
            .unwrap();
        let creds = &result["creds"];
        // All three keys are present with an empty value — none dropped.
        assert_eq!(creds.get("D_EMPTY").map(String::as_str), Some(""));
        assert_eq!(creds.get("S_EMPTY").map(String::as_str), Some(""));
        assert_eq!(creds.get("S_NULL").map(String::as_str), Some(""));
    }
}
