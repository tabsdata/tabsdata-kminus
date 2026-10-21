// Copyright 2026 Tabsdata Inc.

//! Client configuration for connecting to a Pod Runner server.
//!
//! [`VKPRConfig`] deserializes the `vk-prconfig.yaml` file format used by
//! the Python `kpr` client. It provides the server URL, TLS settings,
//! and optional client credentials for mTLS.

use std::path::Path;

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Top-level client config
// ---------------------------------------------------------------------------

/// Deserialized `vk-prconfig.yaml` — client-side connection settings.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct VKPRConfig {
    /// Pod Runner server URL (e.g. `https://127.0.0.1:3000`).
    pub url: String,

    /// Virtual node name.
    #[serde(alias = "nodeName")]
    pub node_name: String,

    /// Default namespace.
    pub namespace: String,

    /// Server TLS verification settings.
    pub server: PrServerConfig,

    /// Client identity for mTLS (optional).
    pub client: Option<PrClientConfig>,

    /// Metrics endpoint URL (HTTP only, e.g. `http://127.0.0.1:3001`).
    #[serde(alias = "metricsUrl", default)]
    pub metrics_url: String,
}

impl VKPRConfig {
    /// Load from a YAML file.
    pub fn from_file(path: &Path) -> Result<Self, String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {}: {}", path.display(), e))?;
        Self::from_str(&content)
    }

    /// Parse from a YAML string.
    // Inherent rather than std::str::FromStr on purpose: the parse is
    // YAML-specific and fails with a message, and making it the trait
    // would change this type's public API and every caller's imports.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(yaml: &str) -> Result<Self, String> {
        serde_yaml_ng::from_str(yaml).map_err(|e| format!("Failed to parse YAML: {e}"))
    }

    /// Whether the config names a client certificate and key, by path or
    /// inline. pod-runner's server demands one on every TLS connection, so an
    /// `https` config without it can never get a response. Pod-runner's own
    /// `prconfig.yaml` parses as a client config too (its server keys are
    /// ignored), which is how it ends up passed here by mistake.
    pub fn has_client_identity(&self) -> bool {
        self.client.as_ref().is_some_and(|c| {
            (!c.certificate.is_empty() || !c.certificate_data.is_empty())
                && (!c.certificate_key.is_empty() || !c.certificate_key_data.is_empty())
        })
    }
}

// ---------------------------------------------------------------------------
// Server config
// ---------------------------------------------------------------------------

/// Server connection settings.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct PrServerConfig {
    pub tls: PrServerTLSConfig,
}

/// TLS settings for verifying the Pod Runner server.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct PrServerTLSConfig {
    /// Path to the CA certificate file for verifying the server.
    #[serde(alias = "certificate-authority")]
    pub certificate_authority: String,

    /// Base64-encoded CA certificate data.
    #[serde(alias = "certificate-authority-data")]
    pub certificate_authority_data: String,

    /// Skip TLS verification (insecure).
    #[serde(alias = "insecure-skip-tls-verify")]
    pub insecure_skip_tls_verify: bool,
}

// ---------------------------------------------------------------------------
// Client identity config
// ---------------------------------------------------------------------------

/// Client identity and credentials for mTLS.
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct PrClientConfig {
    /// Client identifier.
    pub id: String,

    /// Path to client certificate file.
    pub certificate: String,

    /// Base64-encoded client certificate data.
    #[serde(alias = "certificate-data")]
    pub certificate_data: String,

    /// Path to client private key file.
    #[serde(alias = "certificate-key")]
    pub certificate_key: String,

    /// Base64-encoded client private key data.
    #[serde(alias = "certificate-key-data")]
    pub certificate_key_data: String,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_empty_yaml_uses_defaults() {
        let config = VKPRConfig::from_str("").unwrap();
        assert_eq!(config.url, "");
        assert_eq!(config.node_name, "");
        assert_eq!(config.namespace, "");
        assert!(!config.server.tls.insecure_skip_tls_verify);
        assert!(config.client.is_none());
    }

    #[test]
    fn parse_full_yaml() {
        let yaml = r#"
url: "https://10.0.0.1:3000"
nodeName: "my-node"
namespace: "prod"
server:
  tls:
    certificate-authority: "/path/to/ca.pem"
    insecure-skip-tls-verify: false
client:
  id: "my-client"
  certificate: "/path/to/client.crt"
  certificate-key: "/path/to/client.key"
"#;
        let config = VKPRConfig::from_str(yaml).unwrap();
        assert_eq!(config.url, "https://10.0.0.1:3000");
        assert_eq!(config.node_name, "my-node");
        assert_eq!(config.namespace, "prod");
        assert_eq!(config.server.tls.certificate_authority, "/path/to/ca.pem");
        assert!(!config.server.tls.insecure_skip_tls_verify);

        let client = config.client.unwrap();
        assert_eq!(client.id, "my-client");
        assert_eq!(client.certificate, "/path/to/client.crt");
        assert_eq!(client.certificate_key, "/path/to/client.key");
    }

    #[test]
    fn parse_inline_cert_data() {
        let yaml = r#"
server:
  tls:
    certificate-authority-data: "base64ca"
client:
  certificate-data: "base64cert"
  certificate-key-data: "base64key"
"#;
        let config = VKPRConfig::from_str(yaml).unwrap();
        assert_eq!(config.server.tls.certificate_authority_data, "base64ca");

        let client = config.client.unwrap();
        assert_eq!(client.certificate_data, "base64cert");
        assert_eq!(client.certificate_key_data, "base64key");
    }

    #[test]
    fn parse_insecure_skip() {
        let yaml = r#"
server:
  tls:
    insecure-skip-tls-verify: true
"#;
        let config = VKPRConfig::from_str(yaml).unwrap();
        assert!(config.server.tls.insecure_skip_tls_verify);
    }

    #[test]
    fn parse_no_client_section() {
        let yaml = r#"
url: "http://localhost:3000"
"#;
        let config = VKPRConfig::from_str(yaml).unwrap();
        assert!(config.client.is_none());
    }

    #[test]
    fn client_identity_inline_or_paths() {
        let inline = r#"
client:
  certificate-data: "Y2VydA=="
  certificate-key-data: "a2V5"
"#;
        assert!(VKPRConfig::from_str(inline).unwrap().has_client_identity());
        let paths = r#"
client:
  certificate: "/c.pem"
  certificate-key: "/k.pem"
"#;
        assert!(VKPRConfig::from_str(paths).unwrap().has_client_identity());
    }

    #[test]
    fn client_identity_missing_key_or_section() {
        let no_key = r#"
client:
  certificate-data: "Y2VydA=="
"#;
        assert!(!VKPRConfig::from_str(no_key).unwrap().has_client_identity());
        assert!(!VKPRConfig::from_str("").unwrap().has_client_identity());
    }

    #[test]
    fn server_prconfig_has_no_client_identity() {
        // pod-runner's own prconfig.yaml: the server's cert and key sit under
        // server.tls, where a client config has only the CA.
        let yaml = r#"
url: "https://127.0.0.1:10300"
server:
  tls:
    certificate-data: "Y2VydA=="
    certificate-key-data: "a2V5"
    certificate-authority-data: "Y2E="
"#;
        assert!(!VKPRConfig::from_str(yaml).unwrap().has_client_identity());
    }

    #[test]
    fn parse_metrics_url() {
        let yaml = r#"
url: "https://localhost:3000"
metricsUrl: "http://localhost:3001"
"#;
        let config = VKPRConfig::from_str(yaml).unwrap();
        assert_eq!(config.metrics_url, "http://localhost:3001");
    }

    #[test]
    fn parse_metrics_url_default_empty() {
        let config = VKPRConfig::from_str("").unwrap();
        assert_eq!(config.metrics_url, "");
    }

    #[test]
    fn from_file_nonexistent_returns_error() {
        let result = VKPRConfig::from_file(Path::new("/nonexistent/path.yaml"));
        assert!(result.is_err());
    }

    #[test]
    fn from_file_reads_yaml() {
        let tmp =
            std::env::temp_dir().join(format!("vkprconfig-test-{}.yaml", std::process::id(),));
        std::fs::write(&tmp, b"url: \"http://test:3000\"\n").unwrap();

        let config = VKPRConfig::from_file(&tmp).unwrap();
        assert_eq!(config.url, "http://test:3000");

        let _ = std::fs::remove_file(&tmp);
    }
}
