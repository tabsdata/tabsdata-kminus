// Copyright 2026 Tabsdata Inc.

//! TLS utilities shared by server and client.
//!
//! [`CertSource`] abstracts certificate/key sources (file path or inline
//! base64 data). [`ServerTlsConfig`] groups server-side TLS and mTLS
//! settings — the server always uses mTLS (HTTPS + client cert).
//!
//! PEM loading helpers ([`load_certs`], [`load_private_key`]) and the
//! standalone [`resolve_cert_bytes`] function are provided for cases
//! where a full [`CertSource`] is not available (e.g. client config).

use std::path::Path;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tracing::debug;

// ---------------------------------------------------------------------------
// TlsError
// ---------------------------------------------------------------------------

/// Errors raised by TLS configuration loading and PEM parsing.
#[derive(Debug, thiserror::Error)]
pub enum TlsError {
    /// A [`CertSource`] has both `path` and `data` set.
    #[error("{name}: cannot set both path and inline data")]
    BothPathAndData { name: String },
    /// A [`CertSource`] is required but has neither `path` nor `data`.
    #[error("{name}: not configured")]
    NotConfigured { name: String },
    /// Failed to read a cert/key file from disk.
    #[error("{name}: failed to read {path}: {source}")]
    ReadFile {
        name: String,
        path: String,
        #[source]
        source: std::io::Error,
    },
    /// Failed to decode inline base64-encoded PEM data.
    #[error("{name}: invalid base64: {source}")]
    InvalidBase64 {
        name: String,
        #[source]
        source: base64::DecodeError,
    },
    /// Failed to parse PEM-encoded certificates.
    #[error("Failed to parse certificates: {0}")]
    ParseCerts(std::io::Error),
    /// Failed to parse a PEM-encoded private key.
    #[error("Failed to parse private key: {0}")]
    ParseKey(std::io::Error),
    /// PEM data contained no certificates.
    #[error("No certificates found in PEM data")]
    EmptyCerts,
    /// PEM data contained no private key.
    #[error("No private key found in PEM data")]
    EmptyKey,
    /// Failed to add a client CA cert to the trust store.
    #[error("Failed to add client CA cert: {0}")]
    AddCaCert(rustls::Error),
    /// Failed to build the client certificate verifier.
    #[error("Failed to build client verifier: {0}")]
    BuildVerifier(String),
    /// Failed to negotiate TLS protocol versions.
    #[error("TLS protocol error: {0}")]
    Protocol(rustls::Error),
    /// Failed to build the mTLS server config.
    #[error("mTLS config error: {0}")]
    MtlsConfig(rustls::Error),
}

// ---------------------------------------------------------------------------
// CertSource
// ---------------------------------------------------------------------------

/// A certificate or key source: either a file path or inline base64 data.
///
/// The two variants are mutually exclusive. Use [`validate`](CertSource::validate)
/// to check, and [`resolve`](CertSource::resolve) to get the PEM bytes.
#[derive(Debug, Clone, Default)]
pub struct CertSource {
    /// Path to a PEM file on disk.
    pub path: Option<String>,
    /// Base64-encoded PEM data (inline).
    pub data: Option<String>,
}

impl CertSource {
    /// Whether this source is configured (path or data).
    pub fn is_configured(&self) -> bool {
        self.path.is_some() || self.data.is_some()
    }

    /// Validate that path and data are not both set.
    pub fn validate(&self, name: &str) -> Result<(), TlsError> {
        if self.path.is_some() && self.data.is_some() {
            return Err(TlsError::BothPathAndData { name: name.to_string() });
        }
        Ok(())
    }

    /// Resolve the PEM bytes: read from file or decode base64.
    ///
    /// Returns `Err` if not configured, both are set, the file can't be
    /// read, or the base64 is invalid.
    pub fn resolve(&self, name: &str) -> Result<Vec<u8>, TlsError> {
        self.validate(name)?;
        if let Some(ref path) = self.path {
            std::fs::read(path).map_err(|source| TlsError::ReadFile {
                name: name.to_string(),
                path: path.clone(),
                source,
            })
        } else if let Some(ref b64) = self.data {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|source| TlsError::InvalidBase64 {
                    name: name.to_string(),
                    source,
                })
        } else {
            Err(TlsError::NotConfigured { name: name.to_string() })
        }
    }
}

// ---------------------------------------------------------------------------
// Server TLS configuration (always mTLS)
// ---------------------------------------------------------------------------

/// Server TLS configuration. The server always uses mTLS: HTTPS with
/// client certificate verification.
#[derive(Debug, Clone, Default)]
pub struct ServerTlsConfig {
    /// Server certificate (PEM).
    pub certificate: CertSource,
    /// Server private key (PEM).
    pub certificate_key: CertSource,
    /// CA certificate for verifying client certificates (mTLS).
    pub client_ca: CertSource,
}

impl ServerTlsConfig {
    /// Whether a server certificate is configured.
    pub fn has_cert(&self) -> bool {
        self.certificate.is_configured()
    }

    /// Whether a server private key is configured.
    pub fn has_key(&self) -> bool {
        self.certificate_key.is_configured()
    }

    /// Whether a client CA is configured for mTLS verification.
    pub fn has_client_ca(&self) -> bool {
        self.client_ca.is_configured()
    }

    /// Whether TLS is fully configured (cert + key + client CA).
    pub fn is_configured(&self) -> bool {
        self.has_cert() && self.has_key() && self.has_client_ca()
    }

    /// Validate all sources (no path+data conflicts).
    pub fn validate(&self) -> Result<(), TlsError> {
        self.certificate.validate("tls.certificate")?;
        self.certificate_key.validate("tls.certificate_key")?;
        self.client_ca.validate("tls.client_ca")?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Standalone cert resolution
// ---------------------------------------------------------------------------

/// Resolve a certificate/key to PEM bytes from a file path or base64 data.
///
/// Priority: file path > inline base64 data. Returns `None` if neither is set.
/// Used by the client config where fields are separate strings rather than
/// a [`CertSource`].
pub fn resolve_cert_bytes(file_path: &Path, data_b64: &str) -> Result<Option<Vec<u8>>, TlsError> {
    if !file_path.as_os_str().is_empty() {
        let bytes = std::fs::read(file_path).map_err(|source| TlsError::ReadFile {
            name: "cert".to_string(),
            path: file_path.display().to_string(),
            source,
        })?;
        return Ok(Some(bytes));
    }
    if !data_b64.is_empty() {
        use base64::Engine;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data_b64)
            .map_err(|source| TlsError::InvalidBase64 {
                name: "cert".to_string(),
                source,
            })?;
        return Ok(Some(bytes));
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// PEM loading
// ---------------------------------------------------------------------------

/// Parse PEM-encoded certificates, returning DER representations.
pub fn load_certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>, TlsError> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut &*pem)
        .collect::<Result<_, _>>()
        .map_err(TlsError::ParseCerts)?;
    if certs.is_empty() {
        return Err(TlsError::EmptyCerts);
    }
    debug!(count = certs.len(), "Loaded certificates");
    Ok(certs)
}

/// Parse a PEM-encoded private key, returning the DER representation.
pub fn load_private_key(pem: &[u8]) -> Result<PrivateKeyDer<'static>, TlsError> {
    rustls_pemfile::private_key(&mut &*pem)
        .map_err(TlsError::ParseKey)?
        .ok_or(TlsError::EmptyKey)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- CertSource --

    #[test]
    fn cert_source_default_not_configured() {
        let s = CertSource::default();
        assert!(!s.is_configured());
        assert!(s.validate("test").is_ok());
    }

    #[test]
    fn cert_source_path_only() {
        let s = CertSource { path: Some("cert.pem".into()), data: None };
        assert!(s.is_configured());
        assert!(s.validate("test").is_ok());
    }

    #[test]
    fn cert_source_data_only() {
        let s = CertSource { path: None, data: Some("base64".into()) };
        assert!(s.is_configured());
        assert!(s.validate("test").is_ok());
    }

    #[test]
    fn cert_source_both_fails_validation() {
        let s = CertSource {
            path: Some("cert.pem".into()),
            data: Some("base64".into()),
        };
        assert!(s.validate("test").is_err());
    }

    #[test]
    fn cert_source_resolve_from_file() {
        let tmp = std::env::temp_dir().join(format!(
            "pr-cert-test-{}", std::process::id(),
        ));
        std::fs::create_dir_all(&tmp).unwrap();
        let cert_path = tmp.join("test.pem");
        std::fs::write(&cert_path, b"PEM DATA").unwrap();

        let s = CertSource {
            path: Some(cert_path.to_string_lossy().into()),
            data: None,
        };
        let bytes = s.resolve("test").unwrap();
        assert_eq!(bytes, b"PEM DATA");

        let _ = std::fs::remove_dir_all(&tmp);
    }

    #[test]
    fn cert_source_resolve_from_base64() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"PEM DATA");
        let s = CertSource { path: None, data: Some(encoded) };
        let bytes = s.resolve("test").unwrap();
        assert_eq!(bytes, b"PEM DATA");
    }

    #[test]
    fn cert_source_resolve_not_configured() {
        let s = CertSource::default();
        assert!(s.resolve("test").is_err());
    }

    // -- ServerTlsConfig --

    #[test]
    fn server_tls_config_validate_ok() {
        let c = ServerTlsConfig::default();
        assert!(c.validate().is_ok());
    }

    #[test]
    fn server_tls_config_validate_rejects_cert_path_and_data() {
        let c = ServerTlsConfig {
            certificate: CertSource {
                path: Some("cert.pem".into()),
                data: Some("base64".into()),
            },
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    #[test]
    fn server_tls_config_validate_rejects_key_path_and_data() {
        let c = ServerTlsConfig {
            certificate_key: CertSource {
                path: Some("key.pem".into()),
                data: Some("base64".into()),
            },
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }

    // -- resolve_cert_bytes --

    #[test]
    fn resolve_cert_bytes_from_file() {
        let tmp = std::env::temp_dir().join(format!(
            "cert-test-{}.pem", std::process::id(),
        ));
        std::fs::write(&tmp, b"PEM DATA").unwrap();

        let bytes = resolve_cert_bytes(&tmp, "").unwrap();
        assert_eq!(bytes.unwrap(), b"PEM DATA");

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn resolve_cert_bytes_from_base64() {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"PEM DATA");
        let bytes = resolve_cert_bytes(Path::new(""), &encoded).unwrap();
        assert_eq!(bytes.unwrap(), b"PEM DATA");
    }

    #[test]
    fn resolve_cert_bytes_neither_returns_none() {
        let bytes = resolve_cert_bytes(Path::new(""), "").unwrap();
        assert!(bytes.is_none());
    }

    #[test]
    fn resolve_cert_bytes_file_takes_precedence() {
        let tmp = std::env::temp_dir().join(format!(
            "cert-prio-test-{}.pem", std::process::id(),
        ));
        std::fs::write(&tmp, b"FROM FILE").unwrap();

        let bytes = resolve_cert_bytes(&tmp, "base64ignored").unwrap();
        assert_eq!(bytes.unwrap(), b"FROM FILE");

        let _ = std::fs::remove_file(&tmp);
    }

    #[test]
    fn resolve_cert_bytes_bad_file_returns_error() {
        let result = resolve_cert_bytes(Path::new("/no/such/file.pem"), "");
        assert!(result.is_err());
    }

    #[test]
    fn resolve_cert_bytes_bad_base64_returns_error() {
        let result = resolve_cert_bytes(Path::new(""), "not-valid-base64!!!");
        assert!(result.is_err());
    }

    // -- PEM loading --

    fn generate_ca() -> rcgen::CertifiedKey<rcgen::KeyPair> {
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.distinguished_name.push(rcgen::DnType::CommonName, "test-ca");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();
        rcgen::CertifiedKey { cert, signing_key: key }
    }

    #[test]
    fn load_certs_valid_pem() {
        let ca = generate_ca();
        let pem = ca.cert.pem();
        let certs = load_certs(pem.as_bytes()).unwrap();
        assert_eq!(certs.len(), 1);
    }

    #[test]
    fn load_certs_empty() {
        let result = load_certs(b"");
        assert!(result.is_err());
    }

    #[test]
    fn load_private_key_valid_pem() {
        let ca = generate_ca();
        let pem = ca.signing_key.serialize_pem();
        let key = load_private_key(pem.as_bytes());
        assert!(key.is_ok());
    }

    #[test]
    fn load_private_key_empty() {
        let result = load_private_key(b"");
        assert!(result.is_err());
    }
}
