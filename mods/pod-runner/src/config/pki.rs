// Copyright 2026 Tabsdata Inc.

//! PKI auto-generation using `rcgen`.
//!
//! Generates a self-signed CA, server certificate, and client certificate
//! for TLS/mTLS communication. Certs are embedded as base64 inline data
//! in the generated config files.

use std::fs;
use std::path::Path;

use base64::Engine;
use rcgen::{
    BasicConstraints, CertificateParams, CertifiedIssuer, CertifiedKey, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, SanType,
};
use time::{Duration, OffsetDateTime};
use tracing::info;

use super::template::{PRCONFIG_FILENAME, PRSYSCONFIG_FILENAME, PRSYSCONFIG_TEMPLATE};
use super::vk_config;
use crate::server::config::{DEFAULT_NAMESPACE, DEFAULT_NODENAME};

// ---------------------------------------------------------------------------
// Certificate generation
// ---------------------------------------------------------------------------

/// Generated PKI bundle.
pub struct PkiBundle {
    pub ca_cert_pem: String,
    pub ca_key_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
}

/// Generate a complete PKI bundle: CA, server cert, client cert.
pub fn generate_pki(address: &str) -> PkiBundle {
    let (ca, ca_cert_pem, ca_key_pem) = generate_ca();
    let server = generate_server_cert(&ca, address);
    let client = generate_client_cert(&ca, "vk");

    PkiBundle {
        ca_cert_pem,
        ca_key_pem,
        server_cert_pem: server.cert.pem(),
        server_key_pem: server.signing_key.serialize_pem(),
        client_cert_pem: client.cert.pem(),
        client_key_pem: client.signing_key.serialize_pem(),
    }
}

fn generate_ca() -> (CertifiedIssuer<'static, KeyPair>, String, String) {
    let mut params = CertificateParams::default();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params
        .distinguished_name
        .push(DnType::CommonName, "podrunner-ca");
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(3650);
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::CrlSign,
    ];
    let key = KeyPair::generate().expect("CA key generation failed");
    let key_pem = key.serialize_pem();
    let ca = CertifiedIssuer::self_signed(params, key).expect("CA self-sign failed");
    let cert_pem = ca.pem();
    (ca, cert_pem, key_pem)
}

fn generate_server_cert(ca: &CertifiedIssuer<'_, KeyPair>, address: &str) -> CertifiedKey<KeyPair> {
    let mut sans = vec![
        SanType::DnsName(rcgen::string::Ia5String::try_from("localhost").unwrap()),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ];
    if let Ok(ip) = address.parse::<std::net::IpAddr>() {
        if ip != std::net::Ipv4Addr::LOCALHOST {
            sans.push(SanType::IpAddress(ip));
        }
    } else if address != "localhost" {
        sans.push(SanType::DnsName(
            rcgen::string::Ia5String::try_from(address).unwrap(),
        ));
    }

    let mut params = CertificateParams::new(vec![]).expect("server cert params");
    params
        .distinguished_name
        .push(DnType::CommonName, "podrunner-server");
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(365);
    params.subject_alt_names = sans;
    params.key_usages = vec![
        KeyUsagePurpose::DigitalSignature,
        KeyUsagePurpose::KeyEncipherment,
    ];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let key = KeyPair::generate().expect("server key generation failed");
    let cert = params
        .signed_by(&key, ca)
        .expect("server cert signing failed");
    CertifiedKey {
        cert,
        signing_key: key,
    }
}

fn generate_client_cert(ca: &CertifiedIssuer<'_, KeyPair>, id: &str) -> CertifiedKey<KeyPair> {
    let mut params = CertificateParams::new(vec![]).expect("client cert params");
    params.distinguished_name.push(DnType::CommonName, id);
    params.not_before = OffsetDateTime::now_utc();
    params.not_after = OffsetDateTime::now_utc() + Duration::days(365);
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let key = KeyPair::generate().expect("client key generation failed");
    let cert = params
        .signed_by(&key, ca)
        .expect("client cert signing failed");
    CertifiedKey {
        cert,
        signing_key: key,
    }
}

fn b64(pem: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(pem.as_bytes())
}

// ---------------------------------------------------------------------------
// Config file generation
// ---------------------------------------------------------------------------

/// Generate PKI and write prconfig.yaml, vk-prconfig.yaml, and CA key.
///
/// Called when `prconfig.yaml` does not exist in `configdir`.
pub fn generate_and_write_configs(
    configdir: &Path,
    address: &str,
    port: u16,
) -> Result<(), String> {
    fs::create_dir_all(configdir).map_err(|e| format!("Failed to create configdir: {e}"))?;

    let pki = generate_pki(address);
    let url = format!("https://{address}:{port}");

    let ca_cert_b64 = b64(&pki.ca_cert_pem);
    let server_cert_b64 = b64(&pki.server_cert_pem);
    let server_key_b64 = b64(&pki.server_key_pem);
    let client_cert_b64 = b64(&pki.client_cert_pem);
    let client_key_b64 = b64(&pki.client_key_pem);

    // Write prconfig.yaml (endpoint + TLS certs — regenerated on every start)
    let prconfig_path = configdir.join(PRCONFIG_FILENAME);
    let content = format!(
        r#"# Auto-generated — regenerated on every start. Do not edit.
url: "{url}"
server:
  tls:
    certificate-data: {server_cert_b64}
    certificate-key-data: {server_key_b64}
    certificate-authority-data: {ca_cert_b64}
"#
    );
    fs::write(&prconfig_path, &content)
        .map_err(|e| format!("Failed to write {}: {e}", prconfig_path.display()))?;

    // Write prsysconfig.yaml (system settings — only if not present)
    let syscfg_path = configdir.join(PRSYSCONFIG_FILENAME);
    if !syscfg_path.exists() {
        fs::write(&syscfg_path, PRSYSCONFIG_TEMPLATE)
            .map_err(|e| format!("Failed to write {}: {e}", syscfg_path.display()))?;
    }

    // Write vk-prconfig.yaml. The metrics URL here is a placeholder:
    // resolve_config regenerates this file on every start with the
    // resolved value (--metrics-port, or prsysconfig.yaml's metricsUrl).
    vk_config::write_vk_prconfig(
        configdir,
        &url,
        DEFAULT_NODENAME,
        DEFAULT_NAMESPACE,
        &ca_cert_b64,
        &client_cert_b64,
        &client_key_b64,
        crate::metrics::DEFAULT_METRICS_URL,
    )?;

    info!(
        configdir = %configdir.display(),
        "Generated PKI and config files",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::yaml_config::YamlPRConfig;

    fn test_tmp() -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pr-pki-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn generate_pki_produces_valid_certs() {
        let pki = generate_pki("127.0.0.1");
        // Verify PEM strings are non-empty and contain expected markers
        assert!(pki.ca_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(pki.ca_key_pem.contains("BEGIN"));
        assert!(pki.server_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(pki.server_key_pem.contains("BEGIN"));
        assert!(pki.client_cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(pki.client_key_pem.contains("BEGIN"));

        let ca_certs = crate::tls::load_certs(pki.ca_cert_pem.as_bytes()).unwrap();
        assert_eq!(ca_certs.len(), 1);
        let server_certs = crate::tls::load_certs(pki.server_cert_pem.as_bytes()).unwrap();
        assert_eq!(server_certs.len(), 1);
    }

    #[test]
    fn generate_and_write_creates_files() {
        let tmp = test_tmp();
        generate_and_write_configs(&tmp, "127.0.0.1", 10300).unwrap();

        assert!(tmp.join(PRCONFIG_FILENAME).exists());
        assert!(
            tmp.join(crate::config::template::VK_PRCONFIG_FILENAME)
                .exists()
        );
        // CA key is no longer written to disk

        // Verify prconfig.yaml is parseable
        let config = YamlPRConfig::from_file(&tmp.join(PRCONFIG_FILENAME)).unwrap();
        assert_eq!(config.url, "https://127.0.0.1:10300");
        assert!(!config.server.tls.certificate_data.is_empty());
        assert!(!config.server.tls.certificate_key_data.is_empty());

        let _ = fs::remove_dir_all(&tmp);
    }

    #[test]
    fn generated_prconfig_has_valid_tls() {
        let tmp = test_tmp();
        generate_and_write_configs(&tmp, "127.0.0.1", 10300).unwrap();

        // Load the generated config and verify TLS works end-to-end
        let yaml_config = YamlPRConfig::from_file(&tmp.join(PRCONFIG_FILENAME)).unwrap();
        assert!(!yaml_config.server.tls.certificate_data.is_empty());
        assert!(!yaml_config.server.tls.certificate_authority_data.is_empty());

        let _ = fs::remove_dir_all(&tmp);
    }
}
