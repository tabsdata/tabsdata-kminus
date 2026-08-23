// Copyright 2026 Tabsdata Inc.

//! mTLS HTTP client for the pod-runner API, built from a `vk-prconfig.yaml`.
//!
//! Shared by the `kpr` CLI and by the server's own readiness self-probe,
//! so both reach `/capacity` exactly the way the virtual-kubelet does.

use std::path::Path;

use reqwest::Certificate;

use crate::client::config::VKPRConfig;
use crate::tls::resolve_cert_bytes;

/// Build a client honouring the config's server CA (or its
/// insecure-skip-tls-verify) and presenting its client certificate.
pub fn build_client(config: &Option<VKPRConfig>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder();

    if let Some(config) = config {
        if config.server.tls.insecure_skip_tls_verify {
            builder = builder.danger_accept_invalid_certs(true);
        } else {
            let ca_bytes = resolve_cert_bytes(
                Path::new(&config.server.tls.certificate_authority),
                &config.server.tls.certificate_authority_data,
            )
            .map_err(|e| format!("Server CA: {e}"))?;

            if let Some(pem) = ca_bytes {
                let cert = Certificate::from_pem(&pem)
                    .map_err(|e| format!("Server CA: invalid PEM: {e}"))?;
                builder = builder.add_root_certificate(cert);
            }
        }

        if let Some(client_cfg) = config.client.as_ref() {
            let cert_bytes = resolve_cert_bytes(
                Path::new(&client_cfg.certificate),
                &client_cfg.certificate_data,
            )
            .map_err(|e| format!("Client cert: {e}"))?;

            let key_bytes = resolve_cert_bytes(
                Path::new(&client_cfg.certificate_key),
                &client_cfg.certificate_key_data,
            )
            .map_err(|e| format!("Client key: {e}"))?;

            if let (Some(cert), Some(key)) = (cert_bytes, key_bytes) {
                // reqwest expects a combined PEM (cert + key) for Identity
                let mut combined = cert;
                combined.push(b'\n');
                combined.extend_from_slice(&key);
                let identity = reqwest::Identity::from_pem(&combined)
                    .map_err(|e| format!("Client identity: {e}"))?;
                builder = builder.identity(identity);
            }
        }
    }

    builder
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {e}"))
}
