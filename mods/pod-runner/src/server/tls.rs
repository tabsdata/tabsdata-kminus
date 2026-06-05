// Copyright 2026 Tabsdata Inc.

//! TLS setup for the HTTP server.
//!
//! Builds a [`rustls::ServerConfig`] from the [`PRConfig`] TLS settings
//! and provides an [`axum_serve_tls`] function that serves the app over
//! TLS (or mTLS) with graceful shutdown.

use std::pin::Pin;
use std::sync::Arc;

use axum::Router;
use rustls::ServerConfig;
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tracing::{debug, info, warn};

use super::config::PRConfig;
use crate::tls::{load_certs, load_private_key, TlsError};

/// Return the process-default CryptoProvider, falling back to `aws-lc-rs`.
fn crypto_provider() -> Arc<CryptoProvider> {
    CryptoProvider::get_default()
        .cloned()
        .unwrap_or_else(|| Arc::new(rustls::crypto::aws_lc_rs::default_provider()))
}

// ---------------------------------------------------------------------------
// ServerConfig builder
// ---------------------------------------------------------------------------

/// Build a [`rustls::ServerConfig`] from the pod runner configuration.
///
/// Always mTLS: server cert + key, client certs verified against CA.
///
/// Returns `Err` if the config is invalid or certs can't be loaded.
pub fn build_server_config(pr_config: &PRConfig) -> Result<ServerConfig, TlsError> {
    pr_config.tls.validate()?;

    let cert_pem = pr_config.tls.certificate.resolve("tls.certificate")?;
    let key_pem = pr_config.tls.certificate_key.resolve("tls.certificate_key")?;
    let ca_pem = pr_config.tls.client_ca.resolve("tls.client_ca")?;

    let certs = load_certs(&cert_pem)?;
    let key = load_private_key(&key_pem)?;
    let ca_certs = load_certs(&ca_pem)?;

    let mut config = build_mtls_config(certs, key, ca_certs)?;
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(config)
}

fn build_mtls_config(
    certs: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
    ca_certs: Vec<CertificateDer<'static>>,
) -> Result<ServerConfig, TlsError> {
    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert).map_err(TlsError::AddCaCert)?;
    }
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(root_store),
            crypto_provider(),
        )
        .build()
        .map_err(|e| TlsError::BuildVerifier(e.to_string()))?;

    ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()
        .map_err(TlsError::Protocol)?
        .with_client_cert_verifier(verifier)
        .with_single_cert(certs, key)
        .map_err(TlsError::MtlsConfig)
}

// ---------------------------------------------------------------------------
// TLS server
// ---------------------------------------------------------------------------

/// Serve an axum app over TLS with graceful shutdown.
///
/// Accepts TLS connections on the given listener using the provided
/// `ServerConfig`. Each connection is served on a spawned tokio task.
/// Returns when the shutdown future completes and all active connections
/// have finished.
pub async fn axum_serve_tls(
    listener: TcpListener,
    app: Router,
    server_config: ServerConfig,
    shutdown: Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) {
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

    // Spawn the shutdown signal forwarder
    tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
    });

    loop {
        tokio::select! {
            result = listener.accept() => {
                let (stream, addr) = match result {
                    Ok(v) => v,
                    Err(e) => {
                        warn!(error = %e, "Failed to accept TCP connection");
                        continue;
                    }
                };

                let acceptor = acceptor.clone();
                let app = app.clone();
                let mut conn_shutdown = shutdown_rx.clone();

                tokio::spawn(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(e) => {
                            debug!(peer = %addr, error = %e, "TLS handshake failed");
                            return;
                        }
                    };

                    let io = hyper_util::rt::TokioIo::new(tls_stream);
                    let service = hyper_util::service::TowerToHyperService::new(
                        app.into_service(),
                    );
                    let builder = hyper_util::server::conn::auto::Builder::new(
                        hyper_util::rt::TokioExecutor::new(),
                    );
                    let conn = builder.serve_connection(io, service);

                    // Pin the connection future so we can select on it
                    tokio::pin!(conn);

                    tokio::select! {
                        result = &mut conn => {
                            if let Err(e) = result {
                                debug!(peer = %addr, error = %e, "Connection error");
                            }
                        }
                        _ = conn_shutdown.changed() => {
                            debug!(peer = %addr, "Shutdown, closing connection");
                        }
                    }
                });
            }
            _ = shutdown_rx.changed() => {
                info!("TLS server shutting down");
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tls::{CertSource, ServerTlsConfig, load_certs, load_private_key};

    /// Generate a self-signed CA using rcgen.
    fn generate_ca() -> rcgen::CertifiedIssuer<'static, rcgen::KeyPair> {
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-ca");
        let key = rcgen::KeyPair::generate().unwrap();
        rcgen::CertifiedIssuer::self_signed(params, key).unwrap()
    }

    /// Generate a server cert signed by a CA.
    fn generate_server_cert(
        ca: &rcgen::CertifiedIssuer<'_, rcgen::KeyPair>,
    ) -> rcgen::CertifiedKey<rcgen::KeyPair> {
        let mut params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "test-server");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, ca).unwrap();
        rcgen::CertifiedKey { cert, signing_key: key }
    }

    fn make_tls_config(
        cert_pem: &str,
        key_pem: &str,
    ) -> PRConfig {
        let mut c = PRConfig::default();
        c.tls = ServerTlsConfig {
            certificate: CertSource {
                path: None,
                data: Some(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    cert_pem,
                )),
            },
            certificate_key: CertSource {
                path: None,
                data: Some(base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    key_pem,
                )),
            },
            client_ca: CertSource::default(),
        };
        c
    }

    fn make_mtls_config(
        cert_pem: &str,
        key_pem: &str,
        ca_pem: &str,
    ) -> PRConfig {
        let mut c = make_tls_config(cert_pem, key_pem);
        c.tls.client_ca = CertSource {
            path: None,
            data: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                ca_pem,
            )),
        };
        c
    }

    // -- PEM loading --

    #[test]
    fn load_certs_valid_pem() {
        let ca = generate_ca();
        let pem = ca.pem(); // CertifiedIssuer::pem()
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
        // Generate a key directly — CertifiedIssuer's signing_key is private.
        let key = rcgen::KeyPair::generate().unwrap();
        let pem = key.serialize_pem();
        let result = load_private_key(pem.as_bytes());
        assert!(result.is_ok());
    }

    #[test]
    fn load_private_key_empty() {
        let result = load_private_key(b"");
        assert!(result.is_err());
    }

    // -- build_server_config --

    #[test]
    fn build_mtls_config_succeeds() {
        let ca = generate_ca();
        let server = generate_server_cert(&ca);
        let config = make_mtls_config(
            &server.cert.pem(),
            &server.signing_key.serialize_pem(),
            &ca.pem(),
        );

        let result = build_server_config(&config);
        assert!(result.is_ok(), "mTLS config failed: {:?}", result.err());
    }

    #[test]
    fn build_config_fails_with_bad_cert() {
        let mut config = PRConfig::default();
        config.tls.certificate = CertSource {
            path: None,
            data: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                "not a cert",
            )),
        };
        config.tls.certificate_key = CertSource {
            path: None,
            data: Some(base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                "not a key",
            )),
        };
        

        let result = build_server_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn build_config_fails_with_path_and_data() {
        let mut config = PRConfig::default();
        config.tls.certificate = CertSource {
            path: Some("cert.pem".into()),
            data: Some("base64".into()),
        };
        config.tls.certificate_key = CertSource {
            path: None,
            data: Some("key".into()),
        };

        let result = build_server_config(&config);
        assert!(matches!(result, Err(TlsError::BothPathAndData { .. })));
    }
}
