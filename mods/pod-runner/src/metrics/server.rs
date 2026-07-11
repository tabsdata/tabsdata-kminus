// Copyright 2026 Tabsdata Inc.

//! Standalone Prometheus metrics HTTP server.

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus::Encoder;
use tokio::sync::watch;
use tracing::{info, warn};

use super::Metrics;

/// Spawn the metrics HTTP server on the given port.
///
/// Serves `GET /metrics` in Prometheus text format. Refreshes gauges
/// (memory, uptime, tokio tasks) on each scrape. Shuts down when
/// the shutdown signal fires.
pub async fn serve_metrics(port: u16, metrics: Arc<Metrics>, mut shutdown_rx: watch::Receiver<()>) {
    let app = Router::new()
        .route("/metrics", get(handle_metrics))
        .with_state(metrics);

    let addr = format!("0.0.0.0:{port}");
    let listener = match tokio::net::TcpListener::bind(&addr).await {
        Ok(l) => l,
        Err(e) => {
            warn!(error = %e, addr = %addr, "Failed to bind metrics server");
            return;
        }
    };

    info!(addr = %addr, "Metrics server listening");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = shutdown_rx.changed().await;
            info!("Metrics server shutting down");
        })
        .await
        .unwrap_or_else(|e| warn!(error = %e, "Metrics server error"));
}

async fn handle_metrics(State(metrics): State<Arc<Metrics>>) -> impl IntoResponse {
    metrics.update_gauges();

    let families = metrics.registry.gather();
    let mut buf = Vec::new();
    if let Err(e) = prometheus::TextEncoder::new().encode(&families, &mut buf) {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to encode metrics: {e}"),
        )
            .into_response();
    }

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        buf,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn metrics_endpoint_returns_prometheus_text() {
        let metrics = Metrics::new();
        metrics.pods_created_total.inc();

        let app = Router::new()
            .route("/metrics", get(handle_metrics))
            .with_state(metrics);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // Give server a moment to start
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resp = reqwest_lite(&format!("http://{addr}/metrics")).await;
        assert!(resp.contains("pr_pods_created_total 1"));
        assert!(resp.contains("pr_uptime_seconds"));
        assert!(resp.contains("pr_memory_physical_bytes"));
    }

    async fn reqwest_lite(url: &str) -> String {
        use http_body_util::BodyExt;
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;

        let client = Client::builder(TokioExecutor::new())
            .build_http::<http_body_util::Full<bytes::Bytes>>();
        let resp = client.get(url.parse().unwrap()).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(body.to_vec()).unwrap()
    }
}
