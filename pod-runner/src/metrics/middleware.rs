// Copyright 2026 Tabsdata Inc.

//! Axum middleware for recording HTTP request duration.

use std::sync::Arc;
use std::time::Instant;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::Response;

use super::Metrics;

/// Middleware that records HTTP request duration in the Prometheus histogram.
///
/// Labels: `method`, `path`, `status`.
pub async fn track_request_duration(
    axum::extract::State(metrics): axum::extract::State<Arc<Metrics>>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let start = Instant::now();

    let response = next.run(request).await;

    let status = response.status().as_u16().to_string();
    let elapsed = start.elapsed().as_secs_f64();

    metrics
        .http_request_duration
        .with_label_values(&[&method, &path, &status])
        .observe(elapsed);

    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use axum::Router;

    #[tokio::test]
    async fn middleware_records_duration() {
        let metrics = Metrics::new();
        let metrics_clone = Arc::clone(&metrics);

        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn_with_state(
                metrics_clone,
                track_request_duration,
            ));

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // Make a request
        use hyper_util::client::legacy::Client;
        use hyper_util::rt::TokioExecutor;
        let client = Client::builder(TokioExecutor::new())
            .build_http::<http_body_util::Full<bytes::Bytes>>();
        let _resp = client
            .get(format!("http://{addr}/test").parse().unwrap())
            .await
            .unwrap();

        // Verify histogram was recorded
        let count = metrics
            .http_request_duration
            .with_label_values(&["GET", "/test", "200"])
            .get_sample_count();
        assert_eq!(count, 1, "should have recorded 1 request");
    }
}
