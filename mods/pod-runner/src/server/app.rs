// Copyright 2026 Tabsdata Inc.

//! Router, handlers, and shared state for the REST API.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::logic::{ContainerLogOptions, LogicError, PodLogic};
use crate::spec::api::{CreatePodResponse, DeletePodResponse, ErrorResponse, PodObject};
use crate::spec::pod::PodSpec;
use crate::store::SpecView;

// ---------------------------------------------------------------------------
// Shared state
// ---------------------------------------------------------------------------

/// Application state shared across all handlers.
pub struct AppState {
    pub logic: PodLogic,
}

/// Thread-safe shared state handle.
pub type SharedState = Arc<AppState>;

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// Create the Axum router with all pod management endpoints.
///
/// If `metrics` is provided, adds a middleware layer that records
/// HTTP request duration in the Prometheus histogram.
pub fn create_router(
    state: SharedState,
    metrics: Option<Arc<crate::metrics::Metrics>>,
) -> Router {
    let router = Router::new()
        .route("/createPod", post(handle_create_pod))
        .route("/deletePod", delete(handle_delete_pod))
        .route("/getPod", get(handle_get_pod))
        .route("/getPodStatus", get(handle_get_pod_status))
        .route("/getPods", get(handle_get_pods))
        .route("/getAllPodStatuses", get(handle_get_all_pod_statuses))
        .route("/getContainerLogs", get(handle_get_container_logs))
        .route("/capacity", get(handle_capacity))
        .route("/nodeConditions", get(handle_node_conditions))
        .route("/nodeAddresses", get(handle_node_addresses))
        .with_state(state);

    if let Some(metrics) = metrics {
        router.layer(axum::middleware::from_fn_with_state(
            metrics,
            crate::metrics::middleware::track_request_duration,
        ))
    } else {
        router
    }
}

// ---------------------------------------------------------------------------
// Query parameter types
// ---------------------------------------------------------------------------

const DEFAULT_NAMESPACE: &str = "default";

fn default_namespace() -> String {
    DEFAULT_NAMESPACE.to_string()
}

#[derive(Deserialize)]
struct PodQuery {
    name: Option<String>,
    #[serde(default = "default_namespace")]
    namespace: String,
    #[serde(rename = "gracePeriod")]
    grace_period: Option<u64>,
}

#[derive(Deserialize)]
struct LogQuery {
    #[serde(rename = "podName")]
    pod_name: Option<String>,
    #[serde(default = "default_namespace")]
    namespace: String,
    #[serde(rename = "containerName")]
    container_name: Option<String>,
    #[serde(default)]
    follow: bool,
    #[serde(rename = "tailLines")]
    tail_lines: Option<u64>,
    #[serde(default)]
    previous: bool,
    #[serde(default)]
    timestamps: bool,
}

// ---------------------------------------------------------------------------
// Error helpers
// ---------------------------------------------------------------------------

fn error_response(err: LogicError) -> Response {
    let code = match &err {
        LogicError::NotFound { .. } => StatusCode::NOT_FOUND,
        LogicError::Conflict { .. } => StatusCode::CONFLICT,
        LogicError::CapacityExceeded { .. } => StatusCode::FORBIDDEN,
        LogicError::Admission(_) => StatusCode::INTERNAL_SERVER_ERROR,
        LogicError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (code, Json(ErrorResponse { error: err.to_string() })).into_response()
}

fn missing_param(name: &str) -> Response {
    let body = ErrorResponse {
        error: format!("Missing '{}' query parameter", name),
    };
    (StatusCode::BAD_REQUEST, Json(body)).into_response()
}

fn bad_request(message: String) -> Response {
    (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: message })).into_response()
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

/// POST /createPod — submit and start a pod.
async fn handle_create_pod(
    State(state): State<SharedState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let spec: PodSpec = match serde_json::from_value(body) {
        Ok(s) => s,
        Err(e) => return bad_request(format!("Invalid pod spec: {}", e)),
    };

    match state.logic.create_pod(spec).await {
        Ok(snap) => {
            let result = CreatePodResponse {
                pod_id: snap.id.full(),
                phase: snap.status.phase.to_string(),
            };
            (StatusCode::CREATED, Json(result)).into_response()
        }
        Err(e) => error_response(e),
    }
}

/// DELETE /deletePod?name=...&namespace=...&gracePeriod=...
async fn handle_delete_pod(
    State(state): State<SharedState>,
    Query(params): Query<PodQuery>,
) -> Response {
    let name = match params.name {
        Some(n) => n,
        None => return missing_param("name"),
    };
    let namespace = params.namespace;
    let grace = params.grace_period.map(Duration::from_secs);

    match state.logic.delete_pod(&namespace, &name, grace).await {
        Ok(()) => Json(DeletePodResponse { deleted: true }).into_response(),
        Err(e) => error_response(e),
    }
}

/// GET /getPod?name=...&namespace=...
async fn handle_get_pod(
    State(state): State<SharedState>,
    Query(params): Query<PodQuery>,
) -> Response {
    let name = match params.name {
        Some(n) => n,
        None => return missing_param("name"),
    };
    let namespace = params.namespace;

    match state.logic.get_pod(&namespace, &name, SpecView::Running).await {
        Ok(snap) => {
            let pod = PodObject {
                metadata: snap.spec.metadata.clone(),
                spec: snap.spec.spec.clone(),
                status: snap.status,
            };
            Json(pod).into_response()
        }
        Err(e) => error_response(e),
    }
}

/// GET /getPodStatus?name=...&namespace=...
async fn handle_get_pod_status(
    State(state): State<SharedState>,
    Query(params): Query<PodQuery>,
) -> Response {
    let name = match params.name {
        Some(n) => n,
        None => return missing_param("name"),
    };
    let namespace = params.namespace;

    match state.logic.get_pod_status(&namespace, &name).await {
        Ok(status) => Json(status).into_response(),
        Err(e) => error_response(e),
    }
}

/// GET /getPods
async fn handle_get_pods(State(state): State<SharedState>) -> Response {
    let pods: Vec<PodObject> = state
        .logic
        .list_pods(SpecView::Running)
        .await
        .into_iter()
        .map(|snap| PodObject {
            metadata: snap.spec.metadata.clone(),
            spec: snap.spec.spec.clone(),
            status: snap.status,
        })
        .collect();
    Json(pods).into_response()
}

/// GET /getAllPodStatuses
///
/// Returns namespace, name, phase, and full status for every pod in a
/// single call. Designed for the virtual-kubelet's sync loop: one bulk
/// call per tick replaces N individual getPodStatus calls.
async fn handle_get_all_pod_statuses(State(state): State<SharedState>) -> Response {
    Json(state.logic.list_pod_statuses().await).into_response()
}

/// GET /getContainerLogs?podName=...&namespace=...&containerName=...&follow=...&tailLines=...&previous=...
///
/// Streams log content as chunked plain text. With `follow=true`,
/// keeps streaming until the pod reaches a terminal phase or the
/// client disconnects.
async fn handle_get_container_logs(
    State(state): State<SharedState>,
    Query(params): Query<LogQuery>,
) -> Response {
    let pod_name = match params.pod_name {
        Some(n) => n,
        None => return missing_param("podName"),
    };

    let options = ContainerLogOptions {
        follow: params.follow,
        tail_lines: params.tail_lines,
        previous: params.previous,
        timestamps: params.timestamps,
    };

    let rx = match state
        .logic
        .stream_container_logs(&params.namespace, &pod_name, params.container_name.as_deref(), options)
        .await
    {
        Ok(rx) => rx,
        Err(e) => return error_response(e),
    };

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body = axum::body::Body::from_stream(stream);
    Response::builder()
        .header("content-type", "text/plain")
        .body(body)
        .unwrap()
}

/// GET /capacity
async fn handle_capacity(State(state): State<SharedState>) -> Response {
    Json(state.logic.get_capacity()).into_response()
}

/// GET /nodeConditions
async fn handle_node_conditions(State(state): State<SharedState>) -> Response {
    Json(state.logic.get_node_conditions()).into_response()
}

/// GET /nodeAddresses
async fn handle_node_addresses(State(state): State<SharedState>) -> Response {
    Json(state.logic.get_node_addresses()).into_response()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{DefaultExecutorProvider, LogicConfig};
    use crate::pod::resolver::ResolverSet;
    use crate::store::memory::MemoryStore;
    use crate::store::PodStore;
    use crate::testutils::shell_cmd;

    use http_body_util::BodyExt;
    use std::time::Duration;
    use tokio::sync::watch;

    /// Start a test server on a random port, return the address.
    async fn start_server() -> (std::net::SocketAddr, watch::Sender<()>) {
        let store = Arc::new(MemoryStore::new());
        let base = std::env::temp_dir().join(format!(
            "pod-runner-rs2-test-server-{}",
            std::process::id(),
        ));
        let provider = Arc::new(DefaultExecutorProvider {
            pods_dir: base.join("pods"),
            pid_dir: base.join("pids"),
            propagate_env: true,
            die_with_parent: true,
            max_restarts: 0,
            restart_backoff_base: Duration::from_secs(2),
            shutdown_grace_period: Duration::from_secs(30),
        });
        let cleanup = crate::logic::CleanupConfig {
            pods_dir: base.join("pods"),
            
            ..crate::logic::CleanupConfig::default()
        };
        let (shutdown_tx, _) = watch::channel(());
        let logic = PodLogic::new(
            LogicConfig::default(),
            cleanup,
            store as Arc<dyn PodStore>,
            provider,
            ResolverSet::default(),
            shutdown_tx.clone(),
        );
        let state = Arc::new(AppState { logic });
        let app = create_router(state, None);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        (addr, shutdown_tx)
    }

    /// Send an HTTP request to the test server and parse the JSON response.
    async fn request(
        addr: &std::net::SocketAddr,
        method: &str,
        path: &str,
        body: Option<serde_json::Value>,
    ) -> (StatusCode, serde_json::Value) {
        let client = hyper_util::client::legacy::Client::builder(
            hyper_util::rt::TokioExecutor::new(),
        )
        .build_http();

        let uri = format!("http://{}{}", addr, path);
        let req_body = body
            .map(|j| serde_json::to_string(&j).unwrap())
            .unwrap_or_default();

        let req = hyper::Request::builder()
            .method(method)
            .uri(&uri)
            .header("content-type", "application/json")
            .body(req_body)
            .unwrap();

        let resp = client.request(req).await.unwrap();
        let status = StatusCode::from_u16(resp.status().as_u16()).unwrap();
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    fn pod_spec_json(name: &str, cmd: &str) -> serde_json::Value {
        serde_json::json!({
            "metadata": {
                "namespace": "default",
                "name": name,
                "uid": format!("uid-{}", name),
            },
            "spec": {
                "containers": [{
                    "name": "main",
                    "command": shell_cmd(cmd),
                }],
            },
        })
    }

    fn pod_spec_json_uid(name: &str, uid: &str, cmd: &str) -> serde_json::Value {
        let mut v = pod_spec_json(name, cmd);
        v["metadata"]["uid"] = serde_json::json!(uid);
        v
    }

    // -- createPod --

    #[tokio::test]
    async fn create_pod_success() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "POST", "/createPod", Some(pod_spec_json("app", "exit 0"))).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["phase"], "Pending");
        assert!(body["podId"].as_str().unwrap().contains("app"));
    }

    #[tokio::test]
    async fn create_pod_invalid_json() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "POST", "/createPod", Some(serde_json::json!("not a spec"))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("Invalid pod spec"));
    }

    #[tokio::test]
    async fn create_pod_duplicate() {
        let (addr, _) = start_server().await;

        // Same name, new uid, old pod still live → conflict (retryable);
        // the old pod is terminated so a later retry can proceed.
        request(&addr, "POST", "/createPod", Some(pod_spec_json("app", "sleep 60"))).await;
        let (status, body) = request(&addr, "POST", "/createPod", Some(pod_spec_json_uid("app", "uid-app-2", "sleep 60"))).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(body["error"].as_str().unwrap().contains("already exists"));
    }

    // -- deletePod --

    #[tokio::test]
    async fn delete_pod_success() {
        let (addr, _) = start_server().await;

        request(&addr, "POST", "/createPod", Some(pod_spec_json("app", "exit 0"))).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let (status, body) = request(&addr, "DELETE", "/deletePod?name=app", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["deleted"], true);
    }

    #[tokio::test]
    async fn delete_pod_not_found_is_ok() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "DELETE", "/deletePod?name=nope", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["deleted"], true);
    }

    #[tokio::test]
    async fn delete_pod_missing_name() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "DELETE", "/deletePod", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["error"].as_str().unwrap().contains("name"));
    }

    // -- getPod --

    #[tokio::test]
    async fn get_pod_success() {
        let (addr, _) = start_server().await;

        request(&addr, "POST", "/createPod", Some(pod_spec_json("app", "sleep 60"))).await;

        let (status, body) = request(&addr, "GET", "/getPod?name=app", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["metadata"]["name"], "app");
        assert!(body["status"]["phase"].is_string());
    }

    #[tokio::test]
    async fn get_pod_not_found() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "GET", "/getPod?name=nope", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(body["error"].as_str().unwrap().contains("not found"));
    }

    #[tokio::test]
    async fn get_pod_missing_name() {
        let (addr, _) = start_server().await;

        let (status, _body) = request(&addr, "GET", "/getPod", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    // -- getPodStatus --

    #[tokio::test]
    async fn get_pod_status_success() {
        let (addr, _) = start_server().await;

        request(&addr, "POST", "/createPod", Some(pod_spec_json("app", "sleep 60"))).await;

        let (status, body) = request(&addr, "GET", "/getPodStatus?name=app", None).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body["phase"].is_string());
    }

    #[tokio::test]
    async fn get_pod_status_not_found() {
        let (addr, _) = start_server().await;

        let (status, _) = request(&addr, "GET", "/getPodStatus?name=nope", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    // -- getPods --

    #[tokio::test]
    async fn get_pods_empty() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "GET", "/getPods", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn get_pods_multiple() {
        let (addr, _) = start_server().await;

        request(&addr, "POST", "/createPod", Some(pod_spec_json("a", "sleep 60"))).await;
        request(&addr, "POST", "/createPod", Some(pod_spec_json("b", "sleep 60"))).await;

        let (status, body) = request(&addr, "GET", "/getPods", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body.as_array().unwrap().len(), 2);
    }

    // -- getContainerLogs --

    #[tokio::test]
    async fn get_container_logs_success() {
        let (addr, _) = start_server().await;

        // Create a pod that writes output and exits
        request(&addr, "POST", "/createPod", Some(pod_spec_json("logger", "echo hello-log"))).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let (status, _body) = request(&addr, "GET", "/getContainerLogs?podName=logger", None).await;
        assert_eq!(status, StatusCode::OK);
        // body is parsed as JSON — plain text "hello-log\n" won't parse, so it's Null
        // That's expected — the response is text/plain, not JSON
        // Just verify we get 200
    }

    #[tokio::test]
    async fn get_container_logs_pod_not_found() {
        let (addr, _) = start_server().await;

        let (status, _) = request(&addr, "GET", "/getContainerLogs?podName=nope", None).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn get_container_logs_missing_param() {
        let (addr, _) = start_server().await;

        let (status, _) = request(&addr, "GET", "/getContainerLogs", None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    // -- capacity --

    #[tokio::test]
    async fn capacity_returns_defaults() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "GET", "/capacity", None).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["cpu"], "100");
        assert_eq!(body["memory"], "100Gi");
        assert_eq!(body["pods"], "100");
    }

    // -- nodeConditions --

    #[tokio::test]
    async fn node_conditions_returns_three() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "GET", "/nodeConditions", None).await;
        assert_eq!(status, StatusCode::OK);
        let conditions = body.as_array().unwrap();
        assert_eq!(conditions.len(), 3);
        assert_eq!(conditions[0]["type"], "Ready");
        assert_eq!(conditions[0]["status"], "True");
        assert_eq!(conditions[1]["type"], "MemoryPressure");
        assert_eq!(conditions[1]["status"], "False");
        assert_eq!(conditions[2]["type"], "DiskPressure");
        assert_eq!(conditions[2]["status"], "False");
    }

    // -- error_response --

    #[test]
    fn error_response_status_codes() {
        use crate::logic::LogicError;
        use crate::pod::admission::AdmissionError;

        let cases = [
            (
                LogicError::NotFound {
                    namespace: "default".into(),
                    name: "p".into(),
                },
                StatusCode::NOT_FOUND,
            ),
            (
                LogicError::Conflict {
                    namespace: "default".into(),
                    name: "p".into(),
                },
                StatusCode::CONFLICT,
            ),
            (LogicError::CapacityExceeded { max: 10 }, StatusCode::FORBIDDEN),
            (
                LogicError::Admission(AdmissionError::OptionalNotSupported {
                    location: "container 'app', env 'PORT', secretKeyRef 'creds/pass'".into(),
                }),
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (LogicError::Internal("boom".into()), StatusCode::INTERNAL_SERVER_ERROR),
        ];

        for (err, expected) in cases {
            let resp = error_response(err);
            assert_eq!(resp.status(), expected);
        }
    }

    // -- nodeAddresses --

    #[tokio::test]
    async fn node_addresses_returns_internal_ip() {
        let (addr, _) = start_server().await;

        let (status, body) = request(&addr, "GET", "/nodeAddresses", None).await;
        assert_eq!(status, StatusCode::OK);
        let addrs = body.as_array().unwrap();
        assert_eq!(addrs.len(), 1);
        assert_eq!(addrs[0]["type"], "InternalIP");
        assert_eq!(addrs[0]["address"], "127.0.0.1");
    }
}
