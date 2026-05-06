// Copyright 2026 Tabsdata Inc.

//! `kpr` — HTTP client CLI for a running Pod Runner server.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use futures_util::StreamExt;
use reqwest::Certificate;

use pod_runner::client::config::VKPRConfig;
use pod_runner::tls::resolve_cert_bytes;

// ---------------------------------------------------------------------------
// Exit codes
// ---------------------------------------------------------------------------

const EXIT_OK: u8 = 0;
const EXIT_VALIDATION: u8 = 1;
const EXIT_CONNECTION: u8 = 2;
const EXIT_SERVER_ERROR: u8 = 3;

// ---------------------------------------------------------------------------
// Environment variables
// ---------------------------------------------------------------------------

const ENV_KUBECONFIG: &str = "KUBECONFIG";
const ENV_PRCONFIG: &str = "PRCONFIG";

// ---------------------------------------------------------------------------
// API endpoints
// ---------------------------------------------------------------------------

const ENDPOINT_CREATE_POD: &str = "/createPod";
const ENDPOINT_DELETE_POD: &str = "/deletePod";
const ENDPOINT_GET_POD: &str = "/getPod";
const ENDPOINT_GET_POD_STATUS: &str = "/getPodStatus";
const ENDPOINT_GET_PODS: &str = "/getPods";
const ENDPOINT_GET_CONTAINER_LOGS: &str = "/getContainerLogs";
const ENDPOINT_CAPACITY: &str = "/capacity";
const ENDPOINT_NODE_CONDITIONS: &str = "/nodeConditions";
const ENDPOINT_NODE_ADDRESSES: &str = "/nodeAddresses";
const ENDPOINT_METRICS: &str = "/metrics";

// ---------------------------------------------------------------------------
// Query parameter names
// ---------------------------------------------------------------------------

const PARAM_NAME: &str = "name";
const PARAM_NAMESPACE: &str = "namespace";
const PARAM_POD_NAME: &str = "podName";
const PARAM_CONTAINER_NAME: &str = "containerName";
const PARAM_FOLLOW: &str = "follow";
const PARAM_TAIL_LINES: &str = "tailLines";
const PARAM_PREVIOUS: &str = "previous";

// ---------------------------------------------------------------------------
// JSON field names
// ---------------------------------------------------------------------------

const FIELD_METADATA: &str = "metadata";
const FIELD_UID: &str = "uid";
const FIELD_NAME: &str = "name";
const FIELD_ERROR: &str = "error";
const FIELD_CORRELATION_ID: &str = "correlationId";

// ---------------------------------------------------------------------------
// Response handling
// ---------------------------------------------------------------------------

const CONTENT_TYPE: &str = "content-type";
const CONTENT_TYPE_JSON: &str = "json";
const VALUE_TRUE: &str = "true";

// ---------------------------------------------------------------------------
// Defaults
// ---------------------------------------------------------------------------

const DEFAULT_NAMESPACE: &str = "default";
const HTTP_SCHEME_PREFIX: &str = "http://";

// ---------------------------------------------------------------------------
// CLI definition
// ---------------------------------------------------------------------------

/// pod-runner client — interacts with a running pod-runner server via HTTP.
#[derive(Parser, Debug)]
#[command(
    name = "kpr",
    version = pod_runner::VERSION,
    about = "kpr — pod-runner client over HTTP.",
    long_about = format!(
        "kpr — pod-runner client over HTTP.\n\n{}\n{}",
        pod_runner::COPYRIGHT,
        pod_runner::LICENSE,
    ),
)]
struct Cli {
    /// Path to kubeconfig file. Falls back to KUBECONFIG env var.
    #[arg(long)]
    kubeconfig: Option<String>,

    /// Path to the client config YAML file (vk-prconfig.yaml).
    /// Falls back to PRCONFIG env var.
    #[arg(long)]
    prconfig: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Create a pod from a YAML file.
    Create {
        /// Path to pod YAML specification file.
        #[arg(short = 'f', long = "file")]
        deployment: String,
    },

    /// Delete a pod.
    Delete {
        /// Pod name.
        #[arg(short = 'n', long)]
        name: String,

        /// Pod namespace.
        #[arg(short = 'N', long, default_value = DEFAULT_NAMESPACE)]
        namespace: String,
    },

    /// Get full pod object.
    Get {
        /// Pod name.
        #[arg(short = 'n', long)]
        name: String,

        /// Pod namespace.
        #[arg(short = 'N', long, default_value = DEFAULT_NAMESPACE)]
        namespace: String,
    },

    /// Get pod status.
    Status {
        /// Pod name.
        #[arg(short = 'n', long)]
        name: String,

        /// Pod namespace.
        #[arg(short = 'N', long, default_value = DEFAULT_NAMESPACE)]
        namespace: String,
    },

    /// List all active pods.
    List,

    /// Get container logs (stdout and stderr combined).
    Logs {
        /// Pod name.
        #[arg(short = 'n', long)]
        name: String,

        /// Pod namespace.
        #[arg(short = 'N', long, default_value = DEFAULT_NAMESPACE)]
        namespace: String,

        /// Container name.
        #[arg(short = 'c', long)]
        container: String,

        /// Follow log output (stream in real time).
        #[arg(long, default_value_t = false)]
        stream: bool,

        /// Number of lines from the end to show. Default: all logs from beginning.
        #[arg(long)]
        tail: Option<u64>,

        /// Show logs from previous container instance.
        #[arg(long, default_value_t = false)]
        previous: bool,
    },

    /// Node capacity.
    Capacity,

    /// Node conditions.
    #[command(name = "node-conditions")]
    NodeConditions,

    /// Node addresses.
    #[command(name = "node-addresses")]
    NodeAddresses,

    /// Prometheus metrics (HTTP only).
    Metrics,
}

// ---------------------------------------------------------------------------
// HTTP client builder
// ---------------------------------------------------------------------------

fn build_client(config: &Option<VKPRConfig>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder();

    if let Some(config) = config {
        // TLS verification
        if config.server.tls.insecure_skip_tls_verify {
            builder = builder.danger_accept_invalid_certs(true);
        } else {
            // Add server CA certificate
            let ca_bytes = resolve_cert_bytes(
                &config.server.tls.certificate_authority,
                &config.server.tls.certificate_authority_data,
            )
            .map_err(|e| format!("Server CA: {e}"))?;

            if let Some(pem) = ca_bytes {
                let cert = Certificate::from_pem(&pem)
                    .map_err(|e| format!("Server CA: invalid PEM: {e}"))?;
                builder = builder.add_root_certificate(cert);
            }
        }

        // Client certificate for mTLS
        if let Some(client_cfg) = config.client.as_ref() {
            let cert_bytes = resolve_cert_bytes(
                &client_cfg.certificate,
                &client_cfg.certificate_data,
            )
            .map_err(|e| format!("Client cert: {e}"))?;

            let key_bytes = resolve_cert_bytes(
                &client_cfg.certificate_key,
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

    builder.build().map_err(|e| format!("Failed to build HTTP client: {e}"))
}

// ---------------------------------------------------------------------------
// Response handling
// ---------------------------------------------------------------------------

fn print_json(value: &serde_json::Value) {
    println!("{}", serde_json::to_string_pretty(value).unwrap());
}

async fn handle_response(resp: reqwest::Response) -> u8 {
    let status = resp.status();
    if status.is_success() {
        let ct = resp
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if ct.contains(CONTENT_TYPE_JSON) {
            match resp.json::<serde_json::Value>().await {
                Ok(json) => print_json(&json),
                Err(e) => eprintln!("Error: failed to parse response: {e}"),
            }
        } else {
            match resp.text().await {
                Ok(text) => print!("{text}"),
                Err(e) => eprintln!("Error: failed to read response: {e}"),
            }
        }
        return EXIT_OK;
    }

    // Error response
    let body: serde_json::Value = resp
        .json()
        .await
        .unwrap_or(serde_json::Value::Null);
    let msg = body[FIELD_ERROR]
        .as_str()
        .unwrap_or("Unknown error");
    let cid = body[FIELD_CORRELATION_ID].as_str().unwrap_or("");
    if cid.is_empty() {
        eprintln!("Error ({status}): {msg}");
    } else {
        eprintln!("Error [{cid}] ({status}): {msg}");
    }
    EXIT_SERVER_ERROR
}

// ---------------------------------------------------------------------------
// Command handlers
// ---------------------------------------------------------------------------

async fn cmd_create(client: &reqwest::Client, base_url: &str, deployment: &str) -> u8 {
    let path = PathBuf::from(deployment);
    let content = match std::fs::read_to_string(&path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: cannot read {}: {e}", path.display());
            return EXIT_VALIDATION;
        }
    };
    let mut pod_spec: serde_json::Value = match serde_yaml_ng::from_str(&content) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error: invalid YAML: {e}");
            return EXIT_VALIDATION;
        }
    };

    // Auto-generate UID if missing
    let uid_path = format!("/{FIELD_METADATA}/{FIELD_UID}");
    if pod_spec
        .pointer(&uid_path)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .is_empty()
    {
        let uid = uuid::Uuid::new_v4().to_string();
        eprintln!("Warning: metadata.uid not set, auto-generated: {uid}");
        pod_spec
            .as_object_mut()
            .unwrap()
            .entry(FIELD_METADATA)
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
            .unwrap()
            .insert(FIELD_UID.to_string(), serde_json::Value::String(uid));
    }

    let resp = match client
        .post(format!("{base_url}{ENDPOINT_CREATE_POD}"))
        .json(&pod_spec)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };

    if resp.status().is_success() {
        let name_path = format!("/{FIELD_METADATA}/{FIELD_NAME}");
        let name = pod_spec
            .pointer(&name_path)
            .and_then(|v| v.as_str())
            .unwrap_or("?");
        println!("Pod '{name}' created.");
        return EXIT_OK;
    }
    handle_response(resp).await
}

async fn cmd_delete(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
    namespace: &str,
) -> u8 {
    let resp = match client
        .delete(format!("{base_url}{ENDPOINT_DELETE_POD}"))
        .query(&[(PARAM_NAME, name), (PARAM_NAMESPACE, namespace)])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_get(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
    namespace: &str,
) -> u8 {
    let resp = match client
        .get(format!("{base_url}{ENDPOINT_GET_POD}"))
        .query(&[(PARAM_NAME, name), (PARAM_NAMESPACE, namespace)])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_status(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
    namespace: &str,
) -> u8 {
    let resp = match client
        .get(format!("{base_url}{ENDPOINT_GET_POD_STATUS}"))
        .query(&[(PARAM_NAME, name), (PARAM_NAMESPACE, namespace)])
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_list(client: &reqwest::Client, base_url: &str) -> u8 {
    let resp = match client
        .get(format!("{base_url}{ENDPOINT_GET_PODS}"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_logs(
    client: &reqwest::Client,
    base_url: &str,
    name: &str,
    namespace: &str,
    container: &str,
    stream: bool,
    tail: Option<u64>,
    previous: bool,
) -> u8 {
    let mut params: Vec<(&str, String)> = vec![
        (PARAM_POD_NAME, name.to_string()),
        (PARAM_NAMESPACE, namespace.to_string()),
        (PARAM_CONTAINER_NAME, container.to_string()),
    ];
    if stream {
        params.push((PARAM_FOLLOW, VALUE_TRUE.to_string()));
    }
    if let Some(n) = tail {
        params.push((PARAM_TAIL_LINES, n.to_string()));
    }
    if previous {
        params.push((PARAM_PREVIOUS, VALUE_TRUE.to_string()));
    }

    let resp = match client
        .get(format!("{base_url}{ENDPOINT_GET_CONTAINER_LOGS}"))
        .query(&params)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };

    if !resp.status().is_success() {
        return handle_response(resp).await;
    }

    if stream {
        // Stream chunks to stdout
        let mut byte_stream = resp.bytes_stream();
        let mut stdout = std::io::stdout();
        while let Some(chunk) = byte_stream.next().await {
            match chunk {
                Ok(bytes) => {
                    if stdout.write_all(&bytes).is_err() {
                        break;
                    }
                    let _ = stdout.flush();
                }
                Err(e) => {
                    eprintln!("Error: stream interrupted: {e}");
                    return EXIT_SERVER_ERROR;
                }
            }
        }
        EXIT_OK
    } else {
        match resp.text().await {
            Ok(text) => {
                print!("{text}");
                EXIT_OK
            }
            Err(e) => {
                eprintln!("Error: failed to read response: {e}");
                EXIT_SERVER_ERROR
            }
        }
    }
}

async fn cmd_capacity(client: &reqwest::Client, base_url: &str) -> u8 {
    let resp = match client
        .get(format!("{base_url}{ENDPOINT_CAPACITY}"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_node_conditions(client: &reqwest::Client, base_url: &str) -> u8 {
    let resp = match client
        .get(format!("{base_url}{ENDPOINT_NODE_CONDITIONS}"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_node_addresses(client: &reqwest::Client, base_url: &str) -> u8 {
    let resp = match client
        .get(format!("{base_url}{ENDPOINT_NODE_ADDRESSES}"))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => return connection_error(base_url, e),
    };
    handle_response(resp).await
}

async fn cmd_metrics(metrics_url: &str) -> u8 {
    if !metrics_url.starts_with(HTTP_SCHEME_PREFIX) {
        eprintln!("Error: metrics URL must use http:// (got {metrics_url})");
        return EXIT_VALIDATION;
    }
    let url = format!(
        "{}{}",
        metrics_url.trim_end_matches('/'),
        ENDPOINT_METRICS,
    );
    // Metrics endpoint is plain HTTP — use a simple client without TLS.
    let client = reqwest::Client::new();
    let resp = match client.get(&url).send().await {
        Ok(r) => r,
        Err(e) => return connection_error(metrics_url, e),
    };
    if !resp.status().is_success() {
        eprintln!("Error ({}): metrics request failed", resp.status());
        return EXIT_SERVER_ERROR;
    }
    match resp.text().await {
        Ok(text) => {
            print!("{text}");
            EXIT_OK
        }
        Err(e) => {
            eprintln!("Error: failed to read response: {e}");
            EXIT_SERVER_ERROR
        }
    }
}

fn connection_error(base_url: &str, err: reqwest::Error) -> u8 {
    if err.is_connect() {
        eprintln!("Error: cannot connect to {base_url}");
    } else {
        eprintln!("Error: {err}");
    }
    EXIT_CONNECTION
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    use pod_runner::logic::{
        CleanupConfig, DefaultExecutorProvider, PodLogic,
    };
    use pod_runner::pod::resolver::ResolverSet;
    use pod_runner::server::app::{AppState, create_router};
    use pod_runner::store::memory::MemoryStore;
    use pod_runner::store::PodStore;

    fn shell_cmd(cmd: &str) -> Vec<String> {
        if cfg!(windows) {
            vec!["cmd".into(), "/C".into(), cmd.into()]
        } else {
            vec!["sh".into(), "-c".into(), cmd.into()]
        }
    }

    /// Start a test HTTP server on a random port, return the base URL.
    async fn start_test_server() -> (String, tokio::sync::watch::Sender<()>) {
        let store = Arc::new(MemoryStore::new());
        let base = std::env::temp_dir().join(format!(
            "td-prc-test-{}",
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
        let cleanup = CleanupConfig {
            pods_dir: base.join("pods"),
            
            ..CleanupConfig::default()
        };
        let (shutdown_tx, _) = tokio::sync::watch::channel(());
        let logic = PodLogic::new(
            pod_runner::logic::LogicConfig::default(),
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

        (format!("http://{addr}"), shutdown_tx)
    }

    fn test_client() -> reqwest::Client {
        reqwest::Client::new()
    }

    fn write_pod_yaml(dir: &std::path::Path, name: &str, cmd: &str) -> String {
        let cmd_json = serde_json::to_string(&shell_cmd(cmd)).unwrap();
        let yaml = format!(
            r#"metadata:
  namespace: default
  name: {name}
  uid: uid-{name}
spec:
  containers:
    - name: main
      command: {cmd_json}
"#
        );
        let path = dir.join(format!("{name}.yaml"));
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(&path, &yaml).unwrap();
        path.to_string_lossy().to_string()
    }

    // -- create --

    #[tokio::test]
    async fn test_create_pod() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-create-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "app1", "exit 0");

        let code = cmd_create(&client, &url, &file).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_create_pod_duplicate() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-dup-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "dup", "sleep 60");

        let code = cmd_create(&client, &url, &file).await;
        assert_eq!(code, EXIT_OK);

        let code = cmd_create(&client, &url, &file).await;
        assert_eq!(code, EXIT_SERVER_ERROR);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_create_pod_missing_file() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_create(&client, &url, "/nonexistent/pod.yaml").await;
        assert_eq!(code, EXIT_VALIDATION);
    }

    #[tokio::test]
    async fn test_create_pod_auto_generates_uid() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-uid-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cmd_json = serde_json::to_string(&shell_cmd("exit 0")).unwrap();
        let yaml = format!(
            r#"metadata:
  namespace: default
  name: no-uid
spec:
  containers:
    - name: main
      command: {cmd_json}
"#
        );
        let path = dir.join("no-uid.yaml");
        std::fs::write(&path, &yaml).unwrap();

        let code = cmd_create(&client, &url, path.to_str().unwrap()).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_create_pod_invalid_yaml() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-badyaml-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.yaml");
        std::fs::write(&path, b"not: [valid: yaml: {{").unwrap();

        let code = cmd_create(&client, &url, path.to_str().unwrap()).await;
        assert_eq!(code, EXIT_VALIDATION);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- delete --

    #[tokio::test]
    async fn test_delete_pod() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-del-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "del-me", "exit 0");

        cmd_create(&client, &url, &file).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let code = cmd_delete(&client, &url, "del-me", DEFAULT_NAMESPACE).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_delete_not_found_is_ok() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_delete(&client, &url, "nope", DEFAULT_NAMESPACE).await;
        assert_eq!(code, EXIT_OK);
    }

    // -- get --

    #[tokio::test]
    async fn test_get_pod() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-get-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "get-me", "sleep 60");

        cmd_create(&client, &url, &file).await;

        let code = cmd_get(&client, &url, "get-me", DEFAULT_NAMESPACE).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_get_pod_not_found() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_get(&client, &url, "nope", DEFAULT_NAMESPACE).await;
        assert_eq!(code, EXIT_SERVER_ERROR);
    }

    // -- status --

    #[tokio::test]
    async fn test_status_pod() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-status-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "stat-me", "sleep 60");

        cmd_create(&client, &url, &file).await;

        let code = cmd_status(&client, &url, "stat-me", DEFAULT_NAMESPACE).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_status_pod_not_found() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_status(&client, &url, "nope", DEFAULT_NAMESPACE).await;
        assert_eq!(code, EXIT_SERVER_ERROR);
    }

    // -- list --

    #[tokio::test]
    async fn test_list_empty() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_list(&client, &url).await;
        assert_eq!(code, EXIT_OK);
    }

    #[tokio::test]
    async fn test_list_with_pods() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-list-{}", std::process::id()));

        let f1 = write_pod_yaml(&dir, "list-a", "sleep 60");
        let f2 = write_pod_yaml(&dir, "list-b", "sleep 60");
        cmd_create(&client, &url, &f1).await;
        cmd_create(&client, &url, &f2).await;

        let code = cmd_list(&client, &url).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- logs --

    #[tokio::test]
    async fn test_logs() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-logs-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "logger", "echo hello-log");

        cmd_create(&client, &url, &file).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let code = cmd_logs(
            &client, &url, "logger", DEFAULT_NAMESPACE, "main",
            false, None, false,
        ).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_logs_not_found() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_logs(
            &client, &url, "nope", DEFAULT_NAMESPACE, "main",
            false, None, false,
        ).await;
        assert_eq!(code, EXIT_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_logs_with_tail() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-tail-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "tailer", "echo line1; echo line2; echo line3");

        cmd_create(&client, &url, &file).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        let code = cmd_logs(
            &client, &url, "tailer", DEFAULT_NAMESPACE, "main",
            false, Some(1), false,
        ).await;
        assert_eq!(code, EXIT_OK);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn test_logs_with_previous() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();
        let dir = std::env::temp_dir().join(format!("td-prc-prev-{}", std::process::id()));
        let file = write_pod_yaml(&dir, "prev-pod", "echo previous-log");

        cmd_create(&client, &url, &file).await;
        tokio::time::sleep(Duration::from_secs(2)).await;

        // previous=true on a container that hasn't restarted may return
        // 404 or empty — we just verify the client handles it without panic
        let code = cmd_logs(
            &client, &url, "prev-pod", DEFAULT_NAMESPACE, "main",
            false, None, true,
        ).await;
        // Either OK (found previous log) or SERVER_ERROR (no previous log)
        assert!(code == EXIT_OK || code == EXIT_SERVER_ERROR);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // -- capacity --

    #[tokio::test]
    async fn test_capacity() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_capacity(&client, &url).await;
        assert_eq!(code, EXIT_OK);
    }

    // -- node-conditions --

    #[tokio::test]
    async fn test_node_conditions() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_node_conditions(&client, &url).await;
        assert_eq!(code, EXIT_OK);
    }

    // -- node-addresses --

    #[tokio::test]
    async fn test_node_addresses() {
        let (url, _tx) = start_test_server().await;
        let client = test_client();

        let code = cmd_node_addresses(&client, &url).await;
        assert_eq!(code, EXIT_OK);
    }

    // -- metrics --

    #[tokio::test]
    async fn test_metrics() {
        // Start a real metrics server
        let metrics = pod_runner::metrics::Metrics::new();
        let app = axum::Router::new()
            .route(ENDPOINT_METRICS, axum::routing::get({
                let metrics = Arc::clone(&metrics);
                move |_: ()| {
                    let metrics = Arc::clone(&metrics);
                    async move {
                        metrics.update_gauges();
                        let families = metrics.registry.gather();
                        let mut buf = Vec::new();
                        prometheus::Encoder::encode(
                            &prometheus::TextEncoder::new(),
                            &families,
                            &mut buf,
                        ).unwrap();
                        (
                            axum::http::StatusCode::OK,
                            [("content-type", "text/plain")],
                            buf,
                        )
                    }
                }
            }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let metrics_url = format!("http://{addr}");
        let code = cmd_metrics(&metrics_url).await;
        assert_eq!(code, EXIT_OK);
    }

    #[tokio::test]
    async fn test_metrics_rejects_https() {
        let code = cmd_metrics("https://localhost:3001").await;
        assert_eq!(code, EXIT_VALIDATION);
    }

    #[tokio::test]
    async fn test_metrics_connection_error() {
        let code = cmd_metrics("http://127.0.0.1:1").await;
        assert_eq!(code, EXIT_CONNECTION);
    }

    // -- connection error --

    #[tokio::test]
    async fn test_connection_error() {
        let client = test_client();

        let code = cmd_list(&client, "http://127.0.0.1:1").await;
        assert_eq!(code, EXIT_CONNECTION);
    }

    // -- build_client --

    #[test]
    fn test_build_client_no_config() {
        let client = build_client(&None);
        assert!(client.is_ok());
    }

    #[test]
    fn test_build_client_insecure_from_config() {
        let config = VKPRConfig {
            server: pod_runner::client::config::PrServerConfig {
                tls: pod_runner::client::config::PrServerTLSConfig {
                    insecure_skip_tls_verify: true,
                    ..Default::default()
                },
            },
            ..Default::default()
        };
        let client = build_client(&Some(config));
        assert!(client.is_ok());
    }

    #[test]
    fn test_build_client_with_ca_cert() {
        // Generate a self-signed CA cert to test the CA path
        let tmp = std::env::temp_dir().join(format!("td-prc-ca-{}", std::process::id()));
        std::fs::create_dir_all(&tmp).unwrap();
        let ca_path = tmp.join("ca.pem");

        // Create a minimal self-signed cert using rcgen
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        std::fs::write(&ca_path, cert.cert.pem()).unwrap();

        let config = VKPRConfig {
            server: pod_runner::client::config::PrServerConfig {
                tls: pod_runner::client::config::PrServerTLSConfig {
                    certificate_authority: ca_path.to_string_lossy().to_string(),
                    ..Default::default()
                },
            },
            ..Default::default()
        };
        let client = build_client(&Some(config));
        assert!(client.is_ok());

        let _ = std::fs::remove_dir_all(&tmp);
    }

    // -- CLI parsing --

    #[test]
    fn test_cli_parse_create() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "create", "-f", "pod.yaml",
        ]);
        assert!(matches!(cli.command, Command::Create { deployment } if deployment == "pod.yaml"));
    }

    #[test]
    fn test_cli_parse_delete() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "delete", "-n", "app", "-N", "prod",
        ]);
        assert!(matches!(
            cli.command,
            Command::Delete { ref name, ref namespace } if name == "app" && namespace == "prod"
        ));
    }

    #[test]
    fn test_cli_parse_logs_with_options() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "logs", "-n", "app", "-c", "worker", "--stream", "--tail", "100", "--previous",
        ]);
        assert!(matches!(
            cli.command,
            Command::Logs { ref name, ref container, stream, tail, previous, .. }
                if name == "app" && container == "worker" && stream && tail == Some(100) && previous
        ));
    }

    #[test]
    fn test_cli_parse_logs_defaults() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "logs", "-n", "app", "-c", "main",
        ]);
        assert!(matches!(
            cli.command,
            Command::Logs { ref container, stream, tail, previous, .. }
                if container == "main" && !stream && tail.is_none() && !previous
        ));
    }

    #[test]
    fn test_cli_parse_logs_requires_container() {
        let result = Cli::try_parse_from([
            "kpr", "--prconfig", "config.yaml",
            "logs", "-n", "app",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_parse_list() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml", "list",
        ]);
        assert!(matches!(cli.command, Command::List));
    }

    #[test]
    fn test_cli_parse_node_conditions() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml", "node-conditions",
        ]);
        assert!(matches!(cli.command, Command::NodeConditions));
    }

    #[test]
    fn test_cli_parse_node_addresses() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml", "node-addresses",
        ]);
        assert!(matches!(cli.command, Command::NodeAddresses));
    }

    #[test]
    fn test_cli_parse_prconfig() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "/path/to/config.yaml", "capacity",
        ]);
        assert_eq!(cli.prconfig, Some("/path/to/config.yaml".to_string()));
    }

    #[test]
    fn test_cli_default_namespace() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "get", "-n", "myapp",
        ]);
        assert!(matches!(
            cli.command,
            Command::Get { ref name, ref namespace }
                if name == "myapp" && namespace == DEFAULT_NAMESPACE
        ));
    }

    #[test]
    fn test_cli_parse_status() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "status", "-n", "myapp", "-N", "staging",
        ]);
        assert!(matches!(
            cli.command,
            Command::Status { ref name, ref namespace }
                if name == "myapp" && namespace == "staging"
        ));
    }

    #[test]
    fn test_cli_parse_capacity() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml", "capacity",
        ]);
        assert!(matches!(cli.command, Command::Capacity));
    }

    #[test]
    fn test_cli_parse_get_custom_namespace() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml",
            "get", "-n", "myapp", "-N", "prod",
        ]);
        assert!(matches!(
            cli.command,
            Command::Get { ref name, ref namespace }
                if name == "myapp" && namespace == "prod"
        ));
    }

    #[test]
    fn test_cli_parse_kubeconfig() {
        let cli = Cli::parse_from([
            "kpr", "--kubeconfig", "/etc/kube/config",
            "--prconfig", "config.yaml", "list",
        ]);
        assert_eq!(cli.kubeconfig, Some("/etc/kube/config".to_string()));
    }

    #[test]
    fn test_cli_parse_metrics() {
        let cli = Cli::parse_from([
            "kpr", "--prconfig", "config.yaml", "metrics",
        ]);
        assert!(matches!(cli.command, Command::Metrics));
    }

    #[test]
    fn test_cli_no_url_flag() {
        let result = Cli::try_parse_from([
            "kpr", "--url", "http://localhost:3000", "list",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_cli_no_insecure_flag() {
        let result = Cli::try_parse_from([
            "kpr", "--insecure-skip-tls-verify", "list",
        ]);
        assert!(result.is_err());
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> ExitCode {
    let mut cli = Cli::parse();

    // Resolve from env vars
    if cli.kubeconfig.is_none() {
        cli.kubeconfig = std::env::var(ENV_KUBECONFIG).ok();
    }
    if cli.prconfig.is_none() {
        cli.prconfig = std::env::var(ENV_PRCONFIG).ok();
    }

    // Load client config (required)
    let config = match &cli.prconfig {
        Some(path) => match VKPRConfig::from_file(&PathBuf::from(path)) {
            Ok(c) => Some(c),
            Err(e) => {
                eprintln!("Error loading prconfig: {e}");
                return ExitCode::from(EXIT_VALIDATION);
            }
        },
        None => None,
    };

    // Determine base URL from prconfig
    let base_url = match &config {
        Some(cfg) if !cfg.url.is_empty() => cfg.url.trim_end_matches('/').to_string(),
        _ => {
            eprintln!("Error: --prconfig or PRCONFIG env var is required.");
            return ExitCode::from(EXIT_VALIDATION);
        }
    };

    // Build HTTP client
    let client = match build_client(&config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Error: {e}");
            return ExitCode::from(EXIT_VALIDATION);
        }
    };

    // Dispatch command
    let code = match cli.command {
        Command::Create { ref deployment } => {
            cmd_create(&client, &base_url, deployment).await
        }
        Command::Delete {
            ref name,
            ref namespace,
        } => cmd_delete(&client, &base_url, name, namespace).await,
        Command::Get {
            ref name,
            ref namespace,
        } => cmd_get(&client, &base_url, name, namespace).await,
        Command::Status {
            ref name,
            ref namespace,
        } => cmd_status(&client, &base_url, name, namespace).await,
        Command::List => cmd_list(&client, &base_url).await,
        Command::Logs {
            ref name,
            ref namespace,
            ref container,
            stream,
            tail,
            previous,
        } => {
            cmd_logs(
                &client,
                &base_url,
                name,
                namespace,
                container,
                stream,
                tail,
                previous,
            )
            .await
        }
        Command::Capacity => cmd_capacity(&client, &base_url).await,
        Command::NodeConditions => cmd_node_conditions(&client, &base_url).await,
        Command::NodeAddresses => cmd_node_addresses(&client, &base_url).await,
        Command::Metrics => {
            let metrics_url = config
                .as_ref()
                .map(|c| c.metrics_url.as_str())
                .unwrap_or("");
            if metrics_url.is_empty() {
                eprintln!("Error: metricsUrl not configured in prconfig.");
                return ExitCode::from(EXIT_VALIDATION);
            }
            cmd_metrics(metrics_url).await
        }
    };

    ExitCode::from(code)
}
