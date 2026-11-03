// Copyright 2026 Tabsdata Inc.

use std::convert::Infallible;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use http_body_util::Full;

pub(crate) fn status_body(code: u16, message: &str) -> (u16, String) {
    let body = serde_json::json!({
        "kind": "Status",
        "apiVersion": "v1",
        "status": "Failure",
        "message": message,
        "code": code,
    });
    (code, body.to_string())
}

pub(crate) fn ok_body(object: serde_json::Value) -> (u16, String) {
    (200, object.to_string())
}

pub(crate) fn scripted_client(responses: Vec<(u16, String)>) -> (kube::Client, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let script = Arc::new(Mutex::new(responses.into_iter()));
    let counter = Arc::clone(&calls);
    let service = tower::service_fn(move |_: hyper::Request<kube::client::Body>| {
        counter.fetch_add(1, Ordering::SeqCst);
        let (code, body) = script
            .lock()
            .unwrap()
            .next()
            .expect("the kube API was called more often than scripted");
        async move {
            Ok::<_, Infallible>(
                hyper::Response::builder()
                    .status(code)
                    .header("content-type", "application/json")
                    .body(Full::new(Bytes::from(body)))
                    .unwrap(),
            )
        }
    });
    (kube::Client::new(service, "default"), calls)
}
