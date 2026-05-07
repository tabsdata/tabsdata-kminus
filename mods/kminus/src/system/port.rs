// Copyright 2026 Tabsdata Inc.

//! TCP port helpers: free-port allocation and availability checks.

use std::net::TcpListener;

/// Bind to an ephemeral port and return the port number.
/// Used to allocate a guaranteed-free port at startup.
pub fn find_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind ephemeral port")
        .local_addr()
        .unwrap()
        .port()
}

/// Verify that `port` is available on 127.0.0.1. Returns an error
/// message if the port is already in use.
pub fn check_port(port: &str, component: &str) -> Result<(), String> {
    let addr = format!("127.0.0.1:{}", port);
    match TcpListener::bind(&addr) {
        Ok(_) => Ok(()),
        Err(e) => Err(format!(
            "{} port {} is already in use: {}",
            component, port, e
        )),
    }
}

#[cfg(test)]
mod tests;
