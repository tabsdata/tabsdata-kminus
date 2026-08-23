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

/// Verify that every `(port, component)` pair is free on 127.0.0.1.
/// Returns one error message per port in use, so the caller can report
/// all conflicts at once instead of only the first.
pub fn check_ports<S: AsRef<str>>(ports: &[(u16, S)]) -> Result<(), Vec<String>> {
    let errors: Vec<String> = ports
        .iter()
        .filter_map(|(port, component)| check_port(&port.to_string(), component.as_ref()).err())
        .collect();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors)
    }
}

#[cfg(test)]
mod tests;
