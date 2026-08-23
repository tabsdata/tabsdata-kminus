// Copyright 2026 Tabsdata Inc.

//! TCP port helpers: free-port allocation and availability checks.

use std::collections::HashMap;
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

/// Why `port` cannot be bound on 127.0.0.1, or `None` when it is free.
pub fn port_in_use(port: u16) -> Option<String> {
    TcpListener::bind(format!("127.0.0.1:{port}"))
        .err()
        .map(|e| e.to_string())
}

/// What checking one port found.
#[derive(Debug, PartialEq, Eq)]
pub struct PortCheck {
    pub port: u16,
    /// The component the port belongs to, as shown to the operator.
    pub label: String,
    /// Why the port is unusable, or `None` when it is free.
    pub reason: Option<String>,
}

impl PortCheck {
    pub fn is_free(&self) -> bool {
        self.reason.is_none()
    }

    /// The full sentence for an error list or a log line.
    pub fn message(&self) -> Option<String> {
        self.reason
            .as_ref()
            .map(|reason| format!("{} port {} is unusable: {}", self.label, self.port, reason))
    }
}

/// Check every `(port, label)` pair on 127.0.0.1, returning one result per
/// pair in the order given.
///
/// Every port is checked even once one has failed. An operator clearing a
/// port clash wants the whole list in one go, not the first conflict
/// followed by another run to find the next.
///
/// Two limits are worth stating, because the caller reports these results as
/// if they were facts about the future:
///
/// * The check binds each port and releases it again, so it is a snapshot.
///   Anything may claim a port between the check and the component's own
///   bind; the components fail on their own bind when that happens.
/// * Because each port is bound on its own, binding cannot reveal the same
///   port assigned to two components — both would test free while only the
///   first to start gets it. That case is a configuration error rather than
///   a race, so it is detected here by name instead.
pub fn check_ports<S: AsRef<str>>(ports: &[(u16, S)]) -> Vec<PortCheck> {
    let mut first_use: HashMap<u16, String> = HashMap::new();
    let mut checks = Vec::with_capacity(ports.len());

    for (port, label) in ports {
        let label = label.as_ref().to_string();
        let reason = match first_use.get(port) {
            Some(owner) => Some(format!("also assigned to {owner}")),
            None => port_in_use(*port),
        };
        first_use.entry(*port).or_insert_with(|| label.clone());
        checks.push(PortCheck {
            port: *port,
            label,
            reason,
        });
    }
    checks
}

/// The explanations for every port that is not usable, in the order checked.
pub fn port_failures(checks: &[PortCheck]) -> Vec<String> {
    checks.iter().filter_map(|c| c.message()).collect()
}

#[cfg(test)]
mod tests;
