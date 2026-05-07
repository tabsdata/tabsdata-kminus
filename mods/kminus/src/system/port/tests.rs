// Copyright 2026 Tabsdata Inc.

use std::collections::HashSet;
use std::net::TcpListener;

use super::*;

#[test]
fn find_free_port_returns_distinct_ports() {
    let mut seen = HashSet::new();
    for _ in 0..10 {
        seen.insert(find_free_port());
    }
    // Even on a quiet machine, 10 calls in a tight loop should
    // yield several distinct ports.
    assert!(seen.len() > 1);
}

#[test]
fn check_port_succeeds_when_free() {
    let port = find_free_port();
    assert!(check_port(&port.to_string(), "test").is_ok());
}

#[test]
fn check_port_fails_when_bound() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let res = check_port(&port.to_string(), "test");
    assert!(res.is_err());
    let msg = res.unwrap_err();
    assert!(msg.contains("test"));
    assert!(msg.contains(&port.to_string()));
}
