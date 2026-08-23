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

// `find_free_port` releases the port before it is checked again, so another
// test can claim it in between. Retry before believing a failure: a real
// regression fails every attempt.
fn retry_until_ok(mut attempt: impl FnMut() -> Result<(), Vec<String>>) {
    let mut last = None;
    for _ in 0..5 {
        match attempt() {
            Ok(()) => return,
            Err(e) => last = Some(e),
        }
    }
    panic!("never observed a free port: {last:?}");
}

#[test]
fn check_port_succeeds_when_free() {
    retry_until_ok(|| check_port(&find_free_port().to_string(), "test").map_err(|e| vec![e]));
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

#[test]
fn check_ports_succeeds_when_all_free() {
    retry_until_ok(|| {
        let ports = [
            (find_free_port(), "a"),
            (find_free_port(), "b"),
            (find_free_port(), "c"),
        ];
        check_ports(&ports)
    });
}

#[test]
fn check_ports_reports_every_port_in_use() {
    let busy_a = TcpListener::bind("127.0.0.1:0").unwrap();
    let busy_b = TcpListener::bind("127.0.0.1:0").unwrap();
    let port_a = busy_a.local_addr().unwrap().port();
    let port_b = busy_b.local_addr().unwrap().port();
    let ports = [
        (port_a, "first"),
        (find_free_port(), "free"),
        (port_b, "second"),
    ];

    let errors = check_ports(&ports).unwrap_err();
    assert_eq!(errors.len(), 2);
    assert!(errors[0].contains("first"));
    assert!(errors[0].contains(&port_a.to_string()));
    assert!(errors[1].contains("second"));
    assert!(errors[1].contains(&port_b.to_string()));
}

#[test]
fn check_ports_succeeds_when_empty() {
    let ports: [(u16, &str); 0] = [];
    assert!(check_ports(&ports).is_ok());
}

#[test]
fn check_ports_accepts_owned_labels() {
    // Asserted against a held port rather than a free one: proving a port is
    // free races with every other test, while a bound one cannot.
    let busy = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = busy.local_addr().unwrap().port();

    let errors = check_ports(&vec![(port, String::from("owned"))]).unwrap_err();
    assert!(errors[0].contains("owned"));
}
