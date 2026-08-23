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

fn outcome<S: AsRef<str>>(ports: &[(u16, S)]) -> Result<(), Vec<String>> {
    let failures = port_failures(&check_ports(ports));
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures)
    }
}

#[test]
fn port_in_use_is_none_when_free() {
    retry_until_ok(|| match port_in_use(find_free_port()) {
        None => Ok(()),
        Some(e) => Err(vec![e]),
    });
}

#[test]
fn port_in_use_reports_a_bound_port() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    assert!(port_in_use(port).is_some());
}

// Every port is reported, free ones included: the caller prints the whole
// list, so a missing entry would read as a port that was never checked.
#[test]
fn check_ports_returns_one_result_per_port_in_order() {
    let busy = TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = busy.local_addr().unwrap().port();
    let free = find_free_port();

    let checks = check_ports(&[(free, "first"), (taken, "second")]);

    assert_eq!(checks.len(), 2);
    assert_eq!(checks[0].port, free);
    assert_eq!(checks[0].label, "first");
    assert_eq!(checks[1].port, taken);
    assert_eq!(checks[1].label, "second");
    assert!(!checks[1].is_free());
}

#[test]
fn check_ports_succeeds_when_all_free() {
    retry_until_ok(|| {
        let ports = [
            (find_free_port(), "a"),
            (find_free_port(), "b"),
            (find_free_port(), "c"),
        ];
        outcome(&ports)
    });
}

// The check does not stop at the first conflict.
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

    let errors = outcome(&ports).unwrap_err();
    assert_eq!(errors.len(), 2);
    assert!(errors[0].contains("first"));
    assert!(errors[0].contains(&port_a.to_string()));
    assert!(errors[1].contains("second"));
    assert!(errors[1].contains(&port_b.to_string()));
}

// Binding each port on its own cannot see this: both would test free, and
// only the first component to start would get the port.
#[test]
fn check_ports_reports_a_port_assigned_twice() {
    retry_until_ok(|| {
        let shared = find_free_port();
        let checks = check_ports(&[(shared, "first"), (shared, "second")]);
        if !checks[0].is_free() {
            // The port was taken from under us; retry rather than assert.
            return Err(port_failures(&checks));
        }
        let second = checks[1].message().expect("the duplicate must be reported");
        assert!(second.contains("second"), "{second}");
        assert!(second.contains("first"), "{second}");
        assert!(second.contains(&shared.to_string()), "{second}");
        Ok(())
    });
}

// The first holder keeps its name in the duplicate report even when the same
// port appears a third time.
#[test]
fn check_ports_blames_the_first_holder_of_a_repeated_port() {
    retry_until_ok(|| {
        let shared = find_free_port();
        let checks = check_ports(&[(shared, "first"), (shared, "second"), (shared, "third")]);
        if !checks[0].is_free() {
            return Err(port_failures(&checks));
        }
        for c in &checks[1..] {
            let msg = c.message().expect("every repeat must be reported");
            assert!(msg.contains("first"), "{msg}");
        }
        Ok(())
    });
}

#[test]
fn check_ports_succeeds_when_empty() {
    let ports: [(u16, &str); 0] = [];
    assert!(check_ports(&ports).is_empty());
    assert!(port_failures(&check_ports(&ports)).is_empty());
}

#[test]
fn check_ports_accepts_owned_labels() {
    // Asserted against a held port rather than a free one: proving a port is
    // free races with every other test, while a bound one cannot.
    let busy = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = busy.local_addr().unwrap().port();

    let errors = outcome(&[(port, String::from("owned"))]).unwrap_err();
    assert!(errors[0].contains("owned"));
}

#[test]
fn a_free_port_has_no_message() {
    let check = PortCheck {
        port: 1234,
        label: "thing".into(),
        reason: None,
    };
    assert!(check.is_free());
    assert!(check.message().is_none());
}

// -- probe_ports -------------------------------------------------------------

// The plain bind test: one result per pair, no cross-checking, so a port
// listed twice reports its real state both times rather than the second
// being called a duplicate. Confirming a port was released depends on this.
#[test]
fn probe_ports_does_not_treat_a_repeat_as_a_duplicate() {
    retry_until_ok(|| {
        let shared = find_free_port();
        let probed = probe_ports(&[(shared, "first"), (shared, "second")]);
        if !probed[0].is_free() {
            return Err(port_failures(&probed));
        }
        assert!(
            probed[1].is_free(),
            "probe_ports flagged the repeat: {:?}",
            probed[1].reason
        );

        // check_ports, by contrast, is meant to catch exactly that.
        let checked = check_ports(&[(shared, "first"), (shared, "second")]);
        assert!(!checked[1].is_free(), "check_ports missed the duplicate");
        Ok(())
    });
}

#[test]
fn probe_ports_reports_a_held_port() {
    let busy = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = busy.local_addr().unwrap().port();

    let probed = probe_ports(&[(port, "held")]);

    assert_eq!(probed.len(), 1);
    assert!(!probed[0].is_free());
    assert_eq!(probed[0].label, "held");
}

#[test]
fn probe_ports_keeps_order_and_labels() {
    let busy = TcpListener::bind("127.0.0.1:0").unwrap();
    let taken = busy.local_addr().unwrap().port();
    let free = find_free_port();

    let probed = probe_ports(&[(taken, "one"), (free, "two")]);

    assert_eq!(probed[0].port, taken);
    assert_eq!(probed[0].label, "one");
    assert_eq!(probed[1].port, free);
    assert_eq!(probed[1].label, "two");
}
