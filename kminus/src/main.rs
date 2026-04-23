// Copyright 2026 Tabsdata Inc.

mod config;
mod control_plane;
mod pod_runner;
mod utils;
mod virtual_kubelet;

fn main() {
    println!("km {}", env!("CARGO_PKG_VERSION"));
}
