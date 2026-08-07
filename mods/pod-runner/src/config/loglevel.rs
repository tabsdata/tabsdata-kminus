// Copyright 2026 Tabsdata Inc.

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// Third-party crates whose debug/info chatter (HTTP plumbing, TLS
/// handshakes, buffer workers) is only useful when tracing the wire
/// level. Below `trace` they are capped at `warn` so genuine problems
/// still surface.
const NOISY_TARGETS: &[&str] = &[
    "tower",
    "hyper",
    "hyper_util",
    "h2",
    "rustls",
    "tokio_rustls",
    "kube",
    "kube_client",
    "kube_runtime",
    "reqwest",
];

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, ValueEnum, Display, EnumString, Serialize, Deserialize,
)]
#[strum(serialize_all = "lowercase")]
#[serde(rename_all = "lowercase")]
#[value(rename_all = "lower")]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl From<LogLevel> for LevelFilter {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => LevelFilter::TRACE,
            LogLevel::Debug => LevelFilter::DEBUG,
            LogLevel::Info => LevelFilter::INFO,
            LogLevel::Warn => LevelFilter::WARN,
            LogLevel::Error => LevelFilter::ERROR,
        }
    }
}

impl LogLevel {
    /// Build the tracing filter for this level.
    ///
    /// The level applies to pod-runner's own targets. Third-party
    /// [`NOISY_TARGETS`] are capped at `warn` unless the level is
    /// `trace`, where everything is let through.
    pub fn env_filter(self) -> EnvFilter {
        let mut filter = EnvFilter::default().add_directive(LevelFilter::from(self).into());
        if self != LogLevel::Trace {
            for target in NOISY_TARGETS {
                filter = filter.add_directive(
                    format!("{target}=warn")
                        .parse()
                        .expect("static directive must parse"),
                );
            }
        }
        filter
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_filter_caps_noisy_targets_below_trace() {
        let filter = LogLevel::Debug.env_filter().to_string();
        assert!(filter.contains("debug"));
        for target in NOISY_TARGETS {
            assert!(
                filter.contains(&format!("{target}=warn")),
                "missing cap for {target} in: {filter}"
            );
        }
    }

    #[test]
    fn env_filter_trace_has_no_caps() {
        let filter = LogLevel::Trace.env_filter().to_string();
        assert!(filter.contains("trace"));
        assert!(!filter.contains("=warn"), "unexpected caps in: {filter}");
    }
}
