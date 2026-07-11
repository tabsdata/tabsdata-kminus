// Copyright 2026 Tabsdata Inc.

use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use strum::{Display, EnumString};
use tracing_subscriber::filter::LevelFilter;

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
