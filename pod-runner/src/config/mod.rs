// Copyright 2026 Tabsdata Inc.

//! Configuration loading, CLI argument parsing, and PKI generation.
//!
//! This module handles the full startup configuration pipeline:
//! 1. Parse CLI arguments ([`cli::CliArgs`])
//! 2. Load or generate `prconfig.yaml` ([`yaml_config::YamlPRConfig`])
//! 3. Auto-generate PKI certs if needed ([`pki`])
//! 4. Resolve final configuration ([`resolve::resolve_config`])
//! 5. Regenerate `vk-prconfig.yaml` ([`vk_config`])

pub mod cli;
pub mod pki;
pub mod resolve;
pub mod template;
pub mod vk_config;
pub mod yaml_config;
