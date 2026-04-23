// Copyright 2026 Tabsdata Inc.

//! Default configuration file templates.

/// File name for the server endpoint + TLS config.
pub const PRCONFIG_FILENAME: &str = "prconfig.yaml";

/// File name for the system config (persistent, user-editable).
pub const PRSYSCONFIG_FILENAME: &str = "prsysconfig.yaml";

/// File name for the client (VK) config.
pub const VK_PRCONFIG_FILENAME: &str = "vk-prconfig.yaml";

/// Default `prsysconfig.yaml` template with descriptive comments.
pub const PRSYSCONFIG_TEMPLATE: &str = r#"# =============================================================================
# Pod Runner System Configuration (prsysconfig.yaml)
# =============================================================================
#
# Persistent, user-editable system settings for the Pod Runner.
# This file is NOT regenerated on restart — edit freely.
# =============================================================================

# ---------------------------------------------------------------------------
# Node identity
# ---------------------------------------------------------------------------

nodename: "tdknode"
namespace: "default"

# ---------------------------------------------------------------------------
# Pod limits
# ---------------------------------------------------------------------------

defaultGracePeriodSecs: 30
maxRestartCount: 5
restartBackoffBaseSecs: 10
cleanUpForceKillWaitSecs: 10

# ---------------------------------------------------------------------------
# Executor
# ---------------------------------------------------------------------------

propagateEnv: true
dieWithParent: true

# ---------------------------------------------------------------------------
# Security
# ---------------------------------------------------------------------------

writeSecretsToContainerShellScript: false

# ---------------------------------------------------------------------------
# Cleanup and archival
# ---------------------------------------------------------------------------

keepDeletedPodDir: false
keepFailedPodDir: true
deletedPodDirTtlHours: 24
podArchiveTtlDays: 7

# ---------------------------------------------------------------------------
# Node capacity
# ---------------------------------------------------------------------------

capacity:
  cpu: 100
  memoryGB: 100
  pods: 100

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

metricsUrl: "http://127.0.0.1:3001"

# ---------------------------------------------------------------------------
# Resolvers
# ---------------------------------------------------------------------------

secretResolver:
  type: "kube"
  configs: {}

configMapResolver:
  type: "kube"
  configs: {}

nodeEnvsResolver:
  type: "none"
  configs: {}
"#;

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::yaml_config::YamlSysConfig;
    use crate::server::config::*;

    #[test]
    fn syscfg_template_is_valid_yaml() {
        let c = YamlSysConfig::from_str(PRSYSCONFIG_TEMPLATE);
        assert!(c.is_ok(), "template parse error: {:?}", c.err());
    }

    #[test]
    fn syscfg_template_has_expected_defaults() {
        let c = YamlSysConfig::from_str(PRSYSCONFIG_TEMPLATE).unwrap();
        assert_eq!(c.nodename, DEFAULT_NODENAME);
        assert_eq!(c.namespace, DEFAULT_NAMESPACE);
        assert_eq!(c.capacity.pods, 100);
        assert!(c.propagate_env);
        assert!(c.die_with_parent);
        assert!(c.keep_failed_pod_dir);
    }
}
