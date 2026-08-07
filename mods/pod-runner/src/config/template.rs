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
# Images
# ---------------------------------------------------------------------------
#
# Root directory used to resolve relative container `image` paths from pod
# specs. Absolute `image` paths are used as-is, regardless of this setting.
# Supported `image` forms are: a directory tree, or a `.tgz` / `.tar.gz`
# archive. Leave empty to require fully-qualified `image` paths in pod
# specs.

imagesDir: ""

# ---------------------------------------------------------------------------
# Cleanup TTLs
# ---------------------------------------------------------------------------
#
# Two clocks govern a finished pod's disk footprint. The reap TTLs cover the
# data dirs (`work/`, `volumes/`) — the bulk of the footprint — and run from
# pod completion. The delete TTLs cover the whole pod dir, including the
# logs and specs that are the pod's only record, and run from the moment the
# pod is removed from the pod runner, since until then the runner still
# answers queries about it.
#
# A value is `<number><unit>` with unit `s`, `m` or `h`, or `never` to keep
# the files indefinitely. Each can be overridden per pod with the matching
# annotation: prPodReapTtlOnSuccess, prPodReapTtlOnError,
# prPodDeleteTtlOnSuccess, prPodDeleteTtlOnError.

podReapTtlOnSuccess: "0s"
podReapTtlOnError: "1h"
podDeleteTtlOnSuccess: "1h"
podDeleteTtlOnError: "24h"

# ---------------------------------------------------------------------------
# Node capacity
# ---------------------------------------------------------------------------

capacity:
  cpu: 100
  memoryGB: 100
  pods: 250

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
        assert_eq!(c.capacity.pods, 250);
        assert!(c.propagate_env);
        assert!(c.die_with_parent);
    }

    #[test]
    fn syscfg_template_cleanup_ttls_match_the_defaults() {
        use crate::spec::constants::{
            DEFAULT_POD_DELETE_TTL_ON_ERROR, DEFAULT_POD_DELETE_TTL_ON_SUCCESS,
            DEFAULT_POD_REAP_TTL_ON_ERROR, DEFAULT_POD_REAP_TTL_ON_SUCCESS,
        };
        let c = YamlSysConfig::from_str(PRSYSCONFIG_TEMPLATE).unwrap();
        assert_eq!(c.pod_reap_ttl_on_success, DEFAULT_POD_REAP_TTL_ON_SUCCESS);
        assert_eq!(c.pod_reap_ttl_on_error, DEFAULT_POD_REAP_TTL_ON_ERROR);
        assert_eq!(
            c.pod_delete_ttl_on_success,
            DEFAULT_POD_DELETE_TTL_ON_SUCCESS
        );
        assert_eq!(c.pod_delete_ttl_on_error, DEFAULT_POD_DELETE_TTL_ON_ERROR);
    }

    #[test]
    fn syscfg_template_cleanup_ttls_parse() {
        use crate::logic::ttl::TtlSpec;
        let c = YamlSysConfig::from_str(PRSYSCONFIG_TEMPLATE).unwrap();
        for value in [
            &c.pod_reap_ttl_on_success,
            &c.pod_reap_ttl_on_error,
            &c.pod_delete_ttl_on_success,
            &c.pod_delete_ttl_on_error,
        ] {
            assert!(TtlSpec::parse(value).is_ok(), "template TTL {value:?}");
        }
    }
}
