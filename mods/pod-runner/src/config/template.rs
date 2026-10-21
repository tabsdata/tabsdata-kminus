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

defaultGracePeriodSecs: 120
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
# pod's data dir (data/pod-run/<pod_id>/: `work/`, `volumes/`) — the bulk of
# the footprint — and run from pod completion. The delete TTLs cover the
# state dir of each pod incarnation (data/pod-state/<bucket>/<pod_name>/
# <creation_time>/: the logs and specs that are its only record), and run
# from the moment the pod is removed from the pod runner, since until then
# the runner still answers queries about it. Both clocks survive a pod
# runner restart.
#
# A value is `<number><unit>` with unit `s`, `m` or `h`, or `never` to keep
# the files indefinitely. Each can be overridden per pod with the matching
# annotation: prPodDataReapTtlOnPodSuccess, prPodDataReapTtlOnPodFailure,
# prPodDirDeleteTtlPodSuccessOnPodDelete,
# prPodDirDeleteTtlPodFailureOnPodDelete.

podDataReapTtlOnPodSuccess: "0s"
podDataReapTtlOnPodFailure: "1h"
podDirDeleteTtlPodSuccessOnPodDelete: "1h"
podDirDeleteTtlPodFailureOnPodDelete: "24h"

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
#
# The --metrics-port CLI flag, when passed, overrides the port set here.

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
            DEFAULT_POD_DATA_REAP_TTL_ON_POD_FAILURE, DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS,
            DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE,
            DEFAULT_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE,
        };
        let c = YamlSysConfig::from_str(PRSYSCONFIG_TEMPLATE).unwrap();
        assert_eq!(
            c.pod_data_reap_ttl_on_pod_success,
            DEFAULT_POD_DATA_REAP_TTL_ON_POD_SUCCESS
        );
        assert_eq!(
            c.pod_data_reap_ttl_on_pod_failure,
            DEFAULT_POD_DATA_REAP_TTL_ON_POD_FAILURE
        );
        assert_eq!(
            c.pod_dir_delete_ttl_pod_success_on_pod_delete,
            DEFAULT_POD_DIR_DELETE_TTL_POD_SUCCESS_ON_POD_DELETE
        );
        assert_eq!(
            c.pod_dir_delete_ttl_pod_failure_on_pod_delete,
            DEFAULT_POD_DIR_DELETE_TTL_POD_FAILURE_ON_POD_DELETE
        );
    }

    #[test]
    fn syscfg_template_cleanup_ttls_parse() {
        use crate::logic::ttl::TtlSpec;
        let c = YamlSysConfig::from_str(PRSYSCONFIG_TEMPLATE).unwrap();
        for value in [
            &c.pod_data_reap_ttl_on_pod_success,
            &c.pod_data_reap_ttl_on_pod_failure,
            &c.pod_dir_delete_ttl_pod_success_on_pod_delete,
            &c.pod_dir_delete_ttl_pod_failure_on_pod_delete,
        ] {
            assert!(TtlSpec::parse(value).is_ok(), "template TTL {value:?}");
        }
    }
}
