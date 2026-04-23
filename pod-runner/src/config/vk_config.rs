// Copyright 2026 Tabsdata Inc.

//! VK client configuration file generation.
//!
//! Writes `vk-prconfig.yaml` — the client-side config used by `kpr`
//! and `td_vk` to connect to the pod runner server. Regenerated on
//! every startup to reflect the current server configuration.

use std::fs;
use std::path::Path;

use super::template::VK_PRCONFIG_FILENAME;

/// Write (or overwrite) `vk-prconfig.yaml` in the given directory.
pub fn write_vk_prconfig(
    configdir: &Path,
    url: &str,
    nodename: &str,
    namespace: &str,
    ca_cert_b64: &str,
    client_cert_b64: &str,
    client_key_b64: &str,
    metrics_url: &str,
) -> Result<(), String> {
    let path = configdir.join(VK_PRCONFIG_FILENAME);
    let content = format!(
        r#"# Auto-generated — regenerated on every start. Do not edit.

url: "{url}"

nodeName: "{nodename}"
namespace: "{namespace}"

server:
  tls:
    certificate-authority-data: {ca_cert_b64}

client:
  id: "vk"
  certificate-data: {client_cert_b64}
  certificate-key-data: {client_key_b64}

metricsUrl: "{metrics_url}"
"#
    );
    fs::write(&path, &content)
        .map_err(|e| format!("Failed to write {}: {e}", path.display()))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_vk_prconfig_creates_file() {
        let tmp = std::env::temp_dir().join(format!(
            "pr-vk-test-{}",
            std::process::id(),
        ));
        std::fs::create_dir_all(&tmp).unwrap();

        write_vk_prconfig(
            &tmp,
            "https://10.0.0.1:10300",
            "my-node",
            "prod",
            "ca-b64",
            "client-cert-b64",
            "client-key-b64",
            "http://10.0.0.1:3001",
        )
        .unwrap();

        let path = tmp.join(VK_PRCONFIG_FILENAME);
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("https://10.0.0.1:10300"));
        assert!(content.contains("my-node"));
        assert!(content.contains("prod"));
        assert!(content.contains("ca-b64"));
        assert!(content.contains("client-cert-b64"));
        assert!(content.contains("client-key-b64"));
        assert!(content.contains("http://10.0.0.1:3001"));

        let _ = std::fs::remove_dir_all(&tmp);
    }
}
