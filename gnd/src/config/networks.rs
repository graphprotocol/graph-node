//! Network configuration parsing for subgraph deployments.
//!
//! This module handles parsing and applying networks.json configuration files,
//! which allow deploying the same subgraph to different networks with
//! network-specific contract addresses and start blocks.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};

/// Network-specific configuration for a data source.
///
/// Contains the contract address and optional start block for a specific
/// data source on a specific network.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSourceNetworkConfig {
    /// Contract address (with or without 0x prefix)
    pub address: Option<String>,

    /// Block number to start indexing from
    #[serde(rename = "startBlock")]
    pub start_block: Option<u64>,
}

/// Configuration for a specific network.
///
/// Maps data source names to their network-specific configuration.
pub type NetworkConfig = HashMap<String, DataSourceNetworkConfig>;

/// Complete networks configuration file.
///
/// Maps network names to their configuration.
pub type NetworksConfig = HashMap<String, NetworkConfig>;

/// Load a networks.json configuration file.
///
/// # Arguments
/// * `path` - Path to the networks.json file
///
/// # Returns
/// The parsed networks configuration, or an error if the file doesn't exist
/// or is malformed.
pub fn load_networks_config(path: &Path) -> Result<NetworksConfig> {
    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read networks config from {}", path.display()))?;

    serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse networks config from {}", path.display()))
}

/// Get configuration for a specific network.
///
/// # Arguments
/// * `config` - The full networks configuration
/// * `network` - The network name to look up
///
/// # Returns
/// The network configuration, or an error if the network is not found.
pub fn get_network_config<'a>(
    config: &'a NetworksConfig,
    network: &str,
) -> Result<&'a NetworkConfig> {
    config
        .get(network)
        .ok_or_else(|| anyhow!("Network '{}' was not found in networks config", network))
}

/// Apply network configuration to a manifest YAML.
///
/// Updates data source addresses and start blocks based on the network configuration.
///
/// # Arguments
/// * `manifest` - Mutable reference to the parsed manifest YAML
/// * `network` - The target network name
/// * `network_config` - Configuration for the target network
///
/// # Returns
/// Ok(()) on success, or an error if a data source is missing from the config.
pub fn apply_network_config(
    manifest: &mut serde_yaml::Value,
    network: &str,
    network_config: &NetworkConfig,
) -> Result<()> {
    // Update data sources
    if let Some(data_sources) = manifest.get_mut("dataSources") {
        if let Some(arr) = data_sources.as_sequence_mut() {
            for ds in arr.iter_mut() {
                update_data_source(ds, network, network_config)?;
            }
        }
    }

    // Update templates to use the same network
    if let Some(templates) = manifest.get_mut("templates") {
        if let Some(arr) = templates.as_sequence_mut() {
            for template in arr.iter_mut() {
                if let Some(mapping) = template.as_mapping_mut() {
                    mapping.insert(
                        serde_yaml::Value::String("network".to_string()),
                        serde_yaml::Value::String(network.to_string()),
                    );
                }
            }
        }
    }

    Ok(())
}

/// Update a single data source with network configuration.
fn update_data_source(
    ds: &mut serde_yaml::Value,
    network: &str,
    network_config: &NetworkConfig,
) -> Result<()> {
    // Get data source name
    let ds_name = ds
        .get("name")
        .and_then(|n| n.as_str())
        .ok_or_else(|| anyhow!("Data source missing 'name' field"))?
        .to_string();

    // Look up config for this data source
    let ds_config = network_config.get(&ds_name).ok_or_else(|| {
        anyhow!(
            "'{}' was not found in the '{}' network configuration",
            ds_name,
            network
        )
    })?;

    // Update network field
    if let Some(mapping) = ds.as_mapping_mut() {
        mapping.insert(
            serde_yaml::Value::String("network".to_string()),
            serde_yaml::Value::String(network.to_string()),
        );
    }

    // Update source section
    if let Some(source) = ds.get_mut("source") {
        // Preserve existing ABI reference if present
        let abi = source.get("abi").cloned();

        // Clear and rebuild source
        if let Some(mapping) = source.as_mapping_mut() {
            mapping.clear();

            // Restore ABI if it existed
            if let Some(abi_value) = abi {
                mapping.insert(serde_yaml::Value::String("abi".to_string()), abi_value);
            }

            // Add address if configured
            if let Some(address) = &ds_config.address {
                mapping.insert(
                    serde_yaml::Value::String("address".to_string()),
                    serde_yaml::Value::String(address.clone()),
                );
            }

            // Add start block if configured
            if let Some(start_block) = ds_config.start_block {
                mapping.insert(
                    serde_yaml::Value::String("startBlock".to_string()),
                    serde_yaml::Value::Number(start_block.into()),
                );
            }
        }
    }

    Ok(())
}

/// Initialize a networks.json file from an existing manifest.
///
/// Creates a networks.json with the current network configuration
/// extracted from the manifest's data sources.
///
/// # Arguments
/// * `manifest_path` - Path to the subgraph.yaml manifest
/// * `output_path` - Path to write the networks.json file
pub fn init_networks_config(manifest_path: &Path, output_path: &Path) -> Result<()> {
    let content = fs::read_to_string(manifest_path)
        .with_context(|| format!("Failed to read manifest from {}", manifest_path.display()))?;

    let manifest: serde_yaml::Value = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse manifest from {}", manifest_path.display()))?;

    let mut networks_config: NetworksConfig = HashMap::new();

    // Extract configuration from data sources
    if let Some(data_sources) = manifest.get("dataSources").and_then(|ds| ds.as_sequence()) {
        for ds in data_sources {
            let name = ds.get("name").and_then(|n| n.as_str()).unwrap_or("unknown");
            let network = ds
                .get("network")
                .and_then(|n| n.as_str())
                .unwrap_or("mainnet");

            let address = ds
                .get("source")
                .and_then(|s| s.get("address"))
                .and_then(|a| a.as_str())
                .map(String::from);

            let start_block = ds
                .get("source")
                .and_then(|s| s.get("startBlock"))
                .and_then(|b| b.as_u64());

            let ds_config = DataSourceNetworkConfig {
                address,
                start_block,
            };

            networks_config
                .entry(network.to_string())
                .or_default()
                .insert(name.to_string(), ds_config);
        }
    }

    // Write the networks.json file
    let json = serde_json::to_string_pretty(&networks_config)
        .context("Failed to serialize networks config")?;

    fs::write(output_path, json).with_context(|| {
        format!(
            "Failed to write networks config to {}",
            output_path.display()
        )
    })?;

    Ok(())
}

/// Update the networks.json file with a new data source entry.
///
/// # Arguments
/// * `networks_path` - Path to the networks.json file
/// * `network` - Network name
/// * `data_source` - Data source name
/// * `address` - Contract address
/// * `start_block` - Optional start block
pub fn update_networks_file(
    networks_path: &Path,
    network: &str,
    data_source: &str,
    address: &str,
    start_block: Option<u64>,
) -> Result<()> {
    // Load existing config or create new
    let mut config: NetworksConfig = if networks_path.exists() {
        load_networks_config(networks_path)?
    } else {
        HashMap::new()
    };

    // Update or create entry
    let ds_config = DataSourceNetworkConfig {
        address: Some(address.to_string()),
        start_block,
    };

    config
        .entry(network.to_string())
        .or_default()
        .insert(data_source.to_string(), ds_config);

    // Write back
    let json =
        serde_json::to_string_pretty(&config).context("Failed to serialize networks config")?;

    fs::write(networks_path, json).with_context(|| {
        format!(
            "Failed to write networks config to {}",
            networks_path.display()
        )
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_load_networks_config() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("networks.json");

        let config = r#"{
            "mainnet": {
                "Token": {
                    "address": "0x1234567890123456789012345678901234567890",
                    "startBlock": 12345678
                }
            },
            "goerli": {
                "Token": {
                    "address": "0x0987654321098765432109876543210987654321",
                    "startBlock": 100
                }
            }
        }"#;

        fs::write(&path, config).unwrap();

        let loaded = load_networks_config(&path).unwrap();

        assert!(loaded.contains_key("mainnet"));
        assert!(loaded.contains_key("goerli"));

        let mainnet = &loaded["mainnet"];
        let token = &mainnet["Token"];
        assert_eq!(
            token.address,
            Some("0x1234567890123456789012345678901234567890".to_string())
        );
        assert_eq!(token.start_block, Some(12345678));
    }

    #[test]
    fn test_load_networks_config_missing_optional_fields() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("networks.json");

        let config = r#"{
            "mainnet": {
                "Token": {
                    "address": "0x1234567890123456789012345678901234567890"
                }
            }
        }"#;

        fs::write(&path, config).unwrap();

        let loaded = load_networks_config(&path).unwrap();
        let token = &loaded["mainnet"]["Token"];

        assert_eq!(
            token.address,
            Some("0x1234567890123456789012345678901234567890".to_string())
        );
        assert_eq!(token.start_block, None);
    }

    #[test]
    fn test_get_network_config() {
        let mut config = NetworksConfig::new();
        let mut mainnet = NetworkConfig::new();
        mainnet.insert(
            "Token".to_string(),
            DataSourceNetworkConfig {
                address: Some("0x123".to_string()),
                start_block: Some(100),
            },
        );
        config.insert("mainnet".to_string(), mainnet);

        assert!(get_network_config(&config, "mainnet").is_ok());
        assert!(get_network_config(&config, "goerli").is_err());
    }

    #[test]
    fn test_apply_network_config() {
        let manifest_yaml = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    name: Token
    network: mainnet
    source:
      abi: ERC20
      address: "0x0000000000000000000000000000000000000000"
      startBlock: 0
    mapping:
      kind: ethereum/events
      file: ./src/mapping.ts
templates:
  - kind: ethereum/contract
    name: DynamicToken
    network: mainnet
"#;

        let mut manifest: serde_yaml::Value = serde_yaml::from_str(manifest_yaml).unwrap();

        let mut network_config = NetworkConfig::new();
        network_config.insert(
            "Token".to_string(),
            DataSourceNetworkConfig {
                address: Some("0x1234567890123456789012345678901234567890".to_string()),
                start_block: Some(12345678),
            },
        );

        apply_network_config(&mut manifest, "goerli", &network_config).unwrap();

        // Check data source was updated
        let ds = &manifest["dataSources"][0];
        assert_eq!(ds["network"].as_str(), Some("goerli"));
        assert_eq!(
            ds["source"]["address"].as_str(),
            Some("0x1234567890123456789012345678901234567890")
        );
        assert_eq!(ds["source"]["startBlock"].as_u64(), Some(12345678));
        // ABI should be preserved
        assert_eq!(ds["source"]["abi"].as_str(), Some("ERC20"));

        // Check template was updated
        let template = &manifest["templates"][0];
        assert_eq!(template["network"].as_str(), Some("goerli"));
    }

    #[test]
    fn test_apply_network_config_missing_data_source() {
        let manifest_yaml = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    name: Token
    network: mainnet
    source:
      address: "0x0000000000000000000000000000000000000000"
"#;

        let mut manifest: serde_yaml::Value = serde_yaml::from_str(manifest_yaml).unwrap();

        // Network config with different data source name
        let mut network_config = NetworkConfig::new();
        network_config.insert(
            "OtherContract".to_string(),
            DataSourceNetworkConfig {
                address: Some("0x123".to_string()),
                start_block: None,
            },
        );

        let result = apply_network_config(&mut manifest, "goerli", &network_config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Token"));
    }

    #[test]
    fn test_init_networks_config() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");
        let networks_path = temp_dir.path().join("networks.json");

        let manifest = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    name: Token
    network: mainnet
    source:
      address: "0x1234567890123456789012345678901234567890"
      startBlock: 12345678
  - kind: ethereum/contract
    name: Factory
    network: mainnet
    source:
      address: "0xabcdef0123456789abcdef0123456789abcdef01"
      startBlock: 12340000
"#;

        fs::write(&manifest_path, manifest).unwrap();
        init_networks_config(&manifest_path, &networks_path).unwrap();

        let loaded = load_networks_config(&networks_path).unwrap();
        assert!(loaded.contains_key("mainnet"));

        let mainnet = &loaded["mainnet"];
        assert!(mainnet.contains_key("Token"));
        assert!(mainnet.contains_key("Factory"));

        let token = &mainnet["Token"];
        assert_eq!(
            token.address,
            Some("0x1234567890123456789012345678901234567890".to_string())
        );
        assert_eq!(token.start_block, Some(12345678));
    }

    #[test]
    fn test_update_networks_file_new() {
        let temp_dir = TempDir::new().unwrap();
        let networks_path = temp_dir.path().join("networks.json");

        update_networks_file(
            &networks_path,
            "mainnet",
            "Token",
            "0x1234567890123456789012345678901234567890",
            Some(100),
        )
        .unwrap();

        let loaded = load_networks_config(&networks_path).unwrap();
        let token = &loaded["mainnet"]["Token"];
        assert_eq!(
            token.address,
            Some("0x1234567890123456789012345678901234567890".to_string())
        );
        assert_eq!(token.start_block, Some(100));
    }

    #[test]
    fn test_update_networks_file_existing() {
        let temp_dir = TempDir::new().unwrap();
        let networks_path = temp_dir.path().join("networks.json");

        // Create initial config
        let initial = r#"{
            "mainnet": {
                "Token": {
                    "address": "0x0000000000000000000000000000000000000000"
                }
            }
        }"#;
        fs::write(&networks_path, initial).unwrap();

        // Update with new data source
        update_networks_file(
            &networks_path,
            "mainnet",
            "Factory",
            "0x1111111111111111111111111111111111111111",
            None,
        )
        .unwrap();

        let loaded = load_networks_config(&networks_path).unwrap();
        assert!(loaded["mainnet"].contains_key("Token"));
        assert!(loaded["mainnet"].contains_key("Factory"));
    }
}
