use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use url::Url;

/// Default Subgraph Studio deploy URL
const SUBGRAPH_STUDIO_URL: &str = "https://api.studio.thegraph.com/deploy/";

/// Get the path to the config file (~/.graph-cli.json)
fn config_path() -> PathBuf {
    std::env::home_dir()
        .expect("Could not determine home directory")
        .join(".graph-cli.json")
}

/// Normalize a node URL by parsing and re-serializing it
fn normalize_node_url(node: &str) -> Result<String> {
    let url = Url::parse(node).context("Invalid node URL")?;
    Ok(url.to_string())
}

/// Load the config file from a specific path, returning an empty map if it doesn't exist
fn load_config_from(path: &PathBuf) -> Result<HashMap<String, String>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }

    let content = fs::read_to_string(path)
        .with_context(|| format!("Failed to read config file: {}", path.display()))?;

    serde_json::from_str(&content)
        .with_context(|| format!("Failed to parse config file: {}", path.display()))
}

/// Save the config file to a specific path
fn save_config_to(path: &PathBuf, config: &HashMap<String, String>) -> Result<()> {
    let content = serde_json::to_string(config).context("Failed to serialize config")?;
    fs::write(path, content)
        .with_context(|| format!("Failed to write config file: {}", path.display()))
}

/// Save a deploy key for a node (uses default config path)
pub fn save_deploy_key(node: &str, deploy_key: &str) -> Result<()> {
    save_deploy_key_to(&config_path(), node, deploy_key)
}

/// Save a deploy key for a node to a specific config file
fn save_deploy_key_to(config_file: &PathBuf, node: &str, deploy_key: &str) -> Result<()> {
    let normalized_node = normalize_node_url(node)?;
    let mut config = load_config_from(config_file)?;
    config.insert(normalized_node, deploy_key.to_string());
    save_config_to(config_file, &config)
}

#[derive(Clone, Debug, Parser)]
#[clap(about = "Set the deploy key for a Graph Node")]
pub struct AuthOpt {
    /// The deploy key to store
    #[clap(value_name = "DEPLOY_KEY")]
    pub deploy_key: String,

    /// The Graph Node URL to authenticate with
    #[clap(
        long,
        short = 'g',
        value_name = "URL",
        default_value = SUBGRAPH_STUDIO_URL,
        help = "Graph Node URL"
    )]
    pub node: String,
}

/// Validate that a deploy key looks like a valid Subgraph Studio key (32 hex chars)
fn is_valid_studio_key(key: &str) -> bool {
    key.len() == 32 && key.chars().all(|c| c.is_ascii_hexdigit())
}

/// Run the auth command
pub fn run_auth(opt: AuthOpt) -> Result<()> {
    // Validate the deploy key format for Studio
    if opt.node == SUBGRAPH_STUDIO_URL && !is_valid_studio_key(&opt.deploy_key) {
        println!(
            "Warning: Deploy key doesn't look like a valid Subgraph Studio key (expected 32 hex characters)"
        );
    }

    save_deploy_key(&opt.node, &opt.deploy_key)?;

    let normalized_node = normalize_node_url(&opt.node)?;
    println!("✔ Deploy key set for {}", normalized_node);

    Ok(())
}

/// Get the deploy key for a node, if one is saved (uses default config path)
pub fn get_deploy_key(node: &str) -> Result<Option<String>> {
    get_deploy_key_from(&config_path(), node)
}

/// Get the deploy key for a node from a specific config file
fn get_deploy_key_from(config_file: &PathBuf, node: &str) -> Result<Option<String>> {
    let normalized_node = normalize_node_url(node)?;
    let config = load_config_from(config_file)?;
    Ok(config.get(&normalized_node).cloned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_node_url() {
        assert_eq!(
            normalize_node_url("https://example.com").unwrap(),
            "https://example.com/"
        );
        assert_eq!(
            normalize_node_url("https://example.com/").unwrap(),
            "https://example.com/"
        );
        assert_eq!(
            normalize_node_url("https://api.studio.thegraph.com/deploy/").unwrap(),
            "https://api.studio.thegraph.com/deploy/"
        );
    }

    #[test]
    fn test_is_valid_studio_key() {
        assert!(is_valid_studio_key("0123456789abcdef0123456789abcdef"));
        assert!(is_valid_studio_key("ABCDEF0123456789abcdef0123456789"));
        assert!(!is_valid_studio_key("too-short"));
        assert!(!is_valid_studio_key("0123456789abcdef0123456789abcdefXX")); // too long
        assert!(!is_valid_studio_key("0123456789abcdef0123456789abcdeg")); // invalid char
    }

    #[test]
    fn test_save_and_get_deploy_key() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_file = temp_dir.path().join(".graph-cli.json");

        let node = "https://example.com/deploy/";
        let key = "test-deploy-key-12345";

        // Initially no key
        assert!(get_deploy_key_from(&config_file, node).unwrap().is_none());

        // Save key
        save_deploy_key_to(&config_file, node, key).unwrap();

        // Get key back
        assert_eq!(
            get_deploy_key_from(&config_file, node).unwrap(),
            Some(key.to_string())
        );
    }

    #[test]
    fn test_multiple_nodes() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let config_file = temp_dir.path().join(".graph-cli.json");

        let node1 = "https://node1.example.com/";
        let node2 = "https://node2.example.com/";
        let key1 = "key1";
        let key2 = "key2";

        save_deploy_key_to(&config_file, node1, key1).unwrap();
        save_deploy_key_to(&config_file, node2, key2).unwrap();

        assert_eq!(
            get_deploy_key_from(&config_file, node1).unwrap(),
            Some(key1.to_string())
        );
        assert_eq!(
            get_deploy_key_from(&config_file, node2).unwrap(),
            Some(key2.to_string())
        );
    }
}
