//! Scaffold generation for new subgraphs.
//!
//! This module provides functionality to generate the scaffolding files for a new
//! subgraph project, including manifest, schema, mappings, and configuration files.

mod manifest;
mod mapping;
mod schema;

pub use manifest::generate_manifest;
pub use mapping::generate_mapping;
pub use schema::generate_schema;

use std::fs;
use std::path::Path;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use serde_json::Value as JsonValue;

use crate::formatter::format_typescript;
use crate::output::{step, Step};

/// Options for scaffold generation.
#[derive(Debug, Clone)]
pub struct ScaffoldOptions {
    /// Contract address
    pub address: Option<String>,
    /// Network name
    pub network: String,
    /// Contract name
    pub contract_name: String,
    /// Subgraph name
    pub subgraph_name: String,
    /// Start block for indexing
    pub start_block: Option<u64>,
    /// Contract ABI as JSON
    pub abi: Option<JsonValue>,
    /// Whether to index all events
    pub index_events: bool,
}

impl Default for ScaffoldOptions {
    fn default() -> Self {
        Self {
            address: None,
            network: "mainnet".to_string(),
            contract_name: "Contract".to_string(),
            subgraph_name: "my-subgraph".to_string(),
            start_block: None,
            abi: None,
            index_events: false,
        }
    }
}

/// Graph-ts and graph-cli versions for package.json.
const GRAPH_CLI_VERSION: &str = "0.98.0";
const GRAPH_TS_VERSION: &str = "0.37.0";
const MATCHSTICK_VERSION: &str = "0.6.0";

/// Generate all scaffold files and write to directory.
pub fn generate_scaffold(dir: &Path, options: &ScaffoldOptions) -> Result<()> {
    step(Step::Generate, "Generating scaffold files");

    // Create directory structure
    fs::create_dir_all(dir).context("Failed to create project directory")?;
    fs::create_dir_all(dir.join("src")).context("Failed to create src directory")?;
    fs::create_dir_all(dir.join("abis")).context("Failed to create abis directory")?;

    // Generate and write files
    let manifest = generate_manifest(options);
    fs::write(dir.join("subgraph.yaml"), manifest).context("Failed to write subgraph.yaml")?;

    let schema = generate_schema(options);
    fs::write(dir.join("schema.graphql"), schema).context("Failed to write schema.graphql")?;

    let mapping = generate_mapping(options);
    let mapping_formatted = format_typescript(&mapping).unwrap_or(mapping);
    let mapping_file = format!("{}.ts", to_kebab_case(&options.contract_name));
    fs::write(dir.join("src").join(&mapping_file), mapping_formatted)
        .context("Failed to write mapping file")?;

    // Write ABI if provided
    if let Some(abi) = &options.abi {
        let abi_str = serde_json::to_string_pretty(abi).context("Failed to serialize ABI")?;
        let abi_file = format!("{}.json", options.contract_name);
        fs::write(dir.join("abis").join(&abi_file), abi_str).context("Failed to write ABI file")?;
    }

    // Generate package.json
    let package_json = generate_package_json(options);
    fs::write(dir.join("package.json"), package_json).context("Failed to write package.json")?;

    // Generate tsconfig.json
    let tsconfig = generate_tsconfig();
    fs::write(dir.join("tsconfig.json"), tsconfig).context("Failed to write tsconfig.json")?;

    // Generate .gitignore
    let gitignore = generate_gitignore();
    fs::write(dir.join(".gitignore"), gitignore).context("Failed to write .gitignore")?;

    Ok(())
}

/// Initialize git repository in directory.
pub fn init_git(dir: &Path) -> Result<()> {
    step(Step::Generate, "Initializing Git repository");

    let status = Command::new("git")
        .current_dir(dir)
        .arg("init")
        .status()
        .context("Failed to run git init")?;

    if !status.success() {
        return Err(anyhow!("git init failed"));
    }

    Ok(())
}

/// Install npm dependencies.
pub fn install_dependencies(dir: &Path) -> Result<()> {
    step(Step::Generate, "Installing dependencies");

    // Try pnpm first
    let pnpm_result = Command::new("pnpm")
        .current_dir(dir)
        .arg("install")
        .status();

    if let Ok(status) = pnpm_result {
        if status.success() {
            return Ok(());
        }
    }

    // Fall back to npm
    let npm_result = Command::new("npm").current_dir(dir).arg("install").status();

    if let Ok(status) = npm_result {
        if status.success() {
            return Ok(());
        }
    }

    Err(anyhow!(
        "Failed to install dependencies. Please run 'npm install' manually."
    ))
}

/// Generate package.json content.
fn generate_package_json(options: &ScaffoldOptions) -> String {
    let basename = options
        .subgraph_name
        .split('/')
        .next_back()
        .unwrap_or(&options.subgraph_name);

    serde_json::to_string_pretty(&serde_json::json!({
        "name": basename,
        "license": "UNLICENSED",
        "scripts": {
            "codegen": "graph codegen",
            "build": "graph build",
            "deploy": format!("graph deploy --node https://api.studio.thegraph.com/deploy/ {}", options.subgraph_name),
            "create-local": format!("graph create --node http://localhost:8020/ {}", options.subgraph_name),
            "remove-local": format!("graph remove --node http://localhost:8020/ {}", options.subgraph_name),
            "deploy-local": format!("graph deploy --node http://localhost:8020/ --ipfs http://localhost:5001 {}", options.subgraph_name),
            "test": "graph test"
        },
        "dependencies": {
            "@graphprotocol/graph-cli": GRAPH_CLI_VERSION,
            "@graphprotocol/graph-ts": GRAPH_TS_VERSION
        },
        "devDependencies": {
            "matchstick-as": MATCHSTICK_VERSION
        }
    }))
    .unwrap_or_default()
}

/// Generate tsconfig.json content.
fn generate_tsconfig() -> String {
    serde_json::to_string_pretty(&serde_json::json!({
        "extends": "@graphprotocol/graph-ts/types/tsconfig.base.json",
        "include": ["src", "tests"]
    }))
    .unwrap_or_default()
}

/// Generate .gitignore content.
fn generate_gitignore() -> String {
    r#"node_modules/
build/
generated/
.env
.env.local
"#
    .to_string()
}

/// Convert a string to kebab-case.
fn to_kebab_case(s: &str) -> String {
    let mut result = String::new();
    for (i, c) in s.chars().enumerate() {
        if c.is_uppercase() {
            if i > 0 {
                result.push('-');
            }
            result.push(c.to_lowercase().next().unwrap_or(c));
        } else {
            result.push(c);
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_kebab_case() {
        assert_eq!(to_kebab_case("MyContract"), "my-contract");
        assert_eq!(to_kebab_case("ERC20"), "e-r-c20");
        assert_eq!(to_kebab_case("contract"), "contract");
        assert_eq!(to_kebab_case("ABCToken"), "a-b-c-token");
    }

    #[test]
    fn test_generate_package_json() {
        let options = ScaffoldOptions {
            subgraph_name: "user/my-subgraph".to_string(),
            ..Default::default()
        };

        let json = generate_package_json(&options);
        assert!(json.contains(r#""name": "my-subgraph""#));
        assert!(json.contains("@graphprotocol/graph-cli"));
        assert!(json.contains("@graphprotocol/graph-ts"));
    }

    #[test]
    fn test_generate_tsconfig() {
        let json = generate_tsconfig();
        assert!(json.contains("@graphprotocol/graph-ts/types/tsconfig.base.json"));
    }

    #[test]
    fn test_generate_gitignore() {
        let gitignore = generate_gitignore();
        assert!(gitignore.contains("node_modules/"));
        assert!(gitignore.contains("build/"));
        assert!(gitignore.contains("generated/"));
    }
}
