//! Shared manifest types and parsing for gnd commands.
//!
//! This module provides unified manifest loading and types used by build,
//! codegen, and validation commands.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use semver::Version;

use crate::output::{step, Step};

/// A simplified subgraph manifest structure.
///
/// This unified struct contains all fields needed by build, codegen, and
/// validation. Commands use only the fields they need.
///
/// # Data Source Organization
///
/// All data sources from the manifest are stored in the `data_sources` field,
/// regardless of their kind. Subgraph data sources (kind: subgraph) are
/// identified by `DataSource::is_subgraph_source()` and have the `source_address`
/// field set to the IPFS deployment ID of the referenced subgraph.
#[derive(Debug)]
pub struct Manifest {
    pub spec_version: Version,
    pub schema: Option<String>,
    pub features: Vec<String>,
    pub graft: Option<GraftConfig>,
    /// All data sources, including both contract-style sources and subgraph sources.
    pub data_sources: Vec<DataSource>,
    pub templates: Vec<Template>,
}

/// Graft configuration.
#[derive(Debug)]
pub struct GraftConfig {
    pub base: String,
    pub block: u64,
}

/// A data source in the manifest.
#[derive(Debug)]
pub struct DataSource {
    pub name: String,
    pub kind: String,
    pub network: Option<String>,
    pub mapping_file: Option<String>,
    pub api_version: Option<Version>,
    pub abis: Vec<Abi>,
    /// For subgraph data sources: the IPFS deployment ID of the referenced subgraph.
    pub source_address: Option<String>,
}

impl DataSource {
    /// Returns true if this is a valid subgraph data source (kind: subgraph with address).
    pub fn is_subgraph_source(&self) -> bool {
        self.kind == "subgraph" && self.source_address.is_some()
    }
}

/// A data source template in the manifest.
#[derive(Debug)]
pub struct Template {
    pub name: String,
    pub kind: String,
    pub network: Option<String>,
    pub mapping_file: Option<String>,
    pub api_version: Option<Version>,
    pub abis: Vec<Abi>,
}

impl Manifest {
    /// Returns the total count of all data sources.
    pub fn total_source_count(&self) -> usize {
        self.data_sources.len()
    }
}

/// An ABI reference in a data source or template.
#[derive(Debug)]
pub struct Abi {
    pub name: String,
    pub file: String,
}

/// Load a subgraph manifest from a YAML file.
///
/// This is the unified loading function used by build, codegen, and validation.
pub fn load_manifest(path: &Path) -> Result<Manifest> {
    step(
        Step::Load,
        &format!("Load subgraph from {}", path.display()),
    );

    let manifest_str =
        fs::read_to_string(path).with_context(|| format!("Failed to read manifest: {:?}", path))?;

    let value: serde_json::Value = serde_yaml::from_str(&manifest_str)
        .with_context(|| format!("Failed to parse manifest YAML: {:?}", path))?;

    // Extract spec version (default to 0.0.4 if not specified)
    let spec_version_str = value
        .get("specVersion")
        .and_then(|v| v.as_str())
        .unwrap_or("0.0.4");

    let spec_version = Version::parse(spec_version_str).unwrap_or_else(|_| Version::new(0, 0, 4));

    // Extract schema path
    let schema = value
        .get("schema")
        .and_then(|s| s.get("file"))
        .and_then(|f| f.as_str())
        .map(String::from);

    // Extract features
    let features = value
        .get("features")
        .and_then(|f| f.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|f| f.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // Extract graft config
    let graft = value.get("graft").and_then(|g| {
        let base = g.get("base")?.as_str()?.to_string();
        let block = g.get("block")?.as_u64().unwrap_or(0);
        Some(GraftConfig { base, block })
    });

    // Extract data sources (including subgraph sources)
    let mut data_sources = Vec::new();

    if let Some(ds_array) = value.get("dataSources").and_then(|ds| ds.as_array()) {
        for ds in ds_array {
            let kind = ds
                .get("kind")
                .and_then(|k| k.as_str())
                .unwrap_or("ethereum/contract");

            if let Some(parsed) = parse_data_source(ds, kind) {
                data_sources.push(parsed);
            }
        }
    }

    // Extract templates
    let templates = value
        .get("templates")
        .and_then(|t| t.as_array())
        .map(|arr| arr.iter().filter_map(parse_template).collect())
        .unwrap_or_default();

    Ok(Manifest {
        spec_version,
        schema,
        features,
        graft,
        data_sources,
        templates,
    })
}

/// Parse a data source from a JSON value.
fn parse_data_source(ds: &serde_json::Value, kind: &str) -> Option<DataSource> {
    let name = ds.get("name")?.as_str()?.to_string();
    let network = ds.get("network").and_then(|n| n.as_str()).map(String::from);
    let mapping_file = ds
        .get("mapping")
        .and_then(|m| m.get("file"))
        .and_then(|f| f.as_str())
        .map(String::from);
    let api_version = ds
        .get("mapping")
        .and_then(|m| m.get("apiVersion"))
        .and_then(|v| v.as_str())
        .and_then(|v| Version::parse(v).ok());
    let abis = parse_abis(ds.get("mapping").and_then(|m| m.get("abis")));
    let source_address = ds
        .get("source")
        .and_then(|s| s.get("address"))
        .and_then(|a| a.as_str())
        .map(String::from);

    Some(DataSource {
        name,
        kind: kind.to_string(),
        network,
        mapping_file,
        api_version,
        abis,
        source_address,
    })
}

/// Parse a template from a JSON value.
fn parse_template(t: &serde_json::Value) -> Option<Template> {
    let name = t.get("name")?.as_str()?.to_string();
    let kind = t
        .get("kind")
        .and_then(|k| k.as_str())
        .unwrap_or("ethereum/contract")
        .to_string();
    let network = t.get("network").and_then(|n| n.as_str()).map(String::from);
    let mapping_file = t
        .get("mapping")
        .and_then(|m| m.get("file"))
        .and_then(|f| f.as_str())
        .map(String::from);
    let api_version = t
        .get("mapping")
        .and_then(|m| m.get("apiVersion"))
        .and_then(|v| v.as_str())
        .and_then(|v| Version::parse(v).ok());
    let abis = parse_abis(t.get("mapping").and_then(|m| m.get("abis")));

    Some(Template {
        name,
        kind,
        network,
        mapping_file,
        api_version,
        abis,
    })
}

/// Parse ABIs from a JSON value.
fn parse_abis(abis_value: Option<&serde_json::Value>) -> Vec<Abi> {
    abis_value
        .and_then(|a| a.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|abi| {
                    let name = abi.get("name")?.as_str()?.to_string();
                    let file = abi.get("file")?.as_str()?.to_string();
                    Some(Abi { name, file })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Get the directory containing a manifest file.
///
/// This handles the edge case where `manifest_path` is a bare filename
/// like `"subgraph.yaml"` - in that case, `parent()` returns an empty
/// path, so we fall back to `"."` (current directory).
pub fn manifest_dir(manifest_path: &Path) -> &Path {
    manifest_path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Resolve a path relative to the manifest file.
pub fn resolve_path(manifest: &Path, path: &str) -> PathBuf {
    manifest_dir(manifest).join(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_dir_with_directory() {
        let manifest = PathBuf::from("/home/user/project/subgraph.yaml");
        assert_eq!(manifest_dir(&manifest), Path::new("/home/user/project"));
    }

    #[test]
    fn test_manifest_dir_bare_filename() {
        // When manifest_path is just "subgraph.yaml", parent() returns Some("")
        // which is empty, so we should fall back to "."
        let manifest = PathBuf::from("subgraph.yaml");
        assert_eq!(manifest_dir(&manifest), Path::new("."));
    }

    #[test]
    fn test_manifest_dir_relative_path() {
        let manifest = PathBuf::from("./subgraph.yaml");
        assert_eq!(manifest_dir(&manifest), Path::new("."));
    }

    #[test]
    fn test_resolve_path() {
        let manifest = PathBuf::from("/home/user/project/subgraph.yaml");
        let path = "src/mapping.ts";
        let resolved = resolve_path(&manifest, path);
        assert_eq!(resolved, PathBuf::from("/home/user/project/src/mapping.ts"));
    }

    #[test]
    fn test_resolve_path_bare_filename() {
        // When manifest is just "subgraph.yaml", paths should resolve relative to "."
        let manifest = PathBuf::from("subgraph.yaml");
        let path = "src/mapping.ts";
        let resolved = resolve_path(&manifest, path);
        assert_eq!(resolved, PathBuf::from("./src/mapping.ts"));
    }

    #[test]
    fn test_load_manifest_basic() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        let manifest_content = r#"
specVersion: 0.0.4
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: Token
    network: mainnet
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      file: ./src/mapping.ts
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
templates:
  - kind: ethereum/contract
    name: DynamicToken
    network: mainnet
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      file: ./src/dynamic.ts
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.spec_version, Version::new(0, 0, 4));
        assert_eq!(manifest.schema, Some("./schema.graphql".to_string()));
        assert_eq!(manifest.data_sources.len(), 1);
        assert_eq!(manifest.data_sources[0].name, "Token");
        assert_eq!(manifest.data_sources[0].kind, "ethereum/contract");
        assert_eq!(
            manifest.data_sources[0].network,
            Some("mainnet".to_string())
        );
        assert_eq!(
            manifest.data_sources[0].api_version,
            Some(Version::new(0, 0, 6))
        );
        assert_eq!(
            manifest.data_sources[0].mapping_file,
            Some("./src/mapping.ts".to_string())
        );
        assert_eq!(manifest.data_sources[0].abis.len(), 1);
        assert_eq!(manifest.data_sources[0].abis[0].name, "ERC20");
        assert_eq!(manifest.templates.len(), 1);
        assert_eq!(manifest.templates[0].name, "DynamicToken");
        assert_eq!(manifest.templates[0].kind, "ethereum/contract");
        assert_eq!(manifest.templates[0].network, Some("mainnet".to_string()));
        assert_eq!(
            manifest.templates[0].api_version,
            Some(Version::new(0, 0, 6))
        );
    }

    #[test]
    fn test_load_manifest_with_graft_and_features() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        let manifest_content = r#"
specVersion: 0.0.6
features:
  - nonFatalErrors
  - fullTextSearch
graft:
  base: QmXYZ123
  block: 12345
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: Token
    network: mainnet
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      file: ./src/mapping.ts
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.spec_version, Version::new(0, 0, 6));
        assert_eq!(
            manifest.features,
            vec!["nonFatalErrors".to_string(), "fullTextSearch".to_string()]
        );
        assert!(manifest.graft.is_some());
        let graft = manifest.graft.unwrap();
        assert_eq!(graft.base, "QmXYZ123");
        assert_eq!(graft.block, 12345);
    }

    #[test]
    fn test_load_manifest_with_subgraph_sources() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        let manifest_content = r#"
specVersion: 0.0.4
schema:
  file: ./schema.graphql
dataSources:
  - kind: subgraph
    name: SourceSubgraph
    source:
      address: QmSourceHash123
  - kind: ethereum/contract
    name: Token
    network: mainnet
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      file: ./src/mapping.ts
      abis: []
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        // All data sources go to data_sources
        assert_eq!(manifest.data_sources.len(), 2);
        // First is the subgraph source
        assert_eq!(manifest.data_sources[0].name, "SourceSubgraph");
        assert_eq!(manifest.data_sources[0].kind, "subgraph");
        assert!(manifest.data_sources[0].is_subgraph_source());
        assert_eq!(
            manifest.data_sources[0].source_address,
            Some("QmSourceHash123".to_string())
        );
        // Second is the contract source
        assert_eq!(manifest.data_sources[1].name, "Token");
        assert_eq!(manifest.data_sources[1].kind, "ethereum/contract");
        assert!(!manifest.data_sources[1].is_subgraph_source());
        assert_eq!(manifest.data_sources[1].source_address, None);
        // total_source_count includes both
        assert_eq!(manifest.total_source_count(), 2);
    }
}
