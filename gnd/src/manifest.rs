//! Shared manifest types and parsing for gnd commands.
//!
//! This module provides unified manifest loading and types used by build,
//! codegen, and validation commands. Manifest parsing uses graph-node's
//! `UnresolvedSubgraphManifest` types which enforce required fields via serde
//! (e.g. `source.abi`, `mapping.abis`).

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use graph::data::subgraph::UnresolvedSubgraphManifest;
use graph::data_source::{
    UnresolvedDataSource as GraphUnresolvedDS, UnresolvedDataSourceTemplate as GraphUnresolvedDST,
};
use graph::prelude::DeploymentHash;
use graph_chain_ethereum::{BlockHandlerFilter, Chain};
use semver::Version;

use crate::output::{step, Step};

/// Type alias for the graph-node unresolved manifest parameterized on Ethereum.
type GraphManifest = UnresolvedSubgraphManifest<Chain>;

/// A simplified subgraph manifest structure.
///
/// This unified struct contains all fields needed by build, codegen, and
/// validation. Commands use only the fields they need.
///
/// Parsing uses graph-node's `UnresolvedSubgraphManifest::parse()` which
/// enforces required fields via serde (e.g. `source.abi`, `mapping.abis`
/// for Ethereum data sources).
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
    /// The ABI name referenced in `source.abi` (Ethereum data sources only).
    pub source_abi: Option<String>,
    /// The block number at which this data source starts indexing (from source.startBlock).
    pub start_block: u64,
    /// The block number at which this data source stops indexing (from source.endBlock).
    pub end_block: Option<u64>,
    /// Event handlers from the mapping.
    pub event_handlers: Vec<EventHandler>,
    /// Call handlers from the mapping.
    pub call_handlers: Vec<CallHandler>,
    /// Block handlers from the mapping (with filter info for validation).
    pub block_handlers: Vec<BlockHandler>,
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
    /// The ABI name referenced in `source.abi` (Ethereum templates only).
    pub source_abi: Option<String>,
    /// Event handlers from the mapping.
    pub event_handlers: Vec<EventHandler>,
    /// Call handlers from the mapping.
    pub call_handlers: Vec<CallHandler>,
    /// Block handlers from the mapping (with filter info for validation).
    pub block_handlers: Vec<BlockHandler>,
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

/// An event handler with metadata needed for validation.
#[derive(Debug)]
pub struct EventHandler {
    /// The Solidity event signature, e.g. `Transfer(address,address,uint256)`.
    pub event: String,
    pub handler: String,
    /// Whether this handler requires transaction receipts.
    pub receipt: bool,
    /// Whether this handler has eth call declarations.
    pub has_call_decls: bool,
}

/// A call handler with metadata needed for validation.
#[derive(Debug)]
pub struct CallHandler {
    /// The Solidity function signature, e.g. `approve(address,uint256)`.
    pub function: String,
    pub handler: String,
}

/// A block handler with filter info needed for validation.
#[derive(Debug)]
pub struct BlockHandler {
    pub handler: String,
    pub filter: Option<BlockHandlerFilterKind>,
}

/// The kind of filter on a block handler (simplified from graph-node's `BlockHandlerFilter`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BlockHandlerFilterKind {
    Call,
    Once,
    Polling,
}

/// Load a subgraph manifest from a YAML file.
///
/// This uses graph-node's `UnresolvedSubgraphManifest::parse()` which
/// enforces all required fields via serde deserialization. Missing required
/// fields (e.g. `source.abi`, `mapping.abis`) produce clear parse errors.
pub fn load_manifest(path: &Path) -> Result<Manifest> {
    step(
        Step::Load,
        &format!("Load subgraph from {}", path.display()),
    );

    let manifest_str =
        fs::read_to_string(path).with_context(|| format!("Failed to read manifest: {:?}", path))?;

    let raw: serde_yaml::Mapping = serde_yaml::from_str(&manifest_str)
        .with_context(|| format!("Failed to parse manifest YAML: {:?}", path))?;

    // Use a dummy deployment hash for local CLI use
    let id = DeploymentHash::new("QmLocalDev").unwrap();

    let parsed: GraphManifest = GraphManifest::parse(id, raw)
        .with_context(|| format!("Failed to parse manifest: {:?}", path))?;

    Ok(convert_manifest(parsed))
}

/// Convert a graph-node `UnresolvedSubgraphManifest` into gnd's `Manifest`.
fn convert_manifest(m: GraphManifest) -> Manifest {
    let spec_version = m.spec_version;

    let schema = m.schema.map(|s| s.file.link);

    let features = m.features.into_iter().map(|f| f.to_string()).collect();

    let graft = m.graft.map(|g| GraftConfig {
        base: g.base.to_string(),
        block: g.block.max(0) as u64,
    });

    let data_sources = m
        .data_sources
        .into_iter()
        .map(convert_data_source)
        .collect();

    let templates = m.templates.into_iter().map(convert_template).collect();

    Manifest {
        spec_version,
        schema,
        features,
        graft,
        data_sources,
        templates,
    }
}

/// Convert a graph-node `UnresolvedDataSource` enum into gnd's `DataSource`.
fn convert_data_source(ds: GraphUnresolvedDS<Chain>) -> DataSource {
    match ds {
        GraphUnresolvedDS::Onchain(eth) => DataSource {
            name: eth.name.clone(),
            kind: eth.kind.clone(),
            network: eth.network.clone(),
            mapping_file: Some(eth.mapping.file.link.clone()),
            api_version: Version::parse(&eth.mapping.api_version).ok(),
            abis: eth
                .mapping
                .abis
                .iter()
                .map(|a| Abi {
                    name: a.name.clone(),
                    file: a.file.link.clone(),
                })
                .collect(),
            source_address: eth.source.address.map(|a| format!("{:?}", a)),
            source_abi: Some(eth.source.abi.clone()),
            start_block: eth.source.start_block as u64,
            end_block: eth.source.end_block.map(|b| b as u64),
            event_handlers: eth
                .mapping
                .event_handlers
                .iter()
                .map(|h| EventHandler {
                    event: h.event.clone(),
                    handler: h.handler.clone(),
                    receipt: h.receipt,
                    has_call_decls: !h.calls.raw_decls.is_empty(),
                })
                .collect(),
            call_handlers: eth
                .mapping
                .call_handlers
                .iter()
                .map(|h| CallHandler {
                    function: h.function.clone(),
                    handler: h.handler.clone(),
                })
                .collect(),
            block_handlers: eth
                .mapping
                .block_handlers
                .iter()
                .map(|h| BlockHandler {
                    handler: h.handler.clone(),
                    filter: h.filter.as_ref().map(convert_block_handler_filter),
                })
                .collect(),
        },
        GraphUnresolvedDS::Subgraph(sub) => DataSource {
            name: sub.name.clone(),
            kind: sub.kind.clone(),
            network: Some(sub.network.clone()),
            mapping_file: Some(sub.mapping.file.link.clone()),
            api_version: Version::parse(&sub.mapping.api_version).ok(),
            abis: sub
                .mapping
                .abis
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|a| Abi {
                    name: a.name.clone(),
                    file: a.file.link.clone(),
                })
                .collect(),
            source_address: Some(sub.source.address().to_string()),
            source_abi: None,
            start_block: sub.source.start_block() as u64,
            end_block: None, // Subgraph sources don't have end_block
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        },
        GraphUnresolvedDS::Offchain(off) => DataSource {
            name: off.name.clone(),
            kind: off.kind.clone(),
            network: None,
            mapping_file: Some(off.mapping.file.link.clone()),
            api_version: Version::parse(&off.mapping.api_version).ok(),
            abis: vec![],
            source_address: None,
            source_abi: None,
            start_block: 0, // Offchain data sources don't have start_block
            end_block: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        },
        GraphUnresolvedDS::Amp(amp) => DataSource {
            name: amp.name.clone(),
            kind: amp.kind.clone(),
            network: Some(amp.network.clone()),
            mapping_file: None,
            api_version: None,
            abis: vec![],
            source_address: None,
            source_abi: None,
            start_block: amp.source.start_block.unwrap_or(0),
            end_block: amp.source.end_block,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        },
    }
}

/// Convert a graph-node `UnresolvedDataSourceTemplate` enum into gnd's `Template`.
fn convert_template(t: GraphUnresolvedDST<Chain>) -> Template {
    match t {
        GraphUnresolvedDST::Onchain(eth) => Template {
            name: eth.name.clone(),
            kind: eth.kind.clone(),
            network: eth.network.clone(),
            mapping_file: Some(eth.mapping.file.link.clone()),
            api_version: Version::parse(&eth.mapping.api_version).ok(),
            abis: eth
                .mapping
                .abis
                .iter()
                .map(|a| Abi {
                    name: a.name.clone(),
                    file: a.file.link.clone(),
                })
                .collect(),
            source_abi: Some(eth.source.abi.clone()),
            event_handlers: eth
                .mapping
                .event_handlers
                .iter()
                .map(|h| EventHandler {
                    event: h.event.clone(),
                    handler: h.handler.clone(),
                    receipt: h.receipt,
                    has_call_decls: !h.calls.raw_decls.is_empty(),
                })
                .collect(),
            call_handlers: eth
                .mapping
                .call_handlers
                .iter()
                .map(|h| CallHandler {
                    function: h.function.clone(),
                    handler: h.handler.clone(),
                })
                .collect(),
            block_handlers: eth
                .mapping
                .block_handlers
                .iter()
                .map(|h| BlockHandler {
                    handler: h.handler.clone(),
                    filter: h.filter.as_ref().map(convert_block_handler_filter),
                })
                .collect(),
        },
        GraphUnresolvedDST::Offchain(off) => Template {
            name: off.name.clone(),
            kind: off.kind.clone(),
            network: off.network.clone(),
            mapping_file: Some(off.mapping.file.link.clone()),
            api_version: Version::parse(&off.mapping.api_version).ok(),
            abis: vec![],
            source_abi: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        },
        GraphUnresolvedDST::Subgraph(sub) => Template {
            name: sub.name.clone(),
            kind: sub.kind.clone(),
            network: sub.network.clone(),
            mapping_file: Some(sub.mapping.file.link.clone()),
            api_version: Version::parse(&sub.mapping.api_version).ok(),
            abis: sub
                .mapping
                .abis
                .as_deref()
                .unwrap_or(&[])
                .iter()
                .map(|a| Abi {
                    name: a.name.clone(),
                    file: a.file.link.clone(),
                })
                .collect(),
            source_abi: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        },
    }
}

/// Convert a graph-node `BlockHandlerFilter` to gnd's simplified `BlockHandlerFilterKind`.
fn convert_block_handler_filter(filter: &BlockHandlerFilter) -> BlockHandlerFilterKind {
    match filter {
        BlockHandlerFilter::Call => BlockHandlerFilterKind::Call,
        BlockHandlerFilter::Once => BlockHandlerFilterKind::Once,
        BlockHandlerFilter::Polling { .. } => BlockHandlerFilterKind::Polling,
    }
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
    source:
      abi: ERC20
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
templates:
  - kind: ethereum/contract
    name: DynamicToken
    network: mainnet
    source:
      abi: ERC20
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      language: wasm/assemblyscript
      file: ./src/dynamic.ts
      entities:
        - MyEntity
      abis: []
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
  base: QmXYZ123abcdefghijklmnopqrstuvwxyz1234567890ab
  block: 12345
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: Token
    network: mainnet
    source:
      abi: ERC20
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
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
        assert_eq!(graft.base, "QmXYZ123abcdefghijklmnopqrstuvwxyz1234567890ab");
        assert_eq!(graft.block, 12345);
    }

    #[test]
    fn test_load_manifest_with_subgraph_sources() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        let manifest_content = r#"
specVersion: 1.0.0
schema:
  file: ./schema.graphql
dataSources:
  - kind: subgraph
    name: SourceSubgraph
    network: mainnet
    source:
      address: QmSourceHash1234567890abcdefghijklmnopqrstuvwx
      startBlock: 0
    mapping:
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
      handlers:
        - handler: handleEntity
          entity: MyEntity
  - kind: ethereum/contract
    name: Token
    network: mainnet
    source:
      abi: ERC20
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
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
            Some("QmSourceHash1234567890abcdefghijklmnopqrstuvwx".to_string())
        );
        // Second is the contract source
        assert_eq!(manifest.data_sources[1].name, "Token");
        assert_eq!(manifest.data_sources[1].kind, "ethereum/contract");
        assert!(!manifest.data_sources[1].is_subgraph_source());
        assert_eq!(manifest.data_sources[1].source_address, None);
        // total_source_count includes both
        assert_eq!(manifest.total_source_count(), 2);
    }

    #[test]
    fn test_load_manifest_missing_source_abi_fails() {
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
    source: {}
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let result = load_manifest(&manifest_path);
        assert!(result.is_err(), "Missing source.abi should fail parsing");
        let err = result.unwrap_err();
        let err_chain = format!("{:#}", err);
        assert!(
            err_chain.contains("abi"),
            "Error should mention missing abi field, got: {}",
            err_chain
        );
    }

    #[test]
    fn test_load_manifest_missing_mapping_abis_fails() {
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
    source:
      abi: ERC20
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.6
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let result = load_manifest(&manifest_path);
        assert!(result.is_err(), "Missing mapping.abis should fail parsing");
        let err = result.unwrap_err();
        let err_chain = format!("{:#}", err);
        assert!(
            err_chain.contains("abis"),
            "Error should mention missing abis field, got: {}",
            err_chain
        );
    }
}
