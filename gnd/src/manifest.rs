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

use crate::output::{Step, step};

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

    // graph-node no longer supports cosmos, arweave, or substreams chains, so
    // reject such manifests up front with guidance to use an older graph-cli.
    // (This does not affect the still-supported `file/arweave` offchain data
    // source, which is matched neither by `arweave` nor `arweave/`.)
    if let Some(name) = manifest_removed_protocol(&raw) {
        return Err(anyhow::anyhow!(removed_protocol_message(&name)));
    }

    // Non-Ethereum protocols (e.g. NEAR) can't be parsed by graph-node's
    // Ethereum-typed `UnresolvedSubgraphManifest`, and gnd does not depend on
    // their chain crates. Codegen for these protocols only needs the schema
    // path plus data-source/template names and mapping files (NEAR has no
    // ABIs), so we extract just those fields directly from the YAML.
    if manifest_needs_loose_parse(&raw) {
        return parse_manifest_loose(&raw)
            .with_context(|| format!("Failed to parse manifest: {:?}", path));
    }

    // Use a dummy deployment hash for local CLI use
    let id = DeploymentHash::new("QmLocalDev").unwrap();

    let parsed: GraphManifest = GraphManifest::parse(id, raw)
        .with_context(|| format!("Failed to parse manifest: {:?}", path))?;

    Ok(convert_manifest(parsed))
}

/// Onchain data-source kinds for protocols that gnd parses loosely (their chain
/// crates are not gnd dependencies). NEAR is the only non-Ethereum protocol gnd
/// still supports this way; everything else (`ethereum`, `ethereum/contract`,
/// `subgraph`, `file/*`) is handled by the typed path.
const LOOSE_PROTOCOL_KINDS: &[&str] = &["near"];

/// Chain kinds graph-node no longer supports. gnd refuses to create or load
/// subgraphs for these. Note this is the *chain* `arweave`, not the still-
/// supported `file/arweave` offchain file data source: the matcher below keys
/// on `kind == p || kind.starts_with("{p}/")`, and `file/arweave` neither
/// equals `arweave` nor starts with `arweave/`, so it is never matched here.
pub const REMOVED_PROTOCOL_KINDS: &[&str] = &["cosmos", "arweave", "substreams"];

/// Build the user-facing error explaining that a protocol has been dropped.
pub fn removed_protocol_message(name: &str) -> String {
    format!(
        "graph-node no longer supports {name} subgraphs. To work with {name} \
         subgraphs, install and use an older version of graph-cli \
         (https://github.com/graphprotocol/graph-tooling)."
    )
}

/// Returns true if `kind` belongs to `protocol` (either exactly `protocol` or a
/// sub-kind like `protocol/handler`).
fn kind_belongs_to(kind: &str, protocol: &str) -> bool {
    kind == protocol || kind.starts_with(&format!("{}/", protocol))
}

/// Iterate over the `kind` of every data source and template in a manifest.
fn manifest_kinds(raw: &serde_yaml::Mapping) -> impl Iterator<Item = &str> {
    ["dataSources", "templates"].into_iter().flat_map(|key| {
        raw.get(key)
            .and_then(|v| v.as_sequence())
            .into_iter()
            .flatten()
            .filter_map(|ds| ds.get("kind").and_then(|k| k.as_str()))
    })
}

/// Returns the base name (e.g. `cosmos`) of the first removed-protocol data
/// source or template in the manifest, if any.
fn manifest_removed_protocol(raw: &serde_yaml::Mapping) -> Option<String> {
    manifest_kinds(raw).find_map(|kind| {
        REMOVED_PROTOCOL_KINDS
            .iter()
            .find(|p| kind_belongs_to(kind, p))
            .map(|p| p.to_string())
    })
}

/// Returns true if the manifest contains a data source or template whose kind
/// belongs to a non-Ethereum protocol that the typed parser cannot handle.
fn manifest_needs_loose_parse(raw: &serde_yaml::Mapping) -> bool {
    manifest_kinds(raw).any(|kind| {
        LOOSE_PROTOCOL_KINDS
            .iter()
            .any(|p| kind_belongs_to(kind, p))
    })
}

/// Loosely parse a non-Ethereum manifest, extracting only the fields gnd needs
/// for codegen. Ethereum-specific fields (`source_abi`, handlers, start/end
/// block, source address) are left empty/default.
fn parse_manifest_loose(raw: &serde_yaml::Mapping) -> Result<Manifest> {
    let spec_version_str = raw
        .get("specVersion")
        .and_then(|v| v.as_str())
        .context("manifest is missing 'specVersion'")?;
    let spec_version = Version::parse(spec_version_str)
        .with_context(|| format!("invalid specVersion '{}'", spec_version_str))?;

    let schema = raw
        .get("schema")
        .and_then(|s| s.get("file"))
        .and_then(|f| f.as_str())
        .map(String::from);

    let features = raw
        .get("features")
        .and_then(|v| v.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|f| f.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let graft = raw.get("graft").and_then(|g| {
        let base = g.get("base").and_then(|b| b.as_str())?.to_string();
        let block = g.get("block").and_then(|b| b.as_u64()).unwrap_or(0);
        Some(GraftConfig { base, block })
    });

    let data_sources = loose_sequence(raw, "dataSources")
        .iter()
        .map(loose_data_source)
        .collect();

    let templates = loose_sequence(raw, "templates")
        .iter()
        .map(loose_template)
        .collect();

    Ok(Manifest {
        spec_version,
        schema,
        features,
        graft,
        data_sources,
        templates,
    })
}

/// Extract a top-level YAML sequence (e.g. `dataSources`) as a slice of values.
fn loose_sequence<'a>(raw: &'a serde_yaml::Mapping, key: &str) -> &'a [serde_yaml::Value] {
    raw.get(key)
        .and_then(|v| v.as_sequence())
        .map(|s| s.as_slice())
        .unwrap_or(&[])
}

/// Extract the `mapping.abis` entries from a data source / template YAML value.
fn loose_abis(ds: &serde_yaml::Value) -> Vec<Abi> {
    ds.get("mapping")
        .and_then(|m| m.get("abis"))
        .and_then(|a| a.as_sequence())
        .map(|seq| {
            seq.iter()
                .filter_map(|abi| {
                    let name = abi.get("name").and_then(|n| n.as_str())?.to_string();
                    let file = abi.get("file").and_then(|f| f.as_str())?.to_string();
                    Some(Abi { name, file })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Read a string field from a data source / template YAML value.
fn loose_str(ds: &serde_yaml::Value, key: &str) -> Option<String> {
    ds.get(key).and_then(|v| v.as_str()).map(String::from)
}

/// Read `mapping.<key>` as a string from a data source / template YAML value.
fn loose_mapping_str(ds: &serde_yaml::Value, key: &str) -> Option<String> {
    ds.get("mapping")
        .and_then(|m| m.get(key))
        .and_then(|v| v.as_str())
        .map(String::from)
}

/// Build a gnd `DataSource` from a loosely-parsed non-Ethereum YAML value.
fn loose_data_source(ds: &serde_yaml::Value) -> DataSource {
    DataSource {
        name: loose_str(ds, "name").unwrap_or_default(),
        kind: loose_str(ds, "kind").unwrap_or_default(),
        network: loose_str(ds, "network"),
        mapping_file: loose_mapping_str(ds, "file"),
        api_version: loose_mapping_str(ds, "apiVersion").and_then(|v| Version::parse(&v).ok()),
        abis: loose_abis(ds),
        source_address: None,
        source_abi: None,
        start_block: ds
            .get("source")
            .and_then(|s| s.get("startBlock"))
            .and_then(|b| b.as_u64())
            .unwrap_or(0),
        end_block: ds
            .get("source")
            .and_then(|s| s.get("endBlock"))
            .and_then(|b| b.as_u64()),
        event_handlers: vec![],
        call_handlers: vec![],
        block_handlers: vec![],
    }
}

/// Build a gnd `Template` from a loosely-parsed non-Ethereum YAML value.
fn loose_template(t: &serde_yaml::Value) -> Template {
    Template {
        name: loose_str(t, "name").unwrap_or_default(),
        kind: loose_str(t, "kind").unwrap_or_default(),
        network: loose_str(t, "network"),
        mapping_file: loose_mapping_str(t, "file"),
        api_version: loose_mapping_str(t, "apiVersion").and_then(|v| Version::parse(&v).ok()),
        abis: loose_abis(t),
        source_abi: None,
        event_handlers: vec![],
        call_handlers: vec![],
        block_handlers: vec![],
    }
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
    fn test_load_manifest_near() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        // A NEAR manifest: account-based source, receiptHandlers, no abis.
        // Previously this failed to parse because the typed parser is
        // Ethereum-only.
        let manifest_content = r#"
specVersion: 0.0.5
schema:
  file: ./schema.graphql
dataSources:
  - kind: near
    name: receipts
    network: near-mainnet
    source:
      account: wnear.flux-dev
      startBlock: 100
    mapping:
      apiVersion: 0.0.5
      language: wasm/assemblyscript
      entities:
        - ExampleEntity
      receiptHandlers:
        - handler: handleReceipt
      file: ./src/receipts.ts
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.spec_version, Version::new(0, 0, 5));
        assert_eq!(manifest.schema, Some("./schema.graphql".to_string()));
        assert_eq!(manifest.data_sources.len(), 1);
        let ds = &manifest.data_sources[0];
        assert_eq!(ds.name, "receipts");
        assert_eq!(ds.kind, "near");
        assert_eq!(ds.network, Some("near-mainnet".to_string()));
        assert_eq!(ds.mapping_file, Some("./src/receipts.ts".to_string()));
        assert_eq!(ds.start_block, 100);
        // NEAR has no ABIs and no Ethereum-style source.abi
        assert!(ds.abis.is_empty());
        assert!(ds.source_abi.is_none());
    }

    #[test]
    fn test_manifest_needs_loose_parse() {
        let near: serde_yaml::Mapping =
            serde_yaml::from_str("dataSources:\n  - kind: near\n    name: r\n").unwrap();
        assert!(manifest_needs_loose_parse(&near));

        let eth: serde_yaml::Mapping =
            serde_yaml::from_str("dataSources:\n  - kind: ethereum/contract\n    name: t\n")
                .unwrap();
        assert!(!manifest_needs_loose_parse(&eth));

        // Removed protocols are no longer loose-parsed (they are rejected).
        let cosmos: serde_yaml::Mapping =
            serde_yaml::from_str("dataSources:\n  - kind: cosmos\n    name: c\n").unwrap();
        assert!(!manifest_needs_loose_parse(&cosmos));
    }

    #[test]
    fn test_manifest_removed_protocol() {
        for kind in ["cosmos", "arweave", "substreams"] {
            let raw: serde_yaml::Mapping =
                serde_yaml::from_str(&format!("dataSources:\n  - kind: {}\n    name: d\n", kind))
                    .unwrap();
            assert_eq!(manifest_removed_protocol(&raw).as_deref(), Some(kind));
        }

        // Sub-kinds (e.g. `cosmos/events`) are matched and mapped to the base.
        let sub: serde_yaml::Mapping =
            serde_yaml::from_str("templates:\n  - kind: cosmos/events\n    name: d\n").unwrap();
        assert_eq!(manifest_removed_protocol(&sub).as_deref(), Some("cosmos"));

        // Supported kinds are not flagged.
        for kind in ["ethereum/contract", "near", "subgraph", "file/ipfs"] {
            let raw: serde_yaml::Mapping =
                serde_yaml::from_str(&format!("dataSources:\n  - kind: {}\n    name: d\n", kind))
                    .unwrap();
            assert_eq!(manifest_removed_protocol(&raw), None, "kind: {}", kind);
        }

        // The still-supported `file/arweave` offchain data source must NOT be
        // treated as the removed `arweave` chain.
        let file_arweave: serde_yaml::Mapping =
            serde_yaml::from_str("dataSources:\n  - kind: file/arweave\n    name: d\n").unwrap();
        assert_eq!(manifest_removed_protocol(&file_arweave), None);
    }

    #[test]
    fn test_load_manifest_removed_protocol_rejected() {
        for kind in ["cosmos", "arweave", "substreams"] {
            let temp_dir = TempDir::new().unwrap();
            let manifest_path = temp_dir.path().join("subgraph.yaml");

            let manifest_content = format!(
                r#"
specVersion: 0.0.5
schema:
  file: ./schema.graphql
dataSources:
  - kind: {}
    name: source
    network: somenet
    source:
      startBlock: 0
    mapping:
      apiVersion: 0.0.5
      language: wasm/assemblyscript
      entities:
        - ExampleEntity
      file: ./src/mapping.ts
"#,
                kind
            );

            fs::write(&manifest_path, manifest_content).unwrap();

            let result = load_manifest(&manifest_path);
            assert!(result.is_err(), "{} manifest should be rejected", kind);
            let err = format!("{:#}", result.unwrap_err());
            assert!(
                err.contains("no longer supports") && err.contains(kind),
                "Error should mention the dropped protocol, got: {}",
                err
            );
        }
    }

    #[test]
    fn test_load_manifest_file_arweave_not_rejected() {
        // A `file/arweave` offchain data source is still supported and must
        // parse via the typed path, not be rejected as the removed chain.
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        let manifest_content = r#"
specVersion: 1.0.0
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
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
templates:
  - kind: file/arweave
    name: ArweaveContent
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.7
      language: wasm/assemblyscript
      file: ./src/mapping.ts
      entities:
        - MyEntity
      abis: []
      handler: handleArweaveContent
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.templates.len(), 1);
        assert_eq!(manifest.templates[0].kind, "file/arweave");
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
