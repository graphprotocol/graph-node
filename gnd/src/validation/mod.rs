//! Schema and manifest validation using graph-node's validation logic.
//!
//! This module provides validation functions that use the same validation
//! logic as graph-node, ensuring developers get early feedback about issues
//! that would cause deployment failures.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use graph::abi::{Event, Function, JsonAbi};
use graph::data::subgraph::api_version::{LATEST_VERSION, MIN_SPEC_VERSION};
use graph::data::subgraph::manifest_validation;
use graph::data::subgraph::SubgraphManifestValidationError;
use graph::prelude::DeploymentHash;
use graph::schema::{InputSchema, SchemaValidationError};
use semver::Version;

use crate::manifest::{Abi, BlockHandlerFilterKind, CallHandler, DataSource, Manifest, Template};

/// Validate a GraphQL schema using graph-node's InputSchema validation.
///
/// Returns a list of validation errors. An empty list means the schema is valid.
pub fn validate_schema(
    schema_path: &Path,
    spec_version: &Version,
) -> Result<Vec<SchemaValidationError>> {
    let schema_str = std::fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema: {}", schema_path.display()))?;

    // Use a dummy deployment hash for local validation
    // The hash must be alphanumeric only (no dashes or special chars)
    let id = DeploymentHash::new("QmLocalValidation").unwrap();

    Ok(InputSchema::validate(spec_version, &schema_str, id))
}

/// Format validation errors for display.
pub fn format_schema_errors(errors: &[SchemaValidationError]) -> String {
    errors
        .iter()
        .map(|e| format!("  - {}", e))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Errors that can occur during manifest validation in gnd.
#[derive(Debug)]
pub enum ManifestValidationError {
    /// Error from shared validation logic.
    Shared(Box<SubgraphManifestValidationError>),
    /// Unsupported spec version.
    UnsupportedSpecVersion {
        version: Version,
        min: Version,
        max: Version,
    },
    /// File not found.
    FileNotFound { path: String, description: String },
    /// File could not be parsed.
    InvalidFile { path: String, reason: String },
    /// source.abi references an ABI name not listed in mapping.abis.
    SourceAbiNotInMappingAbis {
        data_source: String,
        source_abi: String,
        available: Vec<String>,
    },
    /// Duplicate data source or template name.
    DuplicateName { kind: &'static str, name: String },
    /// Ethereum data source has no handlers defined.
    NoHandlers { data_source: String },
    /// Invalid data source kind (not a valid Ethereum kind).
    InvalidKind { data_source: String, kind: String },
    /// Call or block handlers require a source address.
    SourceAddressRequired { data_source: String },
    /// Block handler constraint violation.
    BlockHandlerConstraint { data_source: String, reason: String },
    /// Event handler receipt requires apiVersion >= 0.0.7.
    ReceiptRequiresApiVersion { data_source: String },
    /// Eth call declarations require specVersion >= 1.2.0.
    CallDeclsRequireSpecVersion { data_source: String },
    /// Handler names from the manifest are missing as exports in the compiled WASM.
    MissingWasmHandlers {
        data_source: String,
        missing: Vec<String>,
    },
    /// Failed to parse a compiled WASM file.
    InvalidWasm { path: String, reason: String },
    /// Event handler references an event signature not found in the ABI.
    EventNotInAbi {
        data_source: String,
        event: String,
        abi_name: String,
    },
    /// Call handler references a function signature not found in the ABI.
    FunctionNotInAbi {
        data_source: String,
        function: String,
        abi_name: String,
    },
}

impl std::fmt::Display for ManifestValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ManifestValidationError::Shared(e) => write!(f, "{}", e),
            ManifestValidationError::UnsupportedSpecVersion { version, min, max } => {
                write!(
                    f,
                    "unsupported spec version {}: must be between {} and {}",
                    version, min, max
                )
            }
            ManifestValidationError::FileNotFound { path, description } => {
                write!(f, "{} not found: {}", description, path)
            }
            ManifestValidationError::InvalidFile { path, reason } => {
                write!(f, "invalid file {}: {}", path, reason)
            }
            ManifestValidationError::SourceAbiNotInMappingAbis {
                data_source,
                source_abi,
                available,
            } => {
                if available.is_empty() {
                    write!(
                        f,
                        "data source '{}': source.abi '{}' not found in mapping.abis (no ABIs defined)",
                        data_source, source_abi
                    )
                } else {
                    write!(
                        f,
                        "data source '{}': source.abi '{}' not found in mapping.abis (available: {})",
                        data_source,
                        source_abi,
                        available.join(", ")
                    )
                }
            }
            ManifestValidationError::DuplicateName { kind, name } => {
                write!(f, "duplicate {} name: '{}'", kind, name)
            }
            ManifestValidationError::NoHandlers { data_source } => {
                write!(
                    f,
                    "data source '{}': no event, call, or block handlers defined",
                    data_source
                )
            }
            ManifestValidationError::InvalidKind { data_source, kind } => {
                write!(
                    f,
                    "data source '{}': invalid kind '{}', expected 'ethereum' or 'ethereum/contract'",
                    data_source, kind
                )
            }
            ManifestValidationError::SourceAddressRequired { data_source } => {
                write!(
                    f,
                    "data source '{}': source address is required when using call or block handlers",
                    data_source
                )
            }
            ManifestValidationError::BlockHandlerConstraint {
                data_source,
                reason,
            } => {
                write!(f, "data source '{}': {}", data_source, reason)
            }
            ManifestValidationError::ReceiptRequiresApiVersion { data_source } => {
                write!(
                    f,
                    "data source '{}': event handlers that require transaction receipts need apiVersion >= 0.0.7",
                    data_source
                )
            }
            ManifestValidationError::CallDeclsRequireSpecVersion { data_source } => {
                write!(
                    f,
                    "data source '{}': eth call declarations on event handlers require specVersion >= 1.2.0",
                    data_source
                )
            }
            ManifestValidationError::MissingWasmHandlers {
                data_source,
                missing,
            } => {
                write!(
                    f,
                    "data source '{}': missing WASM handlers: {}",
                    data_source,
                    missing.join(", ")
                )
            }
            ManifestValidationError::InvalidWasm { path, reason } => {
                write!(f, "invalid WASM file {}: {}", path, reason)
            }
            ManifestValidationError::EventNotInAbi {
                data_source,
                event,
                abi_name,
            } => {
                write!(
                    f,
                    "data source '{}': event '{}' not found in ABI '{}'",
                    data_source, event, abi_name
                )
            }
            ManifestValidationError::FunctionNotInAbi {
                data_source,
                function,
                abi_name,
            } => {
                write!(
                    f,
                    "data source '{}': function '{}' not found in ABI '{}'",
                    data_source, function, abi_name
                )
            }
        }
    }
}

impl From<SubgraphManifestValidationError> for ManifestValidationError {
    fn from(e: SubgraphManifestValidationError) -> Self {
        ManifestValidationError::Shared(Box::new(e))
    }
}

/// Validate manifest file references.
///
/// Checks that all referenced files exist, ABI files are valid JSON,
/// source.abi cross-references mapping.abis, names are unique,
/// Ethereum data sources have at least one handler, and handler
/// signatures match events/functions in the ABI. This validation
/// runs before codegen to catch errors early.
///
/// Note: Ethereum structural constraints (kind, address, block handler
/// combos, receipt/call-decl version requirements) are validated separately
/// in `validate_manifest` which runs during build.
pub(crate) fn validate_manifest_files(
    manifest: &Manifest,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    // Validate file existence
    errors.extend(validate_file_existence(manifest, manifest_dir));

    // Validate ABIs are valid JSON
    errors.extend(validate_abis(manifest, manifest_dir));

    // Validate source.abi references an ABI in mapping.abis
    errors.extend(validate_source_abi_references(manifest));

    // Validate unique data source and template names
    errors.extend(validate_unique_names(manifest));

    // Validate Ethereum data sources have at least one handler
    errors.extend(validate_handler_presence(manifest));

    // Validate event/call handler signatures match ABI definitions
    errors.extend(validate_handler_signatures(manifest, manifest_dir));

    errors
}

/// Validate a subgraph manifest.
///
/// This performs several validation checks:
/// - At least one data source exists
/// - All data sources use the same network
/// - All data sources use consistent API versions
/// - Spec version is within supported range
/// - Referenced files exist (schema, mappings, ABIs)
/// - ABI files are valid JSON
/// - source.abi references an ABI in mapping.abis
/// - Data source and template names are unique
/// - Ethereum data sources have at least one handler
pub(crate) fn validate_manifest(
    manifest: &Manifest,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    // Validate using shared validation functions
    if let Err(e) = manifest_validation::validate_has_data_sources(manifest.total_source_count()) {
        errors.push(e.into());
    }

    // Validate spec version (gnd-specific check)
    if manifest.spec_version < MIN_SPEC_VERSION || manifest.spec_version > *LATEST_VERSION {
        errors.push(ManifestValidationError::UnsupportedSpecVersion {
            version: manifest.spec_version.clone(),
            min: MIN_SPEC_VERSION.clone(),
            max: LATEST_VERSION.clone(),
        });
    }

    // Collect networks from data sources and templates
    let networks: Vec<Option<&str>> = manifest
        .data_sources
        .iter()
        .map(|ds| ds.network.as_deref())
        .chain(manifest.templates.iter().map(|t| t.network.as_deref()))
        .collect();

    // Only validate networks if there are data sources
    if !manifest.data_sources.is_empty() {
        if let Err(e) = manifest_validation::validate_single_network(&networks) {
            errors.push(e.into());
        }
    }

    // Collect API versions from data sources and templates
    let api_versions: Vec<Version> = manifest
        .data_sources
        .iter()
        .filter_map(|ds| ds.api_version.clone())
        .chain(
            manifest
                .templates
                .iter()
                .filter_map(|t| t.api_version.clone()),
        )
        .collect();

    if let Err(e) = manifest_validation::validate_api_versions(&api_versions) {
        errors.push(e.into());
    }

    // Validate file existence
    errors.extend(validate_file_existence(manifest, manifest_dir));

    // Validate ABIs are valid JSON
    errors.extend(validate_abis(manifest, manifest_dir));

    // Validate source.abi references an ABI in mapping.abis
    errors.extend(validate_source_abi_references(manifest));

    // Validate unique data source and template names
    errors.extend(validate_unique_names(manifest));

    // Validate Ethereum data sources have at least one handler
    errors.extend(validate_handler_presence(manifest));

    // Validate Ethereum-specific structural constraints
    errors.extend(validate_ethereum_constraints(manifest));

    // Validate event/call handler signatures match ABI definitions
    errors.extend(validate_handler_signatures(manifest, manifest_dir));

    errors
}

/// Validate that all referenced files exist.
fn validate_file_existence(
    manifest: &Manifest,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    // Check schema file
    if let Some(schema_path) = &manifest.schema {
        let full_path = manifest_dir.join(schema_path.as_str());
        if !full_path.exists() {
            errors.push(ManifestValidationError::FileNotFound {
                path: schema_path.to_string(),
                description: "Schema file".to_string(),
            });
        }
    }

    // Check data source files
    for ds in &manifest.data_sources {
        errors.extend(validate_data_source_files(ds, manifest_dir));
    }

    // Check template files
    for template in &manifest.templates {
        errors.extend(validate_template_files(template, manifest_dir));
    }

    errors
}

/// Validate data source file references.
fn validate_data_source_files(
    ds: &DataSource,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    // Check mapping file
    if let Some(mapping_path) = &ds.mapping_file {
        let full_path = manifest_dir.join(mapping_path.as_str());
        if !full_path.exists() {
            errors.push(ManifestValidationError::FileNotFound {
                path: mapping_path.to_string(),
                description: format!("Mapping file for data source '{}'", ds.name),
            });
        }
    }

    // Check ABI files
    for abi in &ds.abis {
        let full_path = manifest_dir.join(&abi.file);
        if !full_path.exists() {
            errors.push(ManifestValidationError::FileNotFound {
                path: abi.file.clone(),
                description: format!("ABI '{}' for data source '{}'", abi.name, ds.name),
            });
        }
    }

    errors
}

/// Validate template file references.
fn validate_template_files(
    template: &Template,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    // Check mapping file
    if let Some(mapping_path) = &template.mapping_file {
        let full_path = manifest_dir.join(mapping_path.as_str());
        if !full_path.exists() {
            errors.push(ManifestValidationError::FileNotFound {
                path: mapping_path.to_string(),
                description: format!("Mapping file for template '{}'", template.name),
            });
        }
    }

    errors
}

/// Validate that ABI files are valid JSON.
fn validate_abis(manifest: &Manifest, manifest_dir: &Path) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    for ds in &manifest.data_sources {
        errors.extend(validate_data_source_abis(ds, manifest_dir));
    }

    errors
}

/// Validate ABIs for a data source.
fn validate_data_source_abis(ds: &DataSource, manifest_dir: &Path) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    for abi in &ds.abis {
        if let Err(e) = validate_abi_file(abi, manifest_dir) {
            errors.push(e);
        }
    }

    errors
}

/// Validate a single ABI file.
fn validate_abi_file(abi: &Abi, manifest_dir: &Path) -> Result<(), ManifestValidationError> {
    let full_path = manifest_dir.join(&abi.file);

    // Only validate if file exists (file existence is checked separately)
    if !full_path.exists() {
        return Ok(());
    }

    let content =
        std::fs::read_to_string(&full_path).map_err(|e| ManifestValidationError::InvalidFile {
            path: abi.file.clone(),
            reason: format!("failed to read: {}", e),
        })?;

    // Try to parse as JSON
    let _: serde_json::Value =
        serde_json::from_str(&content).map_err(|e| ManifestValidationError::InvalidFile {
            path: abi.file.clone(),
            reason: format!("invalid JSON: {}", e),
        })?;

    Ok(())
}

/// Validate that each data source's `source.abi` references an ABI listed in `mapping.abis`.
fn validate_source_abi_references(manifest: &Manifest) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    for ds in &manifest.data_sources {
        if let Some(source_abi) = &ds.source_abi {
            if !ds.abis.iter().any(|a| a.name == *source_abi) {
                errors.push(ManifestValidationError::SourceAbiNotInMappingAbis {
                    data_source: ds.name.clone(),
                    source_abi: source_abi.clone(),
                    available: ds.abis.iter().map(|a| a.name.clone()).collect(),
                });
            }
        }
    }

    for t in &manifest.templates {
        if let Some(source_abi) = &t.source_abi {
            if !t.abis.iter().any(|a| a.name == *source_abi) {
                errors.push(ManifestValidationError::SourceAbiNotInMappingAbis {
                    data_source: t.name.clone(),
                    source_abi: source_abi.clone(),
                    available: t.abis.iter().map(|a| a.name.clone()).collect(),
                });
            }
        }
    }

    errors
}

/// Validate that data source and template names are unique.
fn validate_unique_names(manifest: &Manifest) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();
    let mut seen_ds_names = std::collections::HashSet::new();
    let mut seen_template_names = std::collections::HashSet::new();

    for ds in &manifest.data_sources {
        if !seen_ds_names.insert(&ds.name) {
            errors.push(ManifestValidationError::DuplicateName {
                kind: "data source",
                name: ds.name.clone(),
            });
        }
    }

    for t in &manifest.templates {
        if !seen_template_names.insert(&t.name) {
            errors.push(ManifestValidationError::DuplicateName {
                kind: "template",
                name: t.name.clone(),
            });
        }
    }

    errors
}

/// Validate that Ethereum data sources have at least one handler defined.
fn validate_handler_presence(manifest: &Manifest) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    for ds in &manifest.data_sources {
        // Only check Ethereum data sources (which have source_abi set)
        if ds.source_abi.is_some()
            && ds.event_handlers.is_empty()
            && ds.call_handlers.is_empty()
            && ds.block_handlers.is_empty()
        {
            errors.push(ManifestValidationError::NoHandlers {
                data_source: ds.name.clone(),
            });
        }
    }

    for t in &manifest.templates {
        if t.source_abi.is_some()
            && t.event_handlers.is_empty()
            && t.call_handlers.is_empty()
            && t.block_handlers.is_empty()
        {
            errors.push(ManifestValidationError::NoHandlers {
                data_source: t.name.clone(),
            });
        }
    }

    errors
}

/// Valid Ethereum data source kinds (matches graph-node's `ETHEREUM_KINDS`).
const ETHEREUM_KINDS: &[&str] = &["ethereum/contract", "ethereum"];

/// Validate Ethereum-specific structural constraints on data sources and templates.
///
/// This covers:
/// - Data source kind must be `ethereum` or `ethereum/contract`
/// - Source address is required when call or block handlers are present
/// - Block handler filter constraints (no duplicates, no invalid combos)
/// - Event handler receipt requires apiVersion >= 0.0.7
/// - Eth call declarations on event handlers require specVersion >= 1.2.0
fn validate_ethereum_constraints(manifest: &Manifest) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    for ds in &manifest.data_sources {
        // Only validate Ethereum data sources (identified by having source_abi)
        if ds.source_abi.is_none() {
            continue;
        }
        errors.extend(validate_ethereum_kind(&ds.name, &ds.kind));
        errors.extend(validate_source_address_required(
            &ds.name,
            ds.source_address.is_some(),
            &ds.call_handlers,
            &ds.block_handlers,
        ));
        errors.extend(validate_block_handler_constraints(
            &ds.name,
            &ds.block_handlers,
        ));
        errors.extend(validate_receipt_api_version(
            &ds.name,
            ds.api_version.as_ref(),
            &ds.event_handlers,
        ));
        errors.extend(validate_call_decls_spec_version(
            &ds.name,
            &manifest.spec_version,
            &ds.event_handlers,
        ));
    }

    for t in &manifest.templates {
        if t.source_abi.is_none() {
            continue;
        }
        errors.extend(validate_ethereum_kind(&t.name, &t.kind));
        // Templates don't have source addresses — skip address validation
        errors.extend(validate_block_handler_constraints(
            &t.name,
            &t.block_handlers,
        ));
        errors.extend(validate_receipt_api_version(
            &t.name,
            t.api_version.as_ref(),
            &t.event_handlers,
        ));
        errors.extend(validate_call_decls_spec_version(
            &t.name,
            &manifest.spec_version,
            &t.event_handlers,
        ));
    }

    errors
}

/// Validate that an Ethereum data source has a valid kind.
fn validate_ethereum_kind(name: &str, kind: &str) -> Vec<ManifestValidationError> {
    if ETHEREUM_KINDS.contains(&kind) {
        vec![]
    } else {
        vec![ManifestValidationError::InvalidKind {
            data_source: name.to_string(),
            kind: kind.to_string(),
        }]
    }
}

/// Validate that a source address is present when call or block handlers are used.
fn validate_source_address_required(
    name: &str,
    has_address: bool,
    call_handlers: &[CallHandler],
    block_handlers: &[crate::manifest::BlockHandler],
) -> Vec<ManifestValidationError> {
    if !has_address && (!call_handlers.is_empty() || !block_handlers.is_empty()) {
        vec![ManifestValidationError::SourceAddressRequired {
            data_source: name.to_string(),
        }]
    } else {
        vec![]
    }
}

/// Validate block handler constraints:
/// - At most one handler of each filter type
/// - Non-filtered handlers cannot be mixed with polling or once handlers
fn validate_block_handler_constraints(
    name: &str,
    block_handlers: &[crate::manifest::BlockHandler],
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    let mut non_filtered = 0u32;
    let mut call_filtered = 0u32;
    let mut polling_filtered = 0u32;
    let mut once_filtered = 0u32;

    for handler in block_handlers {
        match &handler.filter {
            None => non_filtered += 1,
            Some(BlockHandlerFilterKind::Call) => call_filtered += 1,
            Some(BlockHandlerFilterKind::Once) => once_filtered += 1,
            Some(BlockHandlerFilterKind::Polling) => polling_filtered += 1,
        }
    }

    // Check for duplicates of any type
    if non_filtered > 1 || call_filtered > 1 || once_filtered > 1 || polling_filtered > 1 {
        errors.push(ManifestValidationError::BlockHandlerConstraint {
            data_source: name.to_string(),
            reason: "duplicated block handlers".to_string(),
        });
    }

    // Non-filtered handlers cannot be mixed with polling or once handlers
    // (mixing with call filter is allowed)
    if non_filtered > 0 && (polling_filtered > 0 || once_filtered > 0) {
        errors.push(ManifestValidationError::BlockHandlerConstraint {
            data_source: name.to_string(),
            reason: "combination of non-filtered and polling/once block handlers is not allowed"
                .to_string(),
        });
    }

    errors
}

/// Validate that event handlers requiring receipts have apiVersion >= 0.0.7.
fn validate_receipt_api_version(
    name: &str,
    api_version: Option<&Version>,
    event_handlers: &[crate::manifest::EventHandler],
) -> Vec<ManifestValidationError> {
    let api_version = match api_version {
        Some(v) => v,
        None => return vec![],
    };

    if *api_version >= Version::new(0, 0, 7) {
        return vec![];
    }

    if event_handlers.iter().any(|h| h.receipt) {
        vec![ManifestValidationError::ReceiptRequiresApiVersion {
            data_source: name.to_string(),
        }]
    } else {
        vec![]
    }
}

/// Validate that event handlers with eth call declarations have specVersion >= 1.2.0.
fn validate_call_decls_spec_version(
    name: &str,
    spec_version: &Version,
    event_handlers: &[crate::manifest::EventHandler],
) -> Vec<ManifestValidationError> {
    if *spec_version >= Version::new(1, 2, 0) {
        return vec![];
    }

    if event_handlers.iter().any(|h| h.has_call_decls) {
        vec![ManifestValidationError::CallDeclsRequireSpecVersion {
            data_source: name.to_string(),
        }]
    } else {
        vec![]
    }
}

/// Validate that event and call handler signatures match events/functions in the ABI.
///
/// For each Ethereum data source (and template) with a `source.abi`, loads the
/// corresponding ABI file and checks:
/// - Each event handler's `event` signature matches an event in the ABI
/// - Each call handler's `function` signature matches a function in the ABI
fn validate_handler_signatures(
    manifest: &Manifest,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    for ds in &manifest.data_sources {
        if let Some(source_abi) = &ds.source_abi {
            if let Some(abi_entry) = ds.abis.iter().find(|a| a.name == *source_abi) {
                let abi_path = manifest_dir.join(&abi_entry.file);
                if let Some(contract) = load_abi(&abi_path) {
                    errors.extend(validate_event_signatures(
                        &ds.name,
                        source_abi,
                        &ds.event_handlers,
                        &contract,
                    ));
                    errors.extend(validate_function_signatures(
                        &ds.name,
                        source_abi,
                        &ds.call_handlers,
                        &contract,
                    ));
                }
            }
        }
    }

    for t in &manifest.templates {
        if let Some(source_abi) = &t.source_abi {
            if let Some(abi_entry) = t.abis.iter().find(|a| a.name == *source_abi) {
                let abi_path = manifest_dir.join(&abi_entry.file);
                if let Some(contract) = load_abi(&abi_path) {
                    errors.extend(validate_event_signatures(
                        &t.name,
                        source_abi,
                        &t.event_handlers,
                        &contract,
                    ));
                    errors.extend(validate_function_signatures(
                        &t.name,
                        source_abi,
                        &t.call_handlers,
                        &contract,
                    ));
                }
            }
        }
    }

    errors
}

/// Try to load an ABI file as a `JsonAbi`.
///
/// Returns `None` if the file doesn't exist or can't be parsed (those errors
/// are reported separately by file existence and ABI JSON validation).
fn load_abi(abi_path: &Path) -> Option<JsonAbi> {
    let content = std::fs::read_to_string(abi_path).ok()?;
    let normalized = crate::abi::normalize_abi_json(&content).ok()?;
    let json_str = normalized.to_string();
    serde_json::from_str(&json_str).ok()
}

/// Build the event signature with `indexed` hints: `Name(indexed type1,type2,...)`.
fn event_signature_with_indexed(event: &Event) -> String {
    let params: Vec<String> = event
        .inputs
        .iter()
        .map(|p| {
            let ty = p.selector_type();
            if p.indexed {
                format!("indexed {}", ty)
            } else {
                ty.into_owned()
            }
        })
        .collect();
    format!("{}({})", event.name, params.join(","))
}

/// Validate that each event handler's event signature matches an event in the ABI.
///
/// Matching follows the same logic as graph-node's `contract_event_with_signature`:
/// 1. Try exact match with `indexed` hints
/// 2. Fall back to matching without `indexed` if only one event variant exists
fn validate_event_signatures(
    ds_name: &str,
    abi_name: &str,
    event_handlers: &[crate::manifest::EventHandler],
    abi: &JsonAbi,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    let all_events: Vec<&Event> = abi.events.values().flatten().collect();

    for handler in event_handlers {
        let signature = &handler.event;

        // Try exact match with indexed hints
        let exact_match = all_events
            .iter()
            .any(|e| event_signature_with_indexed(e) == *signature);

        if exact_match {
            continue;
        }

        // Fallback: match without indexed if only one event variant with that name
        let event_name = signature.split('(').next().unwrap_or("");
        let matching_by_name: Vec<&&Event> =
            all_events.iter().filter(|e| e.name == event_name).collect();

        let fallback_match =
            matching_by_name.len() == 1 && matching_by_name[0].signature() == *signature;

        if !fallback_match {
            errors.push(ManifestValidationError::EventNotInAbi {
                data_source: ds_name.to_string(),
                event: signature.clone(),
                abi_name: abi_name.to_string(),
            });
        }
    }

    errors
}

/// Validate that each call handler's function signature matches a function in the ABI.
fn validate_function_signatures(
    ds_name: &str,
    abi_name: &str,
    call_handlers: &[CallHandler],
    abi: &JsonAbi,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    let all_functions: Vec<&Function> = abi.functions.values().flatten().collect();

    for handler in call_handlers {
        let target = &handler.function;

        let found = all_functions.iter().any(|f| f.signature() == *target);

        if !found {
            errors.push(ManifestValidationError::FunctionNotInAbi {
                data_source: ds_name.to_string(),
                function: target.clone(),
                abi_name: abi_name.to_string(),
            });
        }
    }

    errors
}

/// Format manifest validation errors for display.
pub(crate) fn format_manifest_errors(errors: &[ManifestValidationError]) -> String {
    errors
        .iter()
        .map(|e| format!("  - {}", e))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Extract exported function names from a WASM binary.
fn wasm_exported_functions(wasm_bytes: &[u8]) -> Result<HashSet<String>> {
    use wasmparser::Payload;

    let mut exports = HashSet::new();
    for payload in wasmparser::Parser::new(0).parse_all(wasm_bytes) {
        if let Payload::ExportSection(s) = payload? {
            for export in s {
                let export = export?;
                if export.kind == wasmparser::ExternalKind::Func {
                    exports.insert(export.name.to_string());
                }
            }
        }
    }
    Ok(exports)
}

/// Validate that all handler names from a data source exist as exports in the
/// compiled WASM file. Returns errors for any handlers missing from the WASM.
pub(crate) fn validate_wasm_handlers(
    wasm_path: &Path,
    ds_name: &str,
    handler_names: &[&str],
) -> Vec<ManifestValidationError> {
    let wasm_bytes = match std::fs::read(wasm_path) {
        Ok(bytes) => bytes,
        Err(e) => {
            return vec![ManifestValidationError::InvalidWasm {
                path: wasm_path.display().to_string(),
                reason: format!("failed to read: {}", e),
            }];
        }
    };

    let exports = match wasm_exported_functions(&wasm_bytes) {
        Ok(exports) => exports,
        Err(e) => {
            return vec![ManifestValidationError::InvalidWasm {
                path: wasm_path.display().to_string(),
                reason: format!("failed to parse: {}", e),
            }];
        }
    };

    let missing: Vec<String> = handler_names
        .iter()
        .filter(|name| !exports.contains(**name))
        .map(|name| name.to_string())
        .collect();

    if missing.is_empty() {
        vec![]
    } else {
        vec![ManifestValidationError::MissingWasmHandlers {
            data_source: ds_name.to_string(),
            missing,
        }]
    }
}

/// Collect all handler names from a data source.
pub(crate) fn collect_handler_names(ds: &DataSource) -> Vec<&str> {
    let mut names = Vec::new();
    for h in &ds.event_handlers {
        names.push(h.handler.as_str());
    }
    for h in &ds.call_handlers {
        names.push(h.handler.as_str());
    }
    for h in &ds.block_handlers {
        names.push(h.handler.as_str());
    }
    names
}

/// Collect all handler names from a template.
pub(crate) fn collect_template_handler_names(t: &Template) -> Vec<&str> {
    let mut names = Vec::new();
    for h in &t.event_handlers {
        names.push(h.handler.as_str());
    }
    for h in &t.call_handlers {
        names.push(h.handler.as_str());
    }
    for h in &t.block_handlers {
        names.push(h.handler.as_str());
    }
    names
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{BlockHandler, EventHandler};
    use std::fs;
    use tempfile::TempDir;

    fn event_handler(name: &str) -> EventHandler {
        EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: name.to_string(),
            receipt: false,
            has_call_decls: false,
        }
    }

    fn call_handler(name: &str) -> CallHandler {
        CallHandler {
            function: "approve(address,uint256)".to_string(),
            handler: name.to_string(),
        }
    }

    fn block_handler(name: &str) -> BlockHandler {
        BlockHandler {
            handler: name.to_string(),
            filter: None,
        }
    }

    fn block_handler_with_filter(name: &str, filter: BlockHandlerFilterKind) -> BlockHandler {
        BlockHandler {
            handler: name.to_string(),
            filter: Some(filter),
        }
    }

    #[test]
    fn test_validate_valid_schema() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.graphql");

        let schema_content = r#"
type MyEntity @entity {
  id: ID!
  name: String!
}
"#;
        fs::write(&schema_path, schema_content).unwrap();

        let spec_version = Version::new(0, 0, 4);
        let errors = validate_schema(&schema_path, &spec_version).unwrap();
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_missing_entity_directive() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.graphql");

        // Schema without @entity directive
        let schema_content = r#"
type MyEntity {
  id: ID!
  name: String!
}
"#;
        fs::write(&schema_path, schema_content).unwrap();

        let spec_version = Version::new(0, 0, 4);
        let errors = validate_schema(&schema_path, &spec_version).unwrap();
        assert!(!errors.is_empty(), "Expected validation errors");
    }

    #[test]
    fn test_validate_missing_id_field() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.graphql");

        // Schema without id field
        let schema_content = r#"
type MyEntity @entity {
  name: String!
}
"#;
        fs::write(&schema_path, schema_content).unwrap();

        let spec_version = Version::new(0, 0, 4);
        let errors = validate_schema(&schema_path, &spec_version).unwrap();
        assert!(!errors.is_empty(), "Expected validation errors");
        let formatted = format_schema_errors(&errors);
        assert!(
            formatted.contains("id"),
            "Error should mention missing id field"
        );
    }

    #[test]
    fn test_validate_invalid_derived_from() {
        let temp_dir = TempDir::new().unwrap();
        let schema_path = temp_dir.path().join("schema.graphql");

        // Schema with invalid @derivedFrom
        let schema_content = r#"
type User @entity {
  id: ID!
  posts: [Post!]! @derivedFrom(field: "nonexistent")
}

type Post @entity {
  id: ID!
  author: User!
}
"#;
        fs::write(&schema_path, schema_content).unwrap();

        let spec_version = Version::new(0, 0, 4);
        let errors = validate_schema(&schema_path, &spec_version).unwrap();
        assert!(!errors.is_empty(), "Expected validation errors");
    }

    #[test]
    fn test_format_schema_errors() {
        let errors = vec![
            SchemaValidationError::IdFieldMissing("TestEntity".to_string()),
            SchemaValidationError::InterfaceUndefined("IFoo".to_string()),
        ];

        let formatted = format_schema_errors(&errors);
        assert!(formatted.contains("TestEntity"));
        assert!(formatted.contains("IFoo"));
        assert!(formatted.contains("  - ")); // Check indent
    }

    // Helper to create a test manifest
    fn create_test_manifest(data_sources: Vec<DataSource>, templates: Vec<Template>) -> Manifest {
        Manifest {
            spec_version: Version::new(0, 0, 4),
            schema: Some("schema.graphql".to_string()),
            features: vec![],
            graft: None,
            data_sources,
            templates,
        }
    }

    fn create_data_source(
        name: &str,
        network: Option<&str>,
        api_version: Option<Version>,
    ) -> DataSource {
        DataSource {
            name: name.to_string(),
            kind: "ethereum/contract".to_string(),
            network: network.map(String::from),
            mapping_file: Some(format!("src/{}.ts", name)),
            api_version,
            abis: vec![],
            source_address: None,
            source_abi: None,
            start_block: 0,
            end_block: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        }
    }

    fn create_subgraph_data_source(name: &str, address: &str) -> DataSource {
        DataSource {
            name: name.to_string(),
            kind: "subgraph".to_string(),
            network: None,
            mapping_file: None,
            api_version: None,
            abis: vec![],
            source_address: Some(address.to_string()),
            source_abi: None,
            start_block: 0,
            end_block: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        }
    }

    #[test]
    fn test_validate_manifest_no_data_sources() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = create_test_manifest(vec![], vec![]);

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.iter().any(|e| matches!(
            e,
            ManifestValidationError::Shared(e) if matches!(e.as_ref(), SubgraphManifestValidationError::NoDataSources)
        )));
    }

    #[test]
    fn test_validate_manifest_only_subgraph_sources() {
        let temp_dir = TempDir::new().unwrap();

        // Create schema file
        fs::write(
            temp_dir.path().join("schema.graphql"),
            "type T @entity { id: ID! }",
        )
        .unwrap();

        // Manifest with only subgraph sources (no regular data sources)
        let data_sources = vec![create_subgraph_data_source(
            "SourceSubgraph",
            "QmSourceHash123",
        )];
        let manifest = Manifest {
            spec_version: Version::new(1, 0, 0),
            schema: Some("schema.graphql".to_string()),
            features: vec![],
            graft: None,
            data_sources,
            templates: vec![],
        };

        let errors = validate_manifest(&manifest, temp_dir.path());

        // Should NOT have "no data sources" error since we have subgraph sources
        assert!(
            !errors.iter().any(|e| matches!(
                e,
                ManifestValidationError::Shared(e) if matches!(e.as_ref(), SubgraphManifestValidationError::NoDataSources)
            )),
            "Manifest with subgraph sources should not fail 'no data sources' validation"
        );
    }

    #[test]
    fn test_validate_manifest_multiple_networks() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = create_test_manifest(
            vec![
                create_data_source("ds1", Some("mainnet"), None),
                create_data_source("ds2", Some("ropsten"), None),
            ],
            vec![],
        );

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.iter().any(|e| matches!(
            e,
            ManifestValidationError::Shared(e) if matches!(e.as_ref(), SubgraphManifestValidationError::MultipleEthereumNetworks)
        )));
    }

    #[test]
    fn test_validate_manifest_different_api_versions() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = create_test_manifest(
            vec![
                create_data_source("ds1", Some("mainnet"), Some(Version::new(0, 0, 5))),
                create_data_source("ds2", Some("mainnet"), Some(Version::new(0, 0, 6))),
            ],
            vec![],
        );

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.iter().any(|e| matches!(
            e,
            ManifestValidationError::Shared(e) if matches!(e.as_ref(), SubgraphManifestValidationError::DifferentApiVersions(_))
        )));
    }

    #[test]
    fn test_validate_manifest_missing_schema_file() {
        let temp_dir = TempDir::new().unwrap();
        let manifest = create_test_manifest(
            vec![create_data_source("ds1", Some("mainnet"), None)],
            vec![],
        );

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.iter().any(|e| matches!(
            e,
            ManifestValidationError::FileNotFound { description, .. } if description.contains("Schema")
        )));
    }

    #[test]
    fn test_validate_manifest_missing_mapping_file() {
        let temp_dir = TempDir::new().unwrap();

        // Create schema file
        fs::write(
            temp_dir.path().join("schema.graphql"),
            "type T @entity { id: ID! }",
        )
        .unwrap();

        let manifest = create_test_manifest(
            vec![create_data_source("ds1", Some("mainnet"), None)],
            vec![],
        );

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.iter().any(|e| matches!(
            e,
            ManifestValidationError::FileNotFound { description, .. } if description.contains("Mapping")
        )));
    }

    #[test]
    fn test_validate_manifest_invalid_abi_json() {
        let temp_dir = TempDir::new().unwrap();

        // Create schema and mapping files
        fs::write(
            temp_dir.path().join("schema.graphql"),
            "type T @entity { id: ID! }",
        )
        .unwrap();
        fs::create_dir_all(temp_dir.path().join("src")).unwrap();
        fs::write(temp_dir.path().join("src/ds1.ts"), "// mapping").unwrap();

        // Create invalid ABI file
        fs::create_dir_all(temp_dir.path().join("abis")).unwrap();
        fs::write(temp_dir.path().join("abis/ERC20.json"), "not valid json").unwrap();

        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];

        let manifest = create_test_manifest(vec![ds], vec![]);

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.iter().any(|e| matches!(
            e,
            ManifestValidationError::InvalidFile { reason, .. } if reason.contains("JSON")
        )));
    }

    #[test]
    fn test_validate_manifest_valid() {
        let temp_dir = TempDir::new().unwrap();

        // Create all required files
        fs::write(
            temp_dir.path().join("schema.graphql"),
            "type T @entity { id: ID! }",
        )
        .unwrap();
        fs::create_dir_all(temp_dir.path().join("src")).unwrap();
        fs::write(temp_dir.path().join("src/ds1.ts"), "// mapping").unwrap();
        fs::create_dir_all(temp_dir.path().join("abis")).unwrap();
        fs::write(temp_dir.path().join("abis/ERC20.json"), erc20_abi_json()).unwrap();

        let mut ds = create_data_source("ds1", Some("mainnet"), Some(Version::new(0, 0, 6)));
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];
        ds.source_abi = Some("ERC20".to_string());
        ds.event_handlers = vec![event_handler("handleTransfer")];

        let manifest = create_test_manifest(vec![ds], vec![]);

        let errors = validate_manifest(&manifest, temp_dir.path());

        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_format_manifest_errors() {
        let errors = vec![
            ManifestValidationError::Shared(Box::new(
                SubgraphManifestValidationError::NoDataSources,
            )),
            ManifestValidationError::FileNotFound {
                path: "schema.graphql".to_string(),
                description: "Schema file".to_string(),
            },
        ];

        let formatted = format_manifest_errors(&errors);
        assert!(formatted.contains("no data sources"));
        assert!(formatted.contains("schema.graphql"));
        assert!(formatted.contains("  - "));
    }

    // --- source.abi cross-reference tests ---

    #[test]
    fn test_validate_source_abi_found_in_mapping_abis() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC20".to_string());
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];
        ds.event_handlers = vec![event_handler("handleTransfer")];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_source_abi_references(&manifest);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_source_abi_not_in_mapping_abis() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC721".to_string());
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_source_abi_references(&manifest);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::SourceAbiNotInMappingAbis {
                data_source,
                source_abi,
                available,
            } if data_source == "ds1" && source_abi == "ERC721" && available == &["ERC20"]
        ));
    }

    #[test]
    fn test_validate_source_abi_no_abis_defined() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC20".to_string());
        // abis is empty (default from create_data_source)

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_source_abi_references(&manifest);
        assert_eq!(errors.len(), 1);
        let msg = format!("{}", errors[0]);
        assert!(msg.contains("no ABIs defined"));
    }

    #[test]
    fn test_validate_source_abi_skips_non_ethereum() {
        // Subgraph data sources have no source_abi, should be skipped
        let ds = create_subgraph_data_source("sub1", "QmHash123");
        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_source_abi_references(&manifest);
        assert!(errors.is_empty());
    }

    // --- unique names tests ---

    #[test]
    fn test_validate_unique_data_source_names() {
        let ds1 = create_data_source("Token", Some("mainnet"), None);
        let ds2 = create_data_source("Token", Some("mainnet"), None);
        let manifest = create_test_manifest(vec![ds1, ds2], vec![]);

        let errors = validate_unique_names(&manifest);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::DuplicateName { kind, name }
                if *kind == "data source" && name == "Token"
        ));
    }

    #[test]
    fn test_validate_unique_template_names() {
        let t1 = Template {
            name: "DynToken".to_string(),
            kind: "ethereum/contract".to_string(),
            network: Some("mainnet".to_string()),
            mapping_file: Some("src/dyn.ts".to_string()),
            api_version: None,
            abis: vec![],
            source_abi: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        };
        let t2 = Template {
            name: "DynToken".to_string(),
            kind: "ethereum/contract".to_string(),
            network: Some("mainnet".to_string()),
            mapping_file: Some("src/dyn2.ts".to_string()),
            api_version: None,
            abis: vec![],
            source_abi: None,
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        };

        let manifest = create_test_manifest(vec![], vec![t1, t2]);
        let errors = validate_unique_names(&manifest);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::DuplicateName { kind, name }
                if *kind == "template" && name == "DynToken"
        ));
    }

    #[test]
    fn test_validate_unique_names_all_distinct() {
        let ds1 = create_data_source("TokenA", Some("mainnet"), None);
        let ds2 = create_data_source("TokenB", Some("mainnet"), None);
        let manifest = create_test_manifest(vec![ds1, ds2], vec![]);

        let errors = validate_unique_names(&manifest);
        assert!(errors.is_empty());
    }

    // --- handler presence tests ---

    #[test]
    fn test_validate_handler_presence_with_event_handler() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC20".to_string());
        ds.event_handlers = vec![event_handler("handleTransfer")];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_presence(&manifest);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_handler_presence_with_call_handler() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC20".to_string());
        ds.call_handlers = vec![call_handler("handleApprove")];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_presence(&manifest);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_handler_presence_with_block_handler() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC20".to_string());
        ds.block_handlers = vec![block_handler("handleBlock")];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_presence(&manifest);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_handler_presence_no_handlers() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.source_abi = Some("ERC20".to_string());
        // No handlers set

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_presence(&manifest);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::NoHandlers { data_source } if data_source == "ds1"
        ));
    }

    #[test]
    fn test_validate_handler_presence_skips_non_ethereum() {
        // Subgraph data sources have no source_abi, should be skipped
        let ds = create_subgraph_data_source("sub1", "QmHash123");
        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_presence(&manifest);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_handler_presence_template_no_handlers() {
        let t = Template {
            name: "DynToken".to_string(),
            kind: "ethereum/contract".to_string(),
            network: Some("mainnet".to_string()),
            mapping_file: Some("src/dyn.ts".to_string()),
            api_version: None,
            abis: vec![],
            source_abi: Some("ERC20".to_string()),
            event_handlers: vec![],
            call_handlers: vec![],
            block_handlers: vec![],
        };

        let manifest = create_test_manifest(vec![], vec![t]);
        let errors = validate_handler_presence(&manifest);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::NoHandlers { data_source } if data_source == "DynToken"
        ));
    }

    // --- Ethereum kind validation tests ---

    #[test]
    fn test_validate_valid_ethereum_kind() {
        assert!(validate_ethereum_kind("ds1", "ethereum/contract").is_empty());
        assert!(validate_ethereum_kind("ds1", "ethereum").is_empty());
    }

    #[test]
    fn test_validate_invalid_ethereum_kind() {
        let errors = validate_ethereum_kind("ds1", "ethereum/invalid");
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::InvalidKind { data_source, kind }
                if data_source == "ds1" && kind == "ethereum/invalid"
        ));
    }

    // --- Source address required tests ---

    #[test]
    fn test_validate_address_not_required_for_event_handlers_only() {
        let errors = validate_source_address_required("ds1", false, &[], &[]);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_address_required_for_call_handlers() {
        let call_handlers = vec![call_handler("handleCall")];
        let errors = validate_source_address_required("ds1", false, &call_handlers, &[]);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::SourceAddressRequired { data_source } if data_source == "ds1"
        ));
    }

    #[test]
    fn test_validate_address_required_for_block_handlers() {
        let block_handlers = vec![block_handler("handleBlock")];
        let errors = validate_source_address_required("ds1", false, &[], &block_handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::SourceAddressRequired { data_source } if data_source == "ds1"
        ));
    }

    #[test]
    fn test_validate_address_present_with_call_handlers() {
        let call_handlers = vec![call_handler("handleCall")];
        let errors = validate_source_address_required("ds1", true, &call_handlers, &[]);
        assert!(errors.is_empty());
    }

    // --- Block handler constraint tests ---

    #[test]
    fn test_validate_block_handlers_single_non_filtered() {
        let handlers = vec![block_handler("handleBlock")];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_block_handlers_single_call_filtered() {
        let handlers = vec![block_handler_with_filter(
            "handleBlock",
            BlockHandlerFilterKind::Call,
        )];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_block_handlers_non_filtered_with_call_filter_allowed() {
        // Non-filtered + call filter is allowed
        let handlers = vec![
            block_handler("handleBlock"),
            block_handler_with_filter("handleCallBlock", BlockHandlerFilterKind::Call),
        ];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_block_handlers_duplicate_non_filtered() {
        let handlers = vec![block_handler("handleBlock1"), block_handler("handleBlock2")];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::BlockHandlerConstraint { data_source, reason }
                if data_source == "ds1" && reason.contains("duplicated")
        ));
    }

    #[test]
    fn test_validate_block_handlers_duplicate_call_filtered() {
        let handlers = vec![
            block_handler_with_filter("h1", BlockHandlerFilterKind::Call),
            block_handler_with_filter("h2", BlockHandlerFilterKind::Call),
        ];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::BlockHandlerConstraint { reason, .. }
                if reason.contains("duplicated")
        ));
    }

    #[test]
    fn test_validate_block_handlers_non_filtered_with_polling_not_allowed() {
        let handlers = vec![
            block_handler("handleBlock"),
            block_handler_with_filter("handlePolling", BlockHandlerFilterKind::Polling),
        ];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::BlockHandlerConstraint { reason, .. }
                if reason.contains("combination")
        ));
    }

    #[test]
    fn test_validate_block_handlers_non_filtered_with_once_not_allowed() {
        let handlers = vec![
            block_handler("handleBlock"),
            block_handler_with_filter("handleInit", BlockHandlerFilterKind::Once),
        ];
        let errors = validate_block_handler_constraints("ds1", &handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::BlockHandlerConstraint { reason, .. }
                if reason.contains("combination")
        ));
    }

    // --- Receipt API version tests ---

    #[test]
    fn test_validate_receipt_api_version_ok() {
        let handlers = vec![EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: true,
            has_call_decls: false,
        }];
        let errors = validate_receipt_api_version("ds1", Some(&Version::new(0, 0, 7)), &handlers);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_receipt_api_version_too_low() {
        let handlers = vec![EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: true,
            has_call_decls: false,
        }];
        let errors = validate_receipt_api_version("ds1", Some(&Version::new(0, 0, 6)), &handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::ReceiptRequiresApiVersion { data_source }
                if data_source == "ds1"
        ));
    }

    #[test]
    fn test_validate_receipt_no_receipt_any_version() {
        let handlers = vec![event_handler("handleTransfer")];
        let errors = validate_receipt_api_version("ds1", Some(&Version::new(0, 0, 5)), &handlers);
        assert!(errors.is_empty());
    }

    // --- Call declarations spec version tests ---

    #[test]
    fn test_validate_call_decls_spec_version_ok() {
        let handlers = vec![EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: false,
            has_call_decls: true,
        }];
        let errors = validate_call_decls_spec_version("ds1", &Version::new(1, 2, 0), &handlers);
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_call_decls_spec_version_too_low() {
        let handlers = vec![EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: false,
            has_call_decls: true,
        }];
        let errors = validate_call_decls_spec_version("ds1", &Version::new(1, 1, 0), &handlers);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::CallDeclsRequireSpecVersion { data_source }
                if data_source == "ds1"
        ));
    }

    #[test]
    fn test_validate_call_decls_no_decls_any_version() {
        let handlers = vec![event_handler("handleTransfer")];
        let errors = validate_call_decls_spec_version("ds1", &Version::new(0, 0, 4), &handlers);
        assert!(errors.is_empty());
    }

    // --- Integration test: validate_ethereum_constraints ---

    #[test]
    fn test_validate_ethereum_constraints_valid() {
        let mut ds = create_data_source("ds1", Some("mainnet"), Some(Version::new(0, 0, 7)));
        ds.source_abi = Some("ERC20".to_string());
        ds.source_address = Some("0x1234".to_string());
        ds.event_handlers = vec![event_handler("handleTransfer")];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_ethereum_constraints(&manifest);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_ethereum_constraints_skips_non_ethereum() {
        let ds = create_subgraph_data_source("sub1", "QmHash123");
        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_ethereum_constraints(&manifest);
        assert!(errors.is_empty());
    }

    // --- ABI signature validation tests ---

    /// Build a minimal ERC20-like ABI JSON with Transfer event and approve function.
    fn erc20_abi_json() -> String {
        r#"[
            {
                "type": "event",
                "name": "Transfer",
                "anonymous": false,
                "inputs": [
                    {"name": "from", "type": "address", "indexed": true},
                    {"name": "to", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ]
            },
            {
                "type": "event",
                "name": "Approval",
                "anonymous": false,
                "inputs": [
                    {"name": "owner", "type": "address", "indexed": true},
                    {"name": "spender", "type": "address", "indexed": true},
                    {"name": "value", "type": "uint256", "indexed": false}
                ]
            },
            {
                "type": "function",
                "name": "approve",
                "inputs": [
                    {"name": "spender", "type": "address"},
                    {"name": "amount", "type": "uint256"}
                ],
                "outputs": [{"name": "", "type": "bool"}],
                "stateMutability": "nonpayable"
            },
            {
                "type": "function",
                "name": "transfer",
                "inputs": [
                    {"name": "to", "type": "address"},
                    {"name": "amount", "type": "uint256"}
                ],
                "outputs": [{"name": "", "type": "bool"}],
                "stateMutability": "nonpayable"
            }
        ]"#
        .to_string()
    }

    fn parse_abi(json: &str) -> JsonAbi {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn test_validate_event_signatures_exact_match_with_indexed() {
        let contract = parse_abi(&erc20_abi_json());
        let handlers = vec![EventHandler {
            event: "Transfer(indexed address,indexed address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: false,
            has_call_decls: false,
        }];
        let errors = validate_event_signatures("ds1", "ERC20", &handlers, &contract);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_event_signatures_fallback_without_indexed() {
        let contract = parse_abi(&erc20_abi_json());
        // Signature without indexed hints — should match via fallback
        let handlers = vec![EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: false,
            has_call_decls: false,
        }];
        let errors = validate_event_signatures("ds1", "ERC20", &handlers, &contract);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_event_signatures_nonexistent_event() {
        let contract = parse_abi(&erc20_abi_json());
        let handlers = vec![EventHandler {
            event: "Mint(address,uint256)".to_string(),
            handler: "handleMint".to_string(),
            receipt: false,
            has_call_decls: false,
        }];
        let errors = validate_event_signatures("ds1", "ERC20", &handlers, &contract);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::EventNotInAbi {
                data_source,
                event,
                abi_name,
            } if data_source == "ds1" && event == "Mint(address,uint256)" && abi_name == "ERC20"
        ));
    }

    #[test]
    fn test_validate_event_signatures_wrong_param_types() {
        let contract = parse_abi(&erc20_abi_json());
        // Transfer exists but with wrong param types
        let handlers = vec![EventHandler {
            event: "Transfer(uint256,uint256,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: false,
            has_call_decls: false,
        }];
        let errors = validate_event_signatures("ds1", "ERC20", &handlers, &contract);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::EventNotInAbi { .. }
        ));
    }

    #[test]
    fn test_validate_function_signatures_valid() {
        let contract = parse_abi(&erc20_abi_json());
        let handlers = vec![CallHandler {
            function: "approve(address,uint256)".to_string(),
            handler: "handleApprove".to_string(),
        }];
        let errors = validate_function_signatures("ds1", "ERC20", &handlers, &contract);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_function_signatures_nonexistent() {
        let contract = parse_abi(&erc20_abi_json());
        let handlers = vec![CallHandler {
            function: "mint(address,uint256)".to_string(),
            handler: "handleMint".to_string(),
        }];
        let errors = validate_function_signatures("ds1", "ERC20", &handlers, &contract);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::FunctionNotInAbi {
                data_source,
                function,
                abi_name,
            } if data_source == "ds1" && function == "mint(address,uint256)" && abi_name == "ERC20"
        ));
    }

    #[test]
    fn test_validate_function_signatures_wrong_params() {
        let contract = parse_abi(&erc20_abi_json());
        // approve exists but with wrong param types
        let handlers = vec![CallHandler {
            function: "approve(uint256,uint256)".to_string(),
            handler: "handleApprove".to_string(),
        }];
        let errors = validate_function_signatures("ds1", "ERC20", &handlers, &contract);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::FunctionNotInAbi { .. }
        ));
    }

    #[test]
    fn test_validate_handler_signatures_integration() {
        let temp_dir = TempDir::new().unwrap();

        // Create ABI file
        fs::create_dir_all(temp_dir.path().join("abis")).unwrap();
        fs::write(temp_dir.path().join("abis/ERC20.json"), erc20_abi_json()).unwrap();

        let mut ds = create_data_source("ds1", Some("mainnet"), Some(Version::new(0, 0, 6)));
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];
        ds.source_abi = Some("ERC20".to_string());
        ds.event_handlers = vec![EventHandler {
            event: "Transfer(address,address,uint256)".to_string(),
            handler: "handleTransfer".to_string(),
            receipt: false,
            has_call_decls: false,
        }];
        ds.call_handlers = vec![CallHandler {
            function: "approve(address,uint256)".to_string(),
            handler: "handleApprove".to_string(),
        }];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_signatures(&manifest, temp_dir.path());
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_handler_signatures_bad_event_good_function() {
        let temp_dir = TempDir::new().unwrap();

        fs::create_dir_all(temp_dir.path().join("abis")).unwrap();
        fs::write(temp_dir.path().join("abis/ERC20.json"), erc20_abi_json()).unwrap();

        let mut ds = create_data_source("ds1", Some("mainnet"), Some(Version::new(0, 0, 6)));
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];
        ds.source_abi = Some("ERC20".to_string());
        ds.event_handlers = vec![EventHandler {
            event: "Mint(address,uint256)".to_string(),
            handler: "handleMint".to_string(),
            receipt: false,
            has_call_decls: false,
        }];
        ds.call_handlers = vec![CallHandler {
            function: "approve(address,uint256)".to_string(),
            handler: "handleApprove".to_string(),
        }];

        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_signatures(&manifest, temp_dir.path());
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::EventNotInAbi { event, .. } if event == "Mint(address,uint256)"
        ));
    }

    #[test]
    fn test_validate_handler_signatures_skips_missing_abi_file() {
        let temp_dir = TempDir::new().unwrap();

        // Don't create the ABI file — validation should silently skip
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];
        ds.source_abi = Some("ERC20".to_string());
        ds.event_handlers = vec![EventHandler {
            event: "Nonexistent(uint256)".to_string(),
            handler: "handleNonexistent".to_string(),
            receipt: false,
            has_call_decls: false,
        }];

        let manifest = create_test_manifest(vec![ds], vec![]);
        // Should produce no errors (file not found is reported elsewhere)
        let errors = validate_handler_signatures(&manifest, temp_dir.path());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_validate_handler_signatures_skips_non_ethereum() {
        let temp_dir = TempDir::new().unwrap();

        let ds = create_subgraph_data_source("sub1", "QmHash123");
        let manifest = create_test_manifest(vec![ds], vec![]);
        let errors = validate_handler_signatures(&manifest, temp_dir.path());
        assert!(errors.is_empty());
    }

    #[test]
    fn test_event_signature() {
        let abi = parse_abi(&erc20_abi_json());
        let transfer = abi.events.get("Transfer").unwrap().first().unwrap();
        assert_eq!(transfer.signature(), "Transfer(address,address,uint256)");
        assert_eq!(
            event_signature_with_indexed(transfer),
            "Transfer(indexed address,indexed address,uint256)"
        );
    }

    #[test]
    fn test_function_signature() {
        let abi = parse_abi(&erc20_abi_json());
        let approve = abi.functions.get("approve").unwrap().first().unwrap();
        assert_eq!(approve.signature(), "approve(address,uint256)");
    }

    #[test]
    fn test_event_not_in_abi_display() {
        let err = ManifestValidationError::EventNotInAbi {
            data_source: "Token".to_string(),
            event: "Mint(address,uint256)".to_string(),
            abi_name: "ERC20".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "data source 'Token': event 'Mint(address,uint256)' not found in ABI 'ERC20'"
        );
    }

    #[test]
    fn test_function_not_in_abi_display() {
        let err = ManifestValidationError::FunctionNotInAbi {
            data_source: "Token".to_string(),
            function: "mint(address,uint256)".to_string(),
            abi_name: "ERC20".to_string(),
        };
        assert_eq!(
            format!("{}", err),
            "data source 'Token': function 'mint(address,uint256)' not found in ABI 'ERC20'"
        );
    }

    // --- WASM handler export validation tests ---

    /// Build a minimal valid WASM module that exports the given function names.
    ///
    /// The module defines one function type `() -> ()`, one function per export
    /// name, and exports each function by name.
    fn build_wasm_with_exports(export_names: &[&str]) -> Vec<u8> {
        let mut wasm = Vec::new();

        // Magic number + version
        wasm.extend_from_slice(b"\0asm");
        wasm.extend_from_slice(&1u32.to_le_bytes());

        let n = export_names.len();

        // Type section: one type `() -> ()`
        {
            let section = vec![
                1,    // count: 1 type
                0x60, // func type
                0,    // 0 params
                0,    // 0 results
            ];
            wasm.push(1); // section id: Type
            leb128_encode(&mut wasm, section.len() as u32);
            wasm.extend_from_slice(&section);
        }

        // Function section: n functions, all type index 0
        {
            let mut section = Vec::new();
            leb128_encode(&mut section, n as u32);
            section.extend(std::iter::repeat_n(0u8, n)); // type index 0 for each
            wasm.push(3); // section id: Function
            leb128_encode(&mut wasm, section.len() as u32);
            wasm.extend_from_slice(&section);
        }

        // Export section: export each function
        {
            let mut section = Vec::new();
            leb128_encode(&mut section, n as u32);
            for (i, name) in export_names.iter().enumerate() {
                leb128_encode(&mut section, name.len() as u32);
                section.extend_from_slice(name.as_bytes());
                section.push(0x00); // export kind: func
                leb128_encode(&mut section, i as u32); // func index
            }
            wasm.push(7); // section id: Export
            leb128_encode(&mut wasm, section.len() as u32);
            wasm.extend_from_slice(&section);
        }

        // Code section: n function bodies, each is just `end`
        {
            let mut section = Vec::new();
            leb128_encode(&mut section, n as u32);
            for _ in 0..n {
                // Function body: size=2, 0 locals, end
                section.push(2); // body size
                section.push(0); // 0 local declarations
                section.push(0x0b); // end
            }
            wasm.push(10); // section id: Code
            leb128_encode(&mut wasm, section.len() as u32);
            wasm.extend_from_slice(&section);
        }

        wasm
    }

    /// Encode a u32 as unsigned LEB128.
    fn leb128_encode(buf: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            buf.push(byte);
            if value == 0 {
                break;
            }
        }
    }

    #[test]
    fn test_wasm_exported_functions_extracts_exports() {
        let wasm = build_wasm_with_exports(&["handleTransfer", "handleApproval"]);
        let exports = wasm_exported_functions(&wasm).unwrap();
        assert!(exports.contains("handleTransfer"));
        assert!(exports.contains("handleApproval"));
        assert_eq!(exports.len(), 2);
    }

    #[test]
    fn test_wasm_exported_functions_empty_module() {
        let wasm = build_wasm_with_exports(&[]);
        let exports = wasm_exported_functions(&wasm).unwrap();
        assert!(exports.is_empty());
    }

    #[test]
    fn test_validate_wasm_handlers_all_present() {
        let temp_dir = TempDir::new().unwrap();
        let wasm_path = temp_dir.path().join("test.wasm");
        let wasm = build_wasm_with_exports(&["handleTransfer", "handleBlock"]);
        fs::write(&wasm_path, wasm).unwrap();

        let errors = validate_wasm_handlers(&wasm_path, "ds1", &["handleTransfer", "handleBlock"]);
        assert!(errors.is_empty(), "Expected no errors, got: {:?}", errors);
    }

    #[test]
    fn test_validate_wasm_handlers_missing_handlers() {
        let temp_dir = TempDir::new().unwrap();
        let wasm_path = temp_dir.path().join("test.wasm");
        let wasm = build_wasm_with_exports(&["handleTransfer"]);
        fs::write(&wasm_path, wasm).unwrap();

        let errors = validate_wasm_handlers(
            &wasm_path,
            "ds1",
            &["handleTransfer", "handleFoo", "handleBar"],
        );
        assert_eq!(errors.len(), 1);
        match &errors[0] {
            ManifestValidationError::MissingWasmHandlers {
                data_source,
                missing,
            } => {
                assert_eq!(data_source, "ds1");
                assert_eq!(missing, &["handleFoo", "handleBar"]);
            }
            other => panic!("Expected MissingWasmHandlers, got: {:?}", other),
        }
    }

    #[test]
    fn test_validate_wasm_handlers_file_not_found() {
        let errors = validate_wasm_handlers(
            Path::new("/nonexistent/path.wasm"),
            "ds1",
            &["handleTransfer"],
        );
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::InvalidWasm { .. }
        ));
    }

    #[test]
    fn test_validate_wasm_handlers_invalid_wasm() {
        let temp_dir = TempDir::new().unwrap();
        let wasm_path = temp_dir.path().join("bad.wasm");
        fs::write(&wasm_path, b"not a wasm file").unwrap();

        let errors = validate_wasm_handlers(&wasm_path, "ds1", &["handleTransfer"]);
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            ManifestValidationError::InvalidWasm { reason, .. } if reason.contains("parse")
        ));
    }

    #[test]
    fn test_validate_wasm_handlers_display_format() {
        let err = ManifestValidationError::MissingWasmHandlers {
            data_source: "Token".to_string(),
            missing: vec!["handleFoo".to_string(), "handleBar".to_string()],
        };
        let msg = format!("{}", err);
        assert_eq!(
            msg,
            "data source 'Token': missing WASM handlers: handleFoo, handleBar"
        );
    }

    #[test]
    fn test_collect_handler_names() {
        let mut ds = create_data_source("ds1", Some("mainnet"), None);
        ds.event_handlers = vec![event_handler("handleTransfer")];
        ds.call_handlers = vec![call_handler("handleApprove")];
        ds.block_handlers = vec![block_handler("handleBlock")];

        let names = collect_handler_names(&ds);
        assert_eq!(
            names,
            vec!["handleTransfer", "handleApprove", "handleBlock"]
        );
    }

    #[test]
    fn test_collect_handler_names_empty() {
        let ds = create_data_source("ds1", Some("mainnet"), None);
        let names = collect_handler_names(&ds);
        assert!(names.is_empty());
    }
}
