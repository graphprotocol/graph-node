//! Schema and manifest validation using graph-node's validation logic.
//!
//! This module provides validation functions that use the same validation
//! logic as graph-node, ensuring developers get early feedback about issues
//! that would cause deployment failures.

use std::path::Path;

use anyhow::{Context, Result};
use graph::data::subgraph::api_version::{LATEST_VERSION, MIN_SPEC_VERSION};
use graph::data::subgraph::manifest_validation;
use graph::data::subgraph::SubgraphManifestValidationError;
use graph::prelude::DeploymentHash;
use graph::schema::{InputSchema, SchemaValidationError};
use semver::Version;

use crate::manifest::{Abi, DataSource, Manifest, Template};

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
/// Checks that all referenced files exist and ABI files are valid JSON.
/// This is the minimal validation needed before codegen — it doesn't check
/// deployment-level concerns like network names or spec versions.
pub(crate) fn validate_manifest_files(
    manifest: &Manifest,
    manifest_dir: &Path,
) -> Vec<ManifestValidationError> {
    let mut errors = Vec::new();

    // Validate file existence
    errors.extend(validate_file_existence(manifest, manifest_dir));

    // Validate ABIs are valid JSON
    errors.extend(validate_abis(manifest, manifest_dir));

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

/// Format manifest validation errors for display.
pub(crate) fn format_manifest_errors(errors: &[ManifestValidationError]) -> String {
    errors
        .iter()
        .map(|e| format!("  - {}", e))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

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
        fs::write(temp_dir.path().join("abis/ERC20.json"), "[]").unwrap();

        let mut ds = create_data_source("ds1", Some("mainnet"), Some(Version::new(0, 0, 6)));
        ds.abis = vec![Abi {
            name: "ERC20".to_string(),
            file: "abis/ERC20.json".to_string(),
        }];

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
}
