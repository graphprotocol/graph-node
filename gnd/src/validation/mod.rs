//! Schema and manifest validation using graph-node's validation logic.
//!
//! This module provides validation functions that use the same validation
//! logic as graph-node, ensuring developers get early feedback about issues
//! that would cause deployment failures.

use std::path::Path;

use anyhow::{Context, Result};
use graph::prelude::DeploymentHash;
use graph::schema::{InputSchema, SchemaValidationError};
use semver::Version;

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
}
