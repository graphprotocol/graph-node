//! Subgraph manifest migrations.
//!
//! This module provides automatic migrations for updating subgraph manifests
//! to newer versions. Migrations handle both:
//! - `specVersion`: The manifest schema version
//! - `apiVersion`: The mapping API version in data sources/templates
//!
//! The migration system checks graph-ts version compatibility when applicable
//! and applies changes in-place using regex-based patching to preserve
//! formatting.

mod api_version;
mod spec_version;
mod versions;

use std::path::Path;

use anyhow::Result;
use regex::Regex;

use crate::output::{step, Step};

pub use versions::get_graph_ts_version;

/// Context for running migrations.
pub struct MigrationContext<'a> {
    /// Path to the manifest file.
    pub manifest_path: &'a Path,
    /// Directory containing the subgraph source (for finding graph-ts).
    pub source_dir: &'a Path,
}

/// Result of checking whether a migration should be applied.
pub enum MigrationCheck {
    /// Migration should be applied.
    ShouldApply,
    /// Migration should be skipped (no changes needed).
    Skip,
    /// Migration should be skipped with an explanation.
    SkipWithReason(String),
}

/// A single migration that can be applied to a manifest.
pub trait Migration {
    /// Human-readable name of the migration.
    fn name(&self) -> &'static str;

    /// Check if this migration should be applied.
    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck>;

    /// Apply the migration to the manifest file.
    fn apply(&self, ctx: &MigrationContext) -> Result<()>;
}

/// All available migrations in order.
fn all_migrations() -> Vec<Box<dyn Migration>> {
    vec![
        // Spec version migrations
        Box::new(spec_version::SpecVersion0_0_1To0_0_2),
        Box::new(spec_version::SpecVersion0_0_2To0_0_4),
        // API version migrations
        Box::new(api_version::ApiVersion0_0_1To0_0_2),
        Box::new(api_version::ApiVersion0_0_2To0_0_3),
        Box::new(api_version::ApiVersion0_0_3To0_0_4),
        Box::new(api_version::ApiVersion0_0_4To0_0_5),
        Box::new(api_version::ApiVersion0_0_5To0_0_6),
    ]
}

/// Apply all applicable migrations to a manifest file.
///
/// Migrations are applied in order, with each migration checking if it should
/// be applied before making changes. The manifest file is modified in-place.
pub fn apply_migrations(manifest_path: &Path) -> Result<()> {
    step(Step::Load, "Apply migrations");

    let source_dir = manifest_path.parent().unwrap_or_else(|| Path::new("."));

    let ctx = MigrationContext {
        manifest_path,
        source_dir,
    };

    for migration in all_migrations() {
        match migration.check(&ctx)? {
            MigrationCheck::ShouldApply => {
                step(
                    Step::Generate,
                    &format!("Apply migration: {}", migration.name()),
                );
                migration.apply(&ctx)?;
            }
            MigrationCheck::Skip => {
                step(Step::Skip, &format!("Skip migration: {}", migration.name()));
            }
            MigrationCheck::SkipWithReason(reason) => {
                step(
                    Step::Skip,
                    &format!("Skip migration: {} ({})", migration.name(), reason),
                );
            }
        }
    }

    Ok(())
}

/// Replace all occurrences of a pattern in a file.
///
/// This handles the common case of replacing version strings that might be
/// written with different quote styles (unquoted, single-quoted, double-quoted).
fn replace_in_file(path: &Path, patterns: &[(&str, &str)]) -> Result<()> {
    let content = std::fs::read_to_string(path)?;
    let mut result = content;

    for (pattern, replacement) in patterns {
        let re = Regex::new(pattern)?;
        result = re.replace_all(&result, *replacement).to_string();
    }

    std::fs::write(path, result)?;
    Ok(())
}

/// Check if manifest has a specific apiVersion in dataSources or templates.
fn manifest_has_api_version(manifest: &serde_json::Value, version: &str) -> bool {
    // Check dataSources
    if let Some(data_sources) = manifest.get("dataSources").and_then(|ds| ds.as_array()) {
        for ds in data_sources {
            if let Some(api_version) = ds
                .get("mapping")
                .and_then(|m| m.get("apiVersion"))
                .and_then(|v| v.as_str())
            {
                if api_version == version {
                    return true;
                }
            }
        }
    }

    // Check templates
    if let Some(templates) = manifest.get("templates").and_then(|t| t.as_array()) {
        for template in templates {
            if let Some(api_version) = template
                .get("mapping")
                .and_then(|m| m.get("apiVersion"))
                .and_then(|v| v.as_str())
            {
                if api_version == version {
                    return true;
                }
            }
        }
    }

    false
}

/// Load manifest as serde_json::Value for inspection.
fn load_manifest(path: &Path) -> Result<serde_json::Value> {
    let content = std::fs::read_to_string(path)?;
    let value: serde_json::Value = serde_yaml::from_str(&content)?;
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_manifest_has_api_version() {
        let manifest: serde_json::Value = serde_json::json!({
            "dataSources": [
                {
                    "mapping": {
                        "apiVersion": "0.0.4"
                    }
                }
            ]
        });

        assert!(manifest_has_api_version(&manifest, "0.0.4"));
        assert!(!manifest_has_api_version(&manifest, "0.0.5"));
    }

    #[test]
    fn test_manifest_has_api_version_in_templates() {
        let manifest: serde_json::Value = serde_json::json!({
            "dataSources": [],
            "templates": [
                {
                    "mapping": {
                        "apiVersion": "0.0.3"
                    }
                }
            ]
        });

        assert!(manifest_has_api_version(&manifest, "0.0.3"));
        assert!(!manifest_has_api_version(&manifest, "0.0.4"));
    }

    #[test]
    fn test_replace_in_file() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.yaml");

        std::fs::write(
            &file_path,
            "apiVersion: 0.0.4\napiVersion: '0.0.4'\napiVersion: \"0.0.4\"\n",
        )
        .unwrap();

        replace_in_file(
            &file_path,
            &[
                (r"apiVersion: 0\.0\.4", "apiVersion: 0.0.5"),
                (r"apiVersion: '0\.0\.4'", "apiVersion: '0.0.5'"),
                (r#"apiVersion: "0\.0\.4""#, r#"apiVersion: "0.0.5""#),
            ],
        )
        .unwrap();

        let content = std::fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("apiVersion: 0.0.5"));
        assert!(content.contains("apiVersion: '0.0.5'"));
        assert!(content.contains("apiVersion: \"0.0.5\""));
        assert!(!content.contains("0.0.4"));
    }
}
