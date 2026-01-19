//! API version migrations.
//!
//! These migrations update the `apiVersion` field in data source and template mappings.
//! Each migration requires a minimum graph-ts version to be installed.

use anyhow::Result;

use super::{
    load_manifest, manifest_has_api_version, replace_in_file, versions, Migration, MigrationCheck,
    MigrationContext,
};

/// Helper to create an API version migration check.
fn check_api_version(
    ctx: &MigrationContext,
    from_version: &str,
    min_graph_ts: (u64, u64, u64),
    use_gte: bool,
) -> Result<MigrationCheck> {
    // Check graph-ts version
    match versions::get_graph_ts_version(ctx.source_dir) {
        Ok(version) => {
            let meets_requirement = if use_gte {
                versions::version_gte(&version, min_graph_ts.0, min_graph_ts.1, min_graph_ts.2)
            } else {
                versions::version_gt(&version, min_graph_ts.0, min_graph_ts.1, min_graph_ts.2)
            };

            if !meets_requirement {
                return Ok(MigrationCheck::Skip);
            }
        }
        Err(_) => {
            return Ok(MigrationCheck::SkipWithReason(
                "graph-ts dependency not installed yet".to_string(),
            ));
        }
    }

    // Check if manifest has the old apiVersion
    let manifest = load_manifest(ctx.manifest_path)?;
    if manifest_has_api_version(&manifest, from_version) {
        Ok(MigrationCheck::ShouldApply)
    } else {
        Ok(MigrationCheck::Skip)
    }
}

/// Helper to apply an API version migration.
fn apply_api_version(ctx: &MigrationContext, from: &str, to: &str) -> Result<()> {
    // Escape dots in version string for regex
    let from_escaped = from.replace('.', r"\.");
    let to_str = to;

    replace_in_file(
        ctx.manifest_path,
        &[
            (
                &format!(r"apiVersion: {}", from_escaped),
                &format!("apiVersion: {}", to_str),
            ),
            (
                &format!(r"apiVersion: '{}'", from_escaped),
                &format!("apiVersion: '{}'", to_str),
            ),
            (
                &format!(r#"apiVersion: "{}""#, from_escaped),
                &format!(r#"apiVersion: "{}""#, to_str),
            ),
        ],
    )
}

// =============================================================================
// Migrations
// =============================================================================

/// Migration: apiVersion 0.0.1 -> 0.0.2
///
/// Requires graph-ts > 0.5.1
pub struct ApiVersion0_0_1To0_0_2;

impl Migration for ApiVersion0_0_1To0_0_2 {
    fn name(&self) -> &'static str {
        "Bump mapping apiVersion from 0.0.1 to 0.0.2"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        check_api_version(ctx, "0.0.1", (0, 5, 1), false)
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        apply_api_version(ctx, "0.0.1", "0.0.2")
    }
}

/// Migration: apiVersion 0.0.2 -> 0.0.3
///
/// Requires graph-ts > 0.12.0
pub struct ApiVersion0_0_2To0_0_3;

impl Migration for ApiVersion0_0_2To0_0_3 {
    fn name(&self) -> &'static str {
        "Bump mapping apiVersion from 0.0.2 to 0.0.3"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        check_api_version(ctx, "0.0.2", (0, 12, 0), false)
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        apply_api_version(ctx, "0.0.2", "0.0.3")
    }
}

/// Migration: apiVersion 0.0.3 -> 0.0.4
///
/// Requires graph-ts > 0.17.0
pub struct ApiVersion0_0_3To0_0_4;

impl Migration for ApiVersion0_0_3To0_0_4 {
    fn name(&self) -> &'static str {
        "Bump mapping apiVersion from 0.0.3 to 0.0.4"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        check_api_version(ctx, "0.0.3", (0, 17, 0), false)
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        apply_api_version(ctx, "0.0.3", "0.0.4")
    }
}

/// Migration: apiVersion 0.0.4 -> 0.0.5
///
/// Requires graph-ts >= 0.22.0
pub struct ApiVersion0_0_4To0_0_5;

impl Migration for ApiVersion0_0_4To0_0_5 {
    fn name(&self) -> &'static str {
        "Bump mapping apiVersion from 0.0.4 to 0.0.5"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        check_api_version(ctx, "0.0.4", (0, 22, 0), true)
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        apply_api_version(ctx, "0.0.4", "0.0.5")
    }
}

/// Migration: apiVersion 0.0.5 -> 0.0.6
///
/// Requires graph-ts >= 0.24.0
pub struct ApiVersion0_0_5To0_0_6;

impl Migration for ApiVersion0_0_5To0_0_6 {
    fn name(&self) -> &'static str {
        "Bump mapping apiVersion from 0.0.5 to 0.0.6"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        check_api_version(ctx, "0.0.5", (0, 24, 0), true)
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        apply_api_version(ctx, "0.0.5", "0.0.6")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_with_graph_ts(
        manifest_content: &str,
        graph_ts_version: &str,
    ) -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();

        // Create manifest
        let manifest_path = temp_dir.path().join("subgraph.yaml");
        std::fs::write(&manifest_path, manifest_content).unwrap();

        // Create graph-ts package.json
        let graph_ts_dir = temp_dir
            .path()
            .join("node_modules")
            .join("@graphprotocol")
            .join("graph-ts");
        std::fs::create_dir_all(&graph_ts_dir).unwrap();
        std::fs::write(
            graph_ts_dir.join("package.json"),
            format!(
                r#"{{"name": "@graphprotocol/graph-ts", "version": "{}"}}"#,
                graph_ts_version
            ),
        )
        .unwrap();

        (temp_dir, manifest_path)
    }

    #[test]
    fn test_api_version_0_0_4_to_0_0_5_check_should_apply() {
        let manifest = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    mapping:
      apiVersion: 0.0.4
"#;
        let (_temp_dir, manifest_path) = setup_with_graph_ts(manifest, "0.25.0");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = ApiVersion0_0_4To0_0_5;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::ShouldApply => {}
            MigrationCheck::Skip => panic!("Expected ShouldApply, got Skip"),
            MigrationCheck::SkipWithReason(r) => {
                panic!("Expected ShouldApply, got SkipWithReason({})", r)
            }
        }
    }

    #[test]
    fn test_api_version_0_0_4_to_0_0_5_skip_old_graph_ts() {
        let manifest = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    mapping:
      apiVersion: 0.0.4
"#;
        let (_temp_dir, manifest_path) = setup_with_graph_ts(manifest, "0.20.0");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = ApiVersion0_0_4To0_0_5;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::Skip => {}
            _ => panic!("Expected Skip"),
        }
    }

    #[test]
    fn test_api_version_0_0_4_to_0_0_5_apply() {
        let manifest = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    mapping:
      apiVersion: 0.0.4
templates:
  - kind: ethereum/contract
    mapping:
      apiVersion: 0.0.4
"#;
        let (_temp_dir, manifest_path) = setup_with_graph_ts(manifest, "0.25.0");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = ApiVersion0_0_4To0_0_5;
        migration.apply(&ctx).unwrap();

        let content = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(content.contains("apiVersion: 0.0.5"));
        assert!(!content.contains("apiVersion: 0.0.4"));
    }

    #[test]
    fn test_api_version_no_graph_ts() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");
        std::fs::write(&manifest_path, "specVersion: 0.0.4\ndataSources: []").unwrap();

        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = ApiVersion0_0_4To0_0_5;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::SkipWithReason(reason) => {
                assert!(reason.contains("not installed"));
            }
            _ => panic!("Expected SkipWithReason"),
        }
    }

    #[test]
    fn test_api_version_migration_with_quoted_versions() {
        // Test with single quotes
        let manifest_single = r#"
specVersion: 0.0.4
dataSources:
  - kind: ethereum/contract
    mapping:
      apiVersion: '0.0.4'
"#;
        let (_temp_dir, manifest_path) = setup_with_graph_ts(manifest_single, "0.25.0");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = ApiVersion0_0_4To0_0_5;
        migration.apply(&ctx).unwrap();

        let content = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(content.contains("apiVersion: '0.0.5'"));
    }
}
