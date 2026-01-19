//! Spec version migrations.
//!
//! These migrations update the manifest's `specVersion` field to newer versions.

use anyhow::Result;

use super::{load_manifest, replace_in_file, Migration, MigrationCheck, MigrationContext};

/// Migration: specVersion 0.0.1 -> 0.0.2
///
/// Spec version 0.0.2 uses top-level templates. graph-cli no longer supports
/// 0.0.1 which used nested templates.
pub struct SpecVersion0_0_1To0_0_2;

impl Migration for SpecVersion0_0_1To0_0_2 {
    fn name(&self) -> &'static str {
        "Bump manifest specVersion from 0.0.1 to 0.0.2"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        let manifest = load_manifest(ctx.manifest_path)?;

        let spec_version = manifest
            .get("specVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if spec_version == "0.0.1" {
            Ok(MigrationCheck::ShouldApply)
        } else {
            Ok(MigrationCheck::Skip)
        }
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        replace_in_file(
            ctx.manifest_path,
            &[
                (r"specVersion: 0\.0\.1", "specVersion: 0.0.2"),
                (r"specVersion: '0\.0\.1'", "specVersion: '0.0.2'"),
                (r#"specVersion: "0\.0\.1""#, r#"specVersion: "0.0.2""#),
            ],
        )
    }
}

/// Migration: specVersion 0.0.2/0.0.3 -> 0.0.4
///
/// Spec version 0.0.4 uses feature management, but features are detected
/// and validated by the graph-node instance during subgraph deployment.
///
/// We skip spec version 0.0.3, which is considered invalid and non-canonical.
pub struct SpecVersion0_0_2To0_0_4;

impl Migration for SpecVersion0_0_2To0_0_4 {
    fn name(&self) -> &'static str {
        "Bump manifest specVersion from 0.0.2 to 0.0.4"
    }

    fn check(&self, ctx: &MigrationContext) -> Result<MigrationCheck> {
        let manifest = load_manifest(ctx.manifest_path)?;

        let spec_version = manifest
            .get("specVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if spec_version == "0.0.2" || spec_version == "0.0.3" {
            Ok(MigrationCheck::ShouldApply)
        } else {
            Ok(MigrationCheck::Skip)
        }
    }

    fn apply(&self, ctx: &MigrationContext) -> Result<()> {
        // Replace both 0.0.2 and 0.0.3 with 0.0.4
        replace_in_file(
            ctx.manifest_path,
            &[(r#"specVersion: ['"]?0\.0\.[23]['"]?"#, "specVersion: 0.0.4")],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup_manifest(content: &str) -> (TempDir, std::path::PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");
        std::fs::write(&manifest_path, content).unwrap();
        (temp_dir, manifest_path)
    }

    #[test]
    fn test_spec_version_0_0_1_to_0_0_2_check() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.1\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_1To0_0_2;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::ShouldApply => {}
            _ => panic!("Expected ShouldApply"),
        }
    }

    #[test]
    fn test_spec_version_0_0_1_to_0_0_2_skip() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.2\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_1To0_0_2;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::Skip => {}
            _ => panic!("Expected Skip"),
        }
    }

    #[test]
    fn test_spec_version_0_0_1_to_0_0_2_apply() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.1\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_1To0_0_2;
        migration.apply(&ctx).unwrap();

        let content = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(content.contains("specVersion: 0.0.2"));
        assert!(!content.contains("specVersion: 0.0.1"));
    }

    #[test]
    fn test_spec_version_0_0_2_to_0_0_4_check() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.2\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_2To0_0_4;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::ShouldApply => {}
            _ => panic!("Expected ShouldApply"),
        }
    }

    #[test]
    fn test_spec_version_0_0_3_to_0_0_4_check() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.3\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_2To0_0_4;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::ShouldApply => {}
            _ => panic!("Expected ShouldApply"),
        }
    }

    #[test]
    fn test_spec_version_0_0_2_to_0_0_4_skip() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.4\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_2To0_0_4;
        match migration.check(&ctx).unwrap() {
            MigrationCheck::Skip => {}
            _ => panic!("Expected Skip"),
        }
    }

    #[test]
    fn test_spec_version_0_0_2_to_0_0_4_apply() {
        let (_temp_dir, manifest_path) = setup_manifest("specVersion: 0.0.2\ndataSources: []");
        let ctx = MigrationContext {
            manifest_path: &manifest_path,
            source_dir: manifest_path.parent().unwrap(),
        };

        let migration = SpecVersion0_0_2To0_0_4;
        migration.apply(&ctx).unwrap();

        let content = std::fs::read_to_string(&manifest_path).unwrap();
        assert!(content.contains("specVersion: 0.0.4"));
        assert!(!content.contains("specVersion: 0.0.2"));
    }
}
