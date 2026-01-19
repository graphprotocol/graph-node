//! Version detection utilities for migrations.
//!
//! Provides functions to detect the installed version of graph-ts
//! by searching for its package.json in node_modules.

use std::path::Path;

use anyhow::{anyhow, Result};
use semver::Version;

/// Get the installed graph-ts version by searching node_modules.
///
/// Searches upward from `source_dir` looking for `node_modules/@graphprotocol/graph-ts/package.json`.
/// Returns the version string from the package.json, or an error if not found.
pub fn get_graph_ts_version(source_dir: &Path) -> Result<Version> {
    let mut current = source_dir.canonicalize().ok();

    while let Some(dir) = current {
        let graph_ts_path = dir
            .join("node_modules")
            .join("@graphprotocol")
            .join("graph-ts")
            .join("package.json");

        if graph_ts_path.exists() {
            let content = std::fs::read_to_string(&graph_ts_path)?;
            let pkg: serde_json::Value = serde_json::from_str(&content)?;

            let version_str = pkg
                .get("version")
                .and_then(|v| v.as_str())
                .ok_or_else(|| anyhow!("No version field in graph-ts package.json"))?;

            // Parse version, handling prerelease versions by coercing
            let version = parse_version(version_str)?;
            return Ok(version);
        }

        // Move up to parent directory
        current = dir.parent().map(|p| p.to_path_buf());
    }

    Err(anyhow!("graph-ts dependency not installed"))
}

/// Parse a version string, handling semver-incompatible formats.
///
/// Semver prerelease versions like "0.25.0-alpha.1" are valid, but we also
/// handle cases where the version might have extra components.
fn parse_version(version_str: &str) -> Result<Version> {
    // Try direct parsing first
    if let Ok(version) = Version::parse(version_str) {
        return Ok(version);
    }

    // Try to extract just the major.minor.patch part
    let parts: Vec<&str> = version_str
        .split('-')
        .next()
        .unwrap_or(version_str)
        .split('.')
        .collect();
    if parts.len() >= 3 {
        let major: u64 = parts[0].parse()?;
        let minor: u64 = parts[1].parse()?;
        let patch: u64 = parts[2].parse()?;
        return Ok(Version::new(major, minor, patch));
    }

    Err(anyhow!("Failed to parse version: {}", version_str))
}

/// Check if a version satisfies a minimum requirement.
pub fn version_gte(version: &Version, major: u64, minor: u64, patch: u64) -> bool {
    let requirement = Version::new(major, minor, patch);
    version >= &requirement
}

/// Check if a version is greater than a reference.
pub fn version_gt(version: &Version, major: u64, minor: u64, patch: u64) -> bool {
    let requirement = Version::new(major, minor, patch);
    version > &requirement
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_parse_version_simple() {
        let v = parse_version("0.25.0").unwrap();
        assert_eq!(v, Version::new(0, 25, 0));
    }

    #[test]
    fn test_parse_version_prerelease() {
        let v = parse_version("0.25.0-alpha.1").unwrap();
        assert_eq!(v.major, 0);
        assert_eq!(v.minor, 25);
        assert_eq!(v.patch, 0);
    }

    #[test]
    fn test_version_gte() {
        let v = Version::new(0, 25, 0);
        assert!(version_gte(&v, 0, 25, 0));
        assert!(version_gte(&v, 0, 24, 0));
        assert!(!version_gte(&v, 0, 26, 0));
    }

    #[test]
    fn test_version_gt() {
        let v = Version::new(0, 25, 0);
        assert!(version_gt(&v, 0, 24, 0));
        assert!(!version_gt(&v, 0, 25, 0));
        assert!(!version_gt(&v, 0, 26, 0));
    }

    #[test]
    fn test_get_graph_ts_version_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let result = get_graph_ts_version(temp_dir.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not installed"));
    }

    #[test]
    fn test_get_graph_ts_version_found() {
        let temp_dir = TempDir::new().unwrap();
        let graph_ts_dir = temp_dir
            .path()
            .join("node_modules")
            .join("@graphprotocol")
            .join("graph-ts");
        std::fs::create_dir_all(&graph_ts_dir).unwrap();

        let package_json = r#"{"name": "@graphprotocol/graph-ts", "version": "0.32.0"}"#;
        std::fs::write(graph_ts_dir.join("package.json"), package_json).unwrap();

        let version = get_graph_ts_version(temp_dir.path()).unwrap();
        assert_eq!(version, Version::new(0, 32, 0));
    }
}
