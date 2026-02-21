//! AssemblyScript compiler wrapper.
//!
//! This module provides a wrapper around the `asc` (AssemblyScript compiler)
//! command-line tool for compiling subgraph mappings to WebAssembly.

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, Context, Result};

/// Options for compiling an AssemblyScript file.
#[derive(Debug, Clone)]
pub struct AscCompileOptions {
    /// The input TypeScript/AssemblyScript file to compile.
    pub input_file: PathBuf,
    /// The base directory for resolving imports.
    pub base_dir: PathBuf,
    /// Comma-separated list of library directories (node_modules paths).
    pub libs: String,
    /// Path to the graph-ts global.ts file.
    pub global_file: PathBuf,
    /// Output file path.
    pub output_file: PathBuf,
    /// Output format: "wasm" or "wast".
    pub output_format: String,
    /// Skip the asc version check (use with caution).
    pub skip_version_check: bool,
}

/// The required AssemblyScript compiler version.
/// gnd functionality is very sensitive to asc version changes.
const REQUIRED_ASC_VERSION: &str = "0.19.23";

/// Compile an AssemblyScript mapping file to WebAssembly.
///
/// This shells out to the `asc` command-line tool with the appropriate flags
/// to match the graph-cli behavior.
///
/// Requires asc version 0.19.23 to be installed.
pub fn compile_mapping(options: &AscCompileOptions) -> Result<()> {
    // Resolve the asc binary, checking global PATH and local node_modules/.bin
    let asc_bin = find_asc_binary(&options.base_dir).ok_or_else(|| {
        anyhow!(
            "AssemblyScript compiler (asc) not found. Install it with:\n  \
             npm install -g assemblyscript@{REQUIRED_ASC_VERSION}\n  \
             or locally:\n  \
             npm install --save-dev assemblyscript@{REQUIRED_ASC_VERSION}"
        )
    })?;

    // Check version unless explicitly skipped
    if !options.skip_version_check {
        let version = get_asc_version(&asc_bin)?;
        if version != REQUIRED_ASC_VERSION {
            return Err(anyhow!(
                "AssemblyScript compiler version mismatch: found {}, required {}.\n  \
                 Please install the correct version with:\n  \
                 npm install -g assemblyscript@{REQUIRED_ASC_VERSION}\n\n  \
                 If you know what you're doing, use --skip-asc-version-check or set GND_SKIP_ASC_VERSION_CHECK=1 to bypass this check.",
                version,
                REQUIRED_ASC_VERSION
            ));
        }
    }

    // Determine input and output paths relative to base_dir
    let input_rel = options
        .input_file
        .strip_prefix(&options.base_dir)
        .unwrap_or(&options.input_file);
    let global_rel = options
        .global_file
        .strip_prefix(&options.base_dir)
        .unwrap_or(&options.global_file);
    let output_rel = options
        .output_file
        .strip_prefix(&options.base_dir)
        .unwrap_or(&options.output_file);

    // Build the asc command
    let mut cmd = Command::new(&asc_bin);

    // Add compiler flags matching graph-cli behavior
    cmd.arg("--explicitStart")
        .arg("--exportRuntime")
        .arg("--runtime")
        .arg("stub")
        .arg(input_rel)
        .arg(global_rel)
        .arg("--baseDir")
        .arg(&options.base_dir)
        .arg("--lib")
        .arg(&options.libs)
        .arg("--outFile")
        .arg(output_rel)
        .arg("--optimize")
        .arg("--debug");

    // Set working directory
    cmd.current_dir(&options.base_dir);

    // Execute the compiler
    let output = cmd
        .output()
        .context("Failed to execute AssemblyScript compiler (asc). Is it installed?")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);

        let error_msg = if !stderr.is_empty() {
            stderr.to_string()
        } else if !stdout.is_empty() {
            stdout.to_string()
        } else {
            "Unknown compilation error".to_string()
        };

        return Err(anyhow!(
            "AssemblyScript compilation failed for {}:\n{}",
            options.input_file.display(),
            error_msg
        ));
    }

    Ok(())
}

/// Find the graph-ts package in node_modules.
///
/// Searches upward from `source_dir` looking for `node_modules/@graphprotocol/graph-ts`.
/// Returns the paths needed for compilation:
/// - List of node_modules directories (for --lib flag)
/// - Path to global/global.ts
pub fn find_graph_ts(source_dir: &Path) -> Result<(Vec<PathBuf>, PathBuf)> {
    let mut lib_dirs = Vec::new();
    let mut graph_ts_dir: Option<PathBuf> = None;

    let mut current = source_dir.canonicalize().ok();

    while let Some(dir) = current {
        let node_modules = dir.join("node_modules");

        if node_modules.exists() {
            lib_dirs.push(node_modules.clone());

            // Check if this node_modules has graph-ts
            let graph_ts_path = node_modules.join("@graphprotocol").join("graph-ts");

            if graph_ts_path.exists() && graph_ts_dir.is_none() {
                graph_ts_dir = Some(graph_ts_path);
            }
        }

        // Move up to parent directory
        current = dir.parent().map(|p| p.to_path_buf());
    }

    if lib_dirs.is_empty() {
        return Err(anyhow!(
            "Could not locate `node_modules` in parent directories of subgraph manifest"
        ));
    }

    let graph_ts = graph_ts_dir.ok_or_else(|| {
        anyhow!(
            "Could not locate `@graphprotocol/graph-ts` package in parent directories of subgraph manifest"
        )
    })?;

    let global_file = graph_ts.join("global").join("global.ts");
    if !global_file.exists() {
        return Err(anyhow!(
            "Could not find global.ts in graph-ts package at {}",
            graph_ts.display()
        ));
    }

    Ok((lib_dirs, global_file))
}

/// Find the `asc` binary by checking the global PATH first, then the project's
/// root `node_modules/.bin/asc`.
fn find_asc_binary(base_dir: &Path) -> Option<PathBuf> {
    // Check global PATH first
    if Command::new("asc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return Some(PathBuf::from("asc"));
    }

    // Backward compatibility with graph-cli: check local node_modules/.bin/asc
    let local_asc = base_dir.join("node_modules").join(".bin").join("asc");
    if local_asc.exists() {
        return Some(local_asc);
    }

    None
}

/// Get the asc compiler version.
fn get_asc_version(asc_bin: &Path) -> Result<String> {
    let output = Command::new(asc_bin)
        .arg("--version")
        .output()
        .context("Failed to execute asc --version")?;

    if !output.status.success() {
        return Err(anyhow!("asc --version failed"));
    }

    // Output is "Version X.Y.Z", extract just the version number
    let version_output = String::from_utf8_lossy(&output.stdout);
    let version = version_output
        .trim()
        .strip_prefix("Version ")
        .unwrap_or(version_output.trim())
        .to_string();
    Ok(version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_find_graph_ts_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let result = find_graph_ts(temp_dir.path());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("node_modules"));
    }

    #[test]
    fn test_find_graph_ts_found() {
        let temp_dir = TempDir::new().unwrap();

        // Create the graph-ts directory structure
        let graph_ts_dir = temp_dir
            .path()
            .join("node_modules")
            .join("@graphprotocol")
            .join("graph-ts")
            .join("global");
        std::fs::create_dir_all(&graph_ts_dir).unwrap();
        std::fs::write(graph_ts_dir.join("global.ts"), "// global").unwrap();

        let (lib_dirs, global_file) = find_graph_ts(temp_dir.path()).unwrap();

        assert_eq!(lib_dirs.len(), 1);
        assert!(lib_dirs[0].ends_with("node_modules"));
        assert!(global_file.ends_with("global.ts"));
    }

    #[test]
    fn test_asc_version_check() {
        // Skip if asc is not installed
        let temp_dir = TempDir::new().unwrap();
        let asc_bin = match find_asc_binary(temp_dir.path()) {
            Some(bin) => bin,
            None => return,
        };

        let version = get_asc_version(&asc_bin).unwrap();
        // Version should be a semver-like string (e.g., "0.19.23")
        assert!(
            version.split('.').count() >= 2,
            "Version '{}' doesn't look like a semver",
            version
        );
    }
}
