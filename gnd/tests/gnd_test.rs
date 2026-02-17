//! Integration tests for `gnd test` — the mock-based subgraph test runner.
//!
//! These tests verify that `gnd test` can:
//! - Build and run fixture subgraph tests end-to-end
//! - Execute individual test files
//! - Report correct pass/fail counts
//!
//! The fixture subgraph at `tests/fixtures/gnd_test/subgraph/` covers:
//! - Event handling with eth_call mocking (transfer.json)
//! - Block handlers with various filters (blocks.json)
//! - Dynamic data source templates (templates.json)
//!
//! # Prerequisites
//!
//! - Build the gnd binary: `cargo build -p gnd`
//! - AssemblyScript compiler (`asc`) in PATH
//! - npm available for dependency installation
//!
//! # Running
//!
//! ```bash
//! just test-gnd-test
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::LazyLock;

use tempfile::TempDir;
use walkdir::WalkDir;

/// Shared fixture: copied once, npm-installed once, codegen'd once.
/// The `TempDir` is kept alive for the entire test binary lifetime.
static FIXTURE: LazyLock<(TempDir, PathBuf)> = LazyLock::new(|| {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let subgraph_dir = temp_dir.path().join("subgraph");
    fs::create_dir_all(&subgraph_dir).unwrap();

    let fixture = fixture_path();
    assert!(
        fixture.exists(),
        "Fixture not found at {}",
        fixture.display()
    );

    copy_dir_recursive(&fixture, &subgraph_dir).expect("Failed to copy fixture to temp directory");

    // Install npm dependencies (graph-ts, graph-cli)
    let npm_output = Command::new("npm")
        .arg("install")
        .current_dir(&subgraph_dir)
        .output()
        .expect("Failed to run `npm install`. Is npm available?");

    assert!(
        npm_output.status.success(),
        "npm install failed in fixture:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&npm_output.stdout),
        String::from_utf8_lossy(&npm_output.stderr),
    );

    verify_asc_available(&subgraph_dir);

    let gnd = verify_gnd_binary();
    let codegen_output = Command::new(&gnd)
        .args(["codegen", "--skip-migrations"])
        .current_dir(&subgraph_dir)
        .output()
        .expect("Failed to run `gnd codegen`");

    assert!(
        codegen_output.status.success(),
        "gnd codegen failed in fixture:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&codegen_output.stdout),
        String::from_utf8_lossy(&codegen_output.stderr),
    );

    (temp_dir, subgraph_dir)
});

/// Get the path to the gnd binary.
fn gnd_binary_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("gnd")
}

/// Verify the gnd binary exists, panic with a helpful message if not.
fn verify_gnd_binary() -> PathBuf {
    let gnd_path = gnd_binary_path();
    assert!(
        gnd_path.exists(),
        "gnd binary not found at {}. Run `cargo build -p gnd` first.",
        gnd_path.display()
    );
    gnd_path
}

/// Get the path to the gnd_test fixture subgraph.
fn fixture_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("gnd_test")
        .join("subgraph")
}

/// Assert that `asc` (AssemblyScript compiler) is available in PATH or in local node_modules.
fn verify_asc_available(subgraph_dir: &Path) {
    // Check global PATH first
    if Command::new("asc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        return;
    }

    // Fall back to local node_modules/.bin/asc
    let local_asc = subgraph_dir.join("node_modules").join(".bin").join("asc");
    assert!(
        local_asc.exists(),
        "asc compiler not found globally or at {}. \
         Install it with: npm install -g assemblyscript@0.19.23",
        local_asc.display()
    );
}

/// Copy a directory recursively.
fn copy_dir_recursive(src: &Path, dst: &Path) -> std::io::Result<()> {
    for entry in WalkDir::new(src).min_depth(1) {
        let entry = entry?;
        let relative_path = entry.path().strip_prefix(src).unwrap();
        let dest_path = dst.join(relative_path);

        if entry.file_type().is_dir() {
            fs::create_dir_all(&dest_path)?;
        } else {
            fs::copy(entry.path(), &dest_path)?;
        }
    }
    Ok(())
}

/// Run `gnd test` with the given args in the given directory.
/// Returns the Output (status, stdout, stderr).
fn run_gnd_test(args: &[&str], cwd: &Path) -> std::process::Output {
    let gnd = verify_gnd_binary();
    Command::new(&gnd)
        .arg("test")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("Failed to execute gnd test")
}

// ============================================================================
// gnd test — run all fixture tests
// ============================================================================

#[test]
fn test_gnd_test_all() {
    let subgraph_dir = &FIXTURE.1;

    // Run only the passing test files (exclude failing.json which is used by the negative test).
    let output = run_gnd_test(
        &[
            "tests/transfer.json",
            "tests/blocks.json",
            "tests/templates.json",
        ],
        subgraph_dir,
    );

    assert!(
        output.status.success(),
        "gnd test failed with exit code: {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

// ============================================================================
// gnd test — verify failure on wrong assertions
// ============================================================================

#[test]
fn test_gnd_test_failing_assertions() {
    let subgraph_dir = &FIXTURE.1;

    let output = run_gnd_test(&["tests/failing.json"], subgraph_dir);

    assert!(
        !output.status.success(),
        "gnd test should have failed for failing.json but exited with code 0\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
