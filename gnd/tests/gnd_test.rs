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
//! - pnpm available for dependency installation
//!
//! # Running
//!
//! ```bash
//! just test-gnd-test
//! ```
//!
//! Tests run with `--test-threads=1` to avoid races when sharing a Postgres
//! instance via `--postgres-url` (CI). With pgtemp (default) each test gets
//! its own isolated database, but serial execution keeps things simple.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;
use walkdir::WalkDir;

/// Copy the fixture subgraph into a fresh temp directory, install pnpm
/// dependencies, and run `gnd codegen`. Returns the temp dir handle (to
/// keep it alive) and the path to the prepared subgraph directory.
fn setup_fixture() -> (TempDir, PathBuf) {
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

    // Install dependencies (graph-ts, graph-cli)
    let npm_output = Command::new("pnpm")
        .arg("install")
        .current_dir(&subgraph_dir)
        .output()
        .expect("Failed to run `pnpm install`. Is pnpm available?");

    assert!(
        npm_output.status.success(),
        "pnpm install failed in fixture:\nstdout: {}\nstderr: {}",
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
}

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
    assert!(
        gnd::compiler::find_asc_binary(subgraph_dir).is_some(),
        "asc compiler not found globally or in {}/node_modules/.bin. \
         Install it with: npm install -g assemblyscript@0.19.23",
        subgraph_dir.display()
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
    let mut cmd = Command::new(&gnd);
    cmd.arg("test");

    // When a database URL is provided via env var (e.g. in CI), pass it through
    // to skip pgtemp which may not be available.
    if let Ok(db_url) = std::env::var("THEGRAPH_STORE_POSTGRES_DIESEL_URL") {
        cmd.arg("--postgres-url").arg(db_url);
    }

    cmd.args(args)
        .current_dir(cwd)
        .output()
        .expect("Failed to execute gnd test")
}

// ============================================================================
// gnd test — run all fixture tests
// ============================================================================

#[test]
fn test_gnd_test_all() {
    let (_temp_dir, subgraph_dir) = setup_fixture();

    // Run only the passing test files (exclude failing.json which is used by the negative test).
    let output = run_gnd_test(
        &[
            "tests/transfer.json",
            "tests/blocks.json",
            "tests/templates.json",
        ],
        &subgraph_dir,
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
    let (_temp_dir, subgraph_dir) = setup_fixture();

    let output = run_gnd_test(&["tests/failing.json"], &subgraph_dir);

    assert!(
        !output.status.success(),
        "gnd test should have failed for failing.json but exited with code 0\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
