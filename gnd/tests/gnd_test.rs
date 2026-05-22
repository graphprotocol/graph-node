//! Integration tests for `gnd test` — the mock-based subgraph test runner.
//!
//! Each fixture under `tests/fixtures/gnd_test/` covers one focused area:
//!
//! - `token/`             — ERC20 event handling, eth_call mocking, dynamic templates
//! - `blocks/`            — Block handlers (`every`, `once`, `polling` filters)
//! - `receipts/`          — Transaction receipts (`receipt: true` handlers)
//! - `file-data-sources/` — IPFS and Arweave file data sources
//!
//! # Prerequisites
//!
//! - Build the gnd binary: `cargo build -p gnd`
//! - AssemblyScript compiler (`asc`) in PATH or local `node_modules/.bin`
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

// ============================================================================
// Shared helpers
// ============================================================================

fn fixtures_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("gnd_test")
}

/// Copy a named fixture into a fresh temp directory, install pnpm dependencies,
/// and run `gnd codegen`. Returns the temp dir handle (to keep it alive) and
/// the path to the prepared subgraph directory.
fn setup_fixture(name: &str) -> (TempDir, PathBuf) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let subgraph_dir = temp_dir.path().join(name);
    fs::create_dir_all(&subgraph_dir).unwrap();

    let fixture = fixtures_root().join(name);
    assert!(
        fixture.exists(),
        "Fixture '{}' not found at {}",
        name,
        fixture.display()
    );

    copy_dir_recursive(&fixture, &subgraph_dir).expect("Failed to copy fixture to temp directory");

    let npm_output = Command::new("pnpm")
        .arg("install")
        .current_dir(&subgraph_dir)
        .output()
        .expect("Failed to run `pnpm install`. Is pnpm available?");

    assert!(
        npm_output.status.success(),
        "pnpm install failed in fixture '{}':\nstdout: {}\nstderr: {}",
        name,
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
        "gnd codegen failed in fixture '{}':\nstdout: {}\nstderr: {}",
        name,
        String::from_utf8_lossy(&codegen_output.stdout),
        String::from_utf8_lossy(&codegen_output.stderr),
    );

    (temp_dir, subgraph_dir)
}

fn gnd_binary_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("gnd")
}

fn verify_gnd_binary() -> PathBuf {
    let gnd_path = gnd_binary_path();
    assert!(
        gnd_path.exists(),
        "gnd binary not found at {}. Run `cargo build -p gnd` first.",
        gnd_path.display()
    );
    gnd_path
}

fn verify_asc_available(subgraph_dir: &Path) {
    assert!(
        gnd::compiler::find_asc_binary(subgraph_dir).is_some(),
        "asc compiler not found globally or in {}/node_modules/.bin. \
         Install it with: npm install -g assemblyscript@0.19.23",
        subgraph_dir.display()
    );
}

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
// token — ERC20 events, eth_call mocking, dynamic templates
// ============================================================================

#[test]
fn test_token_transfer() {
    let (_temp_dir, subgraph_dir) = setup_fixture("token");

    let output = run_gnd_test(&["tests/transfer.json"], &subgraph_dir);

    assert!(
        output.status.success(),
        "gnd test failed for token fixture\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

#[test]
fn test_templates() {
    let (_temp_dir, subgraph_dir) = setup_fixture("token");

    let output = run_gnd_test(&["tests/templates.json"], &subgraph_dir);

    assert!(
        output.status.success(),
        "gnd test failed for token fixture\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Verifies that `gnd test` exits with a non-zero code when an assertion fails.
#[test]
fn test_token_failing_assertion() {
    let (_temp_dir, subgraph_dir) = setup_fixture("token");

    let output = run_gnd_test(&["tests/failing.json"], &subgraph_dir);

    assert!(
        !output.status.success(),
        "gnd test should have failed for failing.json but exited with code 0\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

// ============================================================================
// blocks — block handlers with every/once/polling filters
// ============================================================================

#[test]
fn test_blocks() {
    let (_temp_dir, subgraph_dir) = setup_fixture("blocks");

    let output = run_gnd_test(&["tests/blocks.json"], &subgraph_dir);

    assert!(
        output.status.success(),
        "gnd test failed for blocks fixture\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

// ============================================================================
// receipts — receipt: true event handlers
// ============================================================================

#[test]
fn test_receipts() {
    let (_temp_dir, subgraph_dir) = setup_fixture("receipts");

    let output = run_gnd_test(&["tests/receipts.json"], &subgraph_dir);

    assert!(
        output.status.success(),
        "gnd test failed for receipts fixture\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

// ============================================================================
// file-data-sources — IPFS and Arweave file data sources
// ============================================================================

/// Verifies that an event handler can spawn a `file/ipfs` data source, the mock
/// IPFS client serves the pre-loaded content, and the file handler writes an
/// entity whose content matches the mocked bytes.
#[test]
fn test_file_ipfs() {
    let (_temp_dir, subgraph_dir) = setup_fixture("file-data-sources");

    let output = run_gnd_test(&["tests/file_ipfs.json"], &subgraph_dir);

    assert!(
        output.status.success(),
        "gnd test failed for file_ipfs.json\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

/// Verifies that an event handler can spawn a `file/arweave` data source, the
/// mock Arweave resolver serves the pre-loaded content, and the file handler
/// writes an entity whose content matches the mocked bytes.
#[test]
fn test_file_arweave() {
    let (_temp_dir, subgraph_dir) = setup_fixture("file-data-sources");

    let output = run_gnd_test(&["tests/file_arweave.json"], &subgraph_dir);

    assert!(
        output.status.success(),
        "gnd test failed for file_arweave.json\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}
