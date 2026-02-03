//! Standalone command tests for `gnd` CLI.
//!
//! These tests verify commands that don't require a running Graph Node:
//! - `gnd init` - Scaffold new subgraph
//! - `gnd add` - Add datasource to existing subgraph
//! - `gnd build` - Compile subgraph to WASM
//!
//! # Prerequisites
//!
//! - Build the gnd binary: `cargo build -p gnd`
//! - IPFS running on localhost:5001 (for some tests)
//!
//! # Running
//!
//! ```bash
//! just test-gnd-commands
//! ```

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::TempDir;

/// Get the path to the gnd binary
fn gnd_binary_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("gnd")
}

/// Get the path to test ABIs
fn test_abis_path() -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("tests")
        .join("contracts")
        .join("abis")
}

/// Verify the gnd binary exists
fn verify_gnd_binary() -> PathBuf {
    let gnd_path = gnd_binary_path();
    if !gnd_path.exists() {
        panic!(
            "gnd binary not found at {}. Run `cargo build -p gnd` first.",
            gnd_path.display()
        );
    }
    gnd_path
}

/// Run gnd command and return output
fn run_gnd(args: &[&str], cwd: &Path) -> std::process::Output {
    let gnd = verify_gnd_binary();
    Command::new(&gnd)
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("Failed to execute gnd")
}

/// Run gnd command and assert it succeeds
fn run_gnd_success(args: &[&str], cwd: &Path) -> std::process::Output {
    let output = run_gnd(args, cwd);
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "gnd {:?} failed:\nstdout: {}\nstderr: {}",
            args, stdout, stderr
        );
    }
    output
}

// ============================================================================
// gnd init tests
// ============================================================================

#[test]
fn test_init_from_example() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("my-subgraph");

    // Run gnd init --from-example. We pass --skip-install since `pnpm
    // install` for the example will ask for github credentials (unclear
    // why)
    let output = run_gnd(
        &[
            "init",
            "--skip-install",
            "--from-example",
            "ethereum-gravatar",
            "my-subgraph",
        ],
        temp_dir.path(),
    );

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Example downloads from GitHub, may fail in CI without network
        // Skip test if it fails due to network issues
        if stderr.contains("network")
            || stderr.contains("fetch")
            || stderr.contains("download")
            || stderr.contains("Failed to fetch")
        {
            eprintln!("Skipping test_init_from_example: network unavailable");
            return;
        }
        panic!(
            "gnd init --from-example failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        );
    }

    // Verify scaffold was created
    assert!(subgraph_dir.exists(), "Subgraph directory should exist");
    assert!(
        subgraph_dir.join("subgraph.yaml").exists(),
        "subgraph.yaml should exist"
    );
    assert!(
        subgraph_dir.join("schema.graphql").exists(),
        "schema.graphql should exist"
    );
    assert!(
        subgraph_dir.join("package.json").exists(),
        "package.json should exist"
    );

    // Verify it's actually the ethereum-gravatar example (contains Gravatar entity)
    let schema = fs::read_to_string(subgraph_dir.join("schema.graphql")).unwrap();
    assert!(
        schema.contains("Gravatar"),
        "schema.graphql should contain Gravatar entity"
    );
}

#[test]
fn test_init_from_example_invalid_name() {
    let temp_dir = TempDir::new().unwrap();
    let output = run_gnd(
        &[
            "init",
            "--skip-install",
            "--from-example",
            "nonexistent-example-12345",
            "test",
        ],
        temp_dir.path(),
    );

    // Command should fail
    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Skip test if it fails due to network issues (can't verify example doesn't exist)
    if stderr.contains("Failed to fetch from graph-tooling") {
        eprintln!("Skipping test_init_from_example_invalid_name: network unavailable");
        return;
    }

    // Should show helpful error message
    assert!(
        stderr.contains("not found") || stderr.contains("graph-tooling"),
        "Error should mention example not found or link to graph-tooling. Got: {}",
        stderr
    );
}

#[test]
fn test_init_from_example_aggregations() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("my-agg");

    let output = run_gnd(
        &[
            "init",
            "--skip-install",
            "--from-example",
            "aggregations",
            "my-agg",
        ],
        temp_dir.path(),
    );

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Example downloads from GitHub, may fail in CI without network
        if stderr.contains("network")
            || stderr.contains("fetch")
            || stderr.contains("download")
            || stderr.contains("Failed to fetch")
        {
            eprintln!("Skipping test_init_from_example_aggregations: network unavailable");
            return;
        }
        panic!(
            "gnd init --from-example aggregations failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        );
    }

    // Verify scaffold was created
    assert!(subgraph_dir.exists(), "Subgraph directory should exist");

    // Verify it's actually the aggregations example, not ethereum-gravatar
    let manifest = fs::read_to_string(subgraph_dir.join("subgraph.yaml")).unwrap();

    // The aggregations example uses specVersion 1.1.0 and has blockHandlers,
    // whereas ethereum-gravatar uses 0.0.5 and has eventHandlers
    assert!(
        manifest.contains("specVersion: 1.1.0") || manifest.contains("blockHandlers"),
        "Manifest should be the aggregations example (expected specVersion 1.1.0 or blockHandlers)"
    );
}

#[test]
fn test_init_from_contract_with_abi() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("simple-subgraph");

    // Get the path to SimpleContract ABI
    let abi_path = test_abis_path().join("SimpleContract.json");
    assert!(abi_path.exists(), "SimpleContract.json ABI should exist");

    // Run gnd init with --from-contract and --abi
    let output = run_gnd(
        &[
            "init",
            "--from-contract",
            "0x5fbdb2315678afecb367f032d93f642f64180aa3",
            "--abi",
            abi_path.to_str().unwrap(),
            "--network",
            "mainnet",
            "--contract-name",
            "SimpleContract",
            "simple-subgraph",
        ],
        temp_dir.path(),
    );

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "gnd init --from-contract failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        );
    }

    // Verify scaffold was created
    assert!(subgraph_dir.exists(), "Subgraph directory should exist");
    assert!(
        subgraph_dir.join("subgraph.yaml").exists(),
        "subgraph.yaml should exist"
    );
    assert!(
        subgraph_dir.join("schema.graphql").exists(),
        "schema.graphql should exist"
    );

    // Verify manifest contains the contract
    let manifest = fs::read_to_string(subgraph_dir.join("subgraph.yaml")).unwrap();
    assert!(
        manifest.contains("0x5fbdb2315678afecb367f032d93f642f64180aa3"),
        "Manifest should contain contract address"
    );
    assert!(
        manifest.contains("SimpleContract"),
        "Manifest should contain contract name"
    );

    // Verify networks.json was created with contract config
    let networks_path = subgraph_dir.join("networks.json");
    assert!(networks_path.exists(), "networks.json should exist");

    let networks: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&networks_path).unwrap()).unwrap();
    assert!(
        networks["mainnet"]["SimpleContract"]["address"]
            .as_str()
            .unwrap()
            .contains("0x5fbdb2315678afecb367f032d93f642f64180aa3"),
        "networks.json should contain contract address"
    );
}

#[test]
fn test_init_creates_mapping_file() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("mapping-test");

    // Get the path to SimpleContract ABI
    let abi_path = test_abis_path().join("SimpleContract.json");

    // Run gnd init
    let output = run_gnd(
        &[
            "init",
            "--from-contract",
            "0x1234567890123456789012345678901234567890",
            "--abi",
            abi_path.to_str().unwrap(),
            "--network",
            "mainnet",
            "--contract-name",
            "TestContract",
            "--index-events",
            "mapping-test",
        ],
        temp_dir.path(),
    );

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("gnd init failed:\nstdout: {}\nstderr: {}", stdout, stderr);
    }

    // Verify mapping file was created
    let mapping_path = subgraph_dir.join("src").join("test-contract.ts");
    assert!(
        mapping_path.exists() || subgraph_dir.join("src").join("mapping.ts").exists(),
        "Mapping file should exist"
    );
}

// ============================================================================
// gnd add tests
// ============================================================================

#[test]
fn test_add_datasource() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("add-test");

    // First create a subgraph with init
    let abi_path = test_abis_path().join("SimpleContract.json");
    run_gnd_success(
        &[
            "init",
            "--from-contract",
            "0x1111111111111111111111111111111111111111",
            "--abi",
            abi_path.to_str().unwrap(),
            "--network",
            "mainnet",
            "--contract-name",
            "FirstContract",
            "add-test",
        ],
        temp_dir.path(),
    );

    // Verify initial manifest
    let manifest_before = fs::read_to_string(subgraph_dir.join("subgraph.yaml")).unwrap();
    assert!(
        manifest_before.contains("FirstContract"),
        "Initial manifest should have FirstContract"
    );
    assert!(
        !manifest_before.contains("SecondContract"),
        "Initial manifest should not have SecondContract"
    );

    // Now add another datasource
    let second_abi_path = test_abis_path().join("LimitedContract.json");
    let output = run_gnd(
        &[
            "add",
            "0x2222222222222222222222222222222222222222",
            "--abi",
            second_abi_path.to_str().unwrap(),
            "--contract-name",
            "SecondContract",
        ],
        &subgraph_dir,
    );

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("gnd add failed:\nstdout: {}\nstderr: {}", stdout, stderr);
    }

    // Verify manifest was updated
    let manifest_after = fs::read_to_string(subgraph_dir.join("subgraph.yaml")).unwrap();
    assert!(
        manifest_after.contains("FirstContract"),
        "Updated manifest should still have FirstContract"
    );
    assert!(
        manifest_after.contains("SecondContract"),
        "Updated manifest should have SecondContract"
    );
    assert!(
        manifest_after.contains("0x2222222222222222222222222222222222222222"),
        "Updated manifest should have second contract address"
    );
}

// ============================================================================
// gnd codegen tests
// ============================================================================

#[test]
fn test_codegen_generates_types() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("codegen-test");

    // Create a subgraph
    let abi_path = test_abis_path().join("SimpleContract.json");
    run_gnd_success(
        &[
            "init",
            "--from-contract",
            "0x1234567890123456789012345678901234567890",
            "--abi",
            abi_path.to_str().unwrap(),
            "--network",
            "mainnet",
            "--contract-name",
            "SimpleContract",
            "--index-events",
            "codegen-test",
        ],
        temp_dir.path(),
    );

    // Run codegen
    let output = run_gnd(&["codegen", "--skip-migrations"], &subgraph_dir);

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "gnd codegen failed:\nstdout: {}\nstderr: {}",
            stdout, stderr
        );
    }

    // Verify generated directory exists
    let generated_dir = subgraph_dir.join("generated");
    assert!(generated_dir.exists(), "generated/ directory should exist");

    // Verify schema.ts was generated
    assert!(
        generated_dir.join("schema.ts").exists(),
        "generated/schema.ts should exist"
    );
}

// ============================================================================
// gnd build tests
// ============================================================================

#[test]
fn test_build_after_codegen() {
    // Skip this test if asc is not installed
    if Command::new("asc").arg("--version").output().is_err() {
        eprintln!("Skipping test_build_after_codegen: asc not installed");
        return;
    }

    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("build-test");

    // Create a subgraph
    let abi_path = test_abis_path().join("SimpleContract.json");
    run_gnd_success(
        &[
            "init",
            "--from-contract",
            "0x1234567890123456789012345678901234567890",
            "--abi",
            abi_path.to_str().unwrap(),
            "--network",
            "mainnet",
            "--contract-name",
            "SimpleContract",
            "--index-events",
            "build-test",
        ],
        temp_dir.path(),
    );

    // Install dependencies (required for build)
    let npm_install = Command::new("npm")
        .arg("install")
        .current_dir(&subgraph_dir)
        .output();

    if npm_install.is_err() || !npm_install.unwrap().status.success() {
        eprintln!("Skipping test_build_after_codegen: npm install failed");
        return;
    }

    // Run build (includes codegen)
    let output = run_gnd(&["build", "--skip-migrations"], &subgraph_dir);

    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Build may fail if dependencies aren't set up correctly
        // This is expected in some CI environments
        if stderr.contains("Cannot find module") || stderr.contains("graph-ts") {
            eprintln!("Skipping test_build_after_codegen: dependencies not available");
            return;
        }
        panic!("gnd build failed:\nstdout: {}\nstderr: {}", stdout, stderr);
    }

    // Verify build directory exists
    let build_dir = subgraph_dir.join("build");
    assert!(build_dir.exists(), "build/ directory should exist");

    // Verify WASM was generated
    let wasm_files: Vec<_> = fs::read_dir(&build_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "wasm")
                .unwrap_or(false)
        })
        .collect();

    assert!(
        !wasm_files.is_empty(),
        "build/ should contain at least one .wasm file"
    );
}

// ============================================================================
// gnd clean tests
// ============================================================================

#[test]
fn test_clean_removes_artifacts() {
    let temp_dir = TempDir::new().unwrap();
    let subgraph_dir = temp_dir.path().join("clean-test");

    // Create a subgraph
    let abi_path = test_abis_path().join("SimpleContract.json");
    run_gnd_success(
        &[
            "init",
            "--from-contract",
            "0x1234567890123456789012345678901234567890",
            "--abi",
            abi_path.to_str().unwrap(),
            "--network",
            "mainnet",
            "--contract-name",
            "SimpleContract",
            "clean-test",
        ],
        temp_dir.path(),
    );

    // Run codegen to create generated/
    run_gnd_success(&["codegen", "--skip-migrations"], &subgraph_dir);

    // Verify generated/ exists
    assert!(
        subgraph_dir.join("generated").exists(),
        "generated/ should exist after codegen"
    );

    // Create a fake build/ directory
    fs::create_dir(subgraph_dir.join("build")).unwrap();
    assert!(subgraph_dir.join("build").exists(), "build/ should exist");

    // Run clean
    run_gnd_success(&["clean"], &subgraph_dir);

    // Verify directories were removed
    assert!(
        !subgraph_dir.join("generated").exists(),
        "generated/ should be removed after clean"
    );
    assert!(
        !subgraph_dir.join("build").exists(),
        "build/ should be removed after clean"
    );
}
