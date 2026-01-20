//! Integration tests that verify `gnd` works as a drop-in replacement for `graph-cli`.
//!
//! These tests run a subset of the integration tests using `gnd` instead of `graph-cli`
//! for the deployment workflow (codegen, create, deploy).
//!
//! # How it works
//!
//! The integration test infrastructure uses `CONFIG.graph_cli` for deployment commands.
//! By setting the `GRAPH_CLI` environment variable to point to the `gnd` binary,
//! the tests use `gnd codegen`, `gnd create`, and `gnd deploy` instead of the
//! corresponding `graph` commands.
//!
//! # Prerequisites
//!
//! - Build the gnd binary: `cargo build -p gnd`
//! - Start integration test services: `nix run .#integration`
//!   - PostgreSQL on localhost:3011
//!   - IPFS on localhost:3001
//!   - Anvil on localhost:3021
//!
//! # Running
//!
//! ```bash
//! just test-gnd-cli
//! ```

use std::path::PathBuf;

use anyhow::anyhow;
use graph::futures03::StreamExt;
use graph_tests::contract::Contract;
use graph_tests::{error, status, CONFIG};

mod integration_tests;

use integration_tests::{
    stop_graph_node, test_block_handlers, test_int8, test_value_roundtrip, TestCase, TestResult,
};

/// Get the path to the gnd binary
fn gnd_binary_path() -> PathBuf {
    // The gnd binary should be at target/debug/gnd relative to the workspace root
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    PathBuf::from(manifest_dir)
        .parent()
        .unwrap()
        .join("target")
        .join("debug")
        .join("gnd")
}

/// Verify the gnd binary exists before running tests
fn verify_gnd_binary() -> anyhow::Result<PathBuf> {
    let gnd_path = gnd_binary_path();
    if !gnd_path.exists() {
        return Err(anyhow!(
            "gnd binary not found at {}. Run `cargo build -p gnd` first.",
            gnd_path.display()
        ));
    }
    Ok(gnd_path)
}

/// The main test entrypoint for gnd CLI integration tests.
///
/// This test sets `GRAPH_CLI` to point to the gnd binary, then runs a subset
/// of integration tests to verify gnd works as a drop-in replacement.
#[graph::test]
async fn gnd_cli_tests() -> anyhow::Result<()> {
    // Verify gnd binary exists
    let gnd_path = verify_gnd_binary()?;
    status!("gnd-cli", "Using gnd binary at {}", gnd_path.display());

    // Set GRAPH_CLI to point to gnd - this is already set via the environment
    // when running `just test-gnd-cli`, but we verify it here
    let graph_cli =
        std::env::var("GRAPH_CLI").unwrap_or_else(|_| gnd_path.to_string_lossy().to_string());
    status!("gnd-cli", "GRAPH_CLI={}", graph_cli);

    // Verify the configured CLI is actually gnd
    if !graph_cli.contains("gnd") {
        return Err(anyhow!(
            "GRAPH_CLI ({}) does not point to gnd binary. Set GRAPH_CLI=../target/debug/gnd",
            graph_cli
        ));
    }

    let test_name_to_run = std::env::var("TEST_CASE").ok();

    // Run a subset of integration tests that exercise the core deployment workflow
    // These tests cover:
    // - codegen: Generate AssemblyScript types from schema and ABIs
    // - create: Register subgraph name with Graph Node
    // - deploy: Deploy subgraph to Graph Node
    let cases = vec![
        TestCase::new("block-handlers", test_block_handlers),
        TestCase::new("value-roundtrip", test_value_roundtrip),
        TestCase::new("int8", test_int8),
    ];

    // Filter the test cases if a specific test name is provided
    let cases_to_run: Vec<_> = if let Some(test_name) = test_name_to_run {
        cases
            .into_iter()
            .filter(|case| case.name == test_name)
            .collect()
    } else {
        cases
    };

    if cases_to_run.is_empty() {
        status!("gnd-cli", "No test cases to run");
        return Ok(());
    }

    status!(
        "gnd-cli",
        "Running {} test case(s) with gnd as CLI",
        cases_to_run.len()
    );

    let contracts = Contract::deploy_all().await?;

    status!("setup", "Resetting database");
    CONFIG.reset_database();

    // Spawn graph-node
    status!("graph-node", "Starting graph-node");
    let mut graph_node_child_command = CONFIG.spawn_graph_node().await?;

    let stream = tokio_stream::iter(cases_to_run)
        .map(|case| case.run(&contracts))
        .buffered(CONFIG.num_parallel_tests);

    let mut results: Vec<TestResult> = stream.collect::<Vec<_>>().await;
    results.sort_by_key(|result| result.name.clone());

    // Stop graph-node and read its output
    let graph_node_res = stop_graph_node(&mut graph_node_child_command).await;

    status!(
        "graph-node",
        "graph-node logs are in {}",
        CONFIG.graph_node.log_file.path.display()
    );

    match graph_node_res {
        Ok(_) => {
            status!("graph-node", "Stopped graph-node");
        }
        Err(e) => {
            error!("graph-node", "Failed to stop graph-node: {}", e);
        }
    }

    println!("\n\n{:=<60}", "");
    println!("gnd CLI Test Results:");
    println!("{:-<60}", "");
    for result in &results {
        result.print();
    }
    println!("\n");

    if results.iter().any(|result| !result.success()) {
        Err(anyhow!("Some gnd CLI tests failed"))
    } else {
        status!(
            "gnd-cli",
            "All tests passed - gnd works as drop-in replacement!"
        );
        Ok(())
    }
}
