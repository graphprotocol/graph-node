//! Mock-based subgraph test runner for `gnd test`.
//!
//! This module replaces the old Matchstick-only test command with a mock-based
//! integration testing solution. Tests are defined as JSON files containing mock
//! blockchain data (blocks, log events, block triggers) and GraphQL assertions
//! that validate the resulting entity state.
//!
//! ## How it works
//!
//! 1. Build the subgraph (unless `--skip-build`)
//! 2. Discover `*.json` / `*.test.json` files in the test directory
//! 3. For each test file:
//!    a. Parse JSON into mock blocks with triggers
//!    b. Spin up a temporary PostgreSQL database (pgtemp on Unix)
//!    c. Initialize graph-node stores and deploy the subgraph
//!    d. Feed mock blocks through a static block stream (no real RPC)
//!    e. Wait for the indexer to process all blocks
//!    f. Run GraphQL assertions against the indexed data
//! 4. Report pass/fail results
//!
//! The key insight is that we reuse real graph-node infrastructure (stores,
//! subgraph deployment, WASM runtime) and only mock the blockchain layer.
//! This means tests exercise the same code paths as production indexing.
//!
//! ## Legacy mode
//!
//! The `--matchstick` flag falls back to the external Matchstick test runner
//! for backward compatibility with existing test suites.
//!
//! ## Module structure
//!
//! - [`schema`]: JSON input types (TestFile, TestBlock, etc.) and result types
//! - [`trigger`]: ABI encoding of event parameters into Ethereum log triggers
//! - [`mock_chain`]: Helpers for block pointer construction
//! - [`runner`]: Test execution orchestration (store setup, indexing, sync)
//! - [`assertion`]: GraphQL assertion execution and JSON comparison
//! - [`block_stream`]: Mock block stream that feeds pre-built blocks
//! - [`noop`]: Noop/stub trait implementations for the mock chain
//! - [`matchstick`]: Legacy Matchstick test runner (version resolution, download, Docker)
//! - [`output`]: Console output formatting for test results

mod assertion;
mod block_stream;
mod eth_calls;
mod matchstick;
mod mock_chain;
mod noop;
mod output;
mod runner;
mod schema;
mod trigger;

use anyhow::{anyhow, Result};
use clap::Parser;
use console::style;
use std::path::PathBuf;

use crate::output::{step, Step};

#[derive(Clone, Debug, Parser)]
#[clap(about = "Run subgraph tests")]
pub struct TestOpt {
    /// Path to subgraph manifest
    #[clap(default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

    /// Test files directory
    #[clap(short = 't', long, default_value = "tests")]
    pub test_dir: PathBuf,

    /// Skip building the subgraph before testing
    #[clap(long)]
    pub skip_build: bool,

    /// PostgreSQL connection URL. If not provided, a temporary database will be created (Unix only).
    #[clap(long, env = "POSTGRES_URL")]
    pub postgres_url: Option<String>,

    /// Use Matchstick runner instead (legacy mode)
    #[clap(long)]
    pub matchstick: bool,

    /// Run Matchstick tests in Docker (recommended on macOS where the native binary is bugged)
    #[clap(short = 'd', long, requires = "matchstick")]
    pub docker: bool,

    /// Run tests with coverage reporting (Matchstick only)
    #[clap(short = 'c', long, requires = "matchstick")]
    pub coverage: bool,

    /// Force recompilation of tests (Matchstick only)
    #[clap(short = 'r', long, requires = "matchstick")]
    pub recompile: bool,

    /// Force redownload of Matchstick binary / rebuild Docker image
    #[clap(short = 'f', long, requires = "matchstick")]
    pub force: bool,

    /// Matchstick version to use (default: latest from GitHub)
    #[clap(long, requires = "matchstick")]
    pub matchstick_version: Option<String>,

    /// Specific data source to test (Matchstick only)
    #[clap(long, requires = "matchstick")]
    pub datasource: Option<String>,
}

/// Entry point for the `gnd test` command.
///
/// Orchestrates the full test lifecycle: build -> discover -> run -> report.
/// Each test file gets its own isolated database and subgraph deployment.
/// Returns an error if any tests fail (for non-zero exit code).
pub async fn run_test(opt: TestOpt) -> Result<()> {
    if opt.matchstick {
        return matchstick::run(&opt).await;
    }

    // Build the subgraph first so the WASM and schema are available in build/.
    // This mirrors what a user would do manually before running tests.
    if !opt.skip_build {
        step(Step::Generate, "Building subgraph");
        let build_opt = crate::commands::BuildOpt {
            manifest: opt.manifest.clone(),
            output_dir: std::path::PathBuf::from("build"),
            output_format: "wasm".to_string(),
            skip_migrations: false,
            watch: false,
            ipfs: None,
            network: None,
            network_file: std::path::PathBuf::from("networks.json"),
            skip_asc_version_check: false,
        };
        crate::commands::run_build(build_opt).await?;
        step(Step::Done, "Build complete");
    }

    // Find all test JSON files in the test directory (sorted for deterministic order).
    step(Step::Load, "Discovering test files");
    let test_files = schema::discover_test_files(&opt.test_dir)?;

    if test_files.is_empty() {
        step(Step::Warn, "No test files found");
        println!(
            "  Looking in: {}",
            opt.test_dir
                .canonicalize()
                .unwrap_or(opt.test_dir.clone())
                .display()
        );
        println!("  Expected: *.test.json or *.json files");
        return Ok(());
    }

    let mut passed = 0;
    let mut failed = 0;
    let mut all_failures = Vec::new();

    for path in test_files {
        output::print_test_start(&path);

        // Parse the JSON test file into our schema types.
        let test_file = match schema::parse_test_file(&path) {
            Ok(tf) => tf,
            Err(e) => {
                println!("  {} Failed to parse: {}", style("✘").red(), e);
                failed += 1;
                continue;
            }
        };

        // Run the test: set up infra, index blocks, check assertions.
        // Each test gets a fresh database so tests are fully isolated.
        match runner::run_single_test(&opt, &test_file).await {
            Ok(result) => {
                output::print_test_result(&test_file.name, &result);
                if result.is_passed() {
                    passed += 1;
                } else {
                    all_failures.extend(output::collect_failures(&test_file.name, &result));
                    failed += 1;
                }
            }
            Err(e) => {
                println!("  {} {} - Error: {}", style("✘").red(), test_file.name, e);
                failed += 1;
            }
        }
    }

    output::print_failure_details(&all_failures);
    output::print_summary(passed, failed);

    if failed > 0 {
        Err(anyhow!("{} test(s) failed", failed))
    } else {
        Ok(())
    }
}
