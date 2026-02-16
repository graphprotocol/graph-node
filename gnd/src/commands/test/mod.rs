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

/// Default directory for test file discovery.
const DEFAULT_TEST_DIR: &str = "tests";

#[derive(Clone, Debug, Parser)]
#[clap(about = "Run subgraph tests")]
pub struct TestOpt {
    /// Test files or directories to run. Directories are scanned for *.json / *.test.json.
    /// Defaults to the "tests/" directory when nothing is specified.
    pub tests: Vec<PathBuf>,

    /// Path to subgraph manifest
    #[clap(short = 'm', long, default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

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

    // Resolve test files from positional args. Default to "tests/" when none given.
    let tests = if opt.tests.is_empty() {
        vec![PathBuf::from(DEFAULT_TEST_DIR)]
    } else {
        opt.tests.clone()
    };

    step(Step::Load, "Discovering test files");
    let test_files = resolve_test_paths(&tests)?;

    if test_files.is_empty() {
        step(Step::Warn, "No test files found");
        for test in &tests {
            println!(
                "  Looking in: {}",
                test.canonicalize().unwrap_or(test.clone()).display()
            );
        }
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

/// Resolve a list of paths into concrete test file paths.
///
/// Each path is either a JSON file (used directly) or a directory
/// (scanned for `*.json` / `*.test.json`). Bare filenames that don't
/// exist at the given path are also looked up in the default test
/// directory (e.g. `gnd test foo.json` resolves to `tests/foo.json`).
/// Results are sorted for deterministic execution order.
fn resolve_test_paths(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    for path in paths {
        if path.is_dir() {
            files.extend(schema::discover_test_files(path)?);
        } else if path.exists() {
            files.push(path.clone());
        } else {
            // Try resolving bare filename inside the default test directory.
            let in_default_dir = PathBuf::from(DEFAULT_TEST_DIR).join(path);
            if in_default_dir.exists() {
                files.push(in_default_dir);
            } else {
                anyhow::bail!("Test file not found: {}", path.display());
            }
        }
    }

    files.sort();
    Ok(files)
}
