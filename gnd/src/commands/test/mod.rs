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
//! - [`output`]: Console output formatting for test results

mod assertion;
mod block_stream;
mod mock_chain;
mod noop;
mod output;
mod runner;
mod schema;
mod trigger;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use console::style;
use std::path::PathBuf;

pub use schema::TestResult;

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

    /// Matchstick version to use (default: 0.6.0)
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
        return run_matchstick_tests(&opt);
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
                match result {
                    TestResult::Passed => passed += 1,
                    TestResult::Failed { .. } => failed += 1,
                }
            }
            Err(e) => {
                println!("  {} {} - Error: {}", style("✘").red(), test_file.name, e);
                failed += 1;
            }
        }
    }

    output::print_summary(passed, failed);

    if failed > 0 {
        Err(anyhow!("{} test(s) failed", failed))
    } else {
        Ok(())
    }
}

/// Backward-compatible Matchstick test runner.
///
/// Dispatches to Docker mode or binary mode depending on the `--docker` flag.
/// This is the legacy path for projects that haven't migrated to the new
/// JSON-based test format yet.
fn run_matchstick_tests(opt: &TestOpt) -> Result<()> {
    if opt.docker {
        run_docker_tests(opt)
    } else {
        run_binary_tests(opt)
    }
}

/// Run Matchstick tests using a locally installed binary.
///
/// Searches for the Matchstick binary in well-known locations and executes it,
/// passing through any relevant CLI flags.
fn run_binary_tests(opt: &TestOpt) -> Result<()> {
    step(Step::Generate, "Running Matchstick tests (legacy mode)");

    let path = find_matchstick().ok_or_else(|| {
        anyhow!(
            "Matchstick not found. Please install it with:\n  \
             npm install --save-dev matchstick-as\n\n\
             Or use Docker mode:\n  \
             gnd test --matchstick -d"
        )
    })?;

    let workdir = opt.manifest.parent().unwrap_or(std::path::Path::new("."));
    let mut cmd = std::process::Command::new(&path);
    cmd.current_dir(workdir);

    if opt.coverage {
        cmd.arg("-c");
    }
    if opt.recompile {
        cmd.arg("-r");
    }
    if let Some(datasource) = &opt.datasource {
        cmd.arg(datasource);
    }

    let status = cmd
        .status()
        .with_context(|| format!("Failed to execute Matchstick binary: {}", path))?;

    if status.success() {
        step(Step::Done, "Matchstick tests passed");
        Ok(())
    } else {
        Err(anyhow!("Matchstick tests failed"))
    }
}

/// Find the Matchstick binary by searching well-known locations and PATH.
///
/// Search order:
/// 1. `node_modules/.bin/graph-test`
/// 2. `node_modules/.bin/matchstick`
/// 3. `node_modules/matchstick-as/bin/matchstick`
/// 4. `graph-test` on PATH
/// 5. `matchstick` on PATH
fn find_matchstick() -> Option<String> {
    let local_paths = [
        "node_modules/.bin/graph-test",
        "node_modules/.bin/matchstick",
        "node_modules/matchstick-as/bin/matchstick",
    ];

    local_paths
        .iter()
        .find(|p| std::path::Path::new(p).exists())
        .map(|p| p.to_string())
        .or_else(|| {
            which::which("graph-test")
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        })
        .or_else(|| {
            which::which("matchstick")
                .ok()
                .map(|p| p.to_string_lossy().into_owned())
        })
}

/// Run Matchstick tests inside a Docker container.
///
/// This is the recommended mode on macOS where the native Matchstick binary
/// has known issues. The Docker image is built automatically if it doesn't
/// exist or if `--force` is specified.
fn run_docker_tests(opt: &TestOpt) -> Result<()> {
    step(Step::Generate, "Running Matchstick tests in Docker");

    std::process::Command::new("docker")
        .arg("--version")
        .output()
        .context("Docker not found. Please install Docker to use -d/--docker mode.")?;

    let mut test_args = String::new();
    if opt.coverage {
        test_args.push_str(" -c");
    }
    if opt.recompile {
        test_args.push_str(" -r");
    }
    if let Some(datasource) = &opt.datasource {
        test_args.push_str(&format!(" {}", datasource));
    }

    let cwd = std::env::current_dir().context("Failed to get current directory")?;

    let mut cmd = std::process::Command::new("docker");
    cmd.args([
        "run",
        "-it",
        "--rm",
        "--mount",
        &format!("type=bind,source={},target=/matchstick", cwd.display()),
    ]);
    if !test_args.is_empty() {
        cmd.args(["-e", &format!("ARGS={}", test_args.trim())]);
    }
    cmd.arg("matchstick");

    // Check if the Docker image already exists.
    let image_check = std::process::Command::new("docker")
        .args(["images", "-q", "matchstick"])
        .output()
        .context("Failed to check for Docker image")?;
    let image_exists = !image_check.stdout.is_empty();

    if !image_exists || opt.force {
        step(Step::Generate, "Building Matchstick Docker image");
        let dockerfile_path = PathBuf::from("tests/.docker/Dockerfile");
        if !dockerfile_path.exists() || opt.force {
            create_dockerfile(&dockerfile_path, opt.matchstick_version.as_deref())?;
        }
        let build_status = std::process::Command::new("docker")
            .args([
                "build",
                "-f",
                &dockerfile_path.to_string_lossy(),
                "-t",
                "matchstick",
                ".",
            ])
            .status()
            .context("Failed to build Docker image")?;
        if !build_status.success() {
            return Err(anyhow!("Failed to build Matchstick Docker image"));
        }
    }

    let status = cmd.status().context("Failed to run Docker container")?;
    if status.success() {
        step(Step::Done, "Tests passed");
        Ok(())
    } else {
        Err(anyhow!("Tests failed"))
    }
}

/// Create a Dockerfile for running Matchstick tests in a container.
///
/// The Dockerfile downloads the Matchstick binary directly from GitHub releases
/// (not npm — `matchstick-as` is the AssemblyScript library, not the runner binary).
/// Based on <https://github.com/LimeChain/demo-subgraph/blob/main/Dockerfile>.
fn create_dockerfile(path: &PathBuf, version: Option<&str>) -> Result<()> {
    use std::fs;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let version = version.unwrap_or("0.6.0");
    let dockerfile_content = format!(
        r#"FROM --platform=linux/x86_64 ubuntu:22.04

ARG DEBIAN_FRONTEND=noninteractive
ENV ARGS=""

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     curl ca-certificates postgresql postgresql-contrib \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://deb.nodesource.com/setup_18.x | bash - \
  && apt-get install -y --no-install-recommends nodejs \
  && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL -o /usr/local/bin/matchstick \
     https://github.com/LimeChain/matchstick/releases/download/{version}/binary-linux-22 \
  && chmod +x /usr/local/bin/matchstick

RUN mkdir /matchstick
WORKDIR /matchstick

CMD ["sh", "-c", "matchstick $ARGS"]
"#,
        version = version
    );

    fs::write(path, dockerfile_content)
        .with_context(|| format!("Failed to write Dockerfile to {}", path.display()))?;
    step(Step::Write, &format!("Created {}", path.display()));
    Ok(())
}
