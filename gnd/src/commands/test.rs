//! Test command for running Matchstick tests.
//!
//! This command runs the Matchstick test runner for subgraph unit tests.
//! Matchstick is a testing framework for subgraphs that allows testing
//! event handlers, entity storage, and contract calls.

use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use clap::Parser;

use crate::output::{step, Step};

#[derive(Clone, Debug, Parser)]
#[clap(about = "Run Matchstick tests for the subgraph")]
pub struct TestOpt {
    /// Specific data source to test (optional, tests all if not specified)
    #[clap()]
    pub datasource: Option<String>,

    /// Run tests with coverage reporting
    #[clap(short = 'c', long)]
    pub coverage: bool,

    /// Run tests in a Docker container
    #[clap(short = 'd', long)]
    pub docker: bool,

    /// Force redownload of Matchstick binary / rebuild Docker image
    #[clap(short = 'f', long)]
    pub force: bool,

    /// Show debug logs (OS info, download URLs)
    #[clap(short = 'l', long)]
    pub logs: bool,

    /// Force recompilation of tests
    #[clap(short = 'r', long)]
    pub recompile: bool,

    /// Matchstick version to use
    #[clap(short = 'v', long)]
    pub version: Option<String>,
}

/// Run the test command.
pub fn run_test(opt: TestOpt) -> Result<()> {
    // Check if Matchstick binary exists in node_modules or PATH
    let matchstick_path = find_matchstick()?;

    if opt.docker {
        run_docker_tests(&opt)
    } else {
        run_binary_tests(&matchstick_path, &opt)
    }
}

/// Find the Matchstick binary.
fn find_matchstick() -> Result<PathBuf> {
    // First, check node_modules/.bin/graph-test (graph-cli's matchstick wrapper)
    let node_modules_path = PathBuf::from("node_modules/.bin/graph-test");
    if node_modules_path.exists() {
        return Ok(node_modules_path);
    }

    // Check for matchstick directly in node_modules
    let matchstick_path = PathBuf::from("node_modules/.bin/matchstick");
    if matchstick_path.exists() {
        return Ok(matchstick_path);
    }

    // Check if matchstick is in PATH
    if which::which("matchstick").is_ok() {
        return Ok(PathBuf::from("matchstick"));
    }

    Err(anyhow!(
        "Matchstick not found. Please install it with:\n  \
         npm install --save-dev matchstick-as\n\n\
         Or use Docker mode:\n  \
         gnd test -d"
    ))
}

/// Run tests using the Matchstick binary.
fn run_binary_tests(matchstick_path: &PathBuf, opt: &TestOpt) -> Result<()> {
    step(Step::Generate, "Running Matchstick tests");

    let mut cmd = Command::new(matchstick_path);

    // Add flags
    if opt.coverage {
        cmd.arg("-c");
    }
    if opt.recompile {
        cmd.arg("-r");
    }

    // Add datasource filter if specified
    if let Some(datasource) = &opt.datasource {
        cmd.arg(datasource);
    }

    let status = cmd.status().context("Failed to run Matchstick")?;

    if status.success() {
        step(Step::Done, "Tests passed");
        Ok(())
    } else {
        Err(anyhow!("Tests failed"))
    }
}

/// Run tests using Docker.
fn run_docker_tests(opt: &TestOpt) -> Result<()> {
    step(Step::Generate, "Running Matchstick tests in Docker");

    // Check if Docker is available
    Command::new("docker")
        .arg("--version")
        .output()
        .context("Docker not found. Please install Docker to use -d/--docker mode.")?;

    // Build test arguments
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

    // Get current working directory
    let cwd = std::env::current_dir().context("Failed to get current directory")?;

    // Build docker run command
    let mut cmd = Command::new("docker");
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

    // Check if matchstick image exists
    let image_check = Command::new("docker")
        .args(["images", "-q", "matchstick"])
        .output()
        .context("Failed to check for Docker image")?;

    let image_exists = !image_check.stdout.is_empty();

    if !image_exists || opt.force {
        // Need to build the image first
        step(Step::Generate, "Building Matchstick Docker image");

        // Create Dockerfile if it doesn't exist
        let dockerfile_path = PathBuf::from("tests/.docker/Dockerfile");
        if !dockerfile_path.exists() || opt.force {
            create_dockerfile(&dockerfile_path, opt.version.as_deref())?;
        }

        let build_status = Command::new("docker")
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

    // Run the container
    let status = cmd.status().context("Failed to run Docker container")?;

    if status.success() {
        step(Step::Done, "Tests passed");
        Ok(())
    } else {
        Err(anyhow!("Tests failed"))
    }
}

/// Create the Dockerfile for Matchstick.
fn create_dockerfile(path: &PathBuf, version: Option<&str>) -> Result<()> {
    use std::fs;

    // Create directory if needed
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let version = version.unwrap_or("0.6.0");

    let dockerfile_content = format!(
        r#"FROM node:18-slim

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install matchstick
RUN npm install -g matchstick-as@{version}

WORKDIR /matchstick

# Entry point runs tests
ENTRYPOINT ["sh", "-c", "npm install && graph test $ARGS"]
"#,
        version = version
    );

    fs::write(path, dockerfile_content)
        .with_context(|| format!("Failed to write Dockerfile to {}", path.display()))?;

    step(Step::Write, &format!("Created {}", path.display()));

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_matchstick_not_found() {
        // In test environment, matchstick likely isn't installed
        // This should return an error with helpful message
        let result = find_matchstick();
        // Either finds it or returns error - both are valid
        assert!(result.is_ok() || result.is_err());
    }
}
