//! Init command for creating new subgraphs.
//!
//! This command creates a new subgraph with basic scaffolding. It supports
//! multiple modes:
//! - From an example subgraph template
//! - From an existing contract (fetch ABI from Etherscan/Sourcify)
//! - From an existing deployed subgraph

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::{Parser, ValueEnum};

use crate::output::{step, Step};

/// Available protocols for subgraph development.
#[derive(Clone, Debug, ValueEnum)]
pub enum Protocol {
    Ethereum,
    Near,
    Cosmos,
    Arweave,
    Substreams,
}

impl std::fmt::Display for Protocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Protocol::Ethereum => write!(f, "ethereum"),
            Protocol::Near => write!(f, "near"),
            Protocol::Cosmos => write!(f, "cosmos"),
            Protocol::Arweave => write!(f, "arweave"),
            Protocol::Substreams => write!(f, "substreams"),
        }
    }
}

#[derive(Clone, Debug, Parser)]
#[clap(about = "Create a new subgraph with basic scaffolding")]
pub struct InitOpt {
    /// Name of the subgraph (e.g., "user/my-subgraph")
    #[clap()]
    pub subgraph_name: Option<String>,

    /// Directory to create the subgraph in
    #[clap()]
    pub directory: Option<PathBuf>,

    /// Protocol to use for the subgraph
    #[clap(long, value_enum)]
    pub protocol: Option<Protocol>,

    /// Graph node URL
    #[clap(short = 'g', long)]
    pub node: Option<String>,

    /// Create scaffold from an existing contract address
    #[clap(long, conflicts_with_all = ["from_example", "from_subgraph"])]
    pub from_contract: Option<String>,

    /// Create scaffold from an example subgraph
    #[clap(long, conflicts_with_all = ["from_contract", "from_subgraph"])]
    pub from_example: Option<String>,

    /// Create scaffold based on an existing deployed subgraph
    #[clap(long, conflicts_with_all = ["from_contract", "from_example"])]
    pub from_subgraph: Option<String>,

    /// Name of the contract (used with --from-contract)
    #[clap(long)]
    pub contract_name: Option<String>,

    /// Index contract events as entities
    #[clap(long)]
    pub index_events: bool,

    /// Skip installing dependencies
    #[clap(long)]
    pub skip_install: bool,

    /// Skip initializing a Git repository
    #[clap(long)]
    pub skip_git: bool,

    /// Block number to start indexing from
    #[clap(long)]
    pub start_block: Option<String>,

    /// Path to the contract ABI file
    #[clap(long)]
    pub abi: Option<PathBuf>,

    /// Path to the SPKG file (for Substreams)
    #[clap(long)]
    pub spkg: Option<PathBuf>,

    /// Network the contract is deployed to
    #[clap(long)]
    pub network: Option<String>,

    /// IPFS node URL for fetching subgraph data
    #[clap(short = 'i', long)]
    pub ipfs: Option<String>,
}

/// Run the init command.
pub fn run_init(opt: InitOpt) -> Result<()> {
    // Determine the scaffold source
    let source = if opt.from_contract.is_some() {
        ScaffoldSource::Contract
    } else if opt.from_example.is_some() {
        ScaffoldSource::Example
    } else if opt.from_subgraph.is_some() {
        ScaffoldSource::Subgraph
    } else {
        // Default to example if nothing specified
        ScaffoldSource::Example
    };

    match source {
        ScaffoldSource::Contract => init_from_contract(&opt),
        ScaffoldSource::Example => init_from_example(&opt),
        ScaffoldSource::Subgraph => init_from_subgraph(&opt),
    }
}

enum ScaffoldSource {
    Contract,
    Example,
    Subgraph,
}

/// Initialize a subgraph from a contract address.
fn init_from_contract(_opt: &InitOpt) -> Result<()> {
    Err(anyhow!(
        "Init from contract is not yet implemented.\n\
         This feature requires:\n\
         - Etherscan/Sourcify API integration for ABI fetching\n\
         - ABI parsing and code generation\n\n\
         Please use --from-example instead, or use the TypeScript graph-cli."
    ))
}

/// Initialize a subgraph from an example template.
fn init_from_example(opt: &InitOpt) -> Result<()> {
    use std::fs;
    use std::process::Command;

    let example = opt.from_example.as_deref().unwrap_or("ethereum-gravatar");

    let subgraph_name = opt.subgraph_name.as_deref().unwrap_or("my-subgraph");

    let directory = opt.directory.clone().unwrap_or_else(|| {
        PathBuf::from(subgraph_name.split('/').next_back().unwrap_or("subgraph"))
    });

    step(
        Step::Generate,
        &format!("Creating subgraph from example: {}", example),
    );

    // Check if directory already exists
    if directory.exists() {
        return Err(anyhow!(
            "Directory '{}' already exists. Please choose a different name or remove the existing directory.",
            directory.display()
        ));
    }

    // Clone the example repository
    let repo_url = "https://github.com/graphprotocol/example-subgraph.git";

    step(Step::Load, &format!("Cloning example from {}", repo_url));

    let status = Command::new("git")
        .args([
            "clone",
            "--depth",
            "1",
            repo_url,
            &directory.to_string_lossy(),
        ])
        .status()?;

    if !status.success() {
        return Err(anyhow!("Failed to clone example repository"));
    }

    // Remove .git directory to start fresh
    let git_dir = directory.join(".git");
    if git_dir.exists() {
        fs::remove_dir_all(&git_dir)?;
    }

    // Initialize fresh git repo unless skipped
    if !opt.skip_git {
        step(Step::Generate, "Initializing Git repository");
        let _ = Command::new("git")
            .current_dir(&directory)
            .arg("init")
            .status();
    }

    // Install dependencies unless skipped
    if !opt.skip_install {
        step(Step::Generate, "Installing dependencies");
        // Try pnpm first, then npm
        let pnpm_status = Command::new("pnpm")
            .current_dir(&directory)
            .arg("install")
            .status();

        if pnpm_status.is_err() || !pnpm_status.unwrap().success() {
            let npm_status = Command::new("npm")
                .current_dir(&directory)
                .arg("install")
                .status();

            if npm_status.is_err() || !npm_status.unwrap().success() {
                eprintln!("Warning: Failed to install dependencies. Run 'npm install' manually.");
            }
        }
    }

    step(
        Step::Done,
        &format!("Subgraph created at {}", directory.display()),
    );

    println!();
    println!("Next steps:");
    println!("  cd {}", directory.display());
    println!("  # Edit subgraph.yaml with your contract details");
    println!("  gnd codegen");
    println!("  gnd build");

    Ok(())
}

/// Initialize a subgraph from an existing deployed subgraph.
fn init_from_subgraph(_opt: &InitOpt) -> Result<()> {
    Err(anyhow!(
        "Init from subgraph is not yet implemented.\n\
         This feature requires fetching subgraph manifest from IPFS.\n\n\
         Please use --from-example instead, or use the TypeScript graph-cli."
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_display() {
        assert_eq!(Protocol::Ethereum.to_string(), "ethereum");
        assert_eq!(Protocol::Near.to_string(), "near");
    }
}
