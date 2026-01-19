//! Init command for creating new subgraphs.
//!
//! This command creates a new subgraph with basic scaffolding. It supports
//! multiple modes:
//! - From an example subgraph template
//! - From an existing contract (fetch ABI from Etherscan/Sourcify)
//! - From an existing deployed subgraph
//!
//! When required options are not provided, the command runs in interactive mode,
//! prompting the user for necessary information.

use std::fs;
use std::io::{self, IsTerminal};
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};

use crate::output::{step, Step};
use crate::prompt::{InitForm, SourceType};
use crate::scaffold::{generate_scaffold, init_git, install_dependencies, ScaffoldOptions};
use crate::services::{ContractInfo, ContractService, NetworksRegistry};

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

#[derive(Clone, Debug, Parser, Default)]
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
pub async fn run_init(opt: InitOpt) -> Result<()> {
    // Check if we need interactive mode
    let needs_interactive = should_run_interactive(&opt);

    if needs_interactive {
        // Check if we're in a terminal
        if !io::stdin().is_terminal() {
            return Err(anyhow!(
                "Interactive mode requires a terminal. \
                 Please provide required options via command line flags.\n\n\
                 Required: --from-contract <address> --network <network>\n\
                 Or use: --from-example to create from an example"
            ));
        }

        return run_interactive(opt).await;
    }

    // Non-interactive mode - determine the scaffold source
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
        ScaffoldSource::Contract => init_from_contract(&opt).await,
        ScaffoldSource::Example => init_from_example(&opt),
        ScaffoldSource::Subgraph => init_from_subgraph(&opt),
    }
}

/// Check if we should run in interactive mode.
fn should_run_interactive(opt: &InitOpt) -> bool {
    // If --from-example is specified, we can run non-interactively
    if opt.from_example.is_some() {
        return false;
    }

    // If --from-subgraph is specified, we can run non-interactively (will error)
    if opt.from_subgraph.is_some() {
        return false;
    }

    // If --from-contract is specified with network, we can run non-interactively
    if opt.from_contract.is_some() && opt.network.is_some() {
        return false;
    }

    // If --from-contract is specified without network, need interactive
    if opt.from_contract.is_some() && opt.network.is_none() {
        return true;
    }

    // If no source specified, we need interactive mode
    true
}

/// Run in interactive mode.
async fn run_interactive(opt: InitOpt) -> Result<()> {
    println!("Creating a new subgraph...\n");

    // Load the networks registry
    let registry = NetworksRegistry::load().await?;

    // Parse start block if provided
    let start_block = opt.start_block.as_ref().and_then(|s| s.parse::<u64>().ok());

    // Run the interactive form
    let form = InitForm::run_interactive(
        &registry,
        opt.network.clone(),
        opt.subgraph_name.clone(),
        opt.directory
            .clone()
            .map(|p| p.to_string_lossy().to_string()),
        opt.from_contract.clone(),
        opt.from_example.is_some(),
        opt.contract_name.clone(),
        start_block,
        opt.index_events,
        opt.abi.clone().map(|p| p.to_string_lossy().to_string()),
    )?;

    // Execute based on source type
    match form.source_type {
        SourceType::Example => {
            let example_opt = InitOpt {
                subgraph_name: Some(form.subgraph_name),
                directory: Some(PathBuf::from(&form.directory)),
                from_example: Some("ethereum-gravatar".to_string()),
                skip_install: opt.skip_install,
                skip_git: opt.skip_git,
                ..Default::default()
            };
            init_from_example(&example_opt)
        }
        SourceType::Contract => {
            let contract_opt = InitOpt {
                subgraph_name: Some(form.subgraph_name),
                directory: Some(PathBuf::from(&form.directory)),
                from_contract: form.contract_address,
                contract_name: Some(form.contract_name),
                network: Some(form.network),
                start_block: form.start_block.map(|b| b.to_string()),
                index_events: form.index_events,
                abi: form.abi_path.map(PathBuf::from),
                skip_install: opt.skip_install,
                skip_git: opt.skip_git,
                ..Default::default()
            };
            init_from_contract(&contract_opt).await
        }
    }
}

enum ScaffoldSource {
    Contract,
    Example,
    Subgraph,
}

/// Initialize a subgraph from a contract address.
async fn init_from_contract(opt: &InitOpt) -> Result<()> {
    let address = opt
        .from_contract
        .as_ref()
        .ok_or_else(|| anyhow!("Contract address is required"))?;

    // Validate address format
    if !address.starts_with("0x") || address.len() != 42 {
        return Err(anyhow!(
            "Invalid contract address '{}'. Expected format: 0x followed by 40 hex characters.",
            address
        ));
    }

    let network = opt.network.as_deref().unwrap_or("mainnet");

    step(
        Step::Load,
        &format!("Fetching contract info from {} on {}", address, network),
    );

    let contract_info = {
        // Load ABI from file if provided
        if let Some(abi_path) = &opt.abi {
            let abi_str = fs::read_to_string(abi_path)
                .with_context(|| format!("Failed to read ABI file: {}", abi_path.display()))?;
            let abi: serde_json::Value = serde_json::from_str(&abi_str)
                .with_context(|| format!("Failed to parse ABI file: {}", abi_path.display()))?;

            // Try to get start block from API if not provided
            let start_block = if let Some(block) = &opt.start_block {
                block.parse::<u64>().ok()
            } else {
                // Try to fetch from API
                match ContractService::load().await {
                    Ok(service) => service.get_start_block(network, address).await.ok(),
                    Err(_) => None,
                }
            };

            let name = opt
                .contract_name
                .clone()
                .unwrap_or_else(|| "Contract".to_string());

            ContractInfo {
                abi,
                name,
                start_block,
            }
        } else {
            // Fetch ABI from Etherscan/Sourcify
            let service = ContractService::load()
                .await
                .context("Failed to load contract service")?;

            service
                .get_contract_info(network, address)
                .await
                .context("Failed to fetch contract info")?
        }
    };

    step(
        Step::Done,
        &format!("Found contract: {}", contract_info.name),
    );

    // Determine contract name
    let contract_name = opt
        .contract_name
        .clone()
        .unwrap_or_else(|| contract_info.name.clone());

    // Determine subgraph name
    let subgraph_name = opt
        .subgraph_name
        .clone()
        .unwrap_or_else(|| format!("user/{}", contract_name.to_lowercase()));

    // Determine directory
    let directory = opt.directory.clone().unwrap_or_else(|| {
        PathBuf::from(
            subgraph_name
                .split('/')
                .next_back()
                .unwrap_or(&contract_name),
        )
    });

    // Check if directory already exists
    if directory.exists() {
        return Err(anyhow!(
            "Directory '{}' already exists. Please choose a different name or remove the existing directory.",
            directory.display()
        ));
    }

    // Determine start block
    let start_block = opt
        .start_block
        .as_ref()
        .and_then(|s| s.parse::<u64>().ok())
        .or(contract_info.start_block);

    // Generate scaffold
    let scaffold_options = ScaffoldOptions {
        address: Some(address.clone()),
        network: network.to_string(),
        contract_name: contract_name.clone(),
        subgraph_name: subgraph_name.clone(),
        start_block,
        abi: Some(contract_info.abi),
        index_events: opt.index_events,
    };

    generate_scaffold(&directory, &scaffold_options)?;

    // Initialize git unless skipped
    if !opt.skip_git {
        let _ = init_git(&directory);
    }

    // Install dependencies unless skipped
    if !opt.skip_install {
        if let Err(e) = install_dependencies(&directory) {
            eprintln!("Warning: {}", e);
        }
    }

    step(
        Step::Done,
        &format!("Subgraph created at {}", directory.display()),
    );

    println!();
    println!("Next steps:");
    println!("  cd {}", directory.display());
    println!("  gnd codegen");
    println!("  gnd build");

    Ok(())
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

    #[test]
    fn test_should_run_interactive() {
        // Example mode should not be interactive
        let opt = InitOpt {
            from_example: Some("gravatar".to_string()),
            ..Default::default()
        };
        assert!(!should_run_interactive(&opt));

        // Contract with network should not be interactive
        let opt = InitOpt {
            from_contract: Some("0x1234567890123456789012345678901234567890".to_string()),
            network: Some("mainnet".to_string()),
            ..Default::default()
        };
        assert!(!should_run_interactive(&opt));

        // Contract without network should be interactive
        let opt = InitOpt {
            from_contract: Some("0x1234567890123456789012345678901234567890".to_string()),
            ..Default::default()
        };
        assert!(should_run_interactive(&opt));

        // No source should be interactive
        let opt = InitOpt::default();
        assert!(should_run_interactive(&opt));
    }
}
