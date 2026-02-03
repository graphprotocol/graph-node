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
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use graphql_parser::schema as gql;

use crate::config::networks::update_networks_file;
use crate::output::{step, with_spinner, Step};
use crate::prompt::{
    get_subgraph_basename, prompt_directory_with_confirm, prompt_subgraph_slug_with_confirm,
    InitForm, SourceType,
};
use crate::scaffold::{generate_scaffold, init_git, install_dependencies, ScaffoldOptions};
use crate::services::{ContractInfo, ContractService, IpfsClient, NetworksRegistry};

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
        ScaffoldSource::Subgraph => init_from_subgraph(&opt).await,
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

    // Create networks.json with the contract configuration
    let networks_path = directory.join("networks.json");
    update_networks_file(
        &networks_path,
        network,
        &contract_name,
        address,
        start_block,
    )?;

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

/// Recursively copy a directory and its contents.
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

/// Initialize a subgraph from an example template.
///
/// This matches the graph-cli `init --from-example` behavior:
/// 1. Prompt for subgraph slug (with confirmation output)
/// 2. Prompt for directory (with confirmation output)
/// 3. Clone example with spinner
/// 4. Initialize networks config with spinner
/// 5. Update package.json with spinner
/// 6. Initialize git with spinner
/// 7. Install dependencies with spinner
/// 8. Run codegen with spinner
/// 9. Print "Next steps" message
fn init_from_example(opt: &InitOpt) -> Result<()> {
    use std::io::IsTerminal;

    let example = opt.from_example.as_deref().ok_or_else(|| {
        anyhow!(
            "Example name is required. See available examples at:\n\
             https://github.com/graphprotocol/graph-tooling/tree/main/examples"
        )
    })?;

    // Handle legacy example name format
    let example = match example {
        "ethereum/gravatar" => "ethereum-gravatar",
        other => other,
    };

    // Determine subgraph slug - prompt if not provided and in terminal
    let subgraph_name = if let Some(name) = &opt.subgraph_name {
        name.clone()
    } else if io::stdin().is_terminal() {
        prompt_subgraph_slug_with_confirm(None)?
    } else {
        "my-subgraph".to_string()
    };

    // Determine directory - prompt if not provided and in terminal
    let directory = if let Some(dir) = &opt.directory {
        dir.clone()
    } else if io::stdin().is_terminal() {
        let default_dir = get_subgraph_basename(&subgraph_name);
        PathBuf::from(prompt_directory_with_confirm(Some(&default_dir))?)
    } else {
        PathBuf::from(get_subgraph_basename(&subgraph_name))
    };

    // Check if directory already exists
    if directory.exists() {
        return Err(anyhow!(
            "Directory '{}' already exists. Please choose a different name or remove the existing directory.",
            directory.display()
        ));
    }

    // Clone example with spinner
    clone_example_with_spinner(example, &directory)?;

    // Initialize networks config with spinner
    init_networks_config_with_spinner(&directory)?;

    // Update package.json with subgraph name
    update_package_json_with_spinner(&directory, &subgraph_name)?;

    // Initialize git unless skipped
    if !opt.skip_git {
        init_git_with_spinner(&directory)?;
    }

    // Install dependencies unless skipped
    if !opt.skip_install {
        if let Err(e) = install_dependencies(&directory) {
            eprintln!("Warning: {}", e);
        }
    }

    // Run codegen unless install was skipped (codegen requires dependencies)
    if !opt.skip_install {
        run_codegen_with_spinner(&directory)?;
    }

    // Print final message
    print_next_steps(&subgraph_name, &directory);

    Ok(())
}

/// Clone example subgraph with spinner.
fn clone_example_with_spinner(example: &str, directory: &Path) -> Result<()> {
    use std::process::{Command, Stdio};

    with_spinner(
        "Cloning example subgraph",
        "Failed to clone example",
        "Cloned with warnings",
        |_spinner| {
            let temp_dir = tempfile::tempdir()?;
            let repo_url = "https://github.com/graphprotocol/graph-tooling";
            let example_subpath = format!("examples/{}", example);

            // Initialize empty repo with sparse checkout
            let status = Command::new("git")
                .current_dir(temp_dir.path())
                .args(["init"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            if !status.success() {
                return Err(anyhow!("Failed to initialize git repository"));
            }

            // Configure sparse checkout for just the example directory
            Command::new("git")
                .current_dir(temp_dir.path())
                .args(["sparse-checkout", "init", "--cone"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            Command::new("git")
                .current_dir(temp_dir.path())
                .args(["sparse-checkout", "set", &example_subpath])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            // Add remote and fetch only the needed content
            Command::new("git")
                .current_dir(temp_dir.path())
                .args(["remote", "add", "origin", repo_url])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            let fetch_status = Command::new("git")
                .current_dir(temp_dir.path())
                .args(["fetch", "--depth=1", "origin", "main"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            if !fetch_status.success() {
                return Err(anyhow!("Failed to fetch from graph-tooling repository"));
            }

            Command::new("git")
                .current_dir(temp_dir.path())
                .args(["checkout", "origin/main"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            // Verify example exists
            let example_path = temp_dir.path().join("examples").join(example);
            if !example_path.exists() {
                return Err(anyhow!(
                    "Example '{}' not found. See available examples at:\n\
                     https://github.com/graphprotocol/graph-tooling/tree/main/examples",
                    example
                ));
            }

            // Copy example to target directory
            copy_dir_recursive(&example_path, directory)?;

            Ok(())
        },
    )
}

/// Initialize networks.json with spinner.
fn init_networks_config_with_spinner(directory: &Path) -> Result<()> {
    with_spinner(
        "Initialize networks config",
        "Failed to initialize networks config",
        "Networks config initialized with warnings",
        |_spinner| {
            // Create a minimal networks.json if it doesn't exist
            let networks_path = directory.join("networks.json");
            if !networks_path.exists() {
                let networks_content = serde_json::json!({
                    "mainnet": {
                        "Gravity": {
                            "address": "0x2E645469f354BB4F5c8a05B3b30A929361cf77eC"
                        }
                    }
                });
                fs::write(
                    &networks_path,
                    serde_json::to_string_pretty(&networks_content)?,
                )?;
            }
            Ok(())
        },
    )
}

/// Update package.json with subgraph name with spinner.
fn update_package_json_with_spinner(directory: &Path, subgraph_name: &str) -> Result<()> {
    with_spinner(
        "Update subgraph name and commands in package.json",
        "Failed to update package.json",
        "Updated with warnings",
        |_spinner| {
            let package_path = directory.join("package.json");
            if package_path.exists() {
                let content = fs::read_to_string(&package_path)?;
                let mut package: serde_json::Value = serde_json::from_str(&content)?;

                if let Some(obj) = package.as_object_mut() {
                    // Update name to the subgraph basename
                    let basename = subgraph_name
                        .split('/')
                        .next_back()
                        .unwrap_or(subgraph_name);
                    obj.insert("name".to_string(), serde_json::json!(basename));

                    // Remove license and repository fields (like graph-cli does)
                    obj.remove("license");
                    obj.remove("repository");

                    // Update deploy script with new subgraph name
                    if let Some(scripts) = obj.get_mut("scripts").and_then(|s| s.as_object_mut()) {
                        scripts.insert(
                            "deploy".to_string(),
                            serde_json::json!(format!(
                                "graph deploy --node https://api.studio.thegraph.com/deploy/ {}",
                                subgraph_name
                            )),
                        );
                        scripts.insert(
                            "create-local".to_string(),
                            serde_json::json!(format!(
                                "graph create --node http://localhost:8020/ {}",
                                subgraph_name
                            )),
                        );
                        scripts.insert(
                            "remove-local".to_string(),
                            serde_json::json!(format!(
                                "graph remove --node http://localhost:8020/ {}",
                                subgraph_name
                            )),
                        );
                        scripts.insert(
                            "deploy-local".to_string(),
                            serde_json::json!(format!(
                                "graph deploy --node http://localhost:8020/ --ipfs http://localhost:5001 {}",
                                subgraph_name
                            )),
                        );
                    }
                }

                fs::write(&package_path, serde_json::to_string_pretty(&package)?)?;
            }
            Ok(())
        },
    )
}

/// Initialize git repository with spinner.
fn init_git_with_spinner(directory: &Path) -> Result<()> {
    use std::process::{Command, Stdio};

    with_spinner(
        "Initialize subgraph repository",
        "Failed to initialize git repository",
        "Git initialized with warnings",
        |_spinner| {
            // Remove any existing .git directory (from the cloned example)
            let git_dir = directory.join(".git");
            if git_dir.exists() {
                fs::remove_dir_all(&git_dir)?;
            }

            // Initialize fresh git repo
            let status = Command::new("git")
                .current_dir(directory)
                .arg("init")
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            if !status.success() {
                return Err(anyhow!("git init failed"));
            }

            // Stage all files
            let _ = Command::new("git")
                .current_dir(directory)
                .args(["add", "--all"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            // Create initial commit
            let _ = Command::new("git")
                .current_dir(directory)
                .args(["commit", "-m", "Initial commit"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();

            Ok(())
        },
    )
}

/// Run codegen with spinner.
fn run_codegen_with_spinner(directory: &Path) -> Result<()> {
    use std::process::{Command, Stdio};

    // Detect package manager
    let pkg_manager = if directory.join("pnpm-lock.yaml").exists() {
        "pnpm"
    } else if directory.join("yarn.lock").exists() {
        "yarn"
    } else {
        "npm"
    };

    with_spinner(
        format!("Generate ABI and schema types with {} codegen", pkg_manager),
        "Failed to run codegen",
        "Codegen completed with warnings",
        |_spinner| {
            let status = Command::new(pkg_manager)
                .current_dir(directory)
                .args(["run", "codegen"])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()?;

            if !status.success() {
                return Err(anyhow!("codegen failed"));
            }

            Ok(())
        },
    )
}

/// Print the "Next steps" message in graph-cli format.
fn print_next_steps(subgraph_name: &str, directory: &Path) {
    let basename = subgraph_name
        .split('/')
        .next_back()
        .unwrap_or(subgraph_name);
    let dir_name = directory
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| directory.to_string_lossy().to_string());

    println!();
    println!("Subgraph {} created in {}", basename, dir_name);
    println!();
    println!("Next steps:");
    println!();
    println!("  1. Run `graph auth` to authenticate with your deploy key.");
    println!();
    println!("  2. Type `cd {}` to enter the subgraph.", dir_name);
    println!();
    println!("  3. Run `yarn deploy` to deploy the subgraph.");
    println!();
    println!("Make sure to visit the documentation on https://thegraph.com/docs/ for further information.");
}

/// Initialize a subgraph from an existing deployed subgraph.
///
/// This creates a new subgraph scaffold based on an existing subgraph deployment,
/// extracting immutable entities from the deployed schema.
async fn init_from_subgraph(opt: &InitOpt) -> Result<()> {
    let deployment_id = opt
        .from_subgraph
        .as_ref()
        .ok_or_else(|| anyhow!("Deployment ID is required for --from-subgraph"))?;

    let network = opt.network.as_deref().unwrap_or("mainnet");

    // Determine IPFS URL
    let ipfs_url = opt
        .ipfs
        .as_deref()
        .unwrap_or("https://api.thegraph.com/ipfs");

    step(
        Step::Load,
        &format!("Fetching subgraph {} from IPFS", deployment_id),
    );

    // Create IPFS client
    let ipfs_client = IpfsClient::new(ipfs_url)?;

    // Fetch the manifest from IPFS
    let manifest_yaml = ipfs_client.fetch_manifest(deployment_id).await?;

    step(Step::Done, "Manifest fetched successfully");

    // Parse the manifest
    let manifest: serde_yaml::Value =
        serde_yaml::from_str(&manifest_yaml).context("Failed to parse subgraph manifest YAML")?;

    // Validate network matches
    if let Some(manifest_network) = extract_network(&manifest) {
        if manifest_network != network {
            return Err(anyhow!(
                "Network mismatch: The source subgraph is indexing '{}', but you specified '{}'.\n\
                 When composing subgraphs, they must index the same network.",
                manifest_network,
                network
            ));
        }
    }

    // Get start block from manifest if not provided
    let start_block = opt
        .start_block
        .as_ref()
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| get_min_start_block(&manifest));

    // Extract schema CID from manifest
    let schema_cid = extract_schema_cid(&manifest).ok_or_else(|| {
        anyhow!("Could not find schema CID in manifest. Expected schema.file['/'] field.")
    })?;

    step(Step::Load, &format!("Fetching schema {}", schema_cid));

    // Fetch the schema from IPFS
    let schema_content = ipfs_client.fetch_schema(&schema_cid).await?;

    step(Step::Done, "Schema fetched successfully");

    // Find immutable entities in the schema
    let immutable_entities = find_immutable_entities(&schema_content)?;

    if immutable_entities.is_empty() {
        return Err(anyhow!(
            "Source subgraph must have at least one immutable entity.\n\
             This subgraph cannot be used as a source subgraph since it has no immutable entities.\n\n\
             Immutable entities are marked with @entity(immutable: true) in the schema."
        ));
    }

    step(
        Step::Generate,
        &format!(
            "Found {} immutable entities: {}",
            immutable_entities.len(),
            immutable_entities.join(", ")
        ),
    );

    // Determine subgraph name
    let subgraph_name = opt
        .subgraph_name
        .clone()
        .unwrap_or_else(|| "composed-subgraph".to_string());

    // Determine directory
    let directory = opt.directory.clone().unwrap_or_else(|| {
        PathBuf::from(subgraph_name.split('/').next_back().unwrap_or("subgraph"))
    });

    // Check if directory already exists
    if directory.exists() {
        return Err(anyhow!(
            "Directory '{}' already exists. Please choose a different name or remove the existing directory.",
            directory.display()
        ));
    }

    // Generate scaffold with immutable entities
    let scaffold_options = ScaffoldOptions {
        address: None,
        network: network.to_string(),
        contract_name: "Contract".to_string(),
        subgraph_name: subgraph_name.clone(),
        start_block,
        abi: None,
        index_events: false,
    };

    generate_scaffold(&directory, &scaffold_options)?;

    // Write the fetched schema to the subgraph
    let schema_path = directory.join("schema.graphql");
    fs::write(&schema_path, &schema_content)
        .with_context(|| format!("Failed to write schema to {}", schema_path.display()))?;

    // Update the manifest with the source subgraph reference
    update_manifest_with_source(&directory, deployment_id, &immutable_entities)?;

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
    println!("  # Edit subgraph.yaml to add your own data sources");
    println!("  # The source subgraph's immutable entities are available via grafting");
    println!("  gnd codegen");
    println!("  gnd build");

    Ok(())
}

// ============================================================================
// Manifest Parsing Utilities
// ============================================================================

/// Extract the schema CID from a subgraph manifest.
///
/// The schema is typically at `schema.file["/"]` in the manifest YAML.
fn extract_schema_cid(manifest: &serde_yaml::Value) -> Option<String> {
    let schema = manifest.get("schema")?;
    let file = schema.get("file")?;
    let cid = file.get("/")?;
    let cid_str = cid.as_str()?;

    // Strip /ipfs/ prefix if present
    Some(
        cid_str
            .strip_prefix("/ipfs/")
            .unwrap_or(cid_str)
            .to_string(),
    )
}

/// Extract the network from the first data source in a manifest.
fn extract_network(manifest: &serde_yaml::Value) -> Option<String> {
    let data_sources = manifest.get("dataSources")?.as_sequence()?;
    let first_ds = data_sources.first()?;
    let network = first_ds.get("network")?.as_str()?;
    Some(network.to_string())
}

/// Get the minimum start block from all data sources.
fn get_min_start_block(manifest: &serde_yaml::Value) -> Option<u64> {
    let data_sources = manifest.get("dataSources")?.as_sequence()?;

    let start_blocks: Vec<u64> = data_sources
        .iter()
        .filter_map(|ds| {
            ds.get("source")
                .and_then(|s| s.get("startBlock"))
                .and_then(|b| b.as_u64())
        })
        .collect();

    start_blocks.into_iter().min()
}

// ============================================================================
// Schema Parsing Utilities
// ============================================================================

/// Find all immutable entities in a GraphQL schema.
///
/// Immutable entities are marked with `@entity(immutable: true)`.
fn find_immutable_entities(schema: &str) -> Result<Vec<String>> {
    let ast = gql::parse_schema::<String>(schema)
        .map_err(|e| anyhow!("Failed to parse GraphQL schema: {}", e))?;

    let mut immutable_entities = Vec::new();

    for def in ast.definitions {
        if let gql::Definition::TypeDefinition(gql::TypeDefinition::Object(obj)) = def {
            // Check if this type has @entity(immutable: true)
            if is_immutable_entity(&obj) {
                immutable_entities.push(obj.name);
            }
        }
    }

    Ok(immutable_entities)
}

/// Check if an object type has the @entity(immutable: true) directive.
fn is_immutable_entity(obj: &gql::ObjectType<String>) -> bool {
    for directive in &obj.directives {
        if directive.name == "entity" {
            for (name, value) in &directive.arguments {
                if name == "immutable" {
                    if let gql::Value::Boolean(true) = value {
                        return true;
                    }
                }
            }
        }
    }
    false
}

/// Update the generated manifest to include the source subgraph reference.
fn update_manifest_with_source(
    directory: &Path,
    source_deployment: &str,
    _immutable_entities: &[String],
) -> Result<()> {
    let manifest_path = directory.join("subgraph.yaml");

    // Read the generated manifest
    let manifest_content = fs::read_to_string(&manifest_path)
        .with_context(|| format!("Failed to read manifest at {}", manifest_path.display()))?;

    // Add a comment about the source subgraph at the top
    let updated_content = format!(
        "# This subgraph composes data from an existing subgraph.\n\
         # Source deployment: {}\n\
         #\n\
         # To access the source subgraph's immutable entities, you can use grafting:\n\
         # features:\n\
         #   - grafting\n\
         # graft:\n\
         #   base: {}\n\
         #   block: <block-number>\n\
         #\n{}",
        source_deployment, source_deployment, manifest_content
    );

    fs::write(&manifest_path, updated_content)
        .with_context(|| format!("Failed to update manifest at {}", manifest_path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_display() {
        assert_eq!(Protocol::Ethereum.to_string(), "ethereum");
        assert_eq!(Protocol::Near.to_string(), "near");
        assert_eq!(Protocol::Cosmos.to_string(), "cosmos");
        assert_eq!(Protocol::Arweave.to_string(), "arweave");
        assert_eq!(Protocol::Substreams.to_string(), "substreams");
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

        // from_subgraph should not be interactive (will error)
        let opt = InitOpt {
            from_subgraph: Some("Qm123".to_string()),
            ..Default::default()
        };
        assert!(!should_run_interactive(&opt));
    }

    #[test]
    fn test_init_opt_default() {
        let opt = InitOpt::default();
        assert!(opt.subgraph_name.is_none());
        assert!(opt.directory.is_none());
        assert!(opt.protocol.is_none());
        assert!(opt.node.is_none());
        assert!(opt.from_contract.is_none());
        assert!(opt.from_example.is_none());
        assert!(opt.from_subgraph.is_none());
        assert!(opt.contract_name.is_none());
        assert!(!opt.index_events);
        assert!(!opt.skip_install);
        assert!(!opt.skip_git);
        assert!(opt.start_block.is_none());
        assert!(opt.abi.is_none());
        assert!(opt.spkg.is_none());
        assert!(opt.network.is_none());
        assert!(opt.ipfs.is_none());
    }

    // ========================================================================
    // Manifest Parsing Tests
    // ========================================================================

    #[test]
    fn test_extract_schema_cid() {
        // Standard manifest format with /ipfs/ prefix
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            schema:
              file:
                "/": "/ipfs/QmSchemaHash123"
            "#,
        )
        .unwrap();
        assert_eq!(
            extract_schema_cid(&manifest),
            Some("QmSchemaHash123".to_string())
        );

        // Manifest without /ipfs/ prefix
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            schema:
              file:
                "/": "QmSchemaHash456"
            "#,
        )
        .unwrap();
        assert_eq!(
            extract_schema_cid(&manifest),
            Some("QmSchemaHash456".to_string())
        );

        // Missing schema
        let manifest: serde_yaml::Value = serde_yaml::from_str(r#"dataSources: []"#).unwrap();
        assert_eq!(extract_schema_cid(&manifest), None);

        // Missing file
        let manifest: serde_yaml::Value = serde_yaml::from_str(r#"schema: {}"#).unwrap();
        assert_eq!(extract_schema_cid(&manifest), None);
    }

    #[test]
    fn test_extract_network() {
        // Standard manifest with network
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            dataSources:
              - network: mainnet
                source:
                  address: "0x123"
            "#,
        )
        .unwrap();
        assert_eq!(extract_network(&manifest), Some("mainnet".to_string()));

        // Multiple data sources
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            dataSources:
              - network: polygon
                source:
                  address: "0x123"
              - network: polygon
                source:
                  address: "0x456"
            "#,
        )
        .unwrap();
        assert_eq!(extract_network(&manifest), Some("polygon".to_string()));

        // No data sources
        let manifest: serde_yaml::Value = serde_yaml::from_str(r#"dataSources: []"#).unwrap();
        assert_eq!(extract_network(&manifest), None);

        // Missing dataSources
        let manifest: serde_yaml::Value = serde_yaml::from_str(r#"schema: {}"#).unwrap();
        assert_eq!(extract_network(&manifest), None);
    }

    #[test]
    fn test_get_min_start_block() {
        // Multiple data sources with start blocks
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            dataSources:
              - source:
                  startBlock: 100
              - source:
                  startBlock: 50
              - source:
                  startBlock: 200
            "#,
        )
        .unwrap();
        assert_eq!(get_min_start_block(&manifest), Some(50));

        // Mixed with and without start blocks
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            dataSources:
              - source:
                  address: "0x123"
              - source:
                  startBlock: 150
              - source:
                  startBlock: 75
            "#,
        )
        .unwrap();
        assert_eq!(get_min_start_block(&manifest), Some(75));

        // No start blocks
        let manifest: serde_yaml::Value = serde_yaml::from_str(
            r#"
            dataSources:
              - source:
                  address: "0x123"
            "#,
        )
        .unwrap();
        assert_eq!(get_min_start_block(&manifest), None);

        // Empty data sources
        let manifest: serde_yaml::Value = serde_yaml::from_str(r#"dataSources: []"#).unwrap();
        assert_eq!(get_min_start_block(&manifest), None);
    }

    // ========================================================================
    // Schema Parsing Tests
    // ========================================================================

    #[test]
    fn test_find_immutable_entities() {
        // Schema with immutable entities
        let schema = r#"
            type Transfer @entity(immutable: true) {
                id: ID!
                from: Bytes!
                to: Bytes!
                amount: BigInt!
            }

            type Account @entity {
                id: ID!
                balance: BigInt!
            }

            type Approval @entity(immutable: true) {
                id: ID!
                owner: Bytes!
                spender: Bytes!
            }
        "#;

        let entities = find_immutable_entities(schema).unwrap();
        assert_eq!(entities.len(), 2);
        assert!(entities.contains(&"Transfer".to_string()));
        assert!(entities.contains(&"Approval".to_string()));
        assert!(!entities.contains(&"Account".to_string()));
    }

    #[test]
    fn test_find_immutable_entities_none() {
        // Schema with no immutable entities
        let schema = r#"
            type Account @entity {
                id: ID!
                balance: BigInt!
            }

            type Token @entity {
                id: ID!
                name: String!
            }
        "#;

        let entities = find_immutable_entities(schema).unwrap();
        assert!(entities.is_empty());
    }

    #[test]
    fn test_find_immutable_entities_explicit_false() {
        // Entity with immutable: false should not be included
        let schema = r#"
            type Transfer @entity(immutable: false) {
                id: ID!
                amount: BigInt!
            }

            type Withdrawal @entity(immutable: true) {
                id: ID!
                amount: BigInt!
            }
        "#;

        let entities = find_immutable_entities(schema).unwrap();
        assert_eq!(entities.len(), 1);
        assert!(entities.contains(&"Withdrawal".to_string()));
    }

    #[test]
    fn test_is_immutable_entity() {
        let schema = r#"
            type Transfer @entity(immutable: true) {
                id: ID!
            }
        "#;

        let ast = gql::parse_schema::<String>(schema).unwrap();
        for def in ast.definitions {
            if let gql::Definition::TypeDefinition(gql::TypeDefinition::Object(obj)) = def {
                if obj.name == "Transfer" {
                    assert!(is_immutable_entity(&obj));
                }
            }
        }

        // Non-immutable entity
        let schema = r#"
            type Account @entity {
                id: ID!
            }
        "#;

        let ast = gql::parse_schema::<String>(schema).unwrap();
        for def in ast.definitions {
            if let gql::Definition::TypeDefinition(gql::TypeDefinition::Object(obj)) = def {
                if obj.name == "Account" {
                    assert!(!is_immutable_entity(&obj));
                }
            }
        }
    }
}
