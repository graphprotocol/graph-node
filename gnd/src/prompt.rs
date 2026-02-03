//! Interactive prompt utilities for CLI commands.
//!
//! This module provides wrappers around the `inquire` crate to provide
//! consistent prompting behavior similar to the TypeScript graph-cli.

use anyhow::Result;
use console::{style, Term};
use inquire::validator::Validation;
use inquire::{Autocomplete, Confirm, CustomUserError, Select, Text};

use crate::output::{step, Step};
use crate::services::{ContractInfo, ContractService, Network, NetworksRegistry};

/// Format a network for display.
fn format_network(network: &Network) -> String {
    let full_name = network.full_name.as_deref().unwrap_or(&network.id);
    format!("{} ({})", full_name, network.id)
}

/// A network autocompleter that works with owned data.
#[derive(Clone)]
struct SimpleNetworkCompleter {
    entries: Vec<NetworkEntry>,
}

#[derive(Clone)]
struct NetworkEntry {
    display: String,
    search_terms: String,
}

impl SimpleNetworkCompleter {
    fn new(registry: &NetworksRegistry) -> Self {
        let entries: Vec<NetworkEntry> = registry
            .networks
            .iter()
            .map(|n| {
                let display = format_network(n);
                let search_terms = format!(
                    "{} {} {} {}",
                    n.id,
                    n.full_name.as_deref().unwrap_or(""),
                    n.short_name.as_deref().unwrap_or(""),
                    n.aliases.join(" ")
                )
                .to_lowercase();
                NetworkEntry {
                    display,
                    search_terms,
                }
            })
            .collect();
        Self { entries }
    }
}

impl Autocomplete for SimpleNetworkCompleter {
    fn get_suggestions(&mut self, input: &str) -> Result<Vec<String>, CustomUserError> {
        let input_lower = input.to_lowercase();
        let matches: Vec<String> = self
            .entries
            .iter()
            .filter(|e| {
                e.search_terms.contains(&input_lower)
                    || e.display.to_lowercase().contains(&input_lower)
            })
            .take(15)
            .map(|e| e.display.clone())
            .collect();
        Ok(matches)
    }

    fn get_completion(
        &mut self,
        _input: &str,
        highlighted_suggestion: Option<String>,
    ) -> Result<inquire::autocompletion::Replacement, CustomUserError> {
        Ok(highlighted_suggestion)
    }
}

/// Prompt for the subgraph slug/name.
pub fn prompt_subgraph_name(default: Option<&str>) -> Result<String> {
    let mut prompt = Text::new("Subgraph slug:")
        .with_help_message("e.g., my-subgraph or myorg/my-subgraph")
        .with_validator(|input: &str| {
            if input.trim().is_empty() {
                Ok(Validation::Invalid("Subgraph slug cannot be empty".into()))
            } else {
                Ok(Validation::Valid)
            }
        });

    if let Some(d) = default {
        prompt = prompt.with_default(d);
    }

    let name = prompt.prompt()?;
    Ok(format_subgraph_name(&name))
}

/// Prompt for the directory to create the subgraph in.
pub fn prompt_directory(default: Option<&str>) -> Result<String> {
    let mut prompt = Text::new("Directory:")
        .with_help_message("Directory to create the subgraph in")
        .with_validator(|input: &str| {
            if input.trim().is_empty() {
                Ok(Validation::Invalid("Directory cannot be empty".into()))
            } else {
                Ok(Validation::Valid)
            }
        });

    if let Some(d) = default {
        prompt = prompt.with_default(d);
    }

    Ok(prompt.prompt()?)
}

/// Prompt for a contract address.
pub fn prompt_contract_address() -> Result<String> {
    Text::new("Contract address:")
        .with_help_message("0x... address of the contract")
        .with_validator(|input: &str| {
            if !input.starts_with("0x") || input.len() != 42 {
                Ok(Validation::Invalid(
                    "Address must start with 0x and be 42 characters".into(),
                ))
            } else if !input[2..].chars().all(|c| c.is_ascii_hexdigit()) {
                Ok(Validation::Invalid(
                    "Address must contain only hex characters".into(),
                ))
            } else {
                Ok(Validation::Valid)
            }
        })
        .prompt()
        .map_err(Into::into)
}

/// Prompt for the contract name.
pub fn prompt_contract_name(default: Option<&str>) -> Result<String> {
    let mut prompt = Text::new("Contract name:")
        .with_help_message("Name for the contract (used in generated code)")
        .with_validator(|input: &str| {
            if input.trim().is_empty() {
                Ok(Validation::Invalid("Contract name cannot be empty".into()))
            } else {
                Ok(Validation::Valid)
            }
        });

    if let Some(d) = default {
        prompt = prompt.with_default(d);
    } else {
        prompt = prompt.with_default("Contract");
    }

    Ok(prompt.prompt()?)
}

/// Prompt for the start block.
pub fn prompt_start_block(default: Option<u64>) -> Result<Option<u64>> {
    let default_str = default
        .map(|b| b.to_string())
        .unwrap_or_else(|| "0".to_string());

    let input = Text::new("Start block:")
        .with_help_message("Block number to start indexing from")
        .with_default(&default_str)
        .with_validator(|input: &str| {
            if input.trim().is_empty() {
                Ok(Validation::Valid)
            } else if input.parse::<u64>().is_err() {
                Ok(Validation::Invalid("Must be a valid block number".into()))
            } else {
                Ok(Validation::Valid)
            }
        })
        .prompt()?;

    if input.trim().is_empty() {
        Ok(None)
    } else {
        Ok(input.parse().ok())
    }
}

/// Prompt for whether to index events.
pub fn prompt_index_events() -> Result<bool> {
    Confirm::new("Index contract events as entities?")
        .with_default(true)
        .with_help_message("Generate entities for each event in the contract ABI")
        .prompt()
        .map_err(Into::into)
}

/// Prompt for the source type (contract, example, subgraph).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SourceType {
    Contract,
    Example,
    // Subgraph support is not yet implemented
}

impl std::fmt::Display for SourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceType::Contract => write!(f, "Smart contract"),
            SourceType::Example => write!(f, "Example subgraph"),
        }
    }
}

/// Prompt for the scaffold source type.
pub fn prompt_source_type() -> Result<SourceType> {
    let options = vec![SourceType::Contract, SourceType::Example];

    Select::new("Source:", options)
        .with_help_message("How to initialize the subgraph")
        .prompt()
        .map_err(Into::into)
}

/// Prompt for ABI file path.
pub fn prompt_abi_path() -> Result<Option<String>> {
    let has_abi = Confirm::new("Do you have an ABI file?")
        .with_default(false)
        .with_help_message("If not, we'll try to fetch it from Etherscan/Sourcify")
        .prompt()?;

    if has_abi {
        let path = Text::new("ABI file path:")
            .with_help_message("Path to the contract ABI JSON file")
            .prompt()?;
        Ok(Some(path))
    } else {
        Ok(None)
    }
}

/// Prompt for subgraph slug with graph-cli style confirmation output.
///
/// After the user enters a value, clears the prompt line and shows:
/// `✔ Subgraph slug · <value>`
pub fn prompt_subgraph_slug_with_confirm(default: Option<&str>) -> Result<String> {
    let term = Term::stderr();
    let mut prompt = Text::new("Subgraph slug")
        .with_help_message("e.g., my-subgraph or myorg/my-subgraph")
        .with_validator(|input: &str| {
            if input.trim().is_empty() {
                Ok(Validation::Invalid("Subgraph slug cannot be empty".into()))
            } else {
                Ok(Validation::Valid)
            }
        });

    if let Some(d) = default {
        prompt = prompt.with_default(d);
    }

    let name = prompt.prompt()?;
    let formatted = format_subgraph_name(&name);

    // Clear the previous line and print confirmation
    let _ = term.clear_last_lines(1);
    let label = "Subgraph slug";
    println!(
        "{} {} {} {}",
        style("✔").green(),
        label,
        style("·").dim(),
        formatted
    );

    Ok(formatted)
}

/// Prompt for directory with graph-cli style confirmation output.
///
/// After the user enters a value, clears the prompt line and shows:
/// `✔ Directory to create the subgraph in · <value>`
pub fn prompt_directory_with_confirm(default: Option<&str>) -> Result<String> {
    let term = Term::stderr();
    let mut prompt = Text::new("Directory to create the subgraph in")
        .with_help_message("Directory name for the new subgraph")
        .with_validator(|input: &str| {
            if input.trim().is_empty() {
                Ok(Validation::Invalid("Directory cannot be empty".into()))
            } else {
                Ok(Validation::Valid)
            }
        });

    if let Some(d) = default {
        prompt = prompt.with_default(d);
    }

    let dir = prompt.prompt()?;

    // Clear the previous line and print confirmation
    let _ = term.clear_last_lines(1);
    let label = "Directory to create the subgraph in";
    println!(
        "{} {} {} {}",
        style("✔").green(),
        label,
        style("·").dim(),
        dir
    );

    Ok(dir)
}

/// Format a subgraph name to be valid.
fn format_subgraph_name(name: &str) -> String {
    name.trim()
        .to_lowercase()
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '/' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

/// Extract the directory name from a subgraph name.
pub fn get_subgraph_basename(name: &str) -> String {
    name.split('/').next_back().unwrap_or(name).to_string()
}

/// Interactive init form for when options are not fully provided.
pub struct InitForm {
    pub network: String,
    pub subgraph_name: String,
    pub directory: String,
    pub source_type: SourceType,
    pub contract_address: Option<String>,
    pub contract_name: String,
    pub start_block: Option<u64>,
    pub index_events: bool,
    pub abi_path: Option<String>,
}

impl InitForm {
    /// Run the interactive form, filling in missing values.
    ///
    /// The flow for contract mode is:
    /// 1. Prompt for network (if not provided)
    /// 2. Prompt for contract address (if not provided)
    /// 3. Fetch contract info from Etherscan/Sourcify (ABI, name, start block)
    /// 4. Use fetched values as defaults for remaining prompts
    #[allow(clippy::too_many_arguments)]
    pub async fn run_interactive(
        registry: &NetworksRegistry,
        // Pre-filled values from CLI args
        network: Option<String>,
        subgraph_name: Option<String>,
        directory: Option<String>,
        from_contract: Option<String>,
        from_example: bool,
        contract_name: Option<String>,
        start_block: Option<u64>,
        index_events: bool,
        abi_path: Option<String>,
    ) -> Result<Self> {
        // Determine source type from flags or prompt
        let source_type = if from_contract.is_some() {
            SourceType::Contract
        } else if from_example {
            SourceType::Example
        } else {
            prompt_source_type()?
        };

        // For example mode, we only need subgraph name and directory
        if source_type == SourceType::Example {
            let subgraph_name = match subgraph_name {
                Some(n) => n,
                None => prompt_subgraph_name(None)?,
            };

            let default_dir = get_subgraph_basename(&subgraph_name);
            let directory = match directory {
                Some(d) => d,
                None => prompt_directory(Some(&default_dir))?,
            };

            return Ok(Self {
                network: "mainnet".to_string(), // Not used for example
                subgraph_name,
                directory,
                source_type,
                contract_address: None,
                contract_name: "Contract".to_string(),
                start_block: None,
                index_events: false,
                abi_path: None,
            });
        }

        // For contract mode, we need network, address, etc.

        // Step 1: Network
        let network = match network {
            Some(n) => n,
            None => prompt_network_interactive(registry)?,
        };

        // Step 2: Contract address
        let contract_address = match from_contract {
            Some(addr) => addr,
            None => prompt_contract_address()?,
        };

        // Step 3: Fetch contract info immediately after getting address
        // This allows us to use fetched values as defaults for remaining prompts
        let fetched_info = if abi_path.is_none() {
            fetch_contract_info_interactive(&network, &contract_address).await
        } else {
            None
        };

        // Step 4: Contract name (use fetched name as default)
        let default_contract_name = fetched_info
            .as_ref()
            .map(|info| info.name.clone())
            .or_else(|| contract_name.clone());
        let contract_name = match contract_name {
            Some(n) => n,
            None => prompt_contract_name(default_contract_name.as_deref())?,
        };

        // Subgraph name
        let default_subgraph_name = contract_name.to_lowercase();
        let subgraph_name = match subgraph_name {
            Some(n) => n,
            None => prompt_subgraph_name(Some(&default_subgraph_name))?,
        };

        // Directory
        let default_dir = get_subgraph_basename(&subgraph_name);
        let directory = match directory {
            Some(d) => d,
            None => prompt_directory(Some(&default_dir))?,
        };

        // ABI path - only prompt if we didn't fetch it successfully
        let abi_path = match abi_path {
            Some(p) => Some(p),
            None => {
                if fetched_info.is_some() {
                    // We successfully fetched the ABI, no need to prompt
                    None
                } else {
                    prompt_abi_path()?
                }
            }
        };

        // Start block (use fetched start block as default)
        let default_start_block = fetched_info
            .as_ref()
            .and_then(|info| info.start_block)
            .or(start_block);
        let start_block = if start_block.is_some() {
            start_block
        } else {
            prompt_start_block(default_start_block)?
        };

        // Index events
        let index_events = if index_events {
            true
        } else {
            prompt_index_events()?
        };

        Ok(Self {
            network,
            subgraph_name,
            directory,
            source_type,
            contract_address: Some(contract_address),
            contract_name,
            start_block,
            index_events,
            abi_path,
        })
    }
}

/// Fetch contract info from Etherscan/Sourcify with status feedback.
///
/// Returns None if fetching fails (the prompts will fall back to manual entry).
async fn fetch_contract_info_interactive(network: &str, address: &str) -> Option<ContractInfo> {
    step(
        Step::Load,
        &format!("Fetching contract info for {} on {}", address, network),
    );

    // Load the contract service
    let service = match ContractService::load().await {
        Ok(s) => s,
        Err(e) => {
            step(
                Step::Warn,
                &format!("Could not load contract service: {}", e),
            );
            return None;
        }
    };

    // Fetch the contract info
    match service.get_contract_info(network, address).await {
        Ok(info) => {
            step(Step::Done, &format!("Found contract: {}", info.name));
            Some(info)
        }
        Err(e) => {
            step(
                Step::Warn,
                &format!(
                    "Could not fetch contract info: {}. You'll need to provide an ABI file.",
                    e
                ),
            );
            None
        }
    }
}

/// Prompt for network selection using the simple completer.
fn prompt_network_interactive(registry: &NetworksRegistry) -> Result<String> {
    let completer = SimpleNetworkCompleter::new(registry);

    let input = Text::new("Network:")
        .with_autocomplete(completer)
        .with_help_message("Type to search (mainnet, polygon, arbitrum-one, etc.)")
        .prompt()?;

    // Extract network ID from the selection
    // Format is "Full Name (id)"
    if let Some(start) = input.rfind('(') {
        if let Some(end) = input.rfind(')') {
            let id = &input[start + 1..end];
            if registry.get_network(id).is_some() {
                return Ok(id.to_string());
            }
        }
    }

    // Try the input as-is
    if registry.get_network(&input).is_some() {
        return Ok(input);
    }

    anyhow::bail!(
        "Network '{}' not found in registry. Check available networks at https://thegraph.com/docs/en/supported-networks/",
        input
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_subgraph_name() {
        assert_eq!(format_subgraph_name("My Subgraph"), "my-subgraph");
        assert_eq!(format_subgraph_name("org/my-subgraph"), "org/my-subgraph");
        assert_eq!(format_subgraph_name("  test  "), "test");
        assert_eq!(format_subgraph_name("Test_123"), "test-123");
        assert_eq!(format_subgraph_name("a/b/c"), "a/b/c");
    }

    #[test]
    fn test_get_subgraph_basename() {
        assert_eq!(get_subgraph_basename("my-subgraph"), "my-subgraph");
        assert_eq!(get_subgraph_basename("org/my-subgraph"), "my-subgraph");
        assert_eq!(get_subgraph_basename("a/b/c"), "c");
        assert_eq!(get_subgraph_basename(""), "");
    }

    #[test]
    fn test_format_network() {
        let network = Network {
            id: "mainnet".to_string(),
            short_name: Some("Ethereum".to_string()),
            full_name: Some("Ethereum Mainnet".to_string()),
            aliases: vec!["ethereum".to_string()],
            caip2_id: "eip155:1".to_string(),
            api_urls: vec![],
            rpc_urls: vec![],
        };
        assert_eq!(format_network(&network), "Ethereum Mainnet (mainnet)");

        let network_no_full_name = Network {
            id: "custom".to_string(),
            short_name: None,
            full_name: None,
            aliases: vec![],
            caip2_id: "eip155:999".to_string(),
            api_urls: vec![],
            rpc_urls: vec![],
        };
        assert_eq!(format_network(&network_no_full_name), "custom (custom)");
    }

    #[test]
    fn test_source_type_display() {
        assert_eq!(SourceType::Contract.to_string(), "Smart contract");
        assert_eq!(SourceType::Example.to_string(), "Example subgraph");
    }

    #[test]
    fn test_simple_network_completer() {
        let registry = NetworksRegistry {
            networks: vec![
                Network {
                    id: "mainnet".to_string(),
                    short_name: Some("Ethereum".to_string()),
                    full_name: Some("Ethereum Mainnet".to_string()),
                    aliases: vec!["ethereum".to_string(), "eth".to_string()],
                    caip2_id: "eip155:1".to_string(),
                    api_urls: vec![],
                    rpc_urls: vec![],
                },
                Network {
                    id: "polygon".to_string(),
                    short_name: Some("Polygon".to_string()),
                    full_name: Some("Polygon Mainnet".to_string()),
                    aliases: vec!["matic".to_string()],
                    caip2_id: "eip155:137".to_string(),
                    api_urls: vec![],
                    rpc_urls: vec![],
                },
            ],
        };

        let mut completer = SimpleNetworkCompleter::new(&registry);

        // Test suggestions for "mainnet" (exact ID match)
        let suggestions = completer.get_suggestions("mainnet").unwrap();
        assert!(!suggestions.is_empty());
        assert!(suggestions[0].contains("Ethereum Mainnet"));

        // Test suggestions for "eth" (matches "ethereum" alias)
        let suggestions = completer.get_suggestions("eth").unwrap();
        assert!(!suggestions.is_empty());
        // Should match the network with "ethereum" in aliases
        assert!(suggestions.iter().any(|s| s.contains("Ethereum")));

        // Test suggestions for "matic" (only matches polygon)
        let suggestions = completer.get_suggestions("matic").unwrap();
        assert_eq!(suggestions.len(), 1);
        assert!(suggestions[0].contains("Polygon"));

        // Test suggestions for empty string (should return all)
        let suggestions = completer.get_suggestions("").unwrap();
        assert_eq!(suggestions.len(), 2);

        // Test no matches
        let suggestions = completer.get_suggestions("nonexistent").unwrap();
        assert!(suggestions.is_empty());
    }
}
