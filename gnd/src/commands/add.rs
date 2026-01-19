//! Add command for adding data sources to existing subgraphs.
//!
//! This command adds a new data source to an existing subgraph, generating
//! the necessary manifest entries, schema types, and mapping stubs.

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::Parser;

use crate::output::{step, Step};

#[derive(Clone, Debug, Parser)]
#[clap(about = "Add a data source to an existing subgraph")]
pub struct AddOpt {
    /// Contract address to add
    #[clap()]
    pub address: String,

    /// Path to the subgraph manifest
    #[clap(default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

    /// Path to the contract ABI
    #[clap(long)]
    pub abi: Option<PathBuf>,

    /// Name for the new data source
    #[clap(long)]
    pub contract_name: Option<String>,

    /// Merge entities with the same name (off by default)
    #[clap(long)]
    pub merge_entities: bool,

    /// Network the contract is deployed to
    #[clap(long)]
    pub network: Option<String>,

    /// Block number to start indexing from
    #[clap(long)]
    pub start_block: Option<String>,
}

/// Run the add command.
pub fn run_add(opt: AddOpt) -> Result<()> {
    // Validate address format first
    if !opt.address.starts_with("0x") || opt.address.len() != 42 {
        return Err(anyhow!(
            "Invalid contract address '{}'. Expected format: 0x followed by 40 hex characters.",
            opt.address
        ));
    }

    step(
        Step::Load,
        &format!("Adding data source for {}", opt.address),
    );

    // Check if manifest exists
    if !opt.manifest.exists() {
        return Err(anyhow!(
            "Manifest file '{}' not found. Run 'gnd init' first to create a subgraph.",
            opt.manifest.display()
        ));
    }

    // For now, return an informative error about what's needed
    Err(anyhow!(
        "Add command is not yet fully implemented.\n\
         This feature requires:\n\
         - ABI fetching from Etherscan/Sourcify (unless --abi provided)\n\
         - Schema generation for contract events\n\
         - Mapping stub generation\n\
         - Manifest modification\n\n\
         As a workaround, you can:\n\
         1. Copy the ABI file to your abis/ directory\n\
         2. Add the data source manually to subgraph.yaml\n\
         3. Add event handlers to your schema.graphql\n\
         4. Run 'gnd codegen' to generate types\n\n\
         Or use the TypeScript graph-cli:\n\
         graph add {} --abi <path>",
        opt.address
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalid_address() {
        let opt = AddOpt {
            address: "invalid".to_string(),
            manifest: PathBuf::from("subgraph.yaml"),
            abi: None,
            contract_name: None,
            merge_entities: false,
            network: None,
            start_block: None,
        };

        let result = run_add(opt);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid contract address"));
    }
}
