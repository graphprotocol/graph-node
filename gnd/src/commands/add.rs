//! Add command for adding data sources to existing subgraphs.
//!
//! This command adds a new data source to an existing subgraph, generating
//! the necessary manifest entries, schema types, and mapping stubs.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use inflector::Inflector;
use serde_json::Value as JsonValue;

use crate::config::networks::update_networks_file;
use crate::formatter::format_typescript;
use crate::output::{step, Step};
use crate::scaffold::manifest::{extract_events_from_abi, EventInfo};
use crate::scaffold::ScaffoldOptions;
use crate::services::ContractService;

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

    /// Path to the networks.json file
    #[clap(long, default_value = "networks.json")]
    pub network_file: PathBuf,

    /// Block number to start indexing from
    #[clap(long)]
    pub start_block: Option<String>,
}

/// Run the add command.
pub async fn run_add(opt: AddOpt) -> Result<()> {
    // Validate address format first
    if !opt.address.starts_with("0x") || opt.address.len() != 42 {
        return Err(anyhow!(
            "Invalid contract address '{}'. Expected format: 0x followed by 40 hex characters.",
            opt.address
        ));
    }

    // Check if manifest exists
    if !opt.manifest.exists() {
        return Err(anyhow!(
            "Manifest file '{}' not found. Run 'gnd init' first to create a subgraph.",
            opt.manifest.display()
        ));
    }

    step(
        Step::Load,
        &format!("Adding data source for {}", opt.address),
    );

    // Load existing manifest to get network
    let manifest_content = fs::read_to_string(&opt.manifest)
        .with_context(|| format!("Failed to read manifest: {}", opt.manifest.display()))?;

    let manifest: serde_yaml::Value = serde_yaml::from_str(&manifest_content)
        .with_context(|| format!("Failed to parse manifest: {}", opt.manifest.display()))?;

    // Get network from manifest's first data source
    let network = manifest
        .get("dataSources")
        .and_then(|ds| ds.as_sequence())
        .and_then(|seq| seq.first())
        .and_then(|first| first.get("network"))
        .and_then(|n| n.as_str())
        .map(String::from)
        .unwrap_or_else(|| "mainnet".to_string());

    // Fetch or load ABI
    let (abi, contract_name, start_block) = get_contract_info(&opt, &network).await?;

    // Get project directory
    let project_dir = crate::manifest::manifest_dir(&opt.manifest);

    // Create scaffold options for code generation
    let scaffold_options = ScaffoldOptions {
        address: Some(opt.address.clone()),
        network: network.clone(),
        contract_name: contract_name.clone(),
        subgraph_name: "subgraph".to_string(),
        start_block,
        abi: Some(abi.clone()),
        index_events: true, // Always index events for add command
    };

    // Extract events from ABI
    let events = extract_events_from_abi(&scaffold_options);

    // Add ABI file
    add_abi_file(project_dir, &contract_name, &abi)?;

    // Add mapping file
    add_mapping_file(project_dir, &contract_name, &events)?;

    // Update manifest
    update_manifest(
        &opt.manifest,
        &opt.address,
        &contract_name,
        &network,
        start_block,
        &events,
    )?;

    // Update networks.json
    let networks_path = project_dir.join(&opt.network_file);
    update_networks_file(
        &networks_path,
        &network,
        &contract_name,
        &opt.address,
        start_block,
    )?;
    step(
        Step::Write,
        &format!("Updated {}", opt.network_file.display()),
    );

    step(Step::Done, &format!("Added data source: {}", contract_name));

    println!();
    println!("Next steps:");
    println!("  gnd codegen");
    println!("  gnd build");

    Ok(())
}

/// Get contract info (ABI, name, start block) from local file or network.
async fn get_contract_info(
    opt: &AddOpt,
    network: &str,
) -> Result<(JsonValue, String, Option<u64>)> {
    if let Some(abi_path) = &opt.abi {
        // Load ABI from file
        step(
            Step::Load,
            &format!("Loading ABI from {}", abi_path.display()),
        );

        let abi_str = fs::read_to_string(abi_path)
            .with_context(|| format!("Failed to read ABI file: {}", abi_path.display()))?;

        let abi: JsonValue = serde_json::from_str(&abi_str)
            .with_context(|| format!("Failed to parse ABI file: {}", abi_path.display()))?;

        let contract_name = opt.contract_name.clone().unwrap_or_else(|| {
            abi_path
                .file_stem()
                .and_then(|s| s.to_str())
                .map(|s| s.to_string())
                .unwrap_or_else(|| "Contract".to_string())
        });

        let start_block = opt.start_block.as_ref().and_then(|s| s.parse::<u64>().ok());

        Ok((abi, contract_name, start_block))
    } else {
        // Fetch from network
        step(
            Step::Load,
            &format!("Fetching ABI from {} network", network),
        );

        let contract_info = {
            let service = ContractService::load()
                .await
                .context("Failed to load contract service")?;

            service
                .get_contract_info(network, &opt.address)
                .await
                .context("Failed to fetch contract info")?
        };

        let contract_name = opt.contract_name.clone().unwrap_or(contract_info.name);

        let start_block = opt
            .start_block
            .as_ref()
            .and_then(|s| s.parse::<u64>().ok())
            .or(contract_info.start_block);

        Ok((contract_info.abi, contract_name, start_block))
    }
}

/// Add ABI file to the abis directory.
fn add_abi_file(project_dir: &Path, contract_name: &str, abi: &JsonValue) -> Result<()> {
    let abis_dir = project_dir.join("abis");
    fs::create_dir_all(&abis_dir).context("Failed to create abis directory")?;

    let abi_file = abis_dir.join(format!("{}.json", contract_name));
    let abi_str = serde_json::to_string_pretty(abi).context("Failed to serialize ABI")?;

    step(
        Step::Write,
        &format!("Writing ABI to {}", abi_file.display()),
    );
    fs::write(&abi_file, abi_str).context("Failed to write ABI file")?;

    Ok(())
}

/// Sanitize a field name for GraphQL.
fn sanitize_field_name(name: &str) -> String {
    if name.is_empty() {
        return "value".to_string();
    }

    let mut result = name.to_string();

    // Convert to camelCase if starts with uppercase
    if result
        .chars()
        .next()
        .map(|c| c.is_uppercase())
        .unwrap_or(false)
    {
        let mut chars = result.chars();
        if let Some(first) = chars.next() {
            result = first.to_lowercase().collect::<String>() + chars.as_str();
        }
    }

    // Avoid reserved words
    match result.as_str() {
        "id" => "eventId".to_string(),
        "type" => "eventType".to_string(),
        _ => result,
    }
}

/// Add mapping file for the new data source.
fn add_mapping_file(project_dir: &Path, contract_name: &str, events: &[EventInfo]) -> Result<()> {
    let src_dir = project_dir.join("src");
    fs::create_dir_all(&src_dir).context("Failed to create src directory")?;

    let mapping_file = src_dir.join(format!("{}.ts", contract_name.to_kebab_case()));

    if mapping_file.exists() {
        step(
            Step::Skip,
            &format!("Mapping file {} already exists", mapping_file.display()),
        );
        return Ok(());
    }

    let mapping_content = generate_mapping(contract_name, events);
    let formatted = format_typescript(&mapping_content).unwrap_or(mapping_content);

    step(
        Step::Write,
        &format!("Writing mapping to {}", mapping_file.display()),
    );
    fs::write(&mapping_file, formatted).context("Failed to write mapping file")?;

    Ok(())
}

/// Generate mapping handlers for events.
fn generate_mapping(contract_name: &str, events: &[EventInfo]) -> String {
    let mut imports = String::new();
    let mut handlers = String::new();

    imports.push_str("import { BigInt, Bytes } from \"@graphprotocol/graph-ts\"\n");

    if events.is_empty() {
        return imports;
    }

    // Import event types
    let event_imports: Vec<String> = events
        .iter()
        .map(|e| format!("{} as {}Event", e.name, e.name))
        .collect();

    imports.push_str(&format!(
        "import {{ {} }} from \"../generated/{}/{}\"\n",
        event_imports.join(", "),
        contract_name,
        contract_name
    ));

    // Import entity types
    let entity_imports: Vec<String> = events.iter().map(|e| e.name.clone()).collect();

    imports.push_str(&format!(
        "import {{ {} }} from \"../generated/schema\"\n",
        entity_imports.join(", ")
    ));

    // Generate handler for each event
    for event in events {
        handlers.push('\n');
        handlers.push_str(&generate_event_handler(event));
    }

    format!("{}\n{}", imports, handlers)
}

/// Generate a handler function for an event.
fn generate_event_handler(event: &EventInfo) -> String {
    let event_name = &event.name;

    let mut field_assignments = String::new();
    for input in &event.inputs {
        let field_name = sanitize_field_name(&input.name);
        field_assignments.push_str(&format!(
            "  entity.{} = event.params.{}\n",
            field_name, input.name
        ));
    }

    format!(
        r#"export function handle{event_name}(event: {event_name}Event): void {{
  let entity = new {event_name}(
    event.transaction.hash.concatI32(event.logIndex.toI32())
  )

{field_assignments}  entity.blockNumber = event.block.number
  entity.blockTimestamp = event.block.timestamp
  entity.transactionHash = event.transaction.hash

  entity.save()
}}
"#
    )
}

/// Update the manifest with the new data source.
fn update_manifest(
    manifest_path: &Path,
    address: &str,
    contract_name: &str,
    network: &str,
    start_block: Option<u64>,
    events: &[EventInfo],
) -> Result<()> {
    let content = fs::read_to_string(manifest_path).context("Failed to read manifest")?;

    let mut manifest: serde_yaml::Value =
        serde_yaml::from_str(&content).context("Failed to parse manifest")?;

    // Build the new data source
    let mut source = serde_yaml::Mapping::new();
    source.insert(
        serde_yaml::Value::String("abi".to_string()),
        serde_yaml::Value::String(contract_name.to_string()),
    );
    source.insert(
        serde_yaml::Value::String("address".to_string()),
        serde_yaml::Value::String(address.to_string()),
    );
    if let Some(block) = start_block {
        source.insert(
            serde_yaml::Value::String("startBlock".to_string()),
            serde_yaml::Value::Number(block.into()),
        );
    }

    // Build event handlers
    let mut event_handlers = Vec::new();
    for event in events {
        let mut handler = serde_yaml::Mapping::new();
        handler.insert(
            serde_yaml::Value::String("event".to_string()),
            serde_yaml::Value::String(event.signature.clone()),
        );
        handler.insert(
            serde_yaml::Value::String("handler".to_string()),
            serde_yaml::Value::String(format!("handle{}", event.name)),
        );
        event_handlers.push(serde_yaml::Value::Mapping(handler));
    }

    // Build entities list
    let entities: Vec<serde_yaml::Value> = events
        .iter()
        .map(|e| serde_yaml::Value::String(e.name.clone()))
        .collect();

    // Build ABI entry
    let mut abi_entry = serde_yaml::Mapping::new();
    abi_entry.insert(
        serde_yaml::Value::String("name".to_string()),
        serde_yaml::Value::String(contract_name.to_string()),
    );
    abi_entry.insert(
        serde_yaml::Value::String("file".to_string()),
        serde_yaml::Value::String(format!("./abis/{}.json", contract_name)),
    );

    // Build mapping section
    let mut mapping = serde_yaml::Mapping::new();
    mapping.insert(
        serde_yaml::Value::String("kind".to_string()),
        serde_yaml::Value::String("ethereum/events".to_string()),
    );
    mapping.insert(
        serde_yaml::Value::String("apiVersion".to_string()),
        serde_yaml::Value::String("0.0.9".to_string()),
    );
    mapping.insert(
        serde_yaml::Value::String("language".to_string()),
        serde_yaml::Value::String("wasm/assemblyscript".to_string()),
    );
    mapping.insert(
        serde_yaml::Value::String("entities".to_string()),
        serde_yaml::Value::Sequence(entities),
    );
    mapping.insert(
        serde_yaml::Value::String("abis".to_string()),
        serde_yaml::Value::Sequence(vec![serde_yaml::Value::Mapping(abi_entry)]),
    );
    mapping.insert(
        serde_yaml::Value::String("eventHandlers".to_string()),
        serde_yaml::Value::Sequence(event_handlers),
    );
    mapping.insert(
        serde_yaml::Value::String("file".to_string()),
        serde_yaml::Value::String(format!("./src/{}.ts", contract_name.to_kebab_case())),
    );

    // Build the data source
    let mut data_source = serde_yaml::Mapping::new();
    data_source.insert(
        serde_yaml::Value::String("kind".to_string()),
        serde_yaml::Value::String("ethereum".to_string()),
    );
    data_source.insert(
        serde_yaml::Value::String("name".to_string()),
        serde_yaml::Value::String(contract_name.to_string()),
    );
    data_source.insert(
        serde_yaml::Value::String("network".to_string()),
        serde_yaml::Value::String(network.to_string()),
    );
    data_source.insert(
        serde_yaml::Value::String("source".to_string()),
        serde_yaml::Value::Mapping(source),
    );
    data_source.insert(
        serde_yaml::Value::String("mapping".to_string()),
        serde_yaml::Value::Mapping(mapping),
    );

    // Add to dataSources array
    let data_sources = manifest
        .get_mut("dataSources")
        .and_then(|ds| ds.as_sequence_mut())
        .ok_or_else(|| anyhow!("Manifest missing dataSources array"))?;

    data_sources.push(serde_yaml::Value::Mapping(data_source));

    // Write back
    let updated = serde_yaml::to_string(&manifest)?;
    step(Step::Write, "Updating subgraph.yaml");
    fs::write(manifest_path, updated).context("Failed to write manifest")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scaffold::manifest::{EventInfo, EventInput};

    #[tokio::test]
    async fn test_invalid_address() {
        let opt = AddOpt {
            address: "invalid".to_string(),
            manifest: PathBuf::from("subgraph.yaml"),
            abi: None,
            contract_name: None,
            merge_entities: false,
            network_file: PathBuf::from("networks.json"),
            start_block: None,
        };

        let result = run_add(opt).await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid contract address"));
    }

    #[test]
    fn test_sanitize_field_name() {
        assert_eq!(sanitize_field_name("owner"), "owner");
        assert_eq!(sanitize_field_name("Owner"), "owner");
        assert_eq!(sanitize_field_name(""), "value");
        // Reserved words
        assert_eq!(sanitize_field_name("id"), "eventId");
        assert_eq!(sanitize_field_name("type"), "eventType");
    }

    #[test]
    fn test_to_kebab_case() {
        assert_eq!("MyContract".to_kebab_case(), "my-contract");
        assert_eq!("SimpleToken".to_kebab_case(), "simple-token");
        assert_eq!("Contract".to_kebab_case(), "contract");
        assert_eq!("contract".to_kebab_case(), "contract");
        assert_eq!("ERC20Token".to_kebab_case(), "erc20-token");
    }

    #[test]
    fn test_generate_event_handler() {
        let event = EventInfo {
            name: "Approval".to_string(),
            signature: "Approval(address,address,uint256)".to_string(),
            inputs: vec![
                EventInput {
                    name: "owner".to_string(),
                    solidity_type: "address".to_string(),
                    indexed: true,
                },
                EventInput {
                    name: "spender".to_string(),
                    solidity_type: "address".to_string(),
                    indexed: true,
                },
                EventInput {
                    name: "value".to_string(),
                    solidity_type: "uint256".to_string(),
                    indexed: false,
                },
            ],
        };

        let handler = generate_event_handler(&event);
        assert!(handler.contains("export function handleApproval"));
        assert!(handler.contains("event: ApprovalEvent"));
        assert!(handler.contains("new Approval("));
        assert!(handler.contains("entity.owner = event.params.owner"));
        assert!(handler.contains("entity.spender = event.params.spender"));
        assert!(handler.contains("entity.value = event.params.value"));
        assert!(handler.contains("entity.save()"));
    }

    #[test]
    fn test_generate_mapping() {
        let events = vec![EventInfo {
            name: "Transfer".to_string(),
            signature: "Transfer(address,address,uint256)".to_string(),
            inputs: vec![EventInput {
                name: "from".to_string(),
                solidity_type: "address".to_string(),
                indexed: true,
            }],
        }];

        let mapping = generate_mapping("Token", &events);
        assert!(mapping.contains("import { BigInt, Bytes }"));
        assert!(mapping.contains("Transfer as TransferEvent"));
        assert!(mapping.contains("../generated/Token/Token"));
        assert!(mapping.contains("import { Transfer }"));
        assert!(mapping.contains("../generated/schema"));
    }

    #[test]
    fn test_generate_mapping_empty_events() {
        let events: Vec<EventInfo> = vec![];
        let mapping = generate_mapping("Empty", &events);
        assert!(mapping.contains("import { BigInt, Bytes }"));
        assert!(!mapping.contains("export function handle"));
    }

    #[tokio::test]
    async fn test_missing_manifest() {
        let opt = AddOpt {
            address: "0x1234567890123456789012345678901234567890".to_string(),
            manifest: PathBuf::from("nonexistent.yaml"),
            abi: None,
            contract_name: None,
            merge_entities: false,
            network_file: PathBuf::from("networks.json"),
            start_block: None,
        };

        let result = run_add(opt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Manifest file"));
    }
}
