//! Add command for adding data sources to existing subgraphs.
//!
//! This command adds a new data source to an existing subgraph, generating
//! the necessary manifest entries, schema types, and mapping stubs.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use graphql_tools::parser::schema as gql;
use serde_json::Value as JsonValue;

use crate::config::networks::update_networks_file;
use crate::formatter::format_typescript;
use crate::output::{Step, step};
use crate::scaffold::manifest::{EventInfo, extract_events_from_abi};
use crate::scaffold::{
    MAPPING_API_VERSION, ResolvedEvent, ScaffoldOptions, disambiguate_events,
    generate_event_entity, generate_event_handlers, to_kebab_case,
};
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

    /// Suppress "Next steps" output (used when called from init loop)
    #[clap(skip)]
    pub quiet: bool,
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

    // A data source / template name must be unique within the subgraph.
    if existing_source_names(&manifest).contains(&contract_name) {
        return Err(anyhow!(
            "Data source or template named '{}' already exists. Choose a different name with --contract-name.",
            contract_name
        ));
    }

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

    // Extract events from the ABI and resolve their names against the entities
    // already present in the subgraph (handles overloads and collisions). Both
    // the manifest's entity lists and the types declared in schema.graphql count
    // as existing, since either can be the source of a name collision.
    let events = extract_events_from_abi(&scaffold_options);
    let mut existing = existing_entities(&manifest);
    existing.extend(schema_entity_types(project_dir, &manifest));
    let resolved = resolve_events(events, &existing, &contract_name, opt.merge_entities)?;

    // Add ABI file
    add_abi_file(project_dir, &contract_name, &abi)?;

    // Add mapping file
    add_mapping_file(project_dir, &contract_name, &resolved)?;

    // Update manifest
    update_manifest(
        &opt.manifest,
        &opt.address,
        &contract_name,
        &network,
        start_block,
        &resolved,
    )?;

    // Declare the new event entities in the schema so codegen/build succeed.
    add_schema_entities(project_dir, &manifest, &resolved)?;

    // Update networks.json if the subgraph uses one.
    let networks_path = project_dir.join(&opt.network_file);
    if networks_path.exists() {
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
    }

    step(Step::Done, &format!("Added data source: {}", contract_name));

    if !opt.quiet {
        println!();
        println!("Next steps:");
        println!("  gnd codegen");
        println!("  gnd build");
    }

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

/// Add mapping file for the new data source.
fn add_mapping_file(
    project_dir: &Path,
    contract_name: &str,
    events: &[ResolvedEvent],
) -> Result<()> {
    let src_dir = project_dir.join("src");
    fs::create_dir_all(&src_dir).context("Failed to create src directory")?;

    let mapping_file = src_dir.join(format!("{}.ts", to_kebab_case(contract_name)));

    if mapping_file.exists() {
        step(
            Step::Skip,
            &format!("Mapping file {} already exists", mapping_file.display()),
        );
        return Ok(());
    }

    let mapping_content = generate_event_handlers(contract_name, events);
    let formatted = format_typescript(&mapping_content).unwrap_or(mapping_content);

    step(
        Step::Write,
        &format!("Writing mapping to {}", mapping_file.display()),
    );
    fs::write(&mapping_file, formatted).context("Failed to write mapping file")?;

    Ok(())
}

/// Collect the names of all existing data sources and templates in the manifest.
fn existing_source_names(manifest: &serde_yaml::Value) -> HashSet<String> {
    let mut names = HashSet::new();
    for key in ["dataSources", "templates"] {
        if let Some(seq) = manifest.get(key).and_then(|v| v.as_sequence()) {
            for item in seq {
                if let Some(name) = item.get("name").and_then(|n| n.as_str()) {
                    names.insert(name.to_string());
                }
            }
        }
    }
    names
}

/// Collect the entity names already declared by existing data sources / templates.
fn existing_entities(manifest: &serde_yaml::Value) -> HashSet<String> {
    let mut entities = HashSet::new();
    for key in ["dataSources", "templates"] {
        if let Some(seq) = manifest.get(key).and_then(|v| v.as_sequence()) {
            for item in seq {
                if let Some(list) = item
                    .get("mapping")
                    .and_then(|m| m.get("entities"))
                    .and_then(|e| e.as_sequence())
                {
                    for entity in list.iter().filter_map(|e| e.as_str()) {
                        entities.insert(entity.to_string());
                    }
                }
            }
        }
    }
    entities
}

/// Collect the `@entity` type names declared in the subgraph's schema.graphql.
///
/// The schema is the real source of truth for declared types, so a hand-written
/// `type Foo @entity` that no mapping lists in its `entities` still counts as a
/// collision. Returns empty on a missing or unparseable schema (falling back to
/// the manifest's entity lists).
fn schema_entity_types(project_dir: &Path, manifest: &serde_yaml::Value) -> HashSet<String> {
    let schema_file = manifest
        .get("schema")
        .and_then(|s| s.get("file"))
        .and_then(|f| f.as_str())
        .unwrap_or("schema.graphql");
    let Ok(content) = fs::read_to_string(project_dir.join(schema_file)) else {
        return HashSet::new();
    };
    entity_types_in_schema(&content)
}

/// Parse GraphQL schema text and return the names of object types marked
/// `@entity`. Returns empty if the schema does not parse.
fn entity_types_in_schema(content: &str) -> HashSet<String> {
    let Ok(ast) = gql::parse_schema::<String>(content) else {
        return HashSet::new();
    };
    ast.definitions
        .into_iter()
        .filter_map(|def| match def {
            gql::Definition::TypeDefinition(gql::TypeDefinition::Object(obj))
                if obj.directives.iter().any(|d| d.name == "entity") =>
            {
                Some(obj.name)
            }
            _ => None,
        })
        .collect()
}

/// Resolve event names against the entities already present in the subgraph.
///
/// - Events overloaded within this ABI are disambiguated (`Transfer`, `Transfer1`).
/// - An event whose name collides with an existing entity is either merged into
///   that entity (`merge_entities`: reuse it, keeping the handler so this
///   contract's events are still indexed, but don't redeclare the type) or
///   renamed with the contract prefix so both entities can coexist.
fn resolve_events(
    events: Vec<EventInfo>,
    existing: &HashSet<String>,
    contract_name: &str,
    merge_entities: bool,
) -> Result<Vec<ResolvedEvent>> {
    let mut resolved = disambiguate_events(events);

    for r in &mut resolved {
        // Check the name we would actually declare (the disambiguated alias),
        // not the raw event name, so an overload alias like `Transfer1` that
        // collides with an existing entity is still caught.
        if !existing.contains(&r.entity_name) {
            continue;
        }
        if merge_entities {
            // Reuse the existing entity (entity_name already equals it). Merge is
            // by name only, so a signature mismatch surfaces at codegen/build.
            r.declare_in_schema = false;
        } else {
            let prefixed = format!("{}{}", contract_name, r.alias);
            if existing.contains(&prefixed) {
                return Err(anyhow!(
                    "Entity '{}' already exists; cannot rename '{}' to avoid a collision. Choose a different contract name.",
                    prefixed,
                    r.entity_name
                ));
            }
            r.entity_name = prefixed;
        }
    }

    Ok(resolved)
}

/// Append entity type definitions for the new events to the subgraph schema.
///
/// The mapping and manifest reference one entity per event; without this the
/// referenced types never exist in schema.graphql and codegen/build fail.
fn add_schema_entities(
    project_dir: &Path,
    manifest: &serde_yaml::Value,
    events: &[ResolvedEvent],
) -> Result<()> {
    let mut additions = String::new();
    for resolved in events {
        // Reused entities already exist in the schema; don't redeclare them.
        if !resolved.declare_in_schema {
            continue;
        }
        additions.push('\n');
        additions.push_str(&generate_event_entity(
            &resolved.entity_name,
            &resolved.event.inputs,
        ));
        additions.push('\n');
    }

    if additions.is_empty() {
        return Ok(());
    }

    let schema_file = manifest
        .get("schema")
        .and_then(|s| s.get("file"))
        .and_then(|f| f.as_str())
        .unwrap_or("schema.graphql");
    let schema_path = project_dir.join(schema_file);

    step(Step::Write, &format!("Updating {}", schema_path.display()));

    use std::io::Write;
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&schema_path)
        .with_context(|| format!("Failed to open schema file: {}", schema_path.display()))?;
    file.write_all(additions.as_bytes())
        .context("Failed to append entities to schema")?;

    Ok(())
}

/// Update the manifest with the new data source.
fn update_manifest(
    manifest_path: &Path,
    address: &str,
    contract_name: &str,
    network: &str,
    start_block: Option<u64>,
    events: &[ResolvedEvent],
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
    for resolved in events {
        let mut handler = serde_yaml::Mapping::new();
        handler.insert(
            serde_yaml::Value::String("event".to_string()),
            serde_yaml::Value::String(resolved.event.signature.clone()),
        );
        handler.insert(
            serde_yaml::Value::String("handler".to_string()),
            serde_yaml::Value::String(format!("handle{}", resolved.alias)),
        );
        event_handlers.push(serde_yaml::Value::Mapping(handler));
    }

    // Build entities list
    let entities: Vec<serde_yaml::Value> = events
        .iter()
        .map(|e| serde_yaml::Value::String(e.entity_name.clone()))
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
        serde_yaml::Value::String(MAPPING_API_VERSION.to_string()),
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
        serde_yaml::Value::String(format!("./src/{}.ts", to_kebab_case(contract_name))),
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
            quiet: false,
        };

        let result = run_add(opt).await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Invalid contract address")
        );
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
            quiet: false,
        };

        let result = run_add(opt).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Manifest file"));
    }

    fn ev(name: &str) -> EventInfo {
        EventInfo {
            name: name.to_string(),
            signature: format!("{}()", name),
            inputs: vec![],
        }
    }

    fn entities(names: &[&str]) -> HashSet<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn test_resolve_events_no_collision() {
        let resolved =
            resolve_events(vec![ev("Transfer")], &entities(&[]), "Token", false).unwrap();
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].alias, "Transfer");
        assert_eq!(resolved[0].entity_name, "Transfer");
        assert!(resolved[0].declare_in_schema);
    }

    #[test]
    fn test_resolve_events_overload_disambiguates() {
        // Two events named Transfer in one ABI get distinct aliases.
        let resolved = resolve_events(
            vec![ev("Transfer"), ev("Transfer")],
            &entities(&[]),
            "Token",
            false,
        )
        .unwrap();
        assert_eq!(resolved[0].alias, "Transfer");
        assert_eq!(resolved[1].alias, "Transfer1");
        assert_eq!(resolved[1].entity_name, "Transfer1");
    }

    #[test]
    fn test_resolve_events_collision_merge_reuses() {
        let resolved = resolve_events(
            vec![ev("Transfer")],
            &entities(&["Transfer"]),
            "Token",
            true,
        )
        .unwrap();
        // Reuse the existing entity, don't redeclare it, but keep the handler.
        assert_eq!(resolved[0].entity_name, "Transfer");
        assert!(!resolved[0].declare_in_schema);
    }

    #[test]
    fn test_resolve_events_collision_no_merge_renames() {
        let resolved = resolve_events(
            vec![ev("Transfer")],
            &entities(&["Transfer"]),
            "Token",
            false,
        )
        .unwrap();
        assert_eq!(resolved[0].entity_name, "TokenTransfer");
        assert!(resolved[0].declare_in_schema);
    }

    #[test]
    fn test_resolve_events_collision_renamed_also_exists_errors() {
        let result = resolve_events(
            vec![ev("Transfer")],
            &entities(&["Transfer", "TokenTransfer"]),
            "Token",
            false,
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_resolve_events_overload_alias_collision_renames() {
        // The second overload's disambiguated alias (Transfer1) collides with an
        // existing entity; the collision must be detected on the alias, not the
        // raw event name.
        let resolved = resolve_events(
            vec![ev("Transfer"), ev("Transfer")],
            &entities(&["Transfer1"]),
            "Token",
            false,
        )
        .unwrap();
        assert_eq!(resolved[0].entity_name, "Transfer");
        assert_eq!(resolved[1].alias, "Transfer1");
        assert_eq!(resolved[1].entity_name, "TokenTransfer1");
    }

    #[test]
    fn test_entity_types_in_schema() {
        let schema = r#"
            type Foo @entity { id: Bytes! }
            type Bar @entity(immutable: true) { id: Bytes! }
            type NotAnEntity { id: Bytes! }
        "#;
        let types = entity_types_in_schema(schema);
        assert!(types.contains("Foo"));
        assert!(types.contains("Bar"));
        assert!(!types.contains("NotAnEntity"));
        // A schema that doesn't parse yields no types rather than erroring.
        assert!(entity_types_in_schema("this is not graphql {{{").is_empty());
    }
}
