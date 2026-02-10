//! Code generation command.
//!
//! Generates AssemblyScript types for a subgraph from:
//! - GraphQL schema (entity classes)
//! - Contract ABIs (event and call bindings)
//! - Data source templates
//! - Subgraph data sources (fetches schema from IPFS)

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use ethabi::Contract;
use graphql_parser::schema as gql;
use semver::Version;

use crate::abi::normalize_abi_json;
use crate::codegen::{
    AbiCodeGenerator, Class, ModuleImports, SchemaCodeGenerator, Template as CodegenTemplate,
    TemplateCodeGenerator, TemplateKind, GENERATED_FILE_NOTE,
};
use crate::formatter::try_format_typescript;
use crate::manifest::{load_manifest, resolve_path, DataSource, Manifest, Template};
use crate::migrations;
use crate::output::{step, Step};
use crate::services::IpfsClient;
use crate::validation::{format_schema_errors, validate_schema};
use crate::watch::watch_and_run;

/// Default IPFS URL.
const DEFAULT_IPFS_URL: &str = "https://api.thegraph.com/ipfs/api/v0";

#[derive(Clone, Debug, Parser)]
#[clap(about = "Generate AssemblyScript types for a subgraph")]
pub struct CodegenOpt {
    /// Path to the subgraph manifest
    #[clap(default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

    /// Output directory for generated types
    #[clap(short = 'o', long, default_value = "generated/")]
    pub output_dir: PathBuf,

    /// Skip subgraph migrations
    #[clap(long)]
    pub skip_migrations: bool,

    /// Regenerate types when subgraph files change
    #[clap(short = 'w', long)]
    pub watch: bool,

    /// IPFS node to use for fetching subgraph data
    #[clap(short = 'i', long, default_value = DEFAULT_IPFS_URL)]
    pub ipfs: String,
}

/// Run the codegen command.
pub async fn run_codegen(opt: CodegenOpt) -> Result<()> {
    if opt.watch {
        watch_and_generate(&opt).await
    } else {
        generate_types(&opt).await
    }
}

/// Watch subgraph files and regenerate types on changes.
async fn watch_and_generate(opt: &CodegenOpt) -> Result<()> {
    // Get files to watch (need to load manifest first)
    let manifest = load_manifest(&opt.manifest)?;
    let files_to_watch = get_files_to_watch(&opt.manifest, &manifest);

    watch_and_run(
        files_to_watch,
        "Watching subgraph files for changes...",
        || generate_types(opt),
    )
    .await
}

/// Get the list of files to watch for changes.
fn get_files_to_watch(manifest_path: &Path, manifest: &Manifest) -> Vec<PathBuf> {
    let mut files = vec![manifest_path.to_path_buf()];

    // Add schema file
    if let Some(schema_path) = &manifest.schema {
        files.push(resolve_path(manifest_path, schema_path));
    }

    // Add ABI files
    for ds in &manifest.data_sources {
        for abi in &ds.abis {
            files.push(resolve_path(manifest_path, &abi.file));
        }
    }

    files
}

/// Generate all types for the subgraph.
async fn generate_types(opt: &CodegenOpt) -> Result<()> {
    // Apply migrations unless skipped
    if !opt.skip_migrations {
        migrations::apply_migrations(&opt.manifest)?;
    }

    // Load the subgraph manifest
    let manifest = load_manifest(&opt.manifest)?;

    // Create output directory
    fs::create_dir_all(&opt.output_dir)
        .with_context(|| format!("Failed to create output directory: {:?}", opt.output_dir))?;

    // Generate schema types
    if let Some(schema_path) = manifest.schema.as_ref() {
        let schema_path = resolve_path(&opt.manifest, schema_path);
        let _ = generate_schema_types(&schema_path, &opt.output_dir, &manifest.spec_version)?;
    }

    // Generate ABI types for each data source
    for ds in &manifest.data_sources {
        for abi in &ds.abis {
            let abi_path = resolve_path(&opt.manifest, &abi.file);
            // Output to: <output_dir>/<DataSourceName>/<AbiName>.ts
            let ds_output_dir = opt.output_dir.join(&ds.name);
            generate_abi_types(&abi.name, &abi_path, &ds_output_dir)?;
        }
    }

    // Generate template types
    if !manifest.templates.is_empty() {
        generate_template_types(&manifest.templates, &opt.output_dir)?;

        // Generate ABI types for templates
        for template in &manifest.templates {
            for abi in &template.abis {
                let abi_path = resolve_path(&opt.manifest, &abi.file);
                // Output to: <output_dir>/templates/<TemplateName>/<AbiName>.ts
                let template_output_dir = opt.output_dir.join("templates").join(&template.name);
                generate_abi_types(&abi.name, &abi_path, &template_output_dir)?;
            }
        }
    }

    // Generate types for subgraph data sources
    let subgraph_sources: Vec<&DataSource> = manifest
        .data_sources
        .iter()
        .filter(|ds| ds.is_subgraph_source())
        .collect();
    if !subgraph_sources.is_empty() {
        generate_subgraph_source_types(&subgraph_sources, &opt.output_dir, &opt.ipfs).await?;
    }

    step(Step::Done, "Types generated successfully");
    Ok(())
}

/// Generate types from the GraphQL schema.
///
/// Returns Ok(true) if types were generated successfully, Ok(false) if schema
/// validation failed and schema.ts was skipped.
fn generate_schema_types(
    schema_path: &Path,
    output_dir: &Path,
    spec_version: &Version,
) -> Result<bool> {
    step(
        Step::Load,
        &format!("Load GraphQL schema from {}", schema_path.display()),
    );

    // Run graph-node schema validation
    let validation_errors = validate_schema(schema_path, spec_version)?;

    if !validation_errors.is_empty() {
        eprintln!(
            "Schema validation errors:\n{}",
            format_schema_errors(&validation_errors)
        );
        return Err(anyhow!(
            "Schema validation failed with {} error(s)",
            validation_errors.len()
        ));
    }

    let schema_str = fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {:?}", schema_path))?;

    let ast: gql::Document<'_, String> = gql::parse_schema(&schema_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse GraphQL schema: {}", e))?;

    step(Step::Generate, "Generate types for GraphQL schema");

    let generator = match SchemaCodeGenerator::new(&ast) {
        Ok(gen) => gen,
        Err(e) => {
            // Schema validation failed - skip schema.ts generation but don't fail
            eprintln!("Warning: {}", e);
            return Ok(false);
        }
    };
    let imports = generator.generate_module_imports();
    let entity_classes = generator.generate_types(true);
    let derived_loaders = generator.generate_derived_loaders();

    // Combine entity classes with derived loaders
    let all_classes: Vec<Class> = entity_classes.into_iter().chain(derived_loaders).collect();

    let code = generate_file(&imports, &all_classes);
    let formatted = try_format_typescript(&code);

    let output_file = output_dir.join("schema.ts");
    step(
        Step::Write,
        &format!("Write types to {}", output_file.display()),
    );
    fs::write(&output_file, formatted)
        .with_context(|| format!("Failed to write schema types: {:?}", output_file))?;

    Ok(true)
}

/// Preprocess ABI JSON to add default names for unnamed parameters.
/// The ethabi crate requires all parameters to have names, but Solidity ABIs
/// can have unnamed parameters. This function adds `param0`, `param1`, etc.
fn preprocess_abi_json(abi_str: &str) -> Result<String> {
    // First normalize to get the ABI array from various artifact formats
    let mut abi = normalize_abi_json(abi_str)?;

    if let Some(items) = abi.as_array_mut() {
        for item in items {
            if let Some(obj) = item.as_object_mut() {
                let is_event = obj
                    .get("type")
                    .and_then(|t| t.as_str())
                    .map(|t| t == "event")
                    .unwrap_or(false);

                // Add anonymous: false for events if missing
                if is_event && !obj.contains_key("anonymous") {
                    obj.insert("anonymous".to_string(), serde_json::Value::Bool(false));
                }

                // Process inputs for events and functions
                if let Some(inputs) = obj.get_mut("inputs") {
                    add_default_names_to_params(inputs, is_event);
                }
                // Process outputs for functions
                if let Some(outputs) = obj.get_mut("outputs") {
                    add_default_names_to_params(outputs, false);
                }
            }
        }
    }

    serde_json::to_string(&abi).context("Failed to serialize processed ABI")
}

/// Add required fields to ABI parameters.
/// - For event inputs: adds "name" (using `param{index}` if missing) and "indexed": false
/// - For function inputs/outputs: adds empty "name" if missing (lets ABI codegen use correct prefix)
/// - For tuple components: adds empty "name" if missing
fn add_default_names_to_params(params: &mut serde_json::Value, is_event_input: bool) {
    if let Some(params_arr) = params.as_array_mut() {
        for (index, param) in params_arr.iter_mut().enumerate() {
            if let Some(obj) = param.as_object_mut() {
                // Check if name is missing (not present at all)
                let name_missing = !obj.contains_key("name");

                if name_missing {
                    // For events, use param{index} to match graph-cli behavior
                    // For functions, use empty string so ABI codegen applies correct prefix
                    let default_name = if is_event_input {
                        format!("param{}", index)
                    } else {
                        String::new()
                    };
                    obj.insert("name".to_string(), serde_json::Value::String(default_name));
                }

                // Add indexed: false for event inputs if missing
                if is_event_input && !obj.contains_key("indexed") {
                    obj.insert("indexed".to_string(), serde_json::Value::Bool(false));
                }

                // Recursively process tuple components (always use empty names)
                if let Some(components) = obj.get_mut("components") {
                    add_default_names_to_params(components, false);
                }
            }
        }
    }
}

/// Generate types from an ABI file.
fn generate_abi_types(name: &str, abi_path: &Path, output_dir: &Path) -> Result<()> {
    step(Step::Load, &format!("Load ABI from {}", abi_path.display()));

    let abi_str = fs::read_to_string(abi_path)
        .with_context(|| format!("Failed to read ABI file: {:?}", abi_path))?;

    // Preprocess ABI to add default names for unnamed parameters
    let processed_abi = preprocess_abi_json(&abi_str)
        .with_context(|| format!("Failed to preprocess ABI: {:?}", abi_path))?;

    let contract: Contract = serde_json::from_str(&processed_abi)
        .with_context(|| format!("Failed to parse ABI JSON: {:?}", abi_path))?;

    step(Step::Generate, &format!("Generate types for ABI {}", name));

    // Use new_with_json to preserve struct field names from the ABI
    let generator = AbiCodeGenerator::new_with_json(contract, name, &processed_abi);
    let imports = generator.generate_module_imports();
    let classes = generator.generate_types();

    let code = generate_file(&imports, &classes);
    let formatted = try_format_typescript(&code);

    // Create output directory (for data source subdirectory)
    fs::create_dir_all(output_dir)
        .with_context(|| format!("Failed to create output directory: {:?}", output_dir))?;

    let output_file = output_dir.join(format!("{}.ts", name));
    step(
        Step::Write,
        &format!("Write types to {}", output_file.display()),
    );
    fs::write(&output_file, formatted)
        .with_context(|| format!("Failed to write ABI types: {:?}", output_file))?;

    Ok(())
}

/// Generate types for data source templates.
fn generate_template_types(templates: &[Template], output_dir: &Path) -> Result<()> {
    step(Step::Generate, "Generate types for data source templates");

    let codegen_templates: Vec<CodegenTemplate> = templates
        .iter()
        .filter_map(|t| {
            TemplateKind::from_str_kind(&t.kind).map(|kind| CodegenTemplate::new(&t.name, kind))
        })
        .collect();

    if codegen_templates.is_empty() {
        return Ok(());
    }

    let generator = TemplateCodeGenerator::new(codegen_templates);
    let imports = generator.generate_module_imports();
    let classes = generator.generate_types();

    let code = generate_file(&imports, &classes);
    let formatted = try_format_typescript(&code);

    let output_file = output_dir.join("templates.ts");
    step(
        Step::Write,
        &format!("Write types to {}", output_file.display()),
    );
    fs::write(&output_file, formatted)
        .with_context(|| format!("Failed to write template types: {:?}", output_file))?;

    Ok(())
}

/// Generate types for subgraph data sources.
///
/// For each subgraph data source, this fetches the referenced subgraph's schema
/// from IPFS and generates entity types (without store methods).
async fn generate_subgraph_source_types(
    subgraph_sources: &[&DataSource],
    output_dir: &Path,
    ipfs_url: &str,
) -> Result<()> {
    step(Step::Generate, "Generate types for subgraph data sources");

    let ipfs_client = IpfsClient::new(ipfs_url)?;

    // Validate that all subgraph data source names are unique
    let mut seen_names = std::collections::HashSet::new();
    for source in subgraph_sources {
        if !seen_names.insert(&source.name) {
            return Err(anyhow!(
                "Duplicate subgraph data source name '{}'. Each subgraph data source must have a unique name.",
                source.name
            ));
        }
    }

    for source in subgraph_sources {
        // source_address is guaranteed to be Some because of is_subgraph_source() filter
        let address = source.source_address.as_ref().unwrap();

        step(
            Step::Load,
            &format!("Fetch schema for subgraph {} ({})", source.name, address),
        );

        // Fetch schema from IPFS using block_in_place to allow blocking in async context
        let schema_str = ipfs_client.fetch_subgraph_schema(address).await?;

        // Parse the schema
        let ast: gql::Document<'_, String> = gql::parse_schema(&schema_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse subgraph schema: {}", e))?;

        step(
            Step::Generate,
            &format!("Generate types for subgraph {} ({})", source.name, address),
        );

        // Generate entity types WITHOUT store methods (false = no store methods)
        let generator = match SchemaCodeGenerator::new(&ast) {
            Ok(gen) => gen,
            Err(e) => {
                eprintln!(
                    "Warning: Failed to create schema generator for subgraph {} ({}): {}",
                    source.name, address, e
                );
                continue;
            }
        };

        let imports = generator.generate_module_imports();
        // Pass false to generate entities without store methods
        let entity_classes = generator.generate_types(false);

        let code = generate_file(&imports, &entity_classes);
        let formatted = try_format_typescript(&code);

        // Output to: <output_dir>/subgraph-<NAME>.ts
        // Using name instead of IPFS hash for stable file names
        let output_file = output_dir.join(format!("subgraph-{}.ts", source.name));
        step(
            Step::Write,
            &format!("Write types to {}", output_file.display()),
        );
        fs::write(&output_file, formatted)
            .with_context(|| format!("Failed to write subgraph types: {:?}", output_file))?;
    }

    Ok(())
}

/// Generate a TypeScript file from imports and classes.
fn generate_file(imports: &[ModuleImports], classes: &[Class]) -> String {
    let mut output = String::new();

    // Add generated file note
    output.push_str(GENERATED_FILE_NOTE);
    output.push('\n');

    // Add imports
    for import in imports {
        output.push_str(&import.to_string());
        output.push('\n');
    }

    // Add classes
    for class in classes {
        output.push_str(&class.to_string());
    }

    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_generate_file() {
        let imports = vec![ModuleImports::new(
            vec!["Entity".to_string()],
            "@graphprotocol/graph-ts",
        )];
        let classes = vec![];
        let output = generate_file(&imports, &classes);
        assert!(output.contains("AUTOGENERATED"));
        assert!(output.contains("import { Entity }"));
    }

    /// Test that codegen generates types in the correct directory structure.
    ///
    /// TS CLI generates:
    /// - generated/schema.ts
    /// - generated/<DataSourceName>/<AbiName>.ts
    /// - generated/templates/<TemplateName>/<AbiName>.ts
    #[tokio::test]
    async fn test_codegen_directory_structure() {
        let temp_dir = TempDir::new().unwrap();
        let project_dir = temp_dir.path();
        let output_dir = project_dir.join("generated");

        // Create manifest
        let manifest_content = r#"
specVersion: 0.0.4
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: ExampleSubgraph
    source:
      abi: ExampleContract
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.5
      language: wasm/assemblyscript
      file: ./mapping.ts
      entities:
        - MyEntity
      abis:
        - name: ExampleContract
          file: ./Abi.json
      eventHandlers:
        - event: ExampleEvent(string)
          handler: handleExampleEvent
"#;
        fs::write(project_dir.join("subgraph.yaml"), manifest_content).unwrap();

        // Create schema
        let schema_content = r#"
type MyEntity @entity(immutable: true) {
  id: ID!
  x: BigDecimal!
}
"#;
        fs::write(project_dir.join("schema.graphql"), schema_content).unwrap();

        // Create ABI (ethabi requires anonymous field for events)
        let abi_content = r#"[
  {
    "type": "event",
    "name": "ExampleEvent",
    "anonymous": false,
    "inputs": [{ "type": "string", "name": "param0", "indexed": false }]
  }
]"#;
        fs::write(project_dir.join("Abi.json"), abi_content).unwrap();

        // Create mapping (empty is fine)
        fs::write(project_dir.join("mapping.ts"), "").unwrap();

        // Run codegen
        let opt = CodegenOpt {
            manifest: project_dir.join("subgraph.yaml"),
            output_dir: output_dir.clone(),
            skip_migrations: true,
            watch: false,
            ipfs: "https://api.thegraph.com/ipfs/api/v0".to_string(),
        };
        generate_types(&opt).await.unwrap();

        // Verify directory structure
        assert!(
            output_dir.join("schema.ts").exists(),
            "schema.ts should exist at root of output dir"
        );
        assert!(
            output_dir.join("ExampleSubgraph").is_dir(),
            "ExampleSubgraph directory should exist"
        );
        assert!(
            output_dir
                .join("ExampleSubgraph/ExampleContract.ts")
                .exists(),
            "ExampleContract.ts should be in ExampleSubgraph subdirectory"
        );

        // Verify schema.ts content
        let schema_ts = fs::read_to_string(output_dir.join("schema.ts")).unwrap();
        assert!(
            schema_ts.contains("export class MyEntity"),
            "schema.ts should contain MyEntity class"
        );
        assert!(
            schema_ts.contains("AUTOGENERATED"),
            "schema.ts should have autogenerated note"
        );

        // Verify ABI types content
        let abi_ts =
            fs::read_to_string(output_dir.join("ExampleSubgraph/ExampleContract.ts")).unwrap();
        assert!(
            abi_ts.contains("export class ExampleEvent"),
            "ABI types should contain ExampleEvent class"
        );
        assert!(
            abi_ts.contains("export class ExampleContract"),
            "ABI types should contain ExampleContract class"
        );
    }

    /// Snapshot test for schema codegen output format.
    ///
    /// Tests that schema.ts matches the expected TS CLI format for a simple entity.
    /// This helps ensure byte-for-byte compatibility with the graph-cli.
    #[tokio::test]
    async fn test_schema_codegen_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let project_dir = temp_dir.path();
        let output_dir = project_dir.join("generated");

        // Create manifest
        let manifest_content = r#"
specVersion: 0.0.4
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: TestDataSource
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.5
      language: wasm/assemblyscript
      file: ./mapping.ts
      entities:
        - MyEntity
      abis: []
"#;
        fs::write(project_dir.join("subgraph.yaml"), manifest_content).unwrap();

        // Create a simple schema with BigDecimal field
        let schema_content = r#"
type MyEntity @entity(immutable: true) {
  id: ID!
  x: BigDecimal!
}
"#;
        fs::write(project_dir.join("schema.graphql"), schema_content).unwrap();
        fs::write(project_dir.join("mapping.ts"), "").unwrap();

        // Run codegen
        let opt = CodegenOpt {
            manifest: project_dir.join("subgraph.yaml"),
            output_dir: output_dir.clone(),
            skip_migrations: true,
            watch: false,
            ipfs: "https://api.thegraph.com/ipfs/api/v0".to_string(),
        };
        generate_types(&opt).await.unwrap();

        // Read generated schema.ts
        let schema_ts = fs::read_to_string(output_dir.join("schema.ts")).unwrap();

        // Verify key parts of the output that must match TS CLI format
        assert!(
            schema_ts.contains("// THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY."),
            "Should have standard autogenerated note"
        );
        assert!(schema_ts.contains("import {"), "Should have imports");
        assert!(
            schema_ts.contains("from \"@graphprotocol/graph-ts\""),
            "Should import from @graphprotocol/graph-ts"
        );
        assert!(
            schema_ts.contains("export class MyEntity extends Entity"),
            "Should export MyEntity class extending Entity"
        );
        assert!(
            schema_ts.contains("constructor(id: string)"),
            "Should have constructor with id parameter"
        );
        assert!(
            schema_ts.contains("save(): void"),
            "Should have save method"
        );
        assert!(
            schema_ts.contains("static loadInBlock(id: string): MyEntity | null"),
            "Should have loadInBlock static method"
        );
        assert!(
            schema_ts.contains("static load(id: string): MyEntity | null"),
            "Should have load static method"
        );
        assert!(
            schema_ts.contains("get id(): string"),
            "Should have id getter"
        );
        assert!(
            schema_ts.contains("set id(value: string)"),
            "Should have id setter"
        );
        assert!(
            schema_ts.contains("get x(): BigDecimal"),
            "Should have x getter returning BigDecimal"
        );
        assert!(
            schema_ts.contains("set x(value: BigDecimal)"),
            "Should have x setter accepting BigDecimal"
        );

        // Verify the save method has correct entity type assertion
        assert!(
            schema_ts.contains("store.set(\"MyEntity\""),
            "save() should call store.set with entity type name"
        );

        // Verify load methods use correct store methods
        assert!(
            schema_ts.contains("store.get(\"MyEntity\""),
            "load() should call store.get with entity type name"
        );
        assert!(
            schema_ts.contains("store.get_in_block(\"MyEntity\""),
            "loadInBlock() should call store.get_in_block with entity type name"
        );
    }

    /// Snapshot test for ABI codegen output format.
    ///
    /// Tests that ABI types match the expected TS CLI format for events and contract class.
    #[tokio::test]
    async fn test_abi_codegen_snapshot() {
        let temp_dir = TempDir::new().unwrap();
        let project_dir = temp_dir.path();
        let output_dir = project_dir.join("generated");

        // Create manifest
        let manifest_content = r#"
specVersion: 0.0.4
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: ExampleSubgraph
    mapping:
      kind: ethereum/events
      apiVersion: 0.0.5
      language: wasm/assemblyscript
      file: ./mapping.ts
      entities:
        - MyEntity
      abis:
        - name: ExampleContract
          file: ./Abi.json
"#;
        fs::write(project_dir.join("subgraph.yaml"), manifest_content).unwrap();

        // Create schema
        fs::write(
            project_dir.join("schema.graphql"),
            "type MyEntity @entity { id: ID! }",
        )
        .unwrap();
        fs::write(project_dir.join("mapping.ts"), "").unwrap();

        // Create ABI with event
        let abi_content = r#"[
  {
    "type": "event",
    "name": "ExampleEvent",
    "anonymous": false,
    "inputs": [{ "type": "string", "name": "param0", "indexed": false }]
  }
]"#;
        fs::write(project_dir.join("Abi.json"), abi_content).unwrap();

        // Run codegen
        let opt = CodegenOpt {
            manifest: project_dir.join("subgraph.yaml"),
            output_dir: output_dir.clone(),
            skip_migrations: true,
            watch: false,
            ipfs: "https://api.thegraph.com/ipfs/api/v0".to_string(),
        };
        generate_types(&opt).await.unwrap();

        // Read generated ABI types
        let abi_ts =
            fs::read_to_string(output_dir.join("ExampleSubgraph/ExampleContract.ts")).unwrap();

        // Verify key parts of the output that must match TS CLI format
        assert!(
            abi_ts.contains("// THIS IS AN AUTOGENERATED FILE. DO NOT EDIT THIS FILE DIRECTLY."),
            "Should have standard autogenerated note"
        );
        assert!(abi_ts.contains("import {"), "Should have imports");
        assert!(
            abi_ts.contains("from \"@graphprotocol/graph-ts\""),
            "Should import from @graphprotocol/graph-ts"
        );

        // Event class
        assert!(
            abi_ts.contains("export class ExampleEvent extends ethereum.Event"),
            "Should export event class extending ethereum.Event"
        );
        assert!(
            abi_ts.contains("get params(): ExampleEvent__Params"),
            "Event should have params getter"
        );

        // Params class
        assert!(
            abi_ts.contains("export class ExampleEvent__Params"),
            "Should have params class"
        );
        assert!(
            abi_ts.contains("_event: ExampleEvent"),
            "Params should have _event field"
        );
        assert!(
            abi_ts.contains("get param0(): string"),
            "Params should have param0 getter"
        );

        // Contract class
        assert!(
            abi_ts.contains("export class ExampleContract extends ethereum.SmartContract"),
            "Should export contract class extending ethereum.SmartContract"
        );
        assert!(
            abi_ts.contains("static bind(address: Address): ExampleContract"),
            "Contract should have static bind method"
        );
    }
}
