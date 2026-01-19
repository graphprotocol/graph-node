//! Code generation command.
//!
//! Generates AssemblyScript types for a subgraph from:
//! - GraphQL schema (entity classes)
//! - Contract ABIs (event and call bindings)
//! - Data source templates

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use ethabi::Contract;
use graphql_parser::schema as gql;
use notify::{recommended_watcher, RecursiveMode, Watcher};

use crate::codegen::{
    AbiCodeGenerator, Class, ModuleImports, SchemaCodeGenerator, Template, TemplateCodeGenerator,
    TemplateKind, GENERATED_FILE_NOTE,
};
use crate::formatter::try_format_typescript;
use crate::migrations;
use crate::output::{step, Step};

/// Default IPFS URL.
const DEFAULT_IPFS_URL: &str = "https://api.thegraph.com/ipfs/api/v0";

/// Delay between file change detection and regeneration to batch multiple events.
const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);

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
pub fn run_codegen(opt: CodegenOpt) -> Result<()> {
    if opt.watch {
        watch_and_generate(&opt)
    } else {
        generate_types(&opt)
    }
}

/// Watch subgraph files and regenerate types on changes.
fn watch_and_generate(opt: &CodegenOpt) -> Result<()> {
    // Do initial generation
    if let Err(e) = generate_types(opt) {
        eprintln!("Error during initial generation: {}", e);
    }

    // Get files to watch
    let manifest = load_manifest(&opt.manifest)?;
    let files_to_watch = get_files_to_watch(&opt.manifest, &manifest);

    println!("\nWatching subgraph files for changes...");
    println!("Press Ctrl+C to stop.\n");

    // Set up file watcher
    let (tx, rx) = mpsc::channel();

    let mut watcher = recommended_watcher(move |res| {
        if let Ok(event) = res {
            let _ = tx.send(event);
        }
    })
    .map_err(|e| anyhow!("Failed to create file watcher: {}", e))?;

    // Watch directories containing the files
    let mut watched_dirs = std::collections::HashSet::new();
    for file in &files_to_watch {
        if let Some(dir) = file.parent() {
            if watched_dirs.insert(dir.to_path_buf()) {
                watcher
                    .watch(dir, RecursiveMode::NonRecursive)
                    .map_err(|e| anyhow!("Failed to watch {}: {}", dir.display(), e))?;
            }
        }
    }

    // Event loop
    loop {
        match rx.recv() {
            Ok(event) => {
                // Check if the event is for a file we care about
                let relevant = event.paths.iter().any(|p| {
                    files_to_watch.iter().any(|f| {
                        p.file_name() == f.file_name()
                            || p.ends_with(f.file_name().unwrap_or_default())
                    })
                });

                if relevant {
                    // Debounce: wait a bit and drain any pending events
                    std::thread::sleep(WATCH_DEBOUNCE);
                    while rx.try_recv().is_ok() {}

                    // Get the changed file for logging
                    let changed = event
                        .paths
                        .first()
                        .map(|p| p.display().to_string())
                        .unwrap_or_else(|| "unknown".to_string());

                    println!("\nFile change detected: {}\n", changed);

                    if let Err(e) = generate_types(opt) {
                        eprintln!("Error during regeneration: {}", e);
                    }
                }
            }
            Err(_) => {
                return Err(anyhow!("File watcher channel closed"));
            }
        }
    }
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
fn generate_types(opt: &CodegenOpt) -> Result<()> {
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
        generate_schema_types(&schema_path, &opt.output_dir)?;
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
    }

    step(Step::Done, "Types generated successfully");
    Ok(())
}

/// Generate types from the GraphQL schema.
fn generate_schema_types(schema_path: &Path, output_dir: &Path) -> Result<()> {
    step(
        Step::Load,
        &format!("Load GraphQL schema from {}", schema_path.display()),
    );

    let schema_str = fs::read_to_string(schema_path)
        .with_context(|| format!("Failed to read schema file: {:?}", schema_path))?;

    let ast: gql::Document<'_, String> = gql::parse_schema(&schema_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse GraphQL schema: {}", e))?;

    step(Step::Generate, "Generate types for GraphQL schema");

    let generator = SchemaCodeGenerator::new(&ast);
    let imports = generator.generate_module_imports();
    let classes = generator.generate_types(true);

    let code = generate_file(&imports, &classes);
    let formatted = try_format_typescript(&code);

    let output_file = output_dir.join("schema.ts");
    step(
        Step::Write,
        &format!("Write types to {}", output_file.display()),
    );
    fs::write(&output_file, formatted)
        .with_context(|| format!("Failed to write schema types: {:?}", output_file))?;

    Ok(())
}

/// Generate types from an ABI file.
fn generate_abi_types(name: &str, abi_path: &Path, output_dir: &Path) -> Result<()> {
    step(Step::Load, &format!("Load ABI from {}", abi_path.display()));

    let abi_str = fs::read_to_string(abi_path)
        .with_context(|| format!("Failed to read ABI file: {:?}", abi_path))?;

    let contract: Contract = serde_json::from_str(&abi_str)
        .with_context(|| format!("Failed to parse ABI JSON: {:?}", abi_path))?;

    step(Step::Generate, &format!("Generate types for ABI {}", name));

    let generator = AbiCodeGenerator::new(contract, name);
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
fn generate_template_types(templates: &[ManifestTemplate], output_dir: &Path) -> Result<()> {
    step(Step::Generate, "Generate types for data source templates");

    let templates: Vec<Template> = templates
        .iter()
        .filter_map(|t| {
            TemplateKind::from_str_kind(&t.kind).map(|kind| Template::new(&t.name, kind))
        })
        .collect();

    if templates.is_empty() {
        return Ok(());
    }

    let generator = TemplateCodeGenerator::new(templates);
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

/// Resolve a path relative to the manifest file.
fn resolve_path(manifest: &Path, path: &str) -> PathBuf {
    manifest
        .parent()
        .map(|p| p.join(path))
        .unwrap_or_else(|| PathBuf::from(path))
}

// ============================================================================
// Simplified manifest parsing
// ============================================================================

/// A simplified subgraph manifest structure.
#[derive(Debug)]
struct Manifest {
    schema: Option<String>,
    data_sources: Vec<DataSource>,
    templates: Vec<ManifestTemplate>,
}

#[derive(Debug)]
struct DataSource {
    name: String,
    abis: Vec<Abi>,
}

#[derive(Debug)]
struct Abi {
    name: String,
    file: String,
}

#[derive(Debug)]
struct ManifestTemplate {
    name: String,
    kind: String,
}

/// Load a subgraph manifest from a YAML file.
fn load_manifest(path: &Path) -> Result<Manifest> {
    step(
        Step::Load,
        &format!("Load subgraph from {}", path.display()),
    );

    let manifest_str =
        fs::read_to_string(path).with_context(|| format!("Failed to read manifest: {:?}", path))?;

    let value: serde_json::Value = serde_yaml::from_str(&manifest_str)
        .with_context(|| format!("Failed to parse manifest YAML: {:?}", path))?;

    // Extract schema path
    let schema = value
        .get("schema")
        .and_then(|s| s.get("file"))
        .and_then(|f| f.as_str())
        .map(String::from);

    // Extract data sources
    let data_sources = value
        .get("dataSources")
        .and_then(|ds| ds.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|ds| {
                    let name = ds.get("name")?.as_str()?.to_string();
                    let abis = ds
                        .get("mapping")
                        .and_then(|m| m.get("abis"))
                        .and_then(|a| a.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|abi| {
                                    let name = abi.get("name")?.as_str()?.to_string();
                                    let file = abi.get("file")?.as_str()?.to_string();
                                    Some(Abi { name, file })
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    Some(DataSource { name, abis })
                })
                .collect()
        })
        .unwrap_or_default();

    // Extract templates
    let templates = value
        .get("templates")
        .and_then(|t| t.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let name = t.get("name")?.as_str()?.to_string();
                    let kind = t.get("kind")?.as_str()?.to_string();
                    Some(ManifestTemplate { name, kind })
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(Manifest {
        schema,
        data_sources,
        templates,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_resolve_path() {
        let manifest = PathBuf::from("/home/user/project/subgraph.yaml");
        let path = "schema.graphql";
        let resolved = resolve_path(&manifest, path);
        assert_eq!(resolved, PathBuf::from("/home/user/project/schema.graphql"));
    }

    #[test]
    fn test_load_manifest() {
        let temp_dir = TempDir::new().unwrap();
        let manifest_path = temp_dir.path().join("subgraph.yaml");

        let manifest_content = r#"
specVersion: 0.0.4
schema:
  file: ./schema.graphql
dataSources:
  - kind: ethereum/contract
    name: Token
    mapping:
      kind: ethereum/events
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
templates:
  - kind: ethereum/contract
    name: DynamicToken
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.schema, Some("./schema.graphql".to_string()));
        assert_eq!(manifest.data_sources.len(), 1);
        assert_eq!(manifest.data_sources[0].name, "Token");
        assert_eq!(manifest.data_sources[0].abis.len(), 1);
        assert_eq!(manifest.data_sources[0].abis[0].name, "ERC20");
        assert_eq!(manifest.templates.len(), 1);
        assert_eq!(manifest.templates[0].name, "DynamicToken");
    }

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
    #[test]
    fn test_codegen_directory_structure() {
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
        generate_types(&opt).unwrap();

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
    #[test]
    fn test_schema_codegen_snapshot() {
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
        generate_types(&opt).unwrap();

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
    #[test]
    fn test_abi_codegen_snapshot() {
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
        generate_types(&opt).unwrap();

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
