//! Code generation command.
//!
//! Generates AssemblyScript types for a subgraph from:
//! - GraphQL schema (entity classes)
//! - Contract ABIs (event and call bindings)
//! - Data source templates

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use clap::Parser;
use ethabi::Contract;
use graphql_parser::schema as gql;

use crate::codegen::{
    AbiCodeGenerator, Class, ModuleImports, SchemaCodeGenerator, Template, TemplateCodeGenerator,
    TemplateKind, GENERATED_FILE_NOTE,
};
use crate::formatter::try_format_typescript;
use crate::output::{step, Step};

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
pub fn run_codegen(opt: CodegenOpt) -> Result<()> {
    if opt.watch {
        // TODO: Implement watch mode
        anyhow::bail!("Watch mode not yet implemented");
    }

    generate_types(&opt)
}

/// Generate all types for the subgraph.
fn generate_types(opt: &CodegenOpt) -> Result<()> {
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
            generate_abi_types(&abi.name, &abi_path, &opt.output_dir)?;
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
                .map(|ds| {
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
                    DataSource { abis }
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
}
