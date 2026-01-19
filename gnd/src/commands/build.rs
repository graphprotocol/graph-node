//! Build command for compiling subgraph mappings to WebAssembly.
//!
//! This command compiles AssemblyScript mappings to WASM using the asc compiler,
//! copies all required files to the build directory, and optionally uploads
//! the result to IPFS.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use notify::{recommended_watcher, RecursiveMode, Watcher};
use sha1::{Digest, Sha1};

use crate::compiler::{compile_mapping, find_graph_ts, AscCompileOptions};
use crate::migrations;
use crate::output::{step, Step};

/// Delay between file change detection and rebuild to batch multiple events.
const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);

#[derive(Clone, Debug, Parser)]
#[clap(about = "Build a subgraph and (optionally) upload it to IPFS")]
pub struct BuildOpt {
    /// Path to the subgraph manifest
    #[clap(default_value = "subgraph.yaml")]
    pub manifest: PathBuf,

    /// Output directory for build results
    #[clap(short = 'o', long, default_value = "build/")]
    pub output_dir: PathBuf,

    /// Output format for mappings (wasm or wast)
    #[clap(short = 't', long, default_value = "wasm")]
    pub output_format: String,

    /// Skip subgraph migrations
    #[clap(long)]
    pub skip_migrations: bool,

    /// Rebuild when subgraph files change
    #[clap(short = 'w', long)]
    pub watch: bool,

    /// IPFS node URL to upload build results to
    #[clap(short = 'i', long)]
    pub ipfs: Option<String>,

    /// Network configuration to use from the networks config file
    #[clap(long)]
    pub network: Option<String>,

    /// Networks config file path
    #[clap(long, default_value = "networks.json")]
    pub network_file: PathBuf,

    /// Skip the asc version check (use with caution)
    #[clap(long, env = "GND_SKIP_ASC_VERSION_CHECK")]
    pub skip_asc_version_check: bool,
}

/// Run the build command.
pub fn run_build(opt: BuildOpt) -> Result<()> {
    // Validate output format
    if opt.output_format != "wasm" && opt.output_format != "wast" {
        return Err(anyhow!(
            "Invalid output format '{}'. Must be 'wasm' or 'wast'",
            opt.output_format
        ));
    }

    if opt.watch {
        watch_and_build(&opt)
    } else {
        build_subgraph(&opt)
    }
}

/// Watch subgraph files and rebuild on changes.
fn watch_and_build(opt: &BuildOpt) -> Result<()> {
    // Do initial build
    if let Err(e) = build_subgraph(opt) {
        eprintln!("Error during initial build: {}", e);
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
    let mut watched_dirs = HashSet::new();
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

                    if let Err(e) = build_subgraph(opt) {
                        eprintln!("Error during rebuild: {}", e);
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

    // Add mapping files
    for ds in &manifest.data_sources {
        if let Some(mapping) = &ds.mapping_file {
            files.push(resolve_path(manifest_path, mapping));
        }
        for abi in &ds.abis {
            files.push(resolve_path(manifest_path, &abi.file));
        }
    }

    // Add template mapping files
    for template in &manifest.templates {
        if let Some(mapping) = &template.mapping_file {
            files.push(resolve_path(manifest_path, mapping));
        }
    }

    files
}

/// Build the subgraph.
fn build_subgraph(opt: &BuildOpt) -> Result<()> {
    // Apply migrations unless skipped
    if !opt.skip_migrations {
        migrations::apply_migrations(&opt.manifest)?;
    }

    // Load the manifest
    let manifest = load_manifest(&opt.manifest)?;
    let source_dir = opt
        .manifest
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();

    // Find graph-ts and node_modules
    let (lib_dirs, global_file) = find_graph_ts(&source_dir)?;
    let libs = lib_dirs
        .iter()
        .map(|p| p.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(",");

    // Create output directory
    fs::create_dir_all(&opt.output_dir)
        .with_context(|| format!("Failed to create output directory: {:?}", opt.output_dir))?;

    // Compile data source mappings
    let mut compiled_files: HashSet<String> = HashSet::new();

    for ds in &manifest.data_sources {
        if let Some(mapping) = &ds.mapping_file {
            let mapping_path = resolve_path(&opt.manifest, mapping);
            compile_data_source_mapping(
                &ds.name,
                &mapping_path,
                &source_dir,
                &opt.output_dir,
                &libs,
                &global_file,
                &opt.output_format,
                &mut compiled_files,
                opt.skip_asc_version_check,
            )?;
        }
    }

    // Compile template mappings
    for template in &manifest.templates {
        if let Some(mapping) = &template.mapping_file {
            let mapping_path = resolve_path(&opt.manifest, mapping);
            compile_template_mapping(
                &template.name,
                &mapping_path,
                &source_dir,
                &opt.output_dir,
                &libs,
                &global_file,
                &opt.output_format,
                &mut compiled_files,
                opt.skip_asc_version_check,
            )?;
        }
    }

    // Copy schema file
    if let Some(schema_path) = &manifest.schema {
        let schema_path = resolve_path(&opt.manifest, schema_path);
        copy_schema(&schema_path, &opt.output_dir)?;
    }

    // Copy ABI files
    for ds in &manifest.data_sources {
        for abi in &ds.abis {
            let abi_path = resolve_path(&opt.manifest, &abi.file);
            copy_abi(&abi.name, &abi_path, &ds.name, &opt.output_dir)?;
        }
    }

    // Write output manifest
    write_output_manifest(
        &opt.manifest,
        &manifest,
        &opt.output_dir,
        &opt.output_format,
    )?;

    step(Step::Done, "Build completed");

    // TODO: Upload to IPFS if --ipfs is specified

    Ok(())
}

/// Compile a data source mapping.
#[allow(clippy::too_many_arguments)]
fn compile_data_source_mapping(
    ds_name: &str,
    mapping_path: &Path,
    source_dir: &Path,
    output_dir: &Path,
    libs: &str,
    global_file: &Path,
    output_format: &str,
    compiled_files: &mut HashSet<String>,
    skip_version_check: bool,
) -> Result<()> {
    let cache_key = file_hash(mapping_path)?;

    // Check if already compiled
    if compiled_files.contains(&cache_key) {
        step(
            Step::Skip,
            &format!("Compile data source: {} (already compiled)", ds_name),
        );
        return Ok(());
    }

    step(Step::Generate, &format!("Compile data source: {}", ds_name));

    let output_subdir = output_dir.join(ds_name);
    fs::create_dir_all(&output_subdir)?;

    let extension = if output_format == "wasm" {
        "wasm"
    } else {
        "wast"
    };
    let output_file = output_subdir.join(format!("{}.{}", ds_name, extension));

    let options = AscCompileOptions {
        input_file: mapping_path.to_path_buf(),
        base_dir: source_dir.to_path_buf(),
        libs: libs.to_string(),
        global_file: global_file.to_path_buf(),
        output_file,
        output_format: output_format.to_string(),
        skip_version_check,
    };

    compile_mapping(&options)?;
    compiled_files.insert(cache_key);

    Ok(())
}

/// Compile a template mapping.
#[allow(clippy::too_many_arguments)]
fn compile_template_mapping(
    template_name: &str,
    mapping_path: &Path,
    source_dir: &Path,
    output_dir: &Path,
    libs: &str,
    global_file: &Path,
    output_format: &str,
    compiled_files: &mut HashSet<String>,
    skip_version_check: bool,
) -> Result<()> {
    let cache_key = file_hash(mapping_path)?;

    // Check if already compiled
    if compiled_files.contains(&cache_key) {
        step(
            Step::Skip,
            &format!(
                "Compile data source template: {} (already compiled)",
                template_name
            ),
        );
        return Ok(());
    }

    step(
        Step::Generate,
        &format!("Compile data source template: {}", template_name),
    );

    let output_subdir = output_dir.join("templates").join(template_name);
    fs::create_dir_all(&output_subdir)?;

    let extension = if output_format == "wasm" {
        "wasm"
    } else {
        "wast"
    };
    let output_file = output_subdir.join(format!("{}.{}", template_name, extension));

    let options = AscCompileOptions {
        input_file: mapping_path.to_path_buf(),
        base_dir: source_dir.to_path_buf(),
        libs: libs.to_string(),
        global_file: global_file.to_path_buf(),
        output_file,
        output_format: output_format.to_string(),
        skip_version_check,
    };

    compile_mapping(&options)?;
    compiled_files.insert(cache_key);

    Ok(())
}

/// Copy the schema file to the output directory.
fn copy_schema(schema_path: &Path, output_dir: &Path) -> Result<()> {
    let schema_name = schema_path
        .file_name()
        .ok_or_else(|| anyhow!("Invalid schema path"))?;
    let output_path = output_dir.join(schema_name);

    step(
        Step::Write,
        &format!("Copy schema file to {}", output_path.display()),
    );

    fs::copy(schema_path, &output_path).with_context(|| {
        format!(
            "Failed to copy schema from {} to {}",
            schema_path.display(),
            output_path.display()
        )
    })?;

    Ok(())
}

/// Copy an ABI file to the output directory.
fn copy_abi(abi_name: &str, abi_path: &Path, ds_name: &str, output_dir: &Path) -> Result<()> {
    let output_subdir = output_dir.join(ds_name);
    fs::create_dir_all(&output_subdir)?;

    let abi_filename = format!("{}.json", abi_name);
    let output_path = output_subdir.join(&abi_filename);

    step(
        Step::Write,
        &format!("Copy ABI {} to {}", abi_name, output_path.display()),
    );

    fs::copy(abi_path, &output_path).with_context(|| {
        format!(
            "Failed to copy ABI from {} to {}",
            abi_path.display(),
            output_path.display()
        )
    })?;

    Ok(())
}

/// Write the output manifest file.
fn write_output_manifest(
    manifest_path: &Path,
    manifest: &Manifest,
    output_dir: &Path,
    output_format: &str,
) -> Result<()> {
    let output_path = output_dir.join("subgraph.yaml");

    step(
        Step::Write,
        &format!("Write subgraph manifest to {}", output_path.display()),
    );

    // Load original manifest
    let manifest_str = fs::read_to_string(manifest_path)
        .with_context(|| format!("Failed to read manifest: {:?}", manifest_path))?;

    let mut value: serde_yaml::Value = serde_yaml::from_str(&manifest_str)?;

    // Update schema path
    if let Some(schema) = value.get_mut("schema") {
        if let Some(file) = schema.get_mut("file") {
            if let Some(schema_path) = manifest.schema.as_ref() {
                let schema_name = Path::new(schema_path)
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("schema.graphql");
                *file = serde_yaml::Value::String(schema_name.to_string());
            }
        }
    }

    // Update data source paths
    if let Some(data_sources) = value.get_mut("dataSources") {
        if let Some(arr) = data_sources.as_sequence_mut() {
            for (i, ds) in arr.iter_mut().enumerate() {
                if i < manifest.data_sources.len() {
                    let ds_name = &manifest.data_sources[i].name;
                    update_data_source_paths(ds, ds_name, output_format);
                }
            }
        }
    }

    // Update template paths
    if let Some(templates) = value.get_mut("templates") {
        if let Some(arr) = templates.as_sequence_mut() {
            for (i, template) in arr.iter_mut().enumerate() {
                if i < manifest.templates.len() {
                    let template_name = &manifest.templates[i].name;
                    update_template_paths(template, template_name, output_format);
                }
            }
        }
    }

    // Write output manifest
    let output_str = serde_yaml::to_string(&value)?;
    fs::write(&output_path, output_str)?;

    Ok(())
}

/// Update paths in a data source for the output manifest.
fn update_data_source_paths(ds: &mut serde_yaml::Value, ds_name: &str, output_format: &str) {
    // Update mapping file path
    if let Some(mapping) = ds.get_mut("mapping") {
        if let Some(file) = mapping.get_mut("file") {
            let extension = if output_format == "wasm" {
                "wasm"
            } else {
                "wast"
            };
            *file = serde_yaml::Value::String(format!("{}/{}.{}", ds_name, ds_name, extension));
        }

        // Update ABI paths
        if let Some(abis) = mapping.get_mut("abis") {
            if let Some(arr) = abis.as_sequence_mut() {
                for abi in arr.iter_mut() {
                    // Extract name first to avoid borrow conflicts
                    let abi_name = abi
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string());
                    if let Some(name) = abi_name {
                        if let Some(file) = abi.get_mut("file") {
                            *file = serde_yaml::Value::String(format!("{}/{}.json", ds_name, name));
                        }
                    }
                }
            }
        }
    }
}

/// Update paths in a template for the output manifest.
fn update_template_paths(
    template: &mut serde_yaml::Value,
    template_name: &str,
    output_format: &str,
) {
    // Update mapping file path
    if let Some(mapping) = template.get_mut("mapping") {
        if let Some(file) = mapping.get_mut("file") {
            let extension = if output_format == "wasm" {
                "wasm"
            } else {
                "wast"
            };
            *file = serde_yaml::Value::String(format!(
                "templates/{}/{}.{}",
                template_name, template_name, extension
            ));
        }

        // Update ABI paths
        if let Some(abis) = mapping.get_mut("abis") {
            if let Some(arr) = abis.as_sequence_mut() {
                for abi in arr.iter_mut() {
                    // Extract name first to avoid borrow conflicts
                    let abi_name = abi
                        .get("name")
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_string());
                    if let Some(name) = abi_name {
                        if let Some(file) = abi.get_mut("file") {
                            *file = serde_yaml::Value::String(format!(
                                "templates/{}/{}.json",
                                template_name, name
                            ));
                        }
                    }
                }
            }
        }
    }
}

/// Calculate SHA1 hash of a file for caching.
fn file_hash(path: &Path) -> Result<String> {
    let content = fs::read(path).with_context(|| format!("Failed to read file: {:?}", path))?;
    let mut hasher = Sha1::new();
    hasher.update(&content);
    let result = hasher.finalize();
    Ok(format!("{:x}", result))
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
    templates: Vec<Template>,
}

#[derive(Debug)]
struct DataSource {
    name: String,
    mapping_file: Option<String>,
    abis: Vec<Abi>,
}

#[derive(Debug)]
struct Template {
    name: String,
    mapping_file: Option<String>,
}

#[derive(Debug)]
struct Abi {
    name: String,
    file: String,
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
                    let mapping_file = ds
                        .get("mapping")
                        .and_then(|m| m.get("file"))
                        .and_then(|f| f.as_str())
                        .map(String::from);
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
                    Some(DataSource {
                        name,
                        mapping_file,
                        abis,
                    })
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
                    let mapping_file = t
                        .get("mapping")
                        .and_then(|m| m.get("file"))
                        .and_then(|f| f.as_str())
                        .map(String::from);
                    Some(Template { name, mapping_file })
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
    use tempfile::TempDir;

    #[test]
    fn test_resolve_path() {
        let manifest = PathBuf::from("/home/user/project/subgraph.yaml");
        let path = "src/mapping.ts";
        let resolved = resolve_path(&manifest, path);
        assert_eq!(resolved, PathBuf::from("/home/user/project/src/mapping.ts"));
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
      file: ./src/mapping.ts
      abis:
        - name: ERC20
          file: ./abis/ERC20.json
templates:
  - kind: ethereum/contract
    name: DynamicToken
    mapping:
      kind: ethereum/events
      file: ./src/dynamic.ts
"#;

        fs::write(&manifest_path, manifest_content).unwrap();

        let manifest = load_manifest(&manifest_path).unwrap();
        assert_eq!(manifest.schema, Some("./schema.graphql".to_string()));
        assert_eq!(manifest.data_sources.len(), 1);
        assert_eq!(manifest.data_sources[0].name, "Token");
        assert_eq!(
            manifest.data_sources[0].mapping_file,
            Some("./src/mapping.ts".to_string())
        );
        assert_eq!(manifest.data_sources[0].abis.len(), 1);
        assert_eq!(manifest.data_sources[0].abis[0].name, "ERC20");
        assert_eq!(manifest.templates.len(), 1);
        assert_eq!(manifest.templates[0].name, "DynamicToken");
    }

    #[test]
    fn test_file_hash() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.txt");
        fs::write(&file_path, "hello world").unwrap();

        let hash = file_hash(&file_path).unwrap();
        // SHA1 of "hello world"
        assert_eq!(hash, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }
}
