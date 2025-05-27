use anyhow::{anyhow, Context, Result};
use globset::{Glob, GlobSet, GlobSetBuilder};
use graph::prelude::{DeploymentHash, SubgraphName};
use graph::slog::{self, error, info, Logger};
use graph::tokio::sync::mpsc::Sender;
use notify::{recommended_watcher, Event, RecursiveMode, Watcher};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

use super::helpers::{parse_alias, parse_manifest_arg};

const WATCH_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_BUILD_DIR: &str = "build";

// Parses manifest arguments and returns a vector of paths to the manifest files
pub fn parse_manifest_args(
    manifests: Vec<String>,
    subgraph_sources: Vec<String>,
    logger: &Logger,
) -> Result<(Vec<PathBuf>, HashMap<String, PathBuf>)> {
    let mut manifests_paths = Vec::new();
    let mut source_subgraph_aliases = HashMap::new();

    for subgraph_source in subgraph_sources {
        let (alias_name, manifest_path_str, build_dir_opt) = parse_alias(&subgraph_source)?;
        let manifest_path =
            process_manifest(build_dir_opt, &manifest_path_str, Some(&alias_name), logger)?;

        manifests_paths.push(manifest_path.clone());
        source_subgraph_aliases.insert(alias_name, manifest_path);
    }

    for manifest_str in manifests {
        let (manifest_path_str, build_dir_opt) = parse_manifest_arg(&manifest_str)
            .with_context(|| format!("While parsing manifest '{}'", manifest_str))?;

        let built_manifest_path =
            process_manifest(build_dir_opt, &manifest_path_str, None, logger)?;

        manifests_paths.push(built_manifest_path);
    }

    Ok((manifests_paths, source_subgraph_aliases))
}

/// Helper function to process a manifest
fn process_manifest(
    build_dir_opt: Option<String>,
    manifest_path_str: &str,
    alias_name: Option<&String>,
    logger: &Logger,
) -> Result<PathBuf> {
    let build_dir_str = build_dir_opt.unwrap_or_else(|| DEFAULT_BUILD_DIR.to_owned());

    info!(logger, "Validating manifest: {}", manifest_path_str);

    let manifest_path = Path::new(manifest_path_str);
    let manifest_path = manifest_path
        .canonicalize()
        .with_context(|| format!("Manifest path does not exist: {}", manifest_path_str))?;

    // Get the parent directory of the manifest
    let parent_dir = manifest_path
        .parent()
        .ok_or_else(|| {
            anyhow!(
                "Failed to get parent directory for manifest: {}",
                manifest_path_str
            )
        })?
        .canonicalize()
        .with_context(|| {
            format!(
                "Parent directory does not exist for manifest: {}",
                manifest_path_str
            )
        })?;

    // Create the build directory path by joining the parent directory with the build_dir_str
    let build_dir = parent_dir.join(build_dir_str);
    let build_dir = build_dir
        .canonicalize()
        .with_context(|| format!("Build directory does not exist: {}", build_dir.display()))?;

    let manifest_file_name = manifest_path.file_name().ok_or_else(|| {
        anyhow!(
            "Failed to get file name for manifest: {}",
            manifest_path_str
        )
    })?;

    let built_manifest_path = build_dir.join(manifest_file_name);

    info!(
        logger,
        "Watching manifest: {}",
        built_manifest_path.display()
    );

    if let Some(name) = alias_name {
        info!(
            logger,
            "Using build directory for {}: {}",
            name,
            build_dir.display()
        );
    } else {
        info!(logger, "Using build directory: {}", build_dir.display());
    }

    Ok(built_manifest_path)
}

/// Sets up a watcher for the given directory with optional exclusions.
/// Exclusions can include glob patterns like "pgtemp-*".
pub async fn watch_subgraphs(
    logger: &Logger,
    manifests_paths: Vec<PathBuf>,
    source_subgraph_aliases: HashMap<String, PathBuf>,
    exclusions: Vec<String>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) -> Result<()> {
    let logger = logger.new(slog::o!("component" => "Watcher"));

    watch_subgraph_dirs(
        &logger,
        manifests_paths,
        source_subgraph_aliases,
        exclusions,
        sender,
    )
    .await?;
    Ok(())
}

/// Sets up a watcher for the given directories with optional exclusions.
/// Exclusions can include glob patterns like "pgtemp-*".
pub async fn watch_subgraph_dirs(
    logger: &Logger,
    manifests_paths: Vec<PathBuf>,
    source_subgraph_aliases: HashMap<String, PathBuf>,
    exclusions: Vec<String>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) -> Result<()> {
    if manifests_paths.is_empty() {
        info!(logger, "No directories to watch");
        return Ok(());
    }

    info!(
        logger,
        "Watching for changes in {} directories",
        manifests_paths.len()
    );

    if !exclusions.is_empty() {
        info!(logger, "Excluding patterns: {}", exclusions.join(", "));
    }

    // Create exclusion matcher
    let exclusion_set = build_glob_set(&exclusions, logger);

    // Create a channel to receive the events
    let (tx, rx) = mpsc::channel();

    let mut watcher = match recommended_watcher(tx) {
        Ok(w) => w,
        Err(e) => {
            error!(logger, "Error creating file watcher: {}", e);
            return Err(anyhow!("Error creating file watcher"));
        }
    };

    for manifest_path in manifests_paths.iter() {
        let dir = manifest_path.parent().unwrap();
        if let Err(e) = watcher.watch(dir, RecursiveMode::Recursive) {
            error!(logger, "Error watching directory {}: {}", dir.display(), e);
            std::process::exit(1);
        }
        info!(logger, "Watching directory: {}", dir.display());
    }

    // Process file change events
    process_file_events(
        logger,
        rx,
        &exclusion_set,
        &manifests_paths,
        &source_subgraph_aliases,
        sender,
    )
    .await
}

/// Processes file change events and triggers redeployments
async fn process_file_events(
    logger: &Logger,
    rx: mpsc::Receiver<Result<Event, notify::Error>>,
    exclusion_set: &GlobSet,
    manifests_paths: &Vec<PathBuf>,
    source_subgraph_aliases: &HashMap<String, PathBuf>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) -> Result<()> {
    loop {
        // Wait for an event
        let event = match rx.recv() {
            Ok(Ok(e)) => e,
            Ok(_) => continue,
            Err(_) => {
                error!(logger, "Error receiving file change event");
                return Err(anyhow!("Error receiving file change event"));
            }
        };

        if !is_relevant_event(
            &event,
            manifests_paths
                .iter()
                .map(|p| p.parent().unwrap().to_path_buf())
                .collect(),
            exclusion_set,
        ) {
            continue;
        }

        // Once we receive an event, wait for a short period of time to allow for multiple events to be received
        // This is because running graph build writes multiple files at once
        // Which triggers multiple events, we only need to react to it once
        let start = std::time::Instant::now();
        while start.elapsed() < WATCH_DELAY {
            match rx.try_recv() {
                // Discard all events until the time window has passed
                Ok(_) => continue,
                Err(_) => break,
            }
        }

        // Redeploy all subgraphs
        deploy_all_subgraphs(logger, manifests_paths, source_subgraph_aliases, &sender).await?;
    }
}

/// Checks if an event is relevant for any of the watched directories
fn is_relevant_event(event: &Event, watched_dirs: Vec<PathBuf>, exclusion_set: &GlobSet) -> bool {
    for path in event.paths.iter() {
        for dir in watched_dirs.iter() {
            if path.starts_with(dir) && should_process_event(event, dir, exclusion_set) {
                return true;
            }
        }
    }
    false
}

/// Redeploys all subgraphs in the order it appears in the manifests_paths
async fn deploy_all_subgraphs(
    logger: &Logger,
    manifests_paths: &Vec<PathBuf>,
    source_subgraph_aliases: &HashMap<String, PathBuf>,
    sender: &Sender<(DeploymentHash, SubgraphName)>,
) -> Result<()> {
    info!(logger, "File change detected, redeploying all subgraphs");
    let mut count = 0;
    for manifest_path in manifests_paths {
        let alias_name = source_subgraph_aliases
            .iter()
            .find(|(_, path)| path == &manifest_path)
            .map(|(name, _)| name);

        let id = alias_name
            .map(|s| s.to_owned())
            .unwrap_or_else(|| manifest_path.display().to_string());

        let _ = sender
            .send((
                DeploymentHash::new(id).map_err(|_| anyhow!("Failed to create deployment hash"))?,
                SubgraphName::new(format!("subgraph-{}", count))
                    .map_err(|_| anyhow!("Failed to create subgraph name"))?,
            ))
            .await;
        count += 1;
    }
    Ok(())
}

/// Build a GlobSet from the provided patterns
fn build_glob_set(patterns: &[String], logger: &Logger) -> GlobSet {
    let mut builder = GlobSetBuilder::new();

    for pattern in patterns {
        match Glob::new(pattern) {
            Ok(glob) => {
                builder.add(glob);
            }
            Err(e) => error!(logger, "Invalid glob pattern '{}': {}", pattern, e),
        }
    }

    match builder.build() {
        Ok(set) => set,
        Err(e) => {
            error!(logger, "Failed to build glob set: {}", e);
            GlobSetBuilder::new().build().unwrap()
        }
    }
}

/// Determines if an event should be processed based on exclusion patterns
fn should_process_event(event: &Event, base_dir: &Path, exclusion_set: &GlobSet) -> bool {
    // Check each path in the event
    for path in event.paths.iter() {
        // Get the relative path from the base directory
        if let Ok(rel_path) = path.strip_prefix(base_dir) {
            let path_str = rel_path.to_string_lossy();

            // Check if path matches any exclusion pattern
            if exclusion_set.is_match(path_str.as_ref()) {
                return false;
            }

            // Also check against the file name for basename patterns
            if let Some(file_name) = rel_path.file_name() {
                let name_str = file_name.to_string_lossy();
                if exclusion_set.is_match(name_str.as_ref()) {
                    return false;
                }
            }
        }
    }

    true
}
