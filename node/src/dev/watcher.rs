use globset::{Glob, GlobSet, GlobSetBuilder};
use graph::prelude::{DeploymentHash, SubgraphName};
use graph::slog::{self, error, info, Logger};
use graph::tokio::sync::mpsc::Sender;
use notify::{recommended_watcher, Event, RecursiveMode, Watcher};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

const WATCH_DELAY: Duration = Duration::from_secs(5);

/// Maps manifest files to their parent directories and returns a mapping of deployment hashes to directories
fn deployment_hash_to_dir(
    manifests: Vec<String>,
    logger: &Logger,
) -> BTreeMap<DeploymentHash, PathBuf> {
    let mut hash_to_dir = BTreeMap::new();

    for manifest in manifests {
        info!(logger, "Validating manifest: {}", manifest);
        let manifest_path = Path::new(&manifest);
        let manifest_path = match manifest_path.canonicalize() {
            Ok(canon_path) => canon_path,
            Err(e) => {
                error!(
                    logger,
                    "Failed to canonicalize path for manifest {}: {}", manifest, e
                );
                continue;
            }
        };

        let dir = match manifest_path.parent() {
            Some(parent) => match parent.canonicalize() {
                Ok(canon_path) => canon_path,
                Err(e) => {
                    error!(
                        logger,
                        "Failed to canonicalize path for manifest {}: {}", manifest, e
                    );
                    continue;
                }
            },
            None => {
                error!(
                    logger,
                    "Failed to get parent directory for manifest: {}", manifest
                );
                continue;
            }
        };

        info!(logger, "Watching manifest: {}", manifest_path.display());

        hash_to_dir.insert(
            DeploymentHash::new(manifest_path.display().to_string())
                .expect("Failed to create deployment hash"),
            dir.to_path_buf(),
        );
    }

    hash_to_dir
}

/// Sets up a watcher for the given directory with optional exclusions.
/// Exclusions can include glob patterns like "pgtemp-*".
pub async fn watch_subgraphs(
    logger: &Logger,
    manifests: Vec<String>,
    exclusions: Vec<String>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) {
    let logger = logger.new(slog::o!("component" => ">>>>> Watcher"));
    info!(logger, "Watching subgraphs: {}", manifests.join(", "));
    let hash_to_dir = deployment_hash_to_dir(manifests, &logger);

    watch_subgraph_dirs(&logger, hash_to_dir, exclusions, sender).await;
}

/// Sets up a watcher for the given directories with optional exclusions.
/// Exclusions can include glob patterns like "pgtemp-*".
pub async fn watch_subgraph_dirs(
    logger: &Logger,
    hash_to_dir: BTreeMap<DeploymentHash, PathBuf>,
    exclusions: Vec<String>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) {
    if hash_to_dir.is_empty() {
        info!(logger, "No directories to watch");
        return;
    }

    info!(
        logger,
        "Watching for changes in {} directories",
        hash_to_dir.len()
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
            return;
        }
    };

    for (_, dir) in hash_to_dir.iter() {
        if let Err(e) = watcher.watch(dir, RecursiveMode::Recursive) {
            error!(logger, "Error watching directory {}: {}", dir.display(), e);
            std::process::exit(1);
        }
        info!(logger, "Watching directory: {}", dir.display());
    }

    // Process file change events
    process_file_events(logger, rx, &exclusion_set, &hash_to_dir, sender).await;
}

/// Processes file change events and triggers redeployments
async fn process_file_events(
    logger: &Logger,
    rx: mpsc::Receiver<Result<Event, notify::Error>>,
    exclusion_set: &GlobSet,
    hash_to_dir: &BTreeMap<DeploymentHash, PathBuf>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) {
    loop {
        // Wait for an event
        let event = match rx.recv() {
            Ok(Ok(e)) => e,
            Ok(_) => continue,
            Err(_) => break,
        };

        if !is_relevant_event(&event, hash_to_dir, exclusion_set) {
            continue;
        }

        // Once we receive an event, wait for a short period to batch multiple related events
        let start = std::time::Instant::now();
        while start.elapsed() < WATCH_DELAY {
            match rx.try_recv() {
                // Discard all events until the time window has passed
                Ok(_) => continue,
                Err(_) => break,
            }
        }

        // Redeploy all subgraphs
        redeploy_all_subgraphs(logger, hash_to_dir, &sender).await;
    }
}

/// Checks if an event is relevant for any of the watched directories
fn is_relevant_event(
    event: &Event,
    watched_dirs: &BTreeMap<DeploymentHash, PathBuf>,
    exclusion_set: &GlobSet,
) -> bool {
    for path in event.paths.iter() {
        for dir in watched_dirs.values() {
            if path.starts_with(dir) && should_process_event(event, dir, exclusion_set) {
                return true;
            }
        }
    }
    false
}

/// Redeploys all subgraphs in the order defined by the BTreeMap
async fn redeploy_all_subgraphs(
    logger: &Logger,
    hash_to_dir: &BTreeMap<DeploymentHash, PathBuf>,
    sender: &Sender<(DeploymentHash, SubgraphName)>,
) {
    info!(logger, "File change detected, redeploying all subgraphs");
    let mut count = 0;
    for id in hash_to_dir.keys() {
        let _ = sender
            .send((
                id.clone(),
                SubgraphName::new(format!("subgraph-{}", count))
                    .expect("Failed to create subgraph name"),
            ))
            .await;
        count += 1;
    }
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
    // If no exclusions, process all events
    if exclusion_set.is_empty() {
        return true;
    }

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
