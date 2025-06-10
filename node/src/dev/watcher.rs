use globset::{Glob, GlobSet, GlobSetBuilder};
use graph::prelude::{DeploymentHash, SubgraphName};
use graph::slog::{error, info, Logger};
use graph::tokio::sync::mpsc::Sender;
use notify::{recommended_watcher, Event, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::time::Duration;

const WATCH_DELAY: Duration = Duration::from_secs(5);

/// Sets up a watcher for the given directory with optional exclusions.
/// Exclusions can include glob patterns like "pgtemp-*".
pub async fn watch_subgraph_dir(
    logger: &Logger,
    dir: PathBuf,
    id: String,
    exclusions: Vec<String>,
    sender: Sender<(DeploymentHash, SubgraphName)>,
) {
    info!(
        logger,
        "Watching for changes in directory: {}",
        dir.display()
    );
    if !exclusions.is_empty() {
        info!(logger, "Excluding patterns: {}", exclusions.join(", "));
    }

    // Create exclusion matcher
    let exclusion_set = build_glob_set(&exclusions, logger);

    // Create a channel to receive the events
    let (tx, rx) = mpsc::channel();

    // Create a watcher object
    let mut watcher = match recommended_watcher(tx) {
        Ok(w) => w,
        Err(e) => {
            error!(logger, "Error creating file watcher: {}", e);
            return;
        }
    };

    if let Err(e) = watcher.watch(&dir, RecursiveMode::Recursive) {
        error!(logger, "Error watching directory {}: {}", dir.display(), e);
        return;
    }

    let watch_dir = dir.clone();
    let watch_exclusion_set = exclusion_set.clone();

    loop {
        let first_event = match rx.recv() {
            Ok(Ok(e)) if should_process_event(&e, &watch_dir, &watch_exclusion_set) => Some(e),
            Ok(_) => continue,
            Err(_) => break,
        };

        if first_event.is_none() {
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

        let _ = sender
            .send((
                DeploymentHash::new(id.clone()).unwrap(),
                SubgraphName::new("test").unwrap(),
            ))
            .await;
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
