//! File watching utilities for build and codegen commands.
//!
//! This module provides a unified watch loop implementation used by both
//! the build and codegen commands when running with the --watch flag.

use std::collections::HashSet;
use std::future::Future;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use notify::{recommended_watcher, RecursiveMode, Watcher};

/// Delay between file change detection and callback execution to batch multiple events.
pub const WATCH_DEBOUNCE: Duration = Duration::from_millis(500);

/// Watch files and run a callback when changes are detected.
///
/// This function:
/// - Sets up a file watcher for all directories containing the specified files
/// - Debounces file change events (waits 500ms and drains pending events)
/// - Calls the callback when relevant files change
/// - Runs forever until interrupted or an error occurs
///
/// The callback is called once initially before entering the watch loop.
///
/// # Type Parameters
///
/// * `F` - The callback closure type
/// * `Fut` - The future returned by the callback
///
/// # Arguments
///
/// * `files_to_watch` - List of file paths to watch for changes
/// * `initial_msg` - Message to print after initial run before entering watch mode
/// * `on_change` - Async callback to run when files change
pub async fn watch_and_run<F, Fut>(
    files_to_watch: Vec<PathBuf>,
    initial_msg: &str,
    on_change: F,
) -> Result<()>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    // Do initial run
    if let Err(e) = on_change().await {
        eprintln!("Error during initial run: {}", e);
    }

    println!("\n{}", initial_msg);
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

                    if let Err(e) = on_change().await {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watch_debounce_duration() {
        // Verify the debounce duration is 500ms
        assert_eq!(WATCH_DEBOUNCE, Duration::from_millis(500));
    }
}
