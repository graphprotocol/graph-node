use std::sync::Arc;
use std::time::Instant;

use console::style;
use graph::components::store::RestoreReporter;
use graph::env::ENV_VARS;
use graph::{bail, prelude::anyhow::Result};
use indicatif::ProgressBar;

use graph::prelude::SubgraphName;
use graph_store_postgres::{RestoreMode, Shard, SubgraphStore};

struct RestoreProgress {
    spinner: ProgressBar,
    start: Instant,
    phase_start: Instant,
    table_rows: usize,
    table_total: usize,
    deployment: String,
    namespace: String,
    shard: String,
}

impl RestoreProgress {
    fn new() -> Self {
        let spinner = ProgressBar::new_spinner();
        spinner.enable_steady_tick(std::time::Duration::from_millis(80));
        Self {
            spinner,
            start: Instant::now(),
            phase_start: Instant::now(),
            table_rows: 0,
            table_total: 0,
            deployment: String::new(),
            namespace: String::new(),
            shard: String::new(),
        }
    }
}

impl RestoreReporter for RestoreProgress {
    fn start(&mut self, deployment: &str, table_count: usize) {
        self.deployment = deployment.to_string();
        self.start = Instant::now();
        self.spinner.set_message(format!(
            "Restoring deployment {} ({} tables)",
            style(deployment).cyan(),
            table_count,
        ));
    }

    fn start_create_schema(&mut self, namespace: &str, shard: &str) {
        self.namespace = namespace.to_string();
        self.shard = shard.to_string();
        self.phase_start = Instant::now();
        self.spinner.set_message(format!(
            "Creating schema for {} in shard {}",
            namespace, shard
        ));
    }

    fn finish_create_schema(&mut self) {
        let elapsed = self.phase_start.elapsed().as_secs();
        let line = format!(
            "  {} {:<32} {:>15} {:>4}s",
            style("\u{2714}").green(),
            format!("Schema for {} created", self.namespace),
            "",
            elapsed,
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn start_table(&mut self, table: &str, total_rows: usize) {
        self.phase_start = Instant::now();
        self.table_rows = 0;
        self.table_total = total_rows;
        self.spinner
            .set_message(format!("{:<32} 0/{} rows", table, format_count(total_rows),));
    }

    fn batch_imported(&mut self, table: &str, rows: usize) {
        self.table_rows += rows;
        self.spinner.set_message(format!(
            "{:<32} {}/{} rows",
            table,
            format_count(self.table_rows),
            format_count(self.table_total),
        ));
    }

    fn skip_table(&mut self, table: &str) {
        let line = format!(
            "  {} {:<32} {:>15}",
            style("\u{2714}").green(),
            table,
            "skipped",
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn finish_table(&mut self, table: &str, rows: usize) {
        let elapsed = self.phase_start.elapsed().as_secs();
        let line = format!(
            "  {} {:<32} {:>10} rows {:>4}s",
            style("\u{2714}").green(),
            table,
            format_count(rows),
            elapsed,
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn start_data_sources(&mut self, total_rows: usize) {
        self.phase_start = Instant::now();
        self.table_rows = 0;
        self.table_total = total_rows;
        self.spinner.set_message(format!(
            "data_sources$  0/{} rows",
            format_count(total_rows)
        ));
    }

    fn finish_data_sources(&mut self, rows: usize) {
        let elapsed = self.phase_start.elapsed().as_secs();
        let line = format!(
            "  {} {:<32} {:>10} rows {:>4}s",
            style("\u{2714}").green(),
            "data_sources$",
            format_count(rows),
            elapsed,
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn start_finalize(&mut self) {
        self.phase_start = Instant::now();
        self.spinner.set_message("Finalizing".to_string());
    }

    fn finish_finalize(&mut self) {
        let elapsed = self.phase_start.elapsed().as_secs();
        let line = format!(
            "  {} {:<32} {:>15} {:>4}s",
            style("\u{2714}").green(),
            "Finalized",
            "",
            elapsed,
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn finish(&mut self) {
        let elapsed = self.start.elapsed().as_secs();
        self.spinner.finish_with_message(format!(
            "{} Restore complete ({}s)",
            style("\u{2714}").green(),
            elapsed,
        ));
    }
}

fn format_count(n: usize) -> String {
    if n == 0 {
        return "0".to_string();
    }
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub async fn run(
    subgraph_store: Arc<SubgraphStore>,
    directory: String,
    shard: Option<String>,
    name: Option<String>,
    replace: bool,
    add: bool,
    force: bool,
) -> Result<()> {
    if add && shard.is_none() {
        bail!("--add requires --shard");
    }

    let directory = std::path::Path::new(&directory).canonicalize()?;
    let stat = std::fs::metadata(&directory)?;

    if !stat.is_dir() {
        bail!(
            "The path `{}` is not a directory",
            directory.to_string_lossy()
        );
    }

    let name = name
        .map(|n| SubgraphName::new(n.clone()).map_err(|_| anyhow::anyhow!("invalid name `{n}`")))
        .transpose()?;

    let mode = if replace {
        RestoreMode::Replace
    } else if add {
        RestoreMode::Add
    } else if force {
        RestoreMode::Force
    } else {
        RestoreMode::Default
    };

    let shard = shard.map(Shard::new).transpose()?;

    let version_switching_mode = ENV_VARS.subgraph_version_switching_mode;

    let mut reporter = Box::new(RestoreProgress::new());
    subgraph_store
        .restore(
            &directory,
            shard,
            name,
            mode,
            version_switching_mode,
            &mut *reporter,
        )
        .await?;
    println!(
        "Restored {} into {} in shard {}",
        reporter.deployment, reporter.namespace, reporter.shard
    );
    Ok(())
}
