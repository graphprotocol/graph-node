use std::fs;
use std::sync::Arc;
use std::time::Instant;

use console::style;
use graph::components::store::DumpReporter;
use graph::prelude::anyhow::Result;
use indicatif::ProgressBar;

use graph_store_postgres::{ConnectionPool, SubgraphStore};

use crate::manager::deployment::DeploymentSearch;

struct DumpProgress {
    spinner: ProgressBar,
    start: Instant,
    table_start: Instant,
    table_rows: usize,
}

impl DumpProgress {
    fn new() -> Self {
        let spinner = ProgressBar::new_spinner();
        spinner.enable_steady_tick(std::time::Duration::from_millis(80));
        Self {
            spinner,
            start: Instant::now(),
            table_start: Instant::now(),
            table_rows: 0,
        }
    }
}

impl DumpReporter for DumpProgress {
    fn start(&mut self, deployment: &str, table_count: usize) {
        self.start = Instant::now();
        self.spinner.set_message(format!(
            "Dumping deployment {} ({} tables)",
            style(deployment).cyan(),
            table_count,
        ));
    }

    fn start_table(&mut self, table: &str, rows_approx: usize) {
        self.table_start = Instant::now();
        self.table_rows = 0;
        self.spinner
            .set_message(format!("{:<32} ~{} rows", table, format_count(rows_approx),));
    }

    fn batch_dumped(&mut self, table: &str, rows: usize) {
        self.table_rows += rows;
        self.spinner.set_message(format!(
            "{:<32} {} rows",
            table,
            format_count(self.table_rows),
        ));
    }

    fn finish_table(&mut self, table: &str, rows: usize) {
        let elapsed = self.table_start.elapsed().as_secs();
        let line = format!(
            "  {} {:<32} {:>10} rows {:>4}s",
            style("\u{2714}").green(),
            table,
            format_count(rows),
            elapsed,
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn start_data_sources(&mut self) {
        self.table_start = Instant::now();
        self.table_rows = 0;
        self.spinner.set_message("data_sources$".to_string());
    }

    fn finish_data_sources(&mut self, rows: usize) {
        let elapsed = self.table_start.elapsed().as_secs();
        let line = format!(
            "  {} {:<32} {:>10} rows {:>4}s",
            style("\u{2714}").green(),
            "data_sources$",
            format_count(rows),
            elapsed,
        );
        self.spinner.suspend(|| println!("{line}"));
    }

    fn start_clamps(&mut self, table: &str, rows_approx: usize) {
        self.spinner.set_message(format!(
            "{:<32} clamps ~{} rows",
            table,
            format_count(rows_approx),
        ));
    }

    fn finish_clamps(&mut self, table: &str, rows: usize) {
        if rows > 0 {
            let line = format!(
                "  {} {:<32} {:>10} clamps",
                style("\u{2714}").green(),
                table,
                format_count(rows),
            );
            self.spinner.suspend(|| println!("{line}"));
        }
    }

    fn finish(&mut self) {
        let elapsed = self.start.elapsed().as_secs();
        self.spinner.finish_with_message(format!(
            "{} Dump complete ({}s)",
            style("\u{2714}").green(),
            elapsed
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
    primary_pool: ConnectionPool,
    deployment: DeploymentSearch,
    directory: String,
) -> Result<()> {
    fs::create_dir_all(&directory)?;
    let directory = fs::canonicalize(&directory)?;

    let loc = deployment.locate_unique(&primary_pool).await?;

    let reporter = Box::new(DumpProgress::new());
    subgraph_store.dump(&loc, directory, reporter).await?;
    Ok(())
}
