use std::{
    collections::HashSet,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use graph::{components::store::PrunePhase, env::ENV_VARS};
use graph::{
    components::store::{PruneReporter, StatusStore},
    data::subgraph::status,
    prelude::{anyhow, BlockNumber},
};
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use crate::manager::{
    commands::stats::{abbreviate_table_name, show_stats},
    deployment::DeploymentSearch,
};

struct Progress {
    start: Instant,
    analyze_start: Instant,
    switch_start: Instant,
    switch_time: Duration,
    table_start: Instant,
    table_rows: usize,
    initial_analyze: bool,
}

impl Progress {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            analyze_start: Instant::now(),
            switch_start: Instant::now(),
            switch_time: Duration::from_secs(0),
            table_start: Instant::now(),
            table_rows: 0,
            initial_analyze: true,
        }
    }
}

fn print_copy_header() {
    println!("{:^30} | {:^10} | {:^11}", "table", "versions", "time");
    println!("{:-^30}-+-{:-^10}-+-{:-^11}", "", "", "");
    std::io::stdout().flush().ok();
}

fn print_copy_row(
    table: &str,
    total_rows: usize,
    elapsed: Duration,
    phase: PrunePhase,
    finished: bool,
) {
    let phase = match (finished, phase) {
        (true, _) => "          ",
        (false, PrunePhase::CopyFinal) => "(final)",
        (false, PrunePhase::CopyNonfinal) => "(nonfinal)",
    };
    print!(
        "\r{:<30} | {:>10} | {:>9}s {phase}",
        abbreviate_table_name(table, 30),
        total_rows,
        elapsed.as_secs()
    );
    std::io::stdout().flush().ok();
}

impl PruneReporter for Progress {
    fn start_analyze(&mut self) {
        if !self.initial_analyze {
            println!("");
        }
        print!("Analyze tables");
        self.analyze_start = Instant::now();
    }

    fn start_analyze_table(&mut self, table: &str) {
        print!("\rAnalyze {table:48} ");
        std::io::stdout().flush().ok();
    }

    fn finish_analyze(
        &mut self,
        stats: &[graph::components::store::VersionStats],
        analyzed: &[&str],
    ) {
        let stats: Vec<_> = stats
            .iter()
            .filter(|stat| self.initial_analyze || analyzed.contains(&stat.tablename.as_str()))
            .map(|stats| stats.clone())
            .collect();
        println!(
            "\rAnalyzed {} tables in {}s{: ^30}",
            analyzed.len(),
            self.analyze_start.elapsed().as_secs(),
            ""
        );
        show_stats(stats.as_slice(), HashSet::new()).ok();
        println!();
        self.initial_analyze = false;
    }

    fn start_copy(&mut self) {
        println!("Copying data to new tables and replacing existing tables with them");
        print_copy_header();
    }

    fn start_table(&mut self, _table: &str) {
        self.table_start = Instant::now();
        self.table_rows = 0
    }

    fn prune_batch(&mut self, table: &str, rows: usize, phase: PrunePhase, finished: bool) {
        self.table_rows += rows;
        print_copy_row(
            table,
            self.table_rows,
            self.table_start.elapsed(),
            phase,
            finished,
        );
        std::io::stdout().flush().ok();
    }

    fn start_switch(&mut self) {
        self.switch_start = Instant::now();
    }

    fn finish_switch(&mut self) {
        self.switch_time += self.switch_start.elapsed();
    }

    fn finish_table(&mut self, _table: &str) {
        println!();
    }

    fn finish_prune(&mut self) {
        println!(
            "Finished pruning in {}s. Writing was blocked for {}s",
            self.start.elapsed().as_secs(),
            self.switch_time.as_secs()
        );
    }
}

pub async fn run(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
    prune_ratio: f64,
    once: bool,
) -> Result<(), anyhow::Error> {
    let history = history as BlockNumber;
    let deployment = search.locate_unique(&primary_pool)?;
    let mut info = store
        .status(status::Filter::DeploymentIds(vec![deployment.id]))?
        .pop()
        .ok_or_else(|| anyhow!("deployment {deployment} not found"))?;
    if info.chains.len() > 1 {
        return Err(anyhow!(
            "deployment {deployment} indexes {} chains, not sure how to deal with more than one chain",
            info.chains.len()
        ));
    }
    let status = info
        .chains
        .pop()
        .ok_or_else(|| anyhow!("deployment {} does not index any chain", deployment))?;
    let latest = status.latest_block.map(|ptr| ptr.number()).unwrap_or(0);
    if latest <= history {
        return Err(anyhow!("deployment {deployment} has only indexed up to block {latest} and we can't preserve {history} blocks of history"));
    }

    println!("prune {deployment}");
    println!("    latest: {latest}");
    println!("     final: {}", latest - ENV_VARS.reorg_threshold);
    println!("  earliest: {}\n", latest - history);

    let reporter = Box::new(Progress::new());
    store
        .subgraph_store()
        .prune(
            reporter,
            &deployment,
            Some(history),
            // Using the setting for eth chains is a bit lazy; the value
            // should really depend on the chain, but we don't have a
            // convenient way to figure out how each chain deals with
            // finality
            ENV_VARS.reorg_threshold,
            prune_ratio,
        )
        .await?;

    // Only after everything worked out, make the history setting permanent
    if !once {
        store.subgraph_store().set_history_blocks(
            &deployment,
            history,
            ENV_VARS.reorg_threshold,
        )?;
    }

    Ok(())
}
