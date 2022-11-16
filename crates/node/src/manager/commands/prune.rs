use std::{
    collections::HashSet,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use graph::{
    components::store::{PruneReporter, StatusStore},
    data::subgraph::status,
    prelude::{anyhow, BlockNumber},
};
use graph_chain_ethereum::ENV_VARS as ETH_ENV;
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use crate::manager::{
    commands::stats::{abbreviate_table_name, show_stats},
    deployment::DeploymentSearch,
};

struct Progress {
    start: Instant,
    analyze_start: Instant,
    switch_start: Instant,
    table_start: Instant,
    final_start: Instant,
    nonfinal_start: Instant,
}

impl Progress {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            analyze_start: Instant::now(),
            switch_start: Instant::now(),
            final_start: Instant::now(),
            table_start: Instant::now(),
            nonfinal_start: Instant::now(),
        }
    }
}

fn print_copy_header() {
    println!("{:^30} | {:^10} | {:^11}", "table", "versions", "time");
    println!("{:-^30}-+-{:-^10}-+-{:-^11}", "", "", "");
    std::io::stdout().flush().ok();
}

fn print_copy_row(table: &str, total_rows: usize, elapsed: Duration) {
    print!(
        "\r{:<30} | {:>10} | {:>9}s",
        abbreviate_table_name(table, 30),
        total_rows,
        elapsed.as_secs()
    );
    std::io::stdout().flush().ok();
}

impl PruneReporter for Progress {
    fn start_analyze(&mut self) {
        print!("Analyze tables");
        self.analyze_start = Instant::now();
    }

    fn start_analyze_table(&mut self, table: &str) {
        print!("\rAnalyze {table:48} ");
        std::io::stdout().flush().ok();
    }

    fn finish_analyze(&mut self, stats: &[graph::components::store::VersionStats]) {
        println!(
            "\rAnalyzed {} tables in {}s",
            stats.len(),
            self.analyze_start.elapsed().as_secs()
        );
        show_stats(stats, HashSet::new()).ok();
        println!("");
    }

    fn copy_final_start(&mut self, earliest_block: BlockNumber, final_block: BlockNumber) {
        println!("Copy final entities (versions live between {earliest_block} and {final_block})");
        print_copy_header();

        self.final_start = Instant::now();
        self.table_start = self.final_start;
    }

    fn copy_final_batch(&mut self, table: &str, _rows: usize, total_rows: usize, finished: bool) {
        print_copy_row(table, total_rows, self.table_start.elapsed());
        if finished {
            println!("");
            self.table_start = Instant::now();
        }
        std::io::stdout().flush().ok();
    }

    fn copy_final_finish(&mut self) {
        println!(
            "Finished copying final entity versions in {}s\n",
            self.final_start.elapsed().as_secs()
        );
    }

    fn start_switch(&mut self) {
        println!("Blocking writes and switching tables");
        print_copy_header();
        self.switch_start = Instant::now();
    }

    fn finish_switch(&mut self) {
        println!(
            "Enabling writes. Switching took {}s\n",
            self.switch_start.elapsed().as_secs()
        );
    }

    fn copy_nonfinal_start(&mut self, table: &str) {
        print_copy_row(table, 0, Duration::from_secs(0));
        self.nonfinal_start = Instant::now();
    }

    fn copy_nonfinal_batch(
        &mut self,
        table: &str,
        _rows: usize,
        total_rows: usize,
        finished: bool,
    ) {
        print_copy_row(table, total_rows, self.table_start.elapsed());
        if finished {
            println!("");
            self.table_start = Instant::now();
        }
        std::io::stdout().flush().ok();
    }

    fn finish_prune(&mut self) {
        println!("Finished pruning in {}s", self.start.elapsed().as_secs());
    }
}

pub async fn run(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
    prune_ratio: f64,
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
    println!("     final: {}", latest - ETH_ENV.reorg_threshold);
    println!("  earliest: {}\n", latest - history);

    let reporter = Box::new(Progress::new());
    store
        .subgraph_store()
        .prune(
            reporter,
            &deployment,
            latest - history,
            // Using the setting for eth chains is a bit lazy; the value
            // should really depend on the chain, but we don't have a
            // convenient way to figure out how each chain deals with
            // finality
            ETH_ENV.reorg_threshold,
            prune_ratio,
        )
        .await?;

    Ok(())
}
