use std::{io::Write, sync::Arc, time::Instant};

use graph::{
    components::store::{PruneReporter, StatusStore},
    data::subgraph::status,
    prelude::{anyhow, BlockNumber},
};
use graph_chain_ethereum::ENV_VARS as ETH_ENV;
use graph_store_postgres::{connection_pool::ConnectionPool, Store};

use crate::manager::deployment::DeploymentSearch;

struct Progress {
    start: Instant,
    analyze_start: Instant,
    switch_start: Instant,
    final_start: Instant,
    final_table_start: Instant,
    nonfinal_start: Instant,
}

impl Progress {
    fn new() -> Self {
        Self {
            start: Instant::now(),
            analyze_start: Instant::now(),
            switch_start: Instant::now(),
            final_start: Instant::now(),
            final_table_start: Instant::now(),
            nonfinal_start: Instant::now(),
        }
    }
}

impl PruneReporter for Progress {
    fn start_analyze(&mut self, table: &str) {
        print!("analyze {table:48} ");
        std::io::stdout().flush().ok();
        self.analyze_start = Instant::now();
    }

    fn finish_analyze(&mut self, table: &str) {
        println!(
            "\ranalyze {table:48} (done in {}s)",
            self.analyze_start.elapsed().as_secs()
        );
        std::io::stdout().flush().ok();
    }

    fn copy_final_start(&mut self, earliest_block: BlockNumber, final_block: BlockNumber) {
        println!("copy final entities (versions live between {earliest_block} and {final_block})");
        self.final_start = Instant::now();
        self.final_table_start = self.final_start;
    }

    fn copy_final_batch(&mut self, table: &str, _rows: usize, total_rows: usize, finished: bool) {
        if finished {
            println!(
                "\r  copy final {table:43} ({total_rows} rows in {}s)",
                self.final_table_start.elapsed().as_secs()
            );
            self.final_table_start = Instant::now();
        } else {
            print!(
                "\r  copy final {table:43} ({total_rows} rows in {}s)",
                self.final_table_start.elapsed().as_secs()
            );
        }
        std::io::stdout().flush().ok();
    }

    fn copy_final_finish(&mut self) {
        println!(
            "finished copying final entity versions in {}s",
            self.final_start.elapsed().as_secs()
        );
    }

    fn start_switch(&mut self) {
        println!("blocking writes and switching tables");
        self.switch_start = Instant::now();
    }

    fn finish_switch(&mut self) {
        println!(
            "enabling writes. Switching took {}s",
            self.switch_start.elapsed().as_secs()
        );
    }

    fn copy_nonfinal_start(&mut self, table: &str) {
        print!("  copy nonfinal {table:40}");
        std::io::stdout().flush().ok();
        self.nonfinal_start = Instant::now();
    }

    fn copy_nonfinal_finish(&mut self, table: &str, rows: usize) {
        println!(
            "\r  copy nonfinal {table:40} ({rows} rows in {}s)",
            self.nonfinal_start.elapsed().as_secs()
        );
        std::io::stdout().flush().ok();
    }

    fn finish_prune(&mut self) {
        println!("finished pruning in {}s", self.start.elapsed().as_secs());
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
    println!("  earliest: {}", latest - history);

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
