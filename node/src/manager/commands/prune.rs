use std::{
    collections::HashSet,
    io::Write,
    sync::Arc,
    time::{Duration, Instant},
};

use graph::{
    components::store::{DeploymentLocator, PrunePhase, PruneRequest},
    env::ENV_VARS,
};
use graph::{
    components::store::{PruneReporter, StatusStore},
    data::subgraph::status,
    prelude::{anyhow, BlockNumber},
};
use graph_store_postgres::{
    command_support::{Phase, PruneTableState},
    ConnectionPool, Store,
};
use termcolor::Color;

use crate::manager::{
    color::Terminal,
    commands::stats::show_stats,
    deployment::DeploymentSearch,
    fmt::{self, MapOrNull as _},
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

fn print_batch(
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
        (false, PrunePhase::Delete) => "(delete)",
    };
    print!(
        "\r{:<30} | {:>10} | {:>9}s {phase}",
        fmt::abbreviate(table, 30),
        total_rows,
        elapsed.as_secs()
    );
    std::io::stdout().flush().ok();
}

impl PruneReporter for Progress {
    fn start(&mut self, req: &PruneRequest) {
        println!("Prune to {} historical blocks", req.history_blocks);
    }

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

        if self.initial_analyze {
            // After analyzing, we start the actual work
            println!("Pruning tables");
            print_copy_header();
        }
        self.initial_analyze = false;
    }

    fn start_table(&mut self, _table: &str) {
        self.table_start = Instant::now();
        self.table_rows = 0
    }

    fn prune_batch(&mut self, table: &str, rows: usize, phase: PrunePhase, finished: bool) {
        self.table_rows += rows;
        print_batch(
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

    fn finish(&mut self) {
        println!(
            "Finished pruning in {}s. Writing was blocked for {}s",
            self.start.elapsed().as_secs(),
            self.switch_time.as_secs()
        );
    }
}

struct Args {
    history: BlockNumber,
    deployment: DeploymentLocator,
    earliest_block: BlockNumber,
    latest_block: BlockNumber,
}

fn check_args(
    store: &Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
) -> Result<Args, anyhow::Error> {
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
    let latest_block = status.latest_block.map(|ptr| ptr.number()).unwrap_or(0);
    if latest_block <= history {
        return Err(anyhow!("deployment {deployment} has only indexed up to block {latest_block} and we can't preserve {history} blocks of history"));
    }
    Ok(Args {
        history,
        deployment,
        earliest_block: status.earliest_block_number,
        latest_block,
    })
}

async fn first_prune(
    store: &Arc<Store>,
    args: &Args,
    rebuild_threshold: Option<f64>,
    delete_threshold: Option<f64>,
) -> Result<(), anyhow::Error> {
    println!("prune {}", args.deployment);
    println!(
        "     range: {} - {} ({} blocks)",
        args.earliest_block,
        args.latest_block,
        args.latest_block - args.earliest_block
    );

    let mut req = PruneRequest::new(
        &args.deployment,
        args.history,
        ENV_VARS.reorg_threshold(),
        args.earliest_block,
        args.latest_block,
    )?;
    if let Some(rebuild_threshold) = rebuild_threshold {
        req.rebuild_threshold = rebuild_threshold;
    }
    if let Some(delete_threshold) = delete_threshold {
        req.delete_threshold = delete_threshold;
    }

    let reporter = Box::new(Progress::new());

    store
        .subgraph_store()
        .prune(reporter, &args.deployment, req)
        .await?;
    Ok(())
}

async fn run_inner(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
    rebuild_threshold: Option<f64>,
    delete_threshold: Option<f64>,
    once: bool,
    do_first_prune: bool,
) -> Result<(), anyhow::Error> {
    let args = check_args(&store, primary_pool, search, history)?;

    if do_first_prune {
        first_prune(&store, &args, rebuild_threshold, delete_threshold).await?;
    }

    // Only after everything worked out, make the history setting permanent
    if !once {
        store.subgraph_store().set_history_blocks(
            &args.deployment,
            args.history,
            ENV_VARS.reorg_threshold(),
        )?;
    }

    Ok(())
}

pub async fn run(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
    rebuild_threshold: Option<f64>,
    delete_threshold: Option<f64>,
    once: bool,
) -> Result<(), anyhow::Error> {
    run_inner(
        store,
        primary_pool,
        search,
        history,
        rebuild_threshold,
        delete_threshold,
        once,
        true,
    )
    .await
}

pub async fn set(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    history: usize,
    rebuild_threshold: Option<f64>,
    delete_threshold: Option<f64>,
) -> Result<(), anyhow::Error> {
    run_inner(
        store,
        primary_pool,
        search,
        history,
        rebuild_threshold,
        delete_threshold,
        false,
        false,
    )
    .await
}

pub async fn status(
    store: Arc<Store>,
    primary_pool: ConnectionPool,
    search: DeploymentSearch,
    run: Option<usize>,
) -> Result<(), anyhow::Error> {
    fn percentage(left: Option<i64>, x: Option<i64>, right: Option<i64>) -> String {
        match (left, x, right) {
            (Some(left), Some(x), Some(right)) => {
                let range = right - left;
                if range == 0 {
                    return fmt::null();
                }
                let percent = (x - left) as f64 / range as f64 * 100.0;
                format!("{:.0}%", percent.min(100.0))
            }
            _ => fmt::null(),
        }
    }

    let mut term = Terminal::new();

    let deployment = search.locate_unique(&primary_pool)?;

    let viewer = store.subgraph_store().prune_viewer(&deployment).await?;
    let runs = viewer.runs()?;
    if runs.is_empty() {
        return Err(anyhow!("No prune runs found for deployment {deployment}"));
    }
    let run = run.unwrap_or(*runs.last().unwrap());
    let Some((state, table_states)) = viewer.state(run)? else {
        let runs = match runs.len() {
            0 => unreachable!("we checked that runs is not empty"),
            1 => format!("There is only one prune run #{}", runs[0]),
            2 => format!("Only prune runs #{} and #{} exist", runs[0], runs[1]),
            _ => format!(
                "Only prune runs #{} and #{} up to #{} exist",
                runs[0],
                runs[1],
                runs.last().unwrap()
            ),
        };
        return Err(anyhow!(
            "No information about prune run #{run} found for deployment {deployment}.\n  {runs}"
        ));
    };
    writeln!(term, "prune {deployment} (run #{run})")?;

    if let (Some(errored_at), Some(error)) = (&state.errored_at, &state.error) {
        term.with_color(Color::Red, |term| {
            writeln!(term, "     error: {error}")?;
            writeln!(term, "        at: {}", fmt::date_time(errored_at))
        })?;
    }
    writeln!(
        term,
        "     range: {} - {} ({} blocks, should keep {} blocks)",
        state.first_block,
        state.latest_block,
        state.latest_block - state.first_block,
        state.history_blocks
    )?;
    writeln!(term, "   started: {}", fmt::date_time(&state.started_at))?;
    match &state.finished_at {
        Some(finished_at) => writeln!(term, "  finished: {}", fmt::date_time(finished_at))?,
        None => writeln!(term, "  finished: still running")?,
    }
    writeln!(
        term,
        "  duration: {}",
        fmt::duration(&state.started_at, &state.finished_at)
    )?;

    writeln!(
        term,
        "\n{:^30} | {:^22} | {:^8} | {:^11} | {:^8}",
        "table", "status", "rows", "batch_size", "duration"
    )?;
    writeln!(
        term,
        "{:-^30}-+-{:-^22}-+-{:-^8}-+-{:-^11}-+-{:-^8}",
        "", "", "", "", ""
    )?;
    for ts in table_states {
        #[allow(unused_variables)]
        let PruneTableState {
            vid: _,
            id: _,
            run: _,
            table_name,
            strategy,
            phase,
            start_vid,
            final_vid,
            nonfinal_vid,
            rows,
            next_vid,
            batch_size,
            started_at,
            finished_at,
        } = ts;

        let complete = match phase {
            Phase::Queued | Phase::Started => "0%".to_string(),
            Phase::CopyFinal => percentage(start_vid, next_vid, final_vid),
            Phase::CopyNonfinal | Phase::Delete => percentage(start_vid, next_vid, nonfinal_vid),
            Phase::Done => fmt::check(),
            Phase::Unknown => fmt::null(),
        };

        let table_name = fmt::abbreviate(&table_name, 30);
        let rows = rows.map_or_null(|rows| rows.to_string());
        let batch_size = batch_size.map_or_null(|b| b.to_string());
        let duration = started_at.map_or_null(|s| fmt::duration(&s, &finished_at));
        let phase = phase.as_str();
        writeln!(term,
            "{table_name:<30} | {:<15} {complete:>6} | {rows:>8} | {batch_size:>11} | {duration:>8}",
            format!("{strategy}/{phase}")
        )?;
    }
    Ok(())
}
