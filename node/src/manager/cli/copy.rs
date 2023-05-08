use std::{collections::HashMap, sync::Arc, time::SystemTime};

use graph_store_postgres::{connection_pool::ConnectionPool, Shard, Store, SubgraphStore};

use crate::manager::{
    cli::utils::display::List,
    core::{
        self,
        copy::{CopyState, CreateCopyResult, StatusResult, UtcDateTime},
    },
    deployment::DeploymentSearch,
};

use graph::prelude::{anyhow::Error, chrono::Duration};
use graph::{anyhow::bail, prelude::chrono::SecondsFormat};

pub async fn create(
    store: Arc<Store>,
    primary: ConnectionPool,
    src: DeploymentSearch,
    shard: String,
    shards: Vec<String>,
    node: String,
    block_offset: u32,
    activate: bool,
    replace: bool,
) -> Result<(), Error> {
    let CreateCopyResult { src, dst } = core::copy::create(
        store,
        primary,
        src,
        shard,
        shards,
        node,
        block_offset,
        activate,
        replace,
    )
    .await?;

    println!("created deployment {} as copy of {}", dst, src);

    Ok(())
}

pub fn activate(store: Arc<SubgraphStore>, deployment: String, shard: String) -> Result<(), Error> {
    let deployment = core::copy::activate(store, deployment, shard)?;

    println!("activated copy {}", deployment);

    Ok(())
}

pub fn list(pools: HashMap<Shard, ConnectionPool>) -> Result<(), Error> {
    let copies = core::copy::list(pools.clone())?;

    if copies.is_empty() {
        println!("no active copies");
    } else {
        fn status(name: &str, at: UtcDateTime) {
            println!(
                "{:20} | {}",
                name,
                at.to_rfc3339_opts(SecondsFormat::Secs, false)
            );
        }

        for (src, dst, cancelled_at, queued_at, deployment_hash, shard) in copies {
            println!("{:-<78}", "");

            println!("{:20} | {}", "deployment", deployment_hash);
            println!("{:20} | sgd{} -> sgd{} ({})", "action", src, dst, shard);
            match CopyState::find(&pools, &shard, dst)? {
                Some((state, tables, _)) => match cancelled_at {
                    Some(cancel_requested) => match state.cancelled_at {
                        Some(cancelled_at) => status("cancelled", cancelled_at),
                        None => status("cancel requested", cancel_requested),
                    },
                    None => match state.finished_at {
                        Some(finished_at) => status("finished", finished_at),
                        None => {
                            let target: i64 = tables.iter().map(|table| table.target_vid).sum();
                            let next: i64 = tables.iter().map(|table| table.next_vid).sum();
                            let done = next as f64 / target as f64 * 100.0;
                            status("started", state.started_at);
                            println!("{:20} | {:.2}% done, {}/{}", "progress", done, next, target)
                        }
                    },
                },
                None => status("queued", queued_at),
            };
        }
    }

    Ok(())
}

pub fn status(pools: HashMap<Shard, ConnectionPool>, dst: &DeploymentSearch) -> Result<(), Error> {
    let StatusResult {
        shard,
        deployment,
        cancelled_at,
        dst,
        ..
    } = core::copy::status(pools.clone(), &dst)?;

    fn duration(start: &UtcDateTime, end: &Option<UtcDateTime>) -> String {
        let start = *start;
        let end = *end;

        let end = end.unwrap_or(UtcDateTime::from(SystemTime::now()));
        let duration = end - start;

        human_duration(duration)
    }

    fn human_duration(duration: Duration) -> String {
        if duration.num_seconds() < 5 {
            format!("{}ms", duration.num_milliseconds())
        } else if duration.num_minutes() < 5 {
            format!("{}s", duration.num_seconds())
        } else {
            format!("{}m", duration.num_minutes())
        }
    }

    fn done(ts: &Option<UtcDateTime>) -> String {
        ts.map(|_| "✓").unwrap_or(".").to_string()
    }
    let (state, tables, on_sync) = match CopyState::find(&pools, &shard, dst)? {
        Some((state, tables, on_sync)) => (state, tables, on_sync),
        None => {
            // if active {
            if true {
                println!("copying is queued but has not started");
                return Ok(());
            } else {
                bail!("no copy operation for {} exists", dst);
            }
        }
    };

    let progress = match &state.finished_at {
        Some(_) => done(&state.finished_at),
        None => {
            let target: i64 = tables.iter().map(|table| table.target_vid).sum();
            let next: i64 = tables.iter().map(|table| table.next_vid).sum();
            let pct = next as f64 / target as f64 * 100.0;
            format!("{:.2}% done, {}/{}", pct, next, target)
        }
    };

    let mut lst = vec![
        "deployment",
        "src",
        "dst",
        "target block",
        "on sync",
        "duration",
        "status",
    ];
    let mut vals = vec![
        deployment,
        state.src.to_string(),
        state.dst.to_string(),
        state.target_block_number.to_string(),
        on_sync.to_str().to_string(),
        duration(&state.started_at, &state.finished_at),
        progress,
    ];
    match (cancelled_at, state.cancelled_at) {
        (Some(c), None) => {
            lst.push("cancel");
            vals.push(format!("requested at {}", c));
        }
        (_, Some(c)) => {
            lst.push("cancel");
            vals.push(format!("cancelled at {}", c));
        }
        (None, None) => {}
    }
    let mut lst = List::new(lst);
    lst.append(vals);
    lst.render();

    println!();

    println!(
        "{:^30} | {:^8} | {:^8} | {:^8} | {:^8}",
        "entity type", "next", "target", "batch", "duration"
    );
    println!("{:-<74}", "-");

    for table in tables {
        let status = if table.next_vid > 0 && table.next_vid < table.target_vid {
            ">".to_string()
        } else if table.target_vid < 0 {
            // empty source table
            "✓".to_string()
        } else {
            done(&table.finished_at)
        };

        println!(
            "{} {:<28} | {:>8} | {:>8} | {:>8} | {:>8}",
            status,
            table.entity_type,
            table.next_vid,
            table.target_vid,
            table.batch_size,
            human_duration(Duration::milliseconds(table.duration_ms)),
        );
    }

    Ok(())
}
