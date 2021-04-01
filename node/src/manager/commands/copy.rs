use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl};
use std::{collections::HashMap, sync::Arc, time::SystemTime};

use graph::{
    components::store::BlockStore as _,
    prelude::{
        anyhow::{anyhow, bail, Error},
        chrono::{DateTime, Duration, Utc},
        ChainStore, EthereumBlockPointer, NodeId, QueryStoreManager,
    },
};
use graph_store_postgres::{
    command_support::catalog::{self, copy_state, copy_table_state},
    PRIMARY_SHARD,
};
use graph_store_postgres::{connection_pool::ConnectionPool, Shard, Store, SubgraphStore};

use crate::manager::deployment;
use crate::manager::display::List;

type UtcDateTime = DateTime<Utc>;

#[derive(Queryable, QueryableByName, Debug)]
#[table_name = "copy_state"]
struct CopyState {
    src: i32,
    dst: i32,
    target_block_hash: Vec<u8>,
    target_block_number: i32,
    started_at: UtcDateTime,
    finished_at: Option<UtcDateTime>,
    cancelled_at: Option<UtcDateTime>,
}

#[derive(Queryable, QueryableByName, Debug)]
#[table_name = "copy_table_state"]
struct CopyTableState {
    id: i32,
    entity_type: String,
    dst: i32,
    next_vid: i64,
    target_vid: i64,
    batch_size: i64,
    started_at: UtcDateTime,
    finished_at: Option<UtcDateTime>,
    duration_ms: i64,
}

pub async fn create(
    store: Arc<Store>,
    src: String,
    shard: String,
    node: String,
    block_offset: u32,
) -> Result<(), Error> {
    let block_offset = block_offset as i32;
    let subgraph_store = store.subgraph_store();
    let src = deployment::locate(subgraph_store.as_ref(), src)?;
    let query_store = store.query_store(src.hash.clone().into(), true).await?;
    let network = query_store.network_name();

    let src_ptr = query_store.block_ptr()?.ok_or_else(|| anyhow!("subgraph {} has not indexed any blocks yet and can not be used as the source of a copy", src))?;
    let src_number = if src_ptr.number <= block_offset {
        bail!("subgraph {} has only indexed {} blocks, but we need at least {} blocks before we can copy from it", src, src_ptr.number, block_offset);
    } else {
        src_ptr.number - block_offset
    };

    let chain_store = store
        .block_store()
        .chain_store(&network)
        .ok_or_else(|| anyhow!("could not find chain store for network {}", network))?;
    let hashes = chain_store.block_hashes_by_block_number(src_number)?;
    let hash = match hashes.len() {
        0 => bail!(
            "could not find a block with number {} in our cache",
            src_number
        ),
        1 => hashes[0],
        n => bail!(
            "the cache contains {} hashes for block number {}",
            n,
            src_number
        ),
    };
    let base_ptr = EthereumBlockPointer::from((hash, src_number));

    // let chain_store = store.block_store().chain_head_block(chain)
    let shard = Shard::new(shard)?;
    let node = NodeId::new(node.clone()).map_err(|()| anyhow!("invalid node id `{}`", node))?;

    let dst = subgraph_store.copy_deployment(&src, shard, node, base_ptr)?;

    println!("created deployment {} as copy of {}", dst, src);
    Ok(())
}

pub fn activate(store: Arc<SubgraphStore>, deployment: String, shard: String) -> Result<(), Error> {
    let shard = Shard::new(shard)?;
    let deployment = store
        .locate_in_shard(deployment.clone(), shard.clone())?
        .ok_or_else(|| {
            anyhow!(
                "could not find a copy for {} in shard {}",
                deployment,
                shard
            )
        })?;
    store.activate(&deployment)?;
    println!("activated copy {}", deployment);
    Ok(())
}

pub fn list(pool: ConnectionPool) -> Result<(), Error> {
    use catalog::active_copies as ac;

    let conn = pool.get()?;

    let copies = ac::table
        .select((ac::src, ac::dst, ac::cancelled_at))
        .load::<(i32, i32, Option<UtcDateTime>)>(&conn)?;
    if copies.is_empty() {
        println!("no active copies");
    } else {
        for (src, dst, cancelled_at) in copies {
            let cancelled_at = cancelled_at
                .map(|d| d.format("%c").to_string())
                .unwrap_or("active".to_string());
            println!("{:4} {:4} {}", src, dst, cancelled_at);
        }
    }
    Ok(())
}

pub fn status(pools: HashMap<Shard, ConnectionPool>, dst: i32) -> Result<(), Error> {
    use catalog::active_copies as ac;
    use catalog::copy_state as cs;
    use catalog::copy_table_state as cts;
    use catalog::deployment_schemas as ds;

    fn done(ts: &Option<UtcDateTime>) -> String {
        ts.map(|_| "✓").unwrap_or(".").to_string()
    }

    fn duration(start: &UtcDateTime, end: &Option<UtcDateTime>) -> String {
        let start = start.clone();
        let end = end.clone();

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

    let primary = pools
        .get(&*PRIMARY_SHARD)
        .ok_or_else(|| anyhow!("can not find deployment with id {}", dst))?;
    let pconn = primary.get()?;

    let shard = ds::table
        .filter(ds::id.eq(dst as i32))
        .select(ds::shard)
        .get_result::<Shard>(&pconn)?;
    let dpool = pools
        .get(&shard)
        .ok_or_else(|| anyhow!("can not find pool for shard {}", shard))?;
    let dconn = dpool.get()?;

    let (active, cancelled_at) = ac::table
        .filter(ac::dst.eq(dst))
        .select((ac::src, ac::cancelled_at))
        .get_result::<(i32, Option<UtcDateTime>)>(&pconn)
        .optional()?
        .map(|(_, cancelled_at)| (true, cancelled_at))
        .unwrap_or((false, None));

    let state = cs::table
        .filter(cs::dst.eq(dst))
        .get_result::<CopyState>(&dconn)
        .optional()?;

    let state = match state {
        Some(state) => state,
        None => {
            if active {
                println!("copying is queued but has not started");
                return Ok(());
            } else {
                bail!("no copy operation for {} exists", dst);
            }
        }
    };

    let tables = cts::table
        .filter(cts::dst.eq(dst))
        .order_by(cts::entity_type)
        .load::<CopyTableState>(&dconn)?;

    let mut lst = vec!["src", "dst", "target block", "duration", "status"];
    let mut vals = vec![
        state.src.to_string(),
        state.dst.to_string(),
        state.target_block_number.to_string(),
        duration(&state.started_at, &state.finished_at),
        done(&state.finished_at),
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
    println!("");

    println!(
        "{:^30} | {:^8} | {:^8} | {:^8} | {:^8}",
        "entity type", "next", "target", "batch", "duration"
    );
    println!("{:-<74}", "-");
    for table in tables {
        let status = if table.next_vid > 0 && table.next_vid < table.target_vid {
            ">".to_string()
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
