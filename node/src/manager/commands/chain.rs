use std::sync::Arc;

use diesel::sql_query;
use diesel::Connection;
use diesel::PgConnection;
use diesel::RunQueryDsl;
use graph::blockchain::BlockHash;
use graph::blockchain::BlockPtr;
use graph::blockchain::ChainIdentifier;
use graph::cheap_clone::CheapClone;
use graph::components::network_provider::ChainName;
use graph::components::store::ChainIdStore;
use graph::components::store::StoreError;
use graph::prelude::BlockNumber;
use graph::prelude::ChainStore as _;
use graph::prelude::LightEthereumBlockExt;
use graph::prelude::{anyhow, anyhow::bail};
use graph::slog::Logger;
use graph::{
    components::store::BlockStore as _, components::store::ChainHeadStore as _,
    prelude::anyhow::Error,
};
use graph_chain_ethereum::chain::BlockFinality;
use graph_chain_ethereum::EthereumAdapter;
use graph_chain_ethereum::EthereumAdapterTrait as _;
use graph_store_postgres::add_chain;
use graph_store_postgres::block_store::primary::Chain as PrimaryChain;
use graph_store_postgres::find_chain;
use graph_store_postgres::update_chain_name;
use graph_store_postgres::BlockStore;
use graph_store_postgres::ChainStatus;
use graph_store_postgres::ChainStore;
use graph_store_postgres::PoolCoordinator;
use graph_store_postgres::Shard;
use graph_store_postgres::{command_support::catalog::block_store, ConnectionPool};

use crate::network_setup::Networks;

pub async fn list(primary: ConnectionPool, store: Arc<BlockStore>) -> Result<(), Error> {
    let mut chains = {
        let mut conn = primary.get()?;
        block_store::load_chains(&mut conn)?
    };
    chains.sort_by_key(|chain| chain.name.clone());

    if !chains.is_empty() {
        println!(
            "{:^20} | {:^10} | {:^10} | {:^7} | {:^10}",
            "name", "shard", "namespace", "version", "head block"
        );
        println!(
            "{:-^20}-+-{:-^10}-+-{:-^10}-+-{:-^7}-+-{:-^10}",
            "", "", "", "", ""
        );
    }
    for chain in chains {
        let head_block = match store.chain_store(&chain.name) {
            None => "no chain".to_string(),
            Some(chain_store) => chain_store
                .chain_head_ptr()
                .await?
                .map(|ptr| ptr.number.to_string())
                .unwrap_or("none".to_string()),
        };
        println!(
            "{:<20} | {:<10} | {:<10} | {:>7} | {:>10}",
            chain.name, chain.shard, chain.storage, chain.net_version, head_block
        );
    }
    Ok(())
}

pub async fn clear_call_cache(
    chain_store: Arc<ChainStore>,
    from: i32,
    to: i32,
) -> Result<(), Error> {
    println!(
        "Removing entries for blocks from {from} to {to} from the call cache for `{}`",
        chain_store.chain
    );
    chain_store.clear_call_cache(from, to).await?;
    Ok(())
}

pub async fn clear_stale_call_cache(
    chain_store: Arc<ChainStore>,
    ttl_days: i32,
    ttl_max_contracts: Option<i64>,
) -> Result<(), Error> {
    println!(
        "Removing stale entries from the call cache for `{}`",
        chain_store.chain
    );
    chain_store
        .clear_stale_call_cache(ttl_days, ttl_max_contracts)
        .await?;
    Ok(())
}

pub async fn info(
    primary: ConnectionPool,
    store: Arc<BlockStore>,
    name: String,
    offset: BlockNumber,
    hashes: bool,
) -> Result<(), Error> {
    fn row(label: &str, value: impl std::fmt::Display) {
        println!("{:<16} | {}", label, value);
    }

    fn print_ptr(label: &str, ptr: Option<BlockPtr>, hashes: bool) {
        match ptr {
            None => {
                row(label, "ø");
            }
            Some(ptr) => {
                row(label, ptr.number);
                if hashes {
                    row("", ptr.hash);
                }
            }
        }
    }

    let mut conn = primary.get()?;

    let chain = block_store::find_chain(&mut conn, &name)?
        .ok_or_else(|| anyhow!("unknown chain: {}", name))?;

    let chain_store = store
        .chain_store(&chain.name)
        .ok_or_else(|| anyhow!("unknown chain: {}", name))?;
    let head_block = chain_store.cheap_clone().chain_head_ptr().await?;
    let ancestor = match &head_block {
        None => None,
        Some(head_block) => chain_store
            .ancestor_block(head_block.clone(), offset, None)
            .await?
            .map(|x| x.1),
    };

    row("name", chain.name);
    row("shard", chain.shard);
    row("namespace", chain.storage);
    row("net_version", chain.net_version);
    if hashes {
        row("genesis", chain.genesis_block);
    }
    print_ptr("head block", head_block, hashes);
    row("reorg threshold", offset);
    print_ptr("reorg ancestor", ancestor, hashes);

    Ok(())
}

pub fn remove(primary: ConnectionPool, store: Arc<BlockStore>, name: String) -> Result<(), Error> {
    let sites = {
        let mut conn =
            graph_store_postgres::command_support::catalog::Connection::new(primary.get()?);
        conn.find_sites_for_network(&name)?
    };

    if !sites.is_empty() {
        println!(
            "there are {} deployments using chain {}:",
            sites.len(),
            name
        );
        for site in sites {
            println!("{:<8} | {} ", site.namespace, site.deployment);
        }
        bail!("remove all deployments using chain {} first", name);
    }

    store.drop_chain(&name)?;

    Ok(())
}

pub async fn update_chain_genesis(
    networks: &Networks,
    coord: Arc<PoolCoordinator>,
    store: Arc<dyn ChainIdStore>,
    logger: &Logger,
    chain_id: ChainName,
    genesis_hash: BlockHash,
    force: bool,
) -> Result<(), Error> {
    let ident = networks.chain_identifier(logger, &chain_id).await?;
    if !genesis_hash.eq(&ident.genesis_block_hash) {
        println!(
            "Expected adapter for chain {} to return genesis hash {} but got {}",
            chain_id, genesis_hash, ident.genesis_block_hash
        );
        if !force {
            println!("Not performing update");
            return Ok(());
        } else {
            println!("--force used, updating anyway");
        }
    }

    println!("Updating shard...");
    // Update the local shard's genesis, whether or not it is the primary.
    // The chains table is replicated from the primary and keeps another genesis hash.
    // To keep those in sync we need to update the primary and then refresh the shard tables.
    store.set_chain_identifier(
        &chain_id,
        &ChainIdentifier {
            net_version: ident.net_version.clone(),
            genesis_block_hash: genesis_hash,
        },
    )?;

    // Refresh the new values
    println!("Refresh mappings");
    crate::manager::commands::database::remap(&coord, None, None, false).await?;

    Ok(())
}

struct ChainSwapOutcome {
    latest_backup_name: String,
    previous_backup_final_name: Option<String>,
    reused_previous_backup: bool,
    allocated_chain: Option<PrimaryChain>,
}

fn next_backup_name(conn: &mut PgConnection, chain: &str, base: &str) -> Result<String, StoreError> {
    let mut suffix = 0usize;

    loop {
        let candidate = if suffix == 0 {
            format!("{chain}-{base}")
        } else {
            format!("{chain}-{base}-{suffix}")
        };

        if find_chain(conn, &candidate)?.is_none() {
            return Ok(candidate);
        }

        suffix += 1;
    }
}

pub fn change_block_cache_shard(
    primary_store: ConnectionPool,
    store: Arc<BlockStore>,
    chain_name: String,
    shard: String,
) -> Result<(), Error> {
    println!("Changing block cache shard for {} to {}", chain_name, shard);

    let mut conn = primary_store.get()?;

    let chain = find_chain(&mut conn, &chain_name)?
        .ok_or_else(|| anyhow!("unknown chain: {}", chain_name))?;
    let old_shard = chain.shard;
    let canonical_backup_name = format!("{chain_name}-old");

    let existing_backup = find_chain(&mut conn, &canonical_backup_name)?;
    let had_existing_backup = existing_backup.is_some();
    let existing_backup_store = store.chain_store(&canonical_backup_name);

    println!("Current shard: {}", old_shard);

    let chain_store = store
        .chain_store(&chain_name)
        .ok_or_else(|| anyhow!("unknown chain: {}", &chain_name))?;
    let ident = chain_store.chain_identifier()?;
    let target_shard = Shard::new(shard.clone())?;
    let reuse_existing_backup = existing_backup
        .as_ref()
        .map(|backup| backup.shard.as_str() == target_shard.as_str())
        .unwrap_or(false);

    let outcome = conn.transaction(|conn| -> Result<ChainSwapOutcome, StoreError> {
        sql_query(
            "alter table deployment_schemas drop constraint deployment_schemas_network_fkey;",
        )
        .execute(conn)?;

        let mut previous_backup_final_name: Option<String> = None;

        let temp_backup_name = if let Some(backup) = existing_backup.as_ref() {
            let temp_name = next_backup_name(conn, &chain_name, "old")?;
            update_chain_name(conn, &backup.name, &temp_name)?;
            previous_backup_final_name = Some(temp_name.clone());
            Some(temp_name)
        } else {
            None
        };

        let latest_backup_name = next_backup_name(conn, &chain_name, "old")?;
        update_chain_name(conn, &chain_name, &latest_backup_name)?;

        let mut allocated_chain = None;

        if reuse_existing_backup {
            if let Some(temp_name) = temp_backup_name.as_ref() {
                update_chain_name(conn, temp_name, &chain_name)?;
                previous_backup_final_name = Some(chain_name.clone());
            }
        } else {
            let created_chain = add_chain(conn, &chain_name, &target_shard, ident.clone())?;
            allocated_chain = Some(created_chain);
        }

        sql_query(
            "alter table deployment_schemas add constraint deployment_schemas_network_fkey foreign key (network) references chains(name);",
        )
        .execute(conn)?;

        Ok(ChainSwapOutcome {
            latest_backup_name,
            previous_backup_final_name,
            reused_previous_backup: reuse_existing_backup,
            allocated_chain,
        })
    })?;

    let ChainSwapOutcome {
        latest_backup_name,
        previous_backup_final_name,
        reused_previous_backup,
        allocated_chain,
    } = outcome;

    chain_store.update_name(&latest_backup_name)?;

    if let (Some(backup_store), Some(final_name)) = (
        existing_backup_store.as_ref(),
        previous_backup_final_name.as_ref(),
    ) {
        backup_store.update_name(final_name)?;
    }

    if let Some(ref chain) = allocated_chain {
        store.add_chain_store(chain, ChainStatus::Ingestible, true)?;
    }

    println!(
        "Changed block cache shard for {} from {} to {}",
        chain_name, old_shard, shard
    );
    println!("Latest backup recorded as `{}`", latest_backup_name);

    if reused_previous_backup {
        println!(
            "Reused existing backup `{}` as the active `{}` chain",
            canonical_backup_name, chain_name
        );
    } else if let Some(ref preserved) = previous_backup_final_name {
        if had_existing_backup {
            println!("Preserved earlier backup as `{}`", preserved);
        }
    }

    if allocated_chain.is_some() {
        println!(
            "Allocated new chain state for `{}` on shard {}",
            chain_name, shard
        );
    }

    Ok(())
}

pub async fn ingest(
    logger: &Logger,
    chain_store: Arc<ChainStore>,
    ethereum_adapter: Arc<EthereumAdapter>,
    number: BlockNumber,
) -> Result<(), Error> {
    let Some(block) = ethereum_adapter
        .block_by_number(logger, number)
        .await
        .map_err(|e| anyhow!("error getting block number {number}: {}", e))?
    else {
        bail!("block number {number} not found");
    };
    let ptr = block.block_ptr();
    // For inserting the block, it doesn't matter whether the block is final or not.
    let block = Arc::new(BlockFinality::Final(Arc::new(block)));
    chain_store.upsert_block(block).await?;

    let rows = chain_store.confirm_block_hash(ptr.number, &ptr.hash)?;

    println!("Inserted block {}", ptr);
    if rows > 0 {
        println!("    (also deleted {rows} duplicate row(s) with that number)");
    }
    Ok(())
}
