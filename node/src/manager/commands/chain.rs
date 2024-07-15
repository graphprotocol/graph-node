use std::sync::Arc;

use diesel::sql_query;
use diesel::Connection;
use diesel::RunQueryDsl;
use graph::blockchain::BlockHash;
use graph::blockchain::BlockPtr;
use graph::blockchain::ChainIdentifier;
use graph::cheap_clone::CheapClone;
use graph::components::adapter::ChainId;
use graph::components::adapter::IdentValidator;
use graph::components::store::StoreError;
use graph::prelude::BlockNumber;
use graph::prelude::ChainStore as _;
use graph::prelude::{anyhow, anyhow::bail};
use graph::slog::Logger;
use graph::{components::store::BlockStore as _, prelude::anyhow::Error};
use graph_store_postgres::add_chain;
use graph_store_postgres::connection_pool::PoolCoordinator;
use graph_store_postgres::find_chain;
use graph_store_postgres::update_chain_name;
use graph_store_postgres::BlockStore;
use graph_store_postgres::ChainStatus;
use graph_store_postgres::ChainStore;
use graph_store_postgres::Shard;
use graph_store_postgres::{
    command_support::catalog::block_store, connection_pool::ConnectionPool,
};

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
    store: Arc<BlockStore>,
    logger: &Logger,
    chain_id: ChainId,
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
    store.update_ident(
        &chain_id,
        &ChainIdentifier {
            net_version: ident.net_version.clone(),
            genesis_block_hash: genesis_hash,
        },
    )?;

    // Update the primary public.chains
    println!("Updating primary public.chains");
    store.set_chain_identifier(chain_id, &ident)?;

    // Refresh the new values
    println!("Refresh mappings");
    crate::manager::commands::database::remap(&coord, None, None, false).await?;

    Ok(())
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

    println!("Current shard: {}", old_shard);

    let chain_store = store
        .chain_store(&chain_name)
        .ok_or_else(|| anyhow!("unknown chain: {}", &chain_name))?;
    let new_name = format!("{}-old", &chain_name);
    let ident = chain_store.chain_identifier()?;

    conn.transaction(|conn| -> Result<(), StoreError> {
        let shard = Shard::new(shard.to_string())?;

        let chain = BlockStore::allocate_chain(conn, &chain_name, &shard, &ident)?;

        store.add_chain_store(&chain,ChainStatus::Ingestible, true)?;

        // Drop the foreign key constraint on deployment_schemas
        sql_query(
            "alter table deployment_schemas drop constraint deployment_schemas_network_fkey;",
        )
        .execute(conn)?;

        // Update the current chain name to chain-old
        update_chain_name(conn, &chain_name, &new_name)?;


        // Create a new chain with the name in the destination shard
        let _ = add_chain(conn, &chain_name, &shard, ident)?;

        // Re-add the foreign key constraint
        sql_query(
            "alter table deployment_schemas add constraint deployment_schemas_network_fkey foreign key (network) references chains(name);",
        )
         .execute(conn)?;
        Ok(())
    })?;

    chain_store.update_name(&new_name)?;

    println!(
        "Changed block cache shard for {} from {} to {}",
        chain_name, old_shard, shard
    );

    Ok(())
}
