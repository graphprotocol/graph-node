use std::sync::Arc;

use diesel::sql_query;
use diesel_async::AsyncConnection;
use diesel_async::RunQueryDsl;
use graph::blockchain::BlockHash;
use graph::blockchain::BlockPtr;
use graph::blockchain::ChainIdentifier;
use graph::cheap_clone::CheapClone;
use graph::components::network_provider::ChainName;
use graph::components::store::ChainIdStore;
use graph::components::store::StoreError;
use graph::prelude::BlockNumber;
use graph::prelude::ChainStore as _;
use graph::prelude::LightEthereumBlock;
use graph::prelude::anyhow::Context as _;
use graph::prelude::{anyhow, anyhow::bail};
use graph::slog::Logger;
use graph::{
    components::store::BlockStore as _, components::store::ChainHeadStore as _,
    prelude::anyhow::Error,
};
use graph_chain_ethereum::EthereumAdapter;
use graph_chain_ethereum::EthereumAdapterTrait as _;
use graph_chain_ethereum::chain::BlockFinality;
use graph_store_postgres::BlockStore;
use graph_store_postgres::ChainStore;
use graph_store_postgres::PoolCoordinator;
use graph_store_postgres::Shard;
use graph_store_postgres::Storage;
use graph_store_postgres::add_chain;
use graph_store_postgres::find_chain;
use graph_store_postgres::update_chain_name;
use graph_store_postgres::{ConnectionPool, command_support::catalog::block_store};

use crate::manager::prompt::prompt_for_confirmation;
use crate::network_setup::Networks;

pub async fn list(primary: ConnectionPool, store: BlockStore) -> Result<(), Error> {
    let mut chains = {
        let mut conn = primary.get().await?;
        block_store::load_chains(&mut conn).await?
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
        let head_block = match store.chain_store(&chain.name).await {
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
    ttl_days: usize,
    max_contracts: Option<usize>,
) -> Result<(), Error> {
    println!(
        "Removing stale entries from the call cache for `{}`",
        chain_store.chain
    );
    let result = chain_store
        .clear_stale_call_cache(ttl_days, max_contracts)
        .await?;
    if result.effective_ttl_days != ttl_days {
        println!(
            "Effective TTL: {} days (adjusted from {} to stay within {} contracts)",
            result.effective_ttl_days,
            ttl_days,
            max_contracts.unwrap()
        );
    }
    println!(
        "Deleted {} cache entries for {} contracts",
        result.cache_entries_deleted, result.contracts_deleted
    );
    Ok(())
}

pub async fn info(
    primary: ConnectionPool,
    store: BlockStore,
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

    let mut conn = primary.get().await?;

    let chain = block_store::find_chain(&mut conn, &name)
        .await?
        .ok_or_else(|| anyhow!("unknown chain: {}", name))?;

    let chain_store = store
        .chain_store(&chain.name)
        .await
        .ok_or_else(|| anyhow!("unknown chain: {}", name))?;
    let head_block = chain_store.cheap_clone().chain_head_ptr().await?;
    let ancestor = match &head_block {
        None => None,
        Some(head_block) => {
            chain_store
                .ancestor_block_ptr(head_block.clone(), offset, None)
                .await?
        }
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

pub async fn remove(primary: ConnectionPool, store: BlockStore, name: String) -> Result<(), Error> {
    let sites = {
        let mut conn = graph_store_postgres::command_support::catalog::Connection::new(
            primary.get_permitted().await?,
        );
        conn.find_sites_for_network(&name).await?
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

    store.drop_chain(&name).await?;

    Ok(())
}

pub async fn update_chain_genesis(
    networks: &Networks,
    coord: Arc<PoolCoordinator>,
    store: Box<dyn ChainIdStore>,
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
    store
        .set_chain_identifier(
            &chain_id,
            &ChainIdentifier {
                net_version: ident.net_version.clone(),
                genesis_block_hash: genesis_hash,
            },
        )
        .await?;

    // Refresh the new values
    println!("Refresh mappings");
    crate::manager::commands::database::remap(&coord, None, None, false).await?;

    Ok(())
}

pub async fn change_block_cache_shard(
    primary_store: ConnectionPool,
    store: BlockStore,
    chain_name: String,
    shard: String,
) -> Result<(), Error> {
    println!("Changing block cache shard for {} to {}", chain_name, shard);

    let mut conn = primary_store.get().await?;

    let chain = find_chain(&mut conn, &chain_name)
        .await?
        .ok_or_else(|| anyhow!("unknown chain: {}", chain_name))?;
    let old_shard = chain.shard;
    let canonical_backup_name = format!("{chain_name}-old");

    let existing_backup = find_chain(&mut conn, &canonical_backup_name).await?;

    println!("Current shard: {}", old_shard);

    let chain_store = store
        .chain_store(&chain_name)
        .await
        .ok_or_else(|| anyhow!("unknown chain: {}", &chain_name))?;
    let ident = chain_store.chain_identifier().await?;
    let target_shard = Shard::new(shard)?;

    let reuse_existing_backup = match existing_backup.as_ref() {
        None => false,
        Some(backup) if backup.shard != target_shard => {
            bail!(
                "`{}` already exists on shard `{}`. Remove it with `graphman chain remove {}` before changing `{}` to shard `{}`",
                canonical_backup_name,
                backup.shard,
                canonical_backup_name,
                chain_name,
                target_shard,
            );
        }
        Some(backup) => {
            let backup_ident = backup.network_identifier()?;
            if backup_ident != ident {
                bail!(
                    "`{}` has a different chain identifier ({}) than `{}` ({}). Remove it with `graphman chain remove {}` before changing `{}` to shard `{}`",
                    canonical_backup_name,
                    backup_ident,
                    chain_name,
                    ident,
                    canonical_backup_name,
                    chain_name,
                    target_shard,
                );
            }
            let prompt = format!(
                "`{}` already exists on shard `{}` and will be reused as the active `{}` chain.\nProceed?",
                canonical_backup_name, target_shard, chain_name
            );
            if !prompt_for_confirmation(&prompt)? {
                println!(
                    "Aborting. Remove `{}` with `graphman chain remove {}` if you want to create a fresh cache on shard `{}`.",
                    canonical_backup_name, canonical_backup_name, target_shard
                );
                return Ok(());
            }
            true
        }
    };

    let existing_backup_store = if reuse_existing_backup {
        store.chain_store(&canonical_backup_name).await
    } else {
        None
    };

    if !reuse_existing_backup {
        let chain =
            BlockStore::allocate_chain(&mut conn, &chain_name, &target_shard, &ident).await?;
        store.add_chain_store(&chain, true).await?;
    }

    let temp_backup_name = format!("{chain_name}-old-temp");
    if reuse_existing_backup && find_chain(&mut conn, &temp_backup_name).await?.is_some() {
        bail!(
            "`{}` already exists. Remove it with `graphman chain remove {}` before changing `{}` to shard `{}`",
            temp_backup_name,
            temp_backup_name,
            chain_name,
            target_shard,
        );
    }

    conn.transaction::<(), StoreError, _>(async |conn| {
        sql_query(
            "alter table deployment_schemas drop constraint deployment_schemas_network_fkey;",
        )
        .execute(conn)
        .await?;

        if let Some(backup) = existing_backup.as_ref() {
            update_chain_name(conn, &backup.name, &temp_backup_name).await?;
        }

        update_chain_name(conn, &chain_name, &canonical_backup_name).await?;

        if reuse_existing_backup {
            update_chain_name(conn, &temp_backup_name, &chain_name).await?;
        } else {
            add_chain(conn, &chain_name, &target_shard, ident.clone()).await?;
        }

        sql_query(
            "alter table deployment_schemas add constraint deployment_schemas_network_fkey foreign key (network) references chains(name);",
        )
        .execute(conn)
        .await?;
        Ok(())
    })
    .await?;

    chain_store.update_name(&canonical_backup_name).await?;

    if reuse_existing_backup && let Some(backup_store) = existing_backup_store.as_ref() {
        backup_store.update_name(&chain_name).await?;
    }

    println!(
        "Changed block cache shard for {} from {} to {}",
        chain_name, old_shard, target_shard
    );

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
    let hash = block.header.hash;
    let number = block.header.number;
    // For inserting the block, it doesn't matter whether the block is final or not.
    let block = Arc::new(BlockFinality::Final(Arc::new(LightEthereumBlock::new(
        block,
    ))));
    chain_store.upsert_block(block).await?;

    let hash = hash.into();
    let rows = chain_store.confirm_block_hash(number as i32, &hash).await?;

    println!("Inserted block {}", hash);
    if rows > 0 {
        println!("    (also deleted {rows} duplicate row(s) with that number)");
    }
    Ok(())
}

pub async fn rebuild_storage(
    primary: ConnectionPool,
    store: BlockStore,
    name: String,
    force: bool,
) -> Result<(), Error> {
    let mut conn = primary.get().await?;

    let chain = block_store::find_chain(&mut conn, &name)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "Chain {} not found in public.chains.\n\
                 This command only supports chains already present in metadata.",
                name
            )
        })?;

    if matches!(chain.storage, Storage::Shared) {
        bail!(
            "Chain {} uses shared storage public and cannot be rebuilt with this command.",
            name
        );
    }

    let namespace = chain.storage.to_string();
    let shard = &chain.shard;
    let ident = chain.network_identifier()?;

    let drop_schema = store.has_namespace(&chain).await?;
    if drop_schema {
        let prompt = format!(
            "Storage {namespace} for chain {name} already exists on shard {shard}.\n\
             This will drop and rebuild chain storage. All cached blocks and call cache \
             data in that namespace will be permanently deleted.\n\
             Proceed?"
        );
        if !force && !prompt_for_confirmation(&prompt)? {
            println!("Aborting.");
            return Ok(());
        }
    }

    store
        .rebuild_chain_storage(&name, &ident, drop_schema)
        .await
        .with_context(|| format!("Failed to rebuild storage {namespace} for chain {name}"))?;

    println!("Successfully rebuilt storage {namespace} for chain {name} on shard {shard}.");

    Ok(())
}
