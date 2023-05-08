use crate::manager::core;
use graph::components::store::BlockStore as _;
use graph::prelude::BlockPtr;
use graph::prelude::{anyhow::Error, BlockNumber};
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::BlockStore;
use graph_store_postgres::ChainStore;
use std::sync::Arc;

pub async fn list(primary: ConnectionPool, store: Arc<BlockStore>) -> Result<(), Error> {
    let chains = core::chain::get_chains(primary).await?;

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
            Some(chain_store) => {
                let head_block = core::chain::get_head_block(chain_store).await?;
                head_block
                    .map(|ptr| ptr.number.to_string())
                    .unwrap_or("none".to_string())
            }
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
    core::chain::clear_call_cache(chain_store, from, to).await?;

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

    let chain_info = core::chain::get_chain_info(primary, store, name, offset, hashes).await?;

    row("name", chain_info.name);
    row("shard", chain_info.shard);
    row("namespace", chain_info.namespace);
    row("net_version", chain_info.net_version);
    if hashes {
        row("genesis", chain_info.genesis.unwrap_or_default());
    }
    print_ptr("head block", chain_info.head_block, hashes);
    row("reorg threshold", chain_info.reorg_threshold);
    print_ptr("reorg ancestor", chain_info.ancestor, hashes);

    Ok(())
}

pub fn remove(primary: ConnectionPool, store: Arc<BlockStore>, name: String) -> Result<(), Error> {
    let sites = core::chain::remove_chain(primary, store, name.clone())?;

    if !sites.is_empty() {
        println!(
            "there are {} deployments using chain {}:",
            sites.len(),
            name
        );
        for site in sites {
            println!("{:<8} | {} ", site.namespace, site.deployment);
        }
    }

    Ok(())
}
