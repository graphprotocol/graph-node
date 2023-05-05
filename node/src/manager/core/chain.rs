use std::sync::Arc;

use graph::blockchain::BlockPtr;
use graph::cheap_clone::CheapClone;
use graph::prelude::BlockNumber;
use graph::prelude::ChainStore as _;
use graph::prelude::EthereumBlock;
use graph::prelude::LightEthereumBlockExt as _;
use graph::prelude::{anyhow, anyhow::bail};
use graph::{
    components::store::BlockStore as _, prelude::anyhow::Error, prelude::serde_json as json,
};
use graph_store_postgres::command_support::catalog::block_store::Chain;
use graph_store_postgres::BlockStore;
use graph_store_postgres::ChainStore;
use graph_store_postgres::{
    command_support::catalog::block_store, connection_pool::ConnectionPool,
};

#[derive(Debug)]
pub struct ChainInfoDetails {
    pub name: String,
    pub shard: String,
    pub namespace: String,
    pub net_version: String,
    pub genesis: Option<String>,
    pub head_block: Option<BlockPtr>,
    pub reorg_threshold: BlockNumber,
    pub ancestor: Option<BlockPtr>,
}

pub async fn get_chains(primary: ConnectionPool) -> Result<Vec<Chain>, Error> {
    let conn = primary.get()?;
    let mut chains = block_store::load_chains(&conn)?;
    chains.sort_by_key(|chain| chain.name.clone());
    Ok(chains)
}

pub async fn get_head_block(chain_store: Arc<ChainStore>) -> Result<Option<BlockPtr>, Error> {
    chain_store.chain_head_ptr().await
}

pub async fn clear_call_cache(
    chain_store: Arc<ChainStore>,
    from: i32,
    to: i32,
) -> Result<(), Error> {
    chain_store.clear_call_cache(from, to).await
}

pub async fn get_chain_info(
    primary: ConnectionPool,
    store: Arc<BlockStore>,
    name: String,
    offset: BlockNumber,
    hashes: bool,
) -> Result<ChainInfoDetails, Error> {
    let conn = primary.get()?;

    let chain =
        block_store::find_chain(&conn, &name)?.ok_or_else(|| anyhow!("unknown chain: {}", name))?;

    let chain_store = store
        .chain_store(&chain.name)
        .ok_or_else(|| anyhow!("unknown chain: {}", name))?;
    let head_block = chain_store.cheap_clone().chain_head_ptr().await?;
    let ancestor = match &head_block {
        None => None,
        Some(head_block) => chain_store
            .ancestor_block(head_block.clone(), offset)
            .await?
            .map(json::from_value::<EthereumBlock>)
            .transpose()?
            .map(|b| b.block.block_ptr()),
    };

    Ok(ChainInfoDetails {
        name: chain.name,
        shard: chain.shard.to_string(),
        namespace: chain.storage.to_string(),
        net_version: chain.net_version,
        genesis: hashes.then(|| chain.genesis_block),
        head_block,
        reorg_threshold: offset,
        ancestor,
    })
}

pub fn remove_chain(
    primary: ConnectionPool,
    store: Arc<BlockStore>,
    name: String,
) -> Result<(), Error> {
    let sites = {
        let conn = graph_store_postgres::command_support::catalog::Connection::new(primary.get()?);
        conn.find_sites_for_network(&name)?
    };

    if !sites.is_empty() {
        bail!("remove all deployments using chain {} first", name);
    }

    store.drop_chain(&name)?;

    Ok(())
}
