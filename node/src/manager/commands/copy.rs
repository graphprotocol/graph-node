use std::sync::Arc;

use graph::{
    components::store::BlockStore as _,
    prelude::{
        anyhow::{anyhow, bail, Error},
        ChainStore, EthereumBlockPointer, NodeId, QueryStoreManager,
    },
};
use graph_store_postgres::{Shard, Store, SubgraphStore};

use crate::manager::deployment;

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
