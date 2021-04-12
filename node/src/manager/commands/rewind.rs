use std::convert::TryFrom;
use std::sync::Arc;

use graph::prelude::{anyhow, BlockNumber, EthereumBlockPointer, SubgraphDeploymentId};
use graph_store_postgres::SubgraphStore;

pub fn run(
    store: Arc<SubgraphStore>,
    id: String,
    block_hash: String,
    block_number: BlockNumber,
) -> Result<(), anyhow::Error> {
    let id =
        SubgraphDeploymentId::new(id).map_err(|id| anyhow!("illegal deployment id `{}`", id))?;
    let block_ptr_to = EthereumBlockPointer::try_from((block_hash.as_str(), block_number as i64))
        .map_err(|e| anyhow!("error converting to block pointer: {}", e))?;

    store.rewind(id, block_ptr_to)?;
    Ok(())
}
