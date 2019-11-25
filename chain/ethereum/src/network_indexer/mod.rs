use graph::prelude::*;
use web3::types::{Block, TransactionReceipt, H256};

mod block_writer;
mod convert;
mod network_indexer;
mod subgraph;

pub use self::block_writer::*;
pub use self::convert::*;
pub use self::network_indexer::*;
pub use self::subgraph::*;

pub use self::network_indexer::NetworkIndexerEvent;

const NETWORK_INDEXER_VERSION: u32 = 0;

/// Helper type to bundle blocks and their uncles together.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BlockWithUncles {
    pub block: EthereumBlock,
    pub uncles: Vec<Option<Block<H256>>>,
}

impl BlockWithUncles {
    pub fn inner(&self) -> &LightEthereumBlock {
        &self.block.block
    }

    pub fn _transaction_receipts(&self) -> &Vec<TransactionReceipt> {
        &self.block.transaction_receipts
    }
}

/**
 * Logging helpers.
 */

pub(crate) fn format_light_block(block: &LightEthereumBlock) -> String {
    format!(
        "{} ({})",
        block
            .number
            .map_or(String::from("none"), |number| format!("#{}", number)),
        block
            .hash
            .map_or(String::from("-"), |hash| format!("{:x}", hash))
    )
}

pub(crate) fn format_block(block: &BlockWithUncles) -> String {
    format_light_block(block.inner())
}

pub(crate) fn format_block_pointer(ptr: &EthereumBlockPointer) -> String {
    format!("#{} ({:x})", ptr.number, ptr.hash)
}

pub fn create<S>(
    subgraph_name: String,
    logger: &Logger,
    adapter: Arc<dyn EthereumAdapter>,
    store: Arc<S>,
    metrics_registry: Arc<dyn MetricsRegistry>,
    start_block: Option<EthereumBlockPointer>,
) -> impl Future<Item = NetworkIndexer, Error = ()>
where
    S: Store + ChainStore,
{
    // Create a subgraph name and ID
    let id_str = format!(
        "{}_v{}",
        subgraph_name.replace("/", "_"),
        NETWORK_INDEXER_VERSION
    );
    let subgraph_id = SubgraphDeploymentId::new(id_str).expect("valid network subgraph ID");
    let subgraph_name = SubgraphName::new(subgraph_name).expect("valid network subgraph name");

    let logger = logger.new(o!(
      "subgraph_name" => subgraph_name.to_string(),
      "subgraph_id" => subgraph_id.to_string(),
    ));

    // Ensure subgraph, the wire up the tracer and indexer
    subgraph::ensure_subgraph_exists(
        subgraph_name,
        subgraph_id.clone(),
        logger.clone(),
        store.clone(),
        start_block,
    )
    .and_then(move |_| {
        future::ok(NetworkIndexer::new(
            subgraph_id.clone(),
            &logger,
            adapter.clone(),
            store.clone(),
            metrics_registry.clone(),
        ))
    })
}
