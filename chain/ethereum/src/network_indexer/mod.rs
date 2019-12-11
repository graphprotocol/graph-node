use graph::prelude::*;
use std::fmt;
use web3::types::{Block, H256};

mod block_writer;
mod convert;
mod metrics;
mod network_indexer;
mod subgraph;

pub use self::block_writer::*;
pub use self::convert::*;
pub use self::network_indexer::*;
pub use self::subgraph::*;

pub use self::network_indexer::NetworkIndexerEvent;

const NETWORK_INDEXER_VERSION: u32 = 0;

/// Helper type to represent ommer blocks.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Ommer(Block<H256>);

/// Helper type to bundle blocks and their ommers together.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BlockWithOmmers {
    pub block: EthereumBlock,
    pub ommers: Vec<Ommer>,
}

impl BlockWithOmmers {
    pub fn inner(&self) -> &LightEthereumBlock {
        &self.block.block
    }
}

impl fmt::Display for BlockWithOmmers {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner().format())
    }
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
