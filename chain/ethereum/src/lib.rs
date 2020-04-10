#[macro_use]
extern crate lazy_static;

pub mod chain;
mod ethereum_adapter;
pub mod network_indexer;
mod subgraph_indexer;
mod transport;

// Internal components
pub(crate) use self::subgraph_indexer::BlockStreamBuilder;

// Public components
pub use self::ethereum_adapter::EthereumAdapter;
pub use self::subgraph_indexer::{BlockIngestor, BlockIngestorMetrics, SubgraphIndexer};
pub use self::transport::{EventLoopHandle, Transport};
