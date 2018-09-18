#[macro_use]
extern crate failure;
extern crate futures;
extern crate graph;
extern crate jsonrpc_core;

mod block_ingestor;
mod ethereum_adapter;
mod transport;

pub use self::block_ingestor::BlockIngestor;
pub use self::ethereum_adapter::{EthereumAdapter, EthereumAdapterConfig};
pub use self::transport::{EventLoopHandle, Transport};
