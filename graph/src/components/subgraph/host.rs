use failure::Error;
use futures::prelude::*;
use std::sync::Arc;

use crate::prelude::*;
use web3::types::{Log, Transaction};

/// Common trait for runtime host implementations.
pub trait RuntimeHost: Send + Sync + Debug {
    /// Returns true if the RuntimeHost has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Process an Ethereum event and return a vector of entity operations.
    fn process_log(
        &self,
        logger: Logger,
        block: Arc<EthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        state: BlockState,
    ) -> Box<Future<Item = BlockState, Error = Error> + Send>;
}

pub trait RuntimeHostBuilder: Clone + Send + Sync + 'static {
    type Host: RuntimeHost;

    /// Build a new runtime host for a subgraph data source.
    fn build(
        &self,
        logger: &Logger,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
    ) -> Result<Self::Host, Error>;
}
