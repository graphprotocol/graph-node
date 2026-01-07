use tonic::async_trait;

use super::Blockchain;
use crate::{
    components::store::ChainHeadStore,
    data::value::Word,
    firehose::FirehoseEndpoints,
    prelude::{LoggerFactory, MetricsRegistry},
};
use std::sync::Arc;

/// An implementor of [`BlockchainBuilder`] for chains that don't require
/// particularly fancy builder logic.
pub struct BasicBlockchainBuilder {
    pub logger_factory: LoggerFactory,
    pub name: Word,
    pub chain_head_store: Arc<dyn ChainHeadStore>,
    pub firehose_endpoints: FirehoseEndpoints,
    pub metrics_registry: Arc<MetricsRegistry>,
}

/// Something that can build a [`Blockchain`].
#[async_trait]
pub trait BlockchainBuilder<C>
where
    C: Blockchain,
{
    async fn build(self) -> C;
}
