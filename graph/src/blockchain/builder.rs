use super::Blockchain;
use crate::{
    components::{metrics::MetricsRegistry, store::ChainStore},
    firehose::FirehoseEndpoints,
    prelude::LoggerFactory,
};
use std::sync::Arc;

/// An implementor of [`BlockchainBuilder`] for chains that don't require
/// particularly fancy builder logic.
pub struct BasicBlockchainBuilder {
    pub logger_factory: LoggerFactory,
    pub name: String,
    pub chain_store: Arc<dyn ChainStore>,
    pub firehose_endpoints: FirehoseEndpoints,
    pub metrics_registry: Arc<dyn MetricsRegistry>,
}

/// Something that can build a [`Blockchain`].
pub trait BlockchainBuilder<C>
where
    C: Blockchain,
{
    fn build(self) -> C;
}
