use std::sync::Arc;

use alloy::primitives::{BlockHash, BlockNumber};
use graph::{
    amp::{log::Logger as _, Codec, Manifest},
    cheap_clone::CheapClone,
    components::store::WritableStore,
    data::subgraph::DeploymentHash,
    env::AmpEnv,
    util::backoff::ExponentialBackoff,
};
use slog::Logger;

use super::Compat;
use crate::amp_subgraph::Metrics;

pub(in super::super) struct Context<AC> {
    pub(super) logger: Logger,
    pub(super) client: Arc<AC>,
    pub(super) store: Arc<dyn WritableStore>,
    pub(super) buffer_size: usize,
    pub(super) block_range: usize,
    pub(super) backoff: ExponentialBackoff,
    pub(super) deployment: DeploymentHash,
    pub(super) manifest: Manifest,
    pub(super) metrics: Metrics,
    pub(super) codec: Codec,
}

impl<AC> Context<AC> {
    pub(in super::super) fn new(
        logger: &Logger,
        env: &AmpEnv,
        client: Arc<AC>,
        store: Arc<dyn WritableStore>,
        deployment: DeploymentHash,
        manifest: Manifest,
        metrics: Metrics,
    ) -> Self {
        let logger = logger.component("AmpSubgraphRunner");
        let backoff = ExponentialBackoff::new(env.query_retry_min_delay, env.query_retry_max_delay);
        let codec = Codec::new(manifest.schema.cheap_clone());

        Self {
            logger,
            client,
            store,
            buffer_size: env.buffer_size,
            block_range: env.block_range,
            backoff,
            deployment,
            manifest,
            metrics,
            codec,
        }
    }

    pub(super) fn indexing_completed(&self) -> bool {
        let Some(last_synced_block) = self.latest_synced_block() else {
            return false;
        };

        self.manifest
            .data_sources
            .iter()
            .all(|data_source| last_synced_block >= data_source.source.end_block)
    }

    pub(super) fn latest_synced_block(&self) -> Option<BlockNumber> {
        self.latest_synced_block_ptr()
            .map(|(block_number, _)| block_number)
    }

    pub(super) fn latest_synced_block_ptr(&self) -> Option<(BlockNumber, BlockHash)> {
        self.store
            .block_ptr()
            .map(|block_ptr| (block_ptr.number.compat(), block_ptr.hash.compat()))
    }

    pub(super) fn start_block(&self) -> BlockNumber {
        self.manifest
            .data_sources
            .iter()
            .map(|data_source| data_source.source.start_block)
            .min()
            .unwrap()
    }

    pub(super) fn end_block(&self) -> BlockNumber {
        self.manifest
            .data_sources
            .iter()
            .map(|data_source| data_source.source.end_block)
            .max()
            .unwrap()
    }
}
