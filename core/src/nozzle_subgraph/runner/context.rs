use std::sync::Arc;

use alloy::primitives::{BlockHash, BlockNumber};
use graph::{
    cheap_clone::CheapClone,
    components::store::WritableStore,
    data::subgraph::DeploymentHash,
    env::NozzleEnv,
    nozzle::{log::Logger as _, Codec, Manifest},
    util::backoff::ExponentialBackoff,
};
use slog::Logger;

use super::Compat;
use crate::nozzle_subgraph::Metrics;

pub(in super::super) struct Context<NC> {
    pub(super) logger: Logger,
    pub(super) client: Arc<NC>,
    pub(super) store: Arc<dyn WritableStore>,
    pub(super) max_buffer_size: usize,
    pub(super) max_block_range: usize,
    pub(super) backoff: ExponentialBackoff,
    pub(super) deployment: DeploymentHash,
    pub(super) manifest: Manifest,
    pub(super) metrics: Metrics,
    pub(super) codec: Codec,
}

impl<NC> Context<NC> {
    pub(in super::super) fn new(
        logger: &Logger,
        env: &NozzleEnv,
        client: Arc<NC>,
        store: Arc<dyn WritableStore>,
        deployment: DeploymentHash,
        manifest: Manifest,
        metrics: Metrics,
    ) -> Self {
        let logger = logger.component("NozzleSubgraphRunner");
        let backoff = ExponentialBackoff::new(env.query_retry_min_delay, env.query_retry_max_delay);
        let codec = Codec::new(manifest.schema.cheap_clone());

        Self {
            logger,
            client,
            store,
            max_buffer_size: env.max_buffer_size,
            max_block_range: env.max_block_range,
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

    pub(super) fn total_queries(&self) -> usize {
        self.manifest
            .data_sources
            .iter()
            .map(|data_source| data_source.transformer.tables.len())
            .sum()
    }

    pub(super) fn min_start_block(&self) -> BlockNumber {
        self.manifest
            .data_sources
            .iter()
            .map(|data_source| data_source.source.start_block)
            .min()
            .unwrap()
    }
}
