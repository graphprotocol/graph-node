pub mod instance;

use crate::polling_monitor::{
    ipfs_service::IpfsService, spawn_monitor, PollingMonitor, PollingMonitorMetrics,
};
use anyhow::{self, Error};
use bytes::Bytes;
use cid::Cid;
use graph::{
    blockchain::Blockchain,
    components::{
        store::{DeploymentId, SubgraphFork},
        subgraph::{MappingError, SharedProofOfIndexing},
    },
    data_source::{offchain, DataSource, TriggerData},
    prelude::{
        BlockNumber, BlockState, CancelGuard, DeploymentHash, MetricsRegistry, RuntimeHostBuilder,
        SubgraphInstanceMetrics, TriggerProcessor,
    },
    slog::Logger,
    tokio::sync::mpsc,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use self::instance::SubgraphInstance;

pub type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

// The context keeps track of mutable in-memory state that is retained across blocks.
//
// Currently most of the changes are applied in `runner.rs`, but ideally more of that would be
// refactored into the context so it wouldn't need `pub` fields. The entity cache should probaby
// also be moved here.
pub(crate) struct IndexingContext<C, T>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
{
    instance: SubgraphInstance<C, T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
    pub offchain_monitor: OffchainMonitor,
    trigger_processor: Box<dyn TriggerProcessor<C, T>>,
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> IndexingContext<C, T> {
    pub fn new(
        instance: SubgraphInstance<C, T>,
        instances: SharedInstanceKeepAliveMap,
        filter: C::TriggerFilter,
        offchain_monitor: OffchainMonitor,
        trigger_processor: Box<dyn TriggerProcessor<C, T>>,
    ) -> Self {
        Self {
            instance,
            instances,
            filter,
            offchain_monitor,
            trigger_processor,
        }
    }

    pub async fn process_trigger(
        &self,
        logger: &Logger,
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        self.process_trigger_in_hosts(
            logger,
            &self.instance.hosts(),
            block,
            trigger,
            state,
            proof_of_indexing,
            causality_region,
            debug_fork,
            subgraph_metrics,
        )
        .await
    }

    pub async fn process_trigger_in_hosts(
        &self,
        logger: &Logger,
        hosts: &[Arc<T::Host>],
        block: &Arc<C::Block>,
        trigger: &TriggerData<C>,
        state: BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &str,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        subgraph_metrics: &Arc<SubgraphInstanceMetrics>,
    ) -> Result<BlockState<C>, MappingError> {
        self.trigger_processor
            .process_trigger(
                logger,
                hosts,
                block,
                trigger,
                state,
                proof_of_indexing,
                causality_region,
                debug_fork,
                subgraph_metrics,
            )
            .await
    }

    // Removes data sources hosts with a creation block greater or equal to `reverted_block`, so
    // that they are no longer candidates for `process_trigger`.
    //
    // This does not currently affect the `offchain_monitor` or the `filter`, so they will continue
    // to include data sources that have been reverted. This is not ideal for performance, but it
    // does not affect correctness since triggers that have no matching host will be ignored by
    // `process_trigger`.
    pub fn revert_data_sources(&mut self, reverted_block: BlockNumber) {
        self.instance.revert_data_sources(reverted_block)
    }

    pub fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource<C>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        let source = data_source.as_offchain().map(|ds| ds.source.clone());
        let host = self.instance.add_dynamic_data_source(logger, data_source)?;

        if host.is_some() {
            if let Some(source) = source {
                self.offchain_monitor.add_source(&source)?;
            }
        }

        Ok(host)
    }
}

pub(crate) struct OffchainMonitor {
    ipfs_monitor: PollingMonitor<Cid>,
    ipfs_monitor_rx: mpsc::Receiver<(Cid, Bytes)>,
}

impl OffchainMonitor {
    pub fn new(
        logger: Logger,
        registry: Arc<dyn MetricsRegistry>,
        subgraph_hash: &DeploymentHash,
        ipfs_service: IpfsService,
    ) -> Self {
        let (ipfs_monitor_tx, ipfs_monitor_rx) = mpsc::channel(10);
        let ipfs_monitor = spawn_monitor(
            ipfs_service,
            ipfs_monitor_tx,
            logger,
            PollingMonitorMetrics::new(registry, subgraph_hash),
        );
        Self {
            ipfs_monitor,
            ipfs_monitor_rx,
        }
    }

    fn add_source(&mut self, source: &offchain::Source) -> Result<(), Error> {
        match source {
            offchain::Source::Ipfs(cid) => self.ipfs_monitor.monitor(cid.clone()),
        };
        Ok(())
    }

    pub fn ready_offchain_events(&mut self) -> Result<Vec<offchain::TriggerData>, Error> {
        use graph::tokio::sync::mpsc::error::TryRecvError;

        let mut triggers = vec![];
        loop {
            match self.ipfs_monitor_rx.try_recv() {
                Ok((cid, data)) => triggers.push(offchain::TriggerData {
                    source: offchain::Source::Ipfs(cid),
                    data: Arc::new(data),
                }),
                Err(TryRecvError::Disconnected) => {
                    anyhow::bail!("ipfs monitor unexpectedly terminated")
                }
                Err(TryRecvError::Empty) => break,
            }
        }
        Ok(triggers)
    }
}
