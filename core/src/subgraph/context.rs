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

pub(crate) struct IndexingContext<C, T>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
{
    instance: SubgraphInstance<C, T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
    pub(crate) offchain_monitor: OffchainMonitor,
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
        self.trigger_processor
            .process_trigger(
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

    pub fn revert_data_sources(&mut self, reverted_block: BlockNumber) {
        self.instance.revert_data_sources(reverted_block)
    }

    pub fn add_dynamic_data_source(
        &mut self,
        logger: &Logger,
        data_source: DataSource<C>,
    ) -> Result<Option<Arc<T::Host>>, Error> {
        self.instance.add_dynamic_data_source(logger, data_source)
    }
}

pub(crate) struct OffchainMonitor {
    ipfs_monitor: PollingMonitor<Cid>,
    ipfs_monitor_rx: mpsc::Receiver<(Cid, Bytes)>,
    data_sources: Vec<offchain::DataSource>,
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
            data_sources: Vec::new(),
        }
    }

    pub fn add_data_source(&mut self, ds: offchain::DataSource) -> Result<(), Error> {
        match ds.source {
            offchain::Source::Ipfs(cid) => self.ipfs_monitor.monitor(cid.clone()),
        };
        self.data_sources.push(ds);
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
