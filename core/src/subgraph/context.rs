pub mod instance;

use crate::polling_monitor::{spawn_monitor, IpfsService, PollingMonitor, PollingMonitorMetrics};
use anyhow::{self, Error};
use bytes::Bytes;
use graph::{
    blockchain::Blockchain,
    components::{
        store::{DeploymentId, SubgraphFork},
        subgraph::{MappingError, SharedProofOfIndexing},
    },
    data_source::{offchain, CausalityRegion, DataSource, TriggerData},
    ipfs_client::CidFile,
    prelude::{
        BlockNumber, BlockState, CancelGuard, CheapClone, DeploymentHash, MetricsRegistry,
        RuntimeHostBuilder, SubgraphCountMetric, SubgraphInstanceMetrics, TriggerProcessor,
    },
    slog::Logger,
    tokio::sync::mpsc,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use self::instance::SubgraphInstance;

#[derive(Clone, Debug)]
pub struct SubgraphKeepAlive {
    alive_map: Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>,
    sg_metrics: Arc<SubgraphCountMetric>,
}

impl CheapClone for SubgraphKeepAlive {}

impl SubgraphKeepAlive {
    pub fn new(sg_metrics: Arc<SubgraphCountMetric>) -> Self {
        Self {
            sg_metrics,
            alive_map: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    pub fn remove(&self, deployment_id: &DeploymentId) {
        self.alive_map.write().unwrap().remove(deployment_id);
        self.sg_metrics.running_count.dec();
    }
    pub fn insert(&self, deployment_id: DeploymentId, guard: CancelGuard) {
        self.alive_map.write().unwrap().insert(deployment_id, guard);
        self.sg_metrics.running_count.inc();
    }
}

// The context keeps track of mutable in-memory state that is retained across blocks.
//
// Currently most of the changes are applied in `runner.rs`, but ideally more of that would be
// refactored into the context so it wouldn't need `pub` fields. The entity cache should probably
// also be moved here.
pub struct IndexingContext<C, T>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
{
    instance: SubgraphInstance<C, T>,
    pub instances: SubgraphKeepAlive,
    pub filter: C::TriggerFilter,
    pub offchain_monitor: OffchainMonitor,
    trigger_processor: Box<dyn TriggerProcessor<C, T>>,
}

impl<C: Blockchain, T: RuntimeHostBuilder<C>> IndexingContext<C, T> {
    pub fn new(
        instance: SubgraphInstance<C, T>,
        instances: SubgraphKeepAlive,
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
        instrument: bool,
    ) -> Result<BlockState<C>, MappingError> {
        self.process_trigger_in_hosts(
            logger,
            self.instance.hosts(),
            block,
            trigger,
            state,
            proof_of_indexing,
            causality_region,
            debug_fork,
            subgraph_metrics,
            instrument,
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
        instrument: bool,
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
                instrument,
            )
            .await
    }

    /// Removes data sources hosts with a creation block greater or equal to `reverted_block`, so
    /// that they are no longer candidates for `process_trigger`.
    ///
    /// This does not currently affect the `offchain_monitor` or the `filter`, so they will continue
    /// to include data sources that have been reverted. This is not ideal for performance, but it
    /// does not affect correctness since triggers that have no matching host will be ignored by
    /// `process_trigger`.
    ///
    /// File data sources that have been marked not done during this process will get re-queued
    pub fn revert_data_sources(&mut self, reverted_block: BlockNumber) -> Result<(), Error> {
        let removed = self.instance.revert_data_sources(reverted_block);

        removed
            .into_iter()
            .try_for_each(|source| self.offchain_monitor.add_source(source))
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
                self.offchain_monitor.add_source(source)?;
            }
        }

        Ok(host)
    }

    pub fn causality_region_next_value(&mut self) -> CausalityRegion {
        self.instance.causality_region_next_value()
    }

    #[cfg(debug_assertions)]
    pub fn instance(&self) -> &SubgraphInstance<C, T> {
        &self.instance
    }
}

pub struct OffchainMonitor {
    ipfs_monitor: PollingMonitor<CidFile>,
    ipfs_monitor_rx: mpsc::Receiver<(CidFile, Bytes)>,
}

impl OffchainMonitor {
    pub fn new(
        logger: Logger,
        registry: Arc<MetricsRegistry>,
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

    fn add_source(&mut self, source: offchain::Source) -> Result<(), Error> {
        match source {
            offchain::Source::Ipfs(cid_file) => self.ipfs_monitor.monitor(cid_file),
        };
        Ok(())
    }

    pub fn ready_offchain_events(&mut self) -> Result<Vec<offchain::TriggerData>, Error> {
        use graph::tokio::sync::mpsc::error::TryRecvError;

        let mut triggers = vec![];
        loop {
            match self.ipfs_monitor_rx.try_recv() {
                Ok((cid_file, data)) => triggers.push(offchain::TriggerData {
                    source: offchain::Source::Ipfs(cid_file),
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
