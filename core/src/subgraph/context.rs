use crate::{
    polling_monitor::{
        ipfs_service::IpfsService, spawn_monitor, PollingMonitor, PollingMonitorMetrics,
    },
    subgraph::SubgraphInstance,
};
use anyhow::{self, Error};
use bytes::Bytes;
use cid::Cid;
use graph::{
    blockchain::Blockchain,
    components::store::DeploymentId,
    data_source::offchain,
    prelude::{CancelGuard, DeploymentHash, MetricsRegistry, RuntimeHostBuilder},
    slog::Logger,
    tokio::sync::mpsc,
};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub type SharedInstanceKeepAliveMap = Arc<RwLock<HashMap<DeploymentId, CancelGuard>>>;

pub struct IndexingContext<C, T>
where
    T: RuntimeHostBuilder<C>,
    C: Blockchain,
{
    pub instance: SubgraphInstance<C, T>,
    pub instances: SharedInstanceKeepAliveMap,
    pub filter: C::TriggerFilter,
    pub(crate) offchain_monitor: OffchainMonitor,
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
