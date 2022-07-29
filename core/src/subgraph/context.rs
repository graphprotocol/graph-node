use crate::{
    polling_monitor::{
        ipfs_service::IpfsService, spawn_monitor, PollingMonitor, PollingMonitorMetrics,
    },
    subgraph::SubgraphInstance,
};
use bytes::Bytes;
use cid::Cid;
use graph::{
    blockchain::Blockchain,
    components::store::DeploymentId,
    data_source::offchain,
    env::ENV_VARS,
    ipfs_client::IpfsClient,
    prelude::{CancelGuard, DeploymentHash, MetricsRegistry, RuntimeHostBuilder},
    slog::Logger,
    tokio::{select, spawn, sync::mpsc},
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
    pub offchain_monitor: OffchainMonitor,
    pub offchain_monitor_rx: mpsc::Receiver<(offchain::Source, Bytes)>,
}

pub struct OffchainMonitor {
    ipfs_monitor: PollingMonitor<Cid>,
}

impl OffchainMonitor {
    pub fn new(
        logger: Logger,
        registry: Arc<dyn MetricsRegistry>,
        subgraph_hash: &DeploymentHash,
        client: IpfsClient,
    ) -> (Self, mpsc::Receiver<(offchain::Source, Bytes)>) {
        let (ipfs_monitor_tx, mut ipfs_monitor_rx) = mpsc::channel(10);
        let ipfs_service = IpfsService::new(
            client,
            ENV_VARS.mappings.max_ipfs_file_bytes.unwrap_or(1 << 20) as u64,
            ENV_VARS.mappings.ipfs_timeout,
            10,
        );
        let ipfs_monitor = spawn_monitor(
            ipfs_service,
            ipfs_monitor_tx,
            logger,
            PollingMonitorMetrics::new(registry, subgraph_hash),
        );
        let (tx, rx) = mpsc::channel(10);
        spawn(async move {
            loop {
                select! {
                    msg = ipfs_monitor_rx.recv() => {
                        match msg {
                            Some((cid, bytes)) => {
                                if let Err(_) = tx.send((offchain::Source::Ipfs(cid), bytes)).await {
                                    break;
                                }
                            },
                            None => break,
                        }
                    }
                }
            }
        });
        (Self { ipfs_monitor }, rx)
    }

    pub fn monitor(&self, source: offchain::Source) {
        match source {
            offchain::Source::Ipfs(cid) => {
                self.ipfs_monitor.monitor(cid);
            }
        }
    }
}
