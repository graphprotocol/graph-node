use std::cmp::PartialEq;
use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use failure::Error;
use futures::sync::mpsc;

use crate::components::metrics::HistogramVec;
use crate::components::subgraph::SharedProofOfIndexing;
use crate::prelude::*;
use web3::types::{Log, Transaction};

#[derive(Debug)]
pub enum MappingError {
    /// A possible reorg was detected while running the mapping.
    PossibleReorg(anyhow::Error),
    Unknown(anyhow::Error),
}

impl From<anyhow::Error> for MappingError {
    fn from(e: anyhow::Error) -> Self {
        MappingError::Unknown(e)
    }
}

impl MappingError {
    pub fn context(self, s: String) -> Self {
        use MappingError::*;
        match self {
            PossibleReorg(e) => PossibleReorg(e.context(s)),
            Unknown(e) => Unknown(e.context(s)),
        }
    }
}

/// Common trait for runtime host implementations.
#[async_trait]
pub trait RuntimeHost: Send + Sync + Debug + 'static {
    /// Returns true if the RuntimeHost has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum call.
    fn matches_call(&self, call: &EthereumCall) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum block.
    fn matches_block(&self, call: &EthereumBlockTriggerType, block_number: u64) -> bool;

    /// Process an Ethereum event and return a vector of entity operations.
    async fn process_log(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        transaction: &Arc<Transaction>,
        log: &Arc<Log>,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError>;

    /// Process an Ethereum call and return a vector of entity operations
    async fn process_call(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        transaction: &Arc<Transaction>,
        call: &Arc<EthereumCall>,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError>;

    /// Process an Ethereum block and return a vector of entity operations
    async fn process_block(
        &self,
        logger: &Logger,
        block: &Arc<LightEthereumBlock>,
        trigger_type: &EthereumBlockTriggerType,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError>;
}

pub struct HostMetrics {
    handler_execution_time: Box<HistogramVec>,
    host_fn_execution_time: Box<HistogramVec>,
    pub stopwatch: StopwatchMetrics,
}

impl fmt::Debug for HostMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // TODO: `HistogramVec` does not implement fmt::Debug, what is the best way to deal with this?
        write!(f, "HostMetrics {{ }}")
    }
}

impl HostMetrics {
    pub fn new(
        registry: Arc<impl MetricsRegistry>,
        subgraph: &str,
        stopwatch: StopwatchMetrics,
    ) -> Self {
        let handler_execution_time = registry
            .new_deployment_histogram_vec(
                "deployment_handler_execution_time",
                "Measures the execution time for handlers",
                subgraph,
                vec![String::from("handler")],
                vec![0.1, 0.5, 1.0, 10.0, 100.0],
            )
            .expect("failed to create `deployment_handler_execution_time` histogram");
        let host_fn_execution_time = registry
            .new_deployment_histogram_vec(
                "deployment_host_fn_execution_time",
                "Measures the execution time for host functions",
                subgraph,
                vec![String::from("host_fn_name")],
                vec![0.025, 0.05, 0.2, 2.0, 8.0, 20.0],
            )
            .expect("failed to create `deployment_host_fn_execution_time` histogram");
        Self {
            handler_execution_time,
            host_fn_execution_time,
            stopwatch,
        }
    }

    pub fn observe_handler_execution_time(&self, duration: f64, handler: &str) {
        self.handler_execution_time
            .with_label_values(vec![handler].as_slice())
            .observe(duration);
    }

    pub fn observe_host_fn_execution_time(&self, duration: f64, fn_name: &str) {
        self.host_fn_execution_time
            .with_label_values(vec![fn_name].as_slice())
            .observe(duration);
    }
}

pub trait RuntimeHostBuilder: Clone + Send + Sync + 'static {
    type Host: RuntimeHost + PartialEq;
    type Req: 'static + Send;

    /// Build a new runtime host for a subgraph data source.
    fn build(
        &self,
        network_name: String,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
        top_level_templates: Arc<Vec<DataSourceTemplate>>,
        mapping_request_sender: mpsc::Sender<Self::Req>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error>;

    /// Spawn a mapping and return a channel for mapping requests. The sender should be able to be
    /// cached and shared among mappings that use the same wasm file.
    fn spawn_mapping(
        raw_module: Vec<u8>,
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        metrics: Arc<HostMetrics>,
    ) -> Result<mpsc::Sender<Self::Req>, anyhow::Error>;
}
