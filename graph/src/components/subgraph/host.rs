use failure::Error;
use futures::prelude::*;
use futures::sync::mpsc;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::components::metrics::HistogramVec;
use crate::prelude::*;
use web3::types::{Log, Transaction};

/// Common trait for runtime host implementations.
pub trait RuntimeHost: Send + Sync + Debug + 'static {
    /// Returns true if the RuntimeHost has a handler for an Ethereum event.
    fn matches_log(&self, log: &Log) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum call.
    fn matches_call(&self, call: &EthereumCall) -> bool;

    /// Returns true if the RuntimeHost has a handler for an Ethereum block.
    fn matches_block(&self, call: EthereumBlockTriggerType, block_number: u64) -> bool;

    /// Process an Ethereum event and return a vector of entity operations.
    fn process_log(
        &self,
        logger: Logger,
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;

    /// Process an Ethereum call and return a vector of entity operations
    fn process_call(
        &self,
        logger: Logger,
        block: Arc<LightEthereumBlock>,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;

    /// Process an Ethereum block and return a vector of entity operations
    fn process_block(
        &self,
        logger: Logger,
        block: Arc<LightEthereumBlock>,
        trigger_type: EthereumBlockTriggerType,
        state: BlockState,
    ) -> Box<dyn Future<Item = BlockState, Error = Error> + Send>;
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
    pub fn new<M: MetricsRegistry>(
        registry: Arc<M>,
        subgraph_hash: String,
        stopwatch: StopwatchMetrics,
    ) -> Self {
        let handler_execution_time = registry
            .new_histogram_vec(
                format!("subgraph_handler_execution_time_{}", subgraph_hash),
                String::from("Measures the execution time for handlers"),
                HashMap::new(),
                vec![String::from("handler")],
                vec![0.1, 0.5, 1.0, 10.0, 100.0],
            )
            .expect("failed to create `subgraph_handler_execution_time` histogram");
        let host_fn_execution_time = registry
            .new_histogram_vec(
                format!("subgraph_host_fn_execution_time_{}", subgraph_hash),
                String::from("Measures the execution time for host functions"),
                HashMap::new(),
                vec![String::from("host_fn_name")],
                vec![0.025, 0.05, 0.2, 2.0, 8.0, 20.0],
            )
            .expect("failed to create `subgraph_host_fn_execution_time` histogram");
        Self {
            handler_execution_time,
            host_fn_execution_time,
            stopwatch,
        }
    }

    pub fn observe_handler_execution_time(&self, duration: f64, handler: String) {
        self.handler_execution_time
            .with_label_values(vec![handler.as_ref()].as_slice())
            .observe(duration);
    }

    pub fn observe_host_fn_execution_time(&self, duration: f64, fn_name: String) {
        self.host_fn_execution_time
            .with_label_values(vec![fn_name.as_ref()].as_slice())
            .observe(duration);
    }
}

pub trait RuntimeHostBuilder: Clone + Send + Sync + 'static {
    type Host: RuntimeHost;
    type Req: 'static + Send;

    /// Build a new runtime host for a subgraph data source.
    fn build(
        &self,
        network_name: String,
        subgraph_id: SubgraphDeploymentId,
        data_source: DataSource,
        top_level_templates: Vec<DataSourceTemplate>,
        mapping_request_sender: mpsc::Sender<Self::Req>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error>;

    /// Spawn a mapping and return a channel for mapping requests. The sender should be able to be
    /// cached and shared among mappings that have the same `parsed_module`.
    fn spawn_mapping(
        parsed_module: parity_wasm::elements::Module,
        logger: Logger,
        subgraph_id: SubgraphDeploymentId,
        metrics: Arc<HostMetrics>,
    ) -> Result<mpsc::Sender<Self::Req>, Error>;
}
