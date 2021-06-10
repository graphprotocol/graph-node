use std::cmp::PartialEq;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Error;
use async_trait::async_trait;
use futures::sync::mpsc;

use crate::prelude::*;
use crate::{blockchain::Blockchain, components::subgraph::SharedProofOfIndexing};
use crate::{components::metrics::HistogramVec, runtime::DeterministicHostError};

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

impl From<DeterministicHostError> for MappingError {
    fn from(value: DeterministicHostError) -> MappingError {
        MappingError::Unknown(value.0)
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
pub trait RuntimeHost<C: Blockchain>: Send + Sync + 'static {
    fn match_and_decode(
        &self,
        trigger: &C::TriggerData,
        block: Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<C::MappingTrigger>, Error>;

    async fn process_mapping_trigger(
        &self,
        logger: &Logger,
        block_ptr: BlockPtr,
        trigger: C::MappingTrigger,
        state: BlockState<C>,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState<C>, MappingError>;

    /// Block number in which this host was created.
    /// Returns `None` for static data sources.
    fn creation_block_number(&self) -> Option<BlockNumber>;
}

pub struct HostMetrics {
    handler_execution_time: Box<HistogramVec>,
    host_fn_execution_time: Box<HistogramVec>,
    pub stopwatch: StopwatchMetrics,
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
            .with_label_values(&[handler][..])
            .observe(duration);
    }

    pub fn observe_host_fn_execution_time(&self, duration: f64, fn_name: &str) {
        self.host_fn_execution_time
            .with_label_values(&[fn_name][..])
            .observe(duration);
    }

    pub fn time_host_fn_execution_region(
        self: Arc<HostMetrics>,
        fn_name: &'static str,
    ) -> HostFnExecutionTimer {
        HostFnExecutionTimer {
            start: Instant::now(),
            metrics: self,
            fn_name,
        }
    }
}

#[must_use]
pub struct HostFnExecutionTimer {
    start: Instant,
    metrics: Arc<HostMetrics>,
    fn_name: &'static str,
}

impl Drop for HostFnExecutionTimer {
    fn drop(&mut self) {
        let elapsed = (Instant::now() - self.start).as_secs_f64();
        self.metrics
            .observe_host_fn_execution_time(elapsed, self.fn_name)
    }
}

pub trait RuntimeHostBuilder<C: Blockchain>: Clone + Send + Sync + 'static {
    type Host: RuntimeHost<C> + PartialEq;
    type Req: 'static + Send;

    /// Build a new runtime host for a subgraph data source.
    fn build(
        &self,
        network_name: String,
        subgraph_id: DeploymentHash,
        data_source: C::DataSource,
        top_level_templates: Arc<Vec<C::DataSourceTemplate>>,
        mapping_request_sender: mpsc::Sender<Self::Req>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error>;

    /// Spawn a mapping and return a channel for mapping requests. The sender should be able to be
    /// cached and shared among mappings that use the same wasm file.
    fn spawn_mapping(
        raw_module: Vec<u8>,
        logger: Logger,
        subgraph_id: DeploymentHash,
        metrics: Arc<HostMetrics>,
    ) -> Result<mpsc::Sender<Self::Req>, anyhow::Error>;
}
