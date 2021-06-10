use std::cmp::PartialEq;
use std::str::FromStr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::sync::mpsc::Sender;
use futures03::channel::oneshot::channel;
use graph::blockchain::HostFn;
use graph::blockchain::RuntimeAdapter;
use graph::blockchain::{Blockchain, DataSource, MappingTrigger as _};
use graph::components::arweave::ArweaveAdapter;
use graph::components::store::SubgraphStore;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::three_box::ThreeBoxAdapter;
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};

use crate::mapping::{MappingContext, MappingRequest};
use crate::{host_exports::HostExports, module::ExperimentalFeatures};

lazy_static! {
    static ref TIMEOUT: Option<Duration> = std::env::var("GRAPH_MAPPING_HANDLER_TIMEOUT")
        .ok()
        .map(|s| u64::from_str(&s).expect("Invalid value for GRAPH_MAPPING_HANDLER_TIMEOUT"))
        .map(Duration::from_secs);
    static ref ALLOW_NON_DETERMINISTIC_IPFS: bool =
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_IPFS").is_ok();
    static ref ALLOW_NON_DETERMINISTIC_3BOX: bool =
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_3BOX").is_ok();
    static ref ALLOW_NON_DETERMINISTIC_ARWEAVE: bool =
        std::env::var("GRAPH_ALLOW_NON_DETERMINISTIC_ARWEAVE").is_ok();
}

pub struct RuntimeHostBuilder<C: Blockchain> {
    runtime_adapter: Arc<C::RuntimeAdapter>,
    link_resolver: Arc<dyn LinkResolver>,
    store: Arc<dyn SubgraphStore>,
    arweave_adapter: Arc<dyn ArweaveAdapter>,
    three_box_adapter: Arc<dyn ThreeBoxAdapter>,
}

impl<C: Blockchain> Clone for RuntimeHostBuilder<C> {
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            runtime_adapter: self.runtime_adapter.cheap_clone(),
            link_resolver: self.link_resolver.cheap_clone(),
            store: self.store.cheap_clone(),
            arweave_adapter: self.arweave_adapter.cheap_clone(),
            three_box_adapter: self.three_box_adapter.cheap_clone(),
        }
    }
}

impl<C: Blockchain> RuntimeHostBuilder<C> {
    pub fn new(
        runtime_adapter: Arc<C::RuntimeAdapter>,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<dyn SubgraphStore>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Self {
        RuntimeHostBuilder {
            runtime_adapter,
            link_resolver,
            store,
            arweave_adapter,
            three_box_adapter,
        }
    }
}

impl<C: Blockchain> RuntimeHostBuilderTrait<C> for RuntimeHostBuilder<C> {
    type Host = RuntimeHost<C>;
    type Req = MappingRequest<C>;

    fn spawn_mapping(
        raw_module: Vec<u8>,
        logger: Logger,
        subgraph_id: DeploymentHash,
        metrics: Arc<HostMetrics>,
    ) -> Result<Sender<Self::Req>, Error> {
        let experimental_features = ExperimentalFeatures {
            allow_non_deterministic_arweave: *ALLOW_NON_DETERMINISTIC_ARWEAVE,
            allow_non_deterministic_3box: *ALLOW_NON_DETERMINISTIC_3BOX,
            allow_non_deterministic_ipfs: *ALLOW_NON_DETERMINISTIC_IPFS,
        };
        crate::mapping::spawn_module(
            raw_module,
            logger,
            subgraph_id,
            metrics,
            tokio::runtime::Handle::current(),
            *TIMEOUT,
            experimental_features,
        )
    }

    fn build(
        &self,
        network_name: String,
        subgraph_id: DeploymentHash,
        data_source: C::DataSource,
        templates: Arc<Vec<C::DataSourceTemplate>>,
        mapping_request_sender: Sender<MappingRequest<C>>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error> {
        RuntimeHost::new(
            self.runtime_adapter.cheap_clone(),
            self.link_resolver.clone(),
            self.store.clone(),
            network_name,
            subgraph_id,
            data_source,
            templates,
            mapping_request_sender,
            metrics,
            self.arweave_adapter.cheap_clone(),
            self.three_box_adapter.cheap_clone(),
        )
    }
}

pub struct RuntimeHost<C: Blockchain> {
    host_fns: Arc<Vec<HostFn>>,
    data_source: C::DataSource,
    mapping_request_sender: Sender<MappingRequest<C>>,
    host_exports: Arc<HostExports<C>>,
    metrics: Arc<HostMetrics>,
}

impl<C> RuntimeHost<C>
where
    C: Blockchain,
{
    fn new(
        runtime_adapter: Arc<C::RuntimeAdapter>,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<dyn SubgraphStore>,
        network_name: String,
        subgraph_id: DeploymentHash,
        data_source: C::DataSource,
        templates: Arc<Vec<C::DataSourceTemplate>>,
        mapping_request_sender: Sender<MappingRequest<C>>,
        metrics: Arc<HostMetrics>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Result<Self, Error> {
        // Create new instance of externally hosted functions invoker. The `Arc` is simply to avoid
        // implementing `Clone` for `HostExports`.
        let host_exports = Arc::new(HostExports::new(
            subgraph_id,
            &data_source,
            network_name,
            templates,
            link_resolver,
            store,
            arweave_adapter,
            three_box_adapter,
        ));

        let host_fns = Arc::new(runtime_adapter.host_fns(&data_source)?);

        Ok(RuntimeHost {
            host_fns,
            data_source,
            mapping_request_sender,
            host_exports,
            metrics,
        })
    }

    /// Sends a MappingRequest to the thread which owns the host,
    /// and awaits the result.
    async fn send_mapping_request(
        &self,
        logger: &Logger,
        state: BlockState<C>,
        trigger: C::MappingTrigger,
        block_ptr: BlockPtr,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState<C>, MappingError> {
        let handler = trigger.handler_name().to_string();

        let extras = trigger.logging_extras();
        trace!(
            logger, "Start processing trigger";
            &extras,
            "handler" => &handler,
            "data_source" => &self.data_source.name(),
        );

        let (result_sender, result_receiver) = channel();
        let start_time = Instant::now();
        let metrics = self.metrics.clone();

        self.mapping_request_sender
            .clone()
            .send(MappingRequest {
                ctx: MappingContext {
                    logger: logger.cheap_clone(),
                    state,
                    host_exports: self.host_exports.cheap_clone(),
                    block_ptr,
                    proof_of_indexing,
                    host_fns: self.host_fns.cheap_clone(),
                },
                trigger,
                result_sender,
            })
            .compat()
            .await
            .context("Mapping terminated before passing in trigger")?;

        let result = result_receiver
            .await
            .context("Mapping terminated before handling trigger")?;

        let elapsed = start_time.elapsed();
        metrics.observe_handler_execution_time(elapsed.as_secs_f64(), &handler);

        info!(
            logger, "Done processing trigger";
            &extras,
            "total_ms" => elapsed.as_millis(),
            "handler" => handler,
            "data_source" => &self.data_source.name(),
        );

        result
    }
}

#[async_trait]
impl<C: Blockchain> RuntimeHostTrait<C> for RuntimeHost<C> {
    fn match_and_decode(
        &self,
        trigger: &C::TriggerData,
        block: Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<C::MappingTrigger>, Error> {
        self.data_source.match_and_decode(trigger, block, logger)
    }

    async fn process_mapping_trigger(
        &self,
        logger: &Logger,
        block_ptr: BlockPtr,
        trigger: C::MappingTrigger,
        state: BlockState<C>,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState<C>, MappingError> {
        self.send_mapping_request(logger, state, trigger, block_ptr, proof_of_indexing)
            .await
    }

    fn creation_block_number(&self) -> Option<BlockNumber> {
        self.data_source.creation_block()
    }
}

impl<C: Blockchain> PartialEq for RuntimeHost<C> {
    fn eq(&self, other: &Self) -> bool {
        self.data_source.is_duplicate_of(&other.data_source)
    }
}
