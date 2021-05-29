use std::cmp::PartialEq;
use std::str::FromStr;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::sync::mpsc::Sender;
use futures03::channel::oneshot::channel;
use graph::{
    blockchain::{Blockchain, DataSource},
    components::store::CallCache,
};
use strum::AsStaticRef as _;

use graph::components::arweave::ArweaveAdapter;
use graph::components::ethereum::*;
use graph::components::store::SubgraphStore;
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::components::three_box::ThreeBoxAdapter;
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};
use graph_chain_ethereum::{EthereumAdapterTrait, EthereumNetworks};

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

pub struct RuntimeHostBuilder<S, CC> {
    ethereum_networks: EthereumNetworks,
    link_resolver: Arc<dyn LinkResolver>,
    store: Arc<S>,
    caches: Arc<CC>,
    arweave_adapter: Arc<dyn ArweaveAdapter>,
    three_box_adapter: Arc<dyn ThreeBoxAdapter>,
}

impl<S, CC> Clone for RuntimeHostBuilder<S, CC>
where
    S: SubgraphStore,
    CC: CallCache,
{
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            ethereum_networks: self.ethereum_networks.clone(),
            link_resolver: self.link_resolver.clone(),
            store: self.store.clone(),
            caches: self.caches.clone(),
            arweave_adapter: self.arweave_adapter.cheap_clone(),
            three_box_adapter: self.three_box_adapter.cheap_clone(),
        }
    }
}

impl<S, CC> RuntimeHostBuilder<S, CC>
where
    S: SubgraphStore,
    CC: CallCache,
{
    pub fn new(
        ethereum_networks: EthereumNetworks,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<S>,
        caches: Arc<CC>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Self {
        RuntimeHostBuilder {
            ethereum_networks,
            link_resolver,
            store,
            caches,
            arweave_adapter,
            three_box_adapter,
        }
    }
}

impl<C, S, CC> RuntimeHostBuilderTrait<C> for RuntimeHostBuilder<S, CC>
where
    S: SubgraphStore,
    CC: CallCache,
    C: Blockchain<
        Block = graph_chain_ethereum::WrappedBlockFinality,
        TriggerData = EthereumTrigger,
        DataSource = graph_chain_ethereum::DataSource,
    >,
{
    type Host = RuntimeHost<C>;
    type Req = MappingRequest;

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
        templates: Arc<Vec<DataSourceTemplate>>,
        mapping_request_sender: Sender<MappingRequest>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error> {
        let cache = self
            .caches
            .ethereum_call_cache(&network_name)
            .ok_or_else(|| {
                anyhow!(
                    "No store found that matches subgraph network: \"{}\"",
                    &network_name
                )
            })?;

        let required_capabilities = data_source.mapping().required_capabilities();

        let ethereum_adapter = self
            .ethereum_networks
            .adapter_with_capabilities(network_name.clone(), &required_capabilities)?;

        Ok(RuntimeHost::new(
            ethereum_adapter.clone(),
            self.link_resolver.clone(),
            self.store.clone(),
            cache,
            network_name,
            subgraph_id,
            data_source,
            templates,
            mapping_request_sender,
            metrics,
            self.arweave_adapter.cheap_clone(),
            self.three_box_adapter.cheap_clone(),
        ))
    }
}

#[derive(Debug)]
pub struct RuntimeHost<C: Blockchain> {
    data_source: C::DataSource,
    mapping_request_sender: Sender<MappingRequest>,
    host_exports: Arc<HostExports>,
    metrics: Arc<HostMetrics>,
}

impl<C> RuntimeHost<C>
where
    C: Blockchain<DataSource = graph_chain_ethereum::DataSource>,
{
    fn new(
        ethereum_adapter: Arc<dyn EthereumAdapterTrait>,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<dyn crate::RuntimeStore>,
        call_cache: Arc<dyn EthereumCallCache>,
        network_name: String,
        subgraph_id: DeploymentHash,
        data_source: C::DataSource,
        templates: Arc<Vec<DataSourceTemplate>>,
        mapping_request_sender: Sender<MappingRequest>,
        metrics: Arc<HostMetrics>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Self {
        // Create new instance of externally hosted functions invoker. The `Arc` is simply to avoid
        // implementing `Clone` for `HostExports`.
        let host_exports = Arc::new(HostExports::new(
            subgraph_id,
            &data_source,
            network_name,
            templates,
            ethereum_adapter,
            link_resolver,
            store,
            call_cache,
            arweave_adapter,
            three_box_adapter,
        ));

        RuntimeHost {
            data_source,
            mapping_request_sender,
            host_exports,
            metrics,
        }
    }

    /// Sends a MappingRequest to the thread which owns the host,
    /// and awaits the result.
    async fn send_mapping_request(
        &self,
        logger: &Logger,
        state: BlockState,
        trigger: MappingTrigger,
        block_ptr: BlockPtr,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
        let trigger_type = trigger.as_static();
        let handler = trigger.handler_name().to_string();

        let extras = trigger.logging_extras();
        trace!(
            logger, "Start processing Ethereum trigger";
            &extras,
            "trigger_type" => trigger_type,
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
            logger, "Done processing Ethereum trigger";
            &extras,
            "trigger_type" => trigger_type,
            "total_ms" => elapsed.as_millis(),
            "handler" => handler,
            "data_source" => &self.data_source.name(),
        );

        result
    }
}

#[async_trait]
impl<C> RuntimeHostTrait<C> for RuntimeHost<C>
where
    C: Blockchain<
        Block = graph_chain_ethereum::WrappedBlockFinality,
        TriggerData = EthereumTrigger,
        DataSource = graph_chain_ethereum::DataSource,
    >,
{
    fn match_and_decode(
        &self,
        trigger: &EthereumTrigger,
        block: Arc<LightEthereumBlock>,
        logger: &Logger,
    ) -> Result<Option<MappingTrigger>, Error> {
        self.data_source.match_and_decode(trigger, block, logger)
    }

    async fn process_mapping_trigger(
        &self,
        logger: &Logger,
        block_ptr: BlockPtr,
        trigger: MappingTrigger,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
    ) -> Result<BlockState, MappingError> {
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
