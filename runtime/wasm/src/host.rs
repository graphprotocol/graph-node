use std::cmp::PartialEq;
use std::panic;
use std::time::Instant;

use anyhow::Context;
use async_trait::async_trait;

use graph::blockchain::{Blockchain, HostFn, RuntimeAdapter};
use graph::components::store::{EnsLookup, SubgraphFork};
use graph::components::subgraph::{MappingError, SharedProofOfIndexing};
use graph::data_source::{
    DataSource, DataSourceTemplate, MappingTrigger, TriggerData, TriggerWithHandler,
};
use graph::prelude::{
    RuntimeHost as RuntimeHostTrait, RuntimeHostBuilder as RuntimeHostBuilderTrait, *,
};

use crate::mapping::{MappingContext, ValidModule};
use crate::module::ToAscPtr;
use crate::{host_exports::HostExports, module::ExperimentalFeatures};
use graph::runtime::gas::Gas;

use super::host_exports::DataSourceDetails;

pub struct RuntimeHostBuilder<C: Blockchain> {
    runtime_adapter: Arc<dyn RuntimeAdapter<C>>,
    link_resolver: Arc<dyn LinkResolver>,
    ens_lookup: Arc<dyn EnsLookup>,
}

impl<C: Blockchain> Clone for RuntimeHostBuilder<C> {
    fn clone(&self) -> Self {
        RuntimeHostBuilder {
            runtime_adapter: self.runtime_adapter.cheap_clone(),
            link_resolver: self.link_resolver.cheap_clone(),
            ens_lookup: self.ens_lookup.cheap_clone(),
        }
    }
}

impl<C: Blockchain> RuntimeHostBuilder<C> {
    pub fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter<C>>,
        link_resolver: Arc<dyn LinkResolver>,
        ens_lookup: Arc<dyn EnsLookup>,
    ) -> Self {
        RuntimeHostBuilder {
            runtime_adapter,
            link_resolver,
            ens_lookup,
        }
    }
}

impl<C: Blockchain> RuntimeHostBuilderTrait<C> for RuntimeHostBuilder<C>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    type Host = RuntimeHost<C>;
    type Module = ValidModule;

    fn compile_module(
        raw_module: &[u8],
        logger: Logger,
        _subgraph_id: DeploymentHash,
        _metrics: Arc<HostMetrics>,
    ) -> Result<Arc<ValidModule>, Error> {
        let valid_module = Arc::new(ValidModule::new(
            &logger,
            raw_module,
            ENV_VARS.mappings.timeout,
        )?);
        Ok(valid_module)
    }

    fn build(
        &self,
        network_name: String,
        subgraph_id: DeploymentHash,
        data_source: DataSource<C>,
        templates: Arc<Vec<DataSourceTemplate<C>>>,
        valid_module: Arc<ValidModule>,
        metrics: Arc<HostMetrics>,
    ) -> Result<Self::Host, Error> {
        RuntimeHost::new(
            self.runtime_adapter.cheap_clone(),
            self.link_resolver.clone(),
            network_name,
            subgraph_id,
            data_source,
            templates,
            valid_module,
            metrics,
            self.ens_lookup.cheap_clone(),
        )
    }
}

pub struct RuntimeHost<C: Blockchain> {
    host_fns: Arc<Vec<HostFn>>,
    data_source: DataSource<C>,
    valid_module: Arc<ValidModule>,
    host_exports: Arc<HostExports>,
    metrics: Arc<HostMetrics>,
}

impl<C> RuntimeHost<C>
where
    C: Blockchain,
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    fn new(
        runtime_adapter: Arc<dyn RuntimeAdapter<C>>,
        link_resolver: Arc<dyn LinkResolver>,
        network_name: String,
        subgraph_id: DeploymentHash,
        data_source: DataSource<C>,
        templates: Arc<Vec<DataSourceTemplate<C>>>,
        valid_module: Arc<ValidModule>,
        metrics: Arc<HostMetrics>,
        ens_lookup: Arc<dyn EnsLookup>,
    ) -> Result<Self, Error> {
        let ds_details = DataSourceDetails::from_data_source(
            &data_source,
            Arc::new(templates.iter().map(|t| t.into()).collect()),
        );

        // Create new instance of externally hosted functions invoker. The `Arc` is simply to avoid
        // implementing `Clone` for `HostExports`.
        let host_exports = Arc::new(HostExports::new(
            subgraph_id,
            network_name,
            ds_details,
            link_resolver,
            ens_lookup,
        ));

        let host_fns = runtime_adapter.host_fns(&data_source).unwrap_or_default();

        Ok(RuntimeHost {
            host_fns: Arc::new(host_fns),
            data_source,
            valid_module,
            host_exports,
            metrics,
        })
    }

    /// Instantiate the WASM module and run the trigger handler directly
    /// in the current async context, using `block_in_place` to avoid
    /// blocking the tokio executor.
    async fn run_mapping(
        &self,
        logger: &Logger,
        state: BlockState,
        trigger: TriggerWithHandler<MappingTrigger<C>>,
        proof_of_indexing: SharedProofOfIndexing,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        instrument: bool,
    ) -> Result<BlockState, MappingError> {
        let handler = trigger.handler_name().to_string();

        let extras = trigger.logging_extras();
        trace!(
            logger, "Start processing trigger";
            &extras,
            "handler" => &handler,
            "data_source" => &self.data_source.name(),
        );

        let start_time = Instant::now();
        let metrics = self.metrics.clone();
        let valid_module = self.valid_module.cheap_clone();
        let host_exports = self.host_exports.cheap_clone();
        let host_fns = self.host_fns.cheap_clone();

        let experimental_features = ExperimentalFeatures {
            allow_non_deterministic_ipfs: ENV_VARS.mappings.allow_non_deterministic_ipfs,
        };

        let ctx = MappingContext {
            logger: logger.cheap_clone(),
            state,
            host_exports,
            block_ptr: trigger.block_ptr(),
            timestamp: trigger.timestamp(),
            proof_of_indexing,
            host_fns,
            debug_fork: debug_fork.cheap_clone(),
            mapping_logger: Logger::new(logger, o!("component" => "UserMapping")),
            instrument,
        };

        let logger_for_panic = logger.cheap_clone();
        let metrics_for_trigger = metrics.cheap_clone();

        // Run the WASM instantiation and handler inside block_in_place.
        // This tells tokio "I'm about to block" so it can move async work
        // to other threads, preventing executor starvation.
        let result = tokio::task::block_in_place(|| {
            let handle_fut = async {
                let _section = metrics_for_trigger.stopwatch.start_section("module_init");
                let module = crate::module::WasmInstance::from_valid_module_with_ctx(
                    valid_module,
                    ctx,
                    metrics_for_trigger.cheap_clone(),
                    experimental_features,
                )
                .await
                .context("module instantiation failed")?;
                drop(_section);

                let _section = metrics_for_trigger.stopwatch.start_section("run_handler");
                if ENV_VARS.log_trigger_data {
                    debug!(logger_for_panic, "trigger data: {:?}", trigger);
                }
                module.handle_trigger(trigger).await
            };

            panic::catch_unwind(panic::AssertUnwindSafe(|| graph::block_on(handle_fut)))
        });

        let result = match result {
            Ok(result) => result,
            Err(panic_info) => {
                let err_msg = if let Some(payload) = panic_info
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or(panic_info.downcast_ref::<&str>().copied())
                {
                    anyhow!("Subgraph panicked with message: {}", payload)
                } else {
                    anyhow!("Subgraph panicked with an unknown payload.")
                };
                Err(MappingError::Unknown(err_msg))
            }
        };

        let elapsed = start_time.elapsed();
        metrics.observe_handler_execution_time(elapsed.as_secs_f64(), &handler);

        // If there is an error, "gas_used" is incorrectly reported as 0.
        let gas_used = result.as_ref().map(|(_, gas)| gas).unwrap_or(&Gas::ZERO);
        debug!(
            logger, "Done processing trigger";
            &extras,
            "total_ms" => elapsed.as_millis(),
            "handler" => handler,
            "data_source" => &self.data_source.name(),
            "gas_used" => gas_used.to_string(),
        );

        // Discard the gas value
        result.map(|(block_state, _)| block_state)
    }
}

#[async_trait]
impl<C: Blockchain> RuntimeHostTrait<C> for RuntimeHost<C>
where
    <C as Blockchain>::MappingTrigger: ToAscPtr,
{
    fn data_source(&self) -> &DataSource<C> {
        &self.data_source
    }

    fn match_and_decode(
        &self,
        trigger: &TriggerData<C>,
        block: &Arc<C::Block>,
        logger: &Logger,
    ) -> Result<Option<TriggerWithHandler<MappingTrigger<C>>>, Error> {
        self.data_source.match_and_decode(trigger, block, logger)
    }

    async fn process_mapping_trigger(
        &self,
        logger: &Logger,
        trigger: TriggerWithHandler<MappingTrigger<C>>,
        state: BlockState,
        proof_of_indexing: SharedProofOfIndexing,
        debug_fork: &Option<Arc<dyn SubgraphFork>>,
        instrument: bool,
    ) -> Result<BlockState, MappingError> {
        self.run_mapping(
            logger,
            state,
            trigger,
            proof_of_indexing,
            debug_fork,
            instrument,
        )
        .await
    }

    fn creation_block_number(&self) -> Option<BlockNumber> {
        self.data_source.creation_block()
    }

    /// Offchain data sources track done_at which is set once the
    /// trigger has been processed.
    fn done_at(&self) -> Option<BlockNumber> {
        match self.data_source() {
            DataSource::Onchain(_) => None,
            DataSource::Offchain(ds) => ds.done_at(),
            DataSource::Subgraph(_) => None,
            DataSource::Amp(_) => None,
        }
    }

    fn set_done_at(&self, block: Option<BlockNumber>) {
        match self.data_source() {
            DataSource::Onchain(_) => {}
            DataSource::Offchain(ds) => ds.set_done_at(block),
            DataSource::Subgraph(_) => {}
            DataSource::Amp(_) => {}
        }
    }

    fn host_metrics(&self) -> Arc<HostMetrics> {
        self.metrics.cheap_clone()
    }
}

impl<C: Blockchain> PartialEq for RuntimeHost<C> {
    fn eq(&self, other: &Self) -> bool {
        self.data_source.is_duplicate_of(&other.data_source)
    }
}
