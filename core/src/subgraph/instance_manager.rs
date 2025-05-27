use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use crate::polling_monitor::{ArweaveService, IpfsService};
use crate::subgraph::context::{IndexingContext, SubgraphKeepAlive};
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::loader::load_dynamic_data_sources;
use crate::subgraph::Decoder;
use std::collections::BTreeSet;

use crate::subgraph::runner::SubgraphRunner;
use graph::blockchain::block_stream::{BlockStreamMetrics, TriggersAdapterWrapper};
use graph::blockchain::{Blockchain, BlockchainKind, DataSource, NodeCapabilities};
use graph::components::metrics::gas::GasMetrics;
use graph::components::metrics::subgraph::DeploymentStatusMetric;
use graph::components::store::SourceableStore;
use graph::components::subgraph::ProofOfIndexingVersion;
use graph::data::subgraph::{UnresolvedSubgraphManifest, SPEC_VERSION_0_0_6};
use graph::data::value::Word;
use graph::data_source::causality_region::CausalityRegionSeq;
use graph::env::EnvVars;
use graph::prelude::{SubgraphInstanceManager as SubgraphInstanceManagerTrait, *};
use graph::{blockchain::BlockchainMap, components::store::DeploymentLocator};
use graph_runtime_wasm::module::ToAscPtr;
use graph_runtime_wasm::RuntimeHostBuilder;
use tokio::task;

use super::context::OffchainMonitor;
use super::SubgraphTriggerProcessor;
use crate::subgraph::runner::SubgraphRunnerError;

#[derive(Clone)]
pub struct SubgraphInstanceManager<S: SubgraphStore> {
    logger_factory: LoggerFactory,
    subgraph_store: Arc<S>,
    chains: Arc<BlockchainMap>,
    metrics_registry: Arc<MetricsRegistry>,
    instances: SubgraphKeepAlive,
    link_resolver: Arc<dyn LinkResolver>,
    ipfs_service: IpfsService,
    arweave_service: ArweaveService,
    static_filters: bool,
    env_vars: Arc<EnvVars>,

    /// By design, there should be only one subgraph runner process per subgraph, but the current
    /// implementation does not completely prevent multiple runners from being active at the same
    /// time, and we have already had a [bug][0] due to this limitation. Investigating the problem
    /// was quite complicated because there was no way to know that the logs were coming from two
    /// different processes because all the logs looked the same. Ideally, the implementation
    /// should be refactored to make it more strict, but until then, we keep this counter, which
    /// is incremented each time a new runner is started, and the previous count is embedded in
    /// each log of the started runner, to make debugging future issues easier.
    ///
    /// [0]: https://github.com/graphprotocol/graph-node/issues/5452
    subgraph_start_counter: Arc<AtomicU64>,
}

#[async_trait]
impl<S: SubgraphStore> SubgraphInstanceManagerTrait for SubgraphInstanceManager<S> {
    async fn start_subgraph(
        self: Arc<Self>,
        loc: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
    ) {
        let runner_index = self.subgraph_start_counter.fetch_add(1, Ordering::SeqCst);

        let logger = self.logger_factory.subgraph_logger(&loc);
        let logger = logger.new(o!("runner_index" => runner_index));

        let err_logger = logger.clone();
        let instance_manager = self.cheap_clone();

        let deployment_status_metric = self.new_deployment_status_metric(&loc);
        deployment_status_metric.starting();

        let subgraph_start_future = {
            let deployment_status_metric = deployment_status_metric.clone();

            async move {
                match BlockchainKind::from_manifest(&manifest)? {
                    BlockchainKind::Arweave => {
                        let runner = instance_manager
                            .build_subgraph_runner::<graph_chain_arweave::Chain>(
                                logger.clone(),
                                self.env_vars.cheap_clone(),
                                loc.clone(),
                                manifest,
                                stop_block,
                                Box::new(SubgraphTriggerProcessor {}),
                                deployment_status_metric,
                            )
                            .await?;

                        self.start_subgraph_inner(logger, loc, runner).await
                    }
                    BlockchainKind::Ethereum => {
                        let runner = instance_manager
                            .build_subgraph_runner::<graph_chain_ethereum::Chain>(
                                logger.clone(),
                                self.env_vars.cheap_clone(),
                                loc.clone(),
                                manifest,
                                stop_block,
                                Box::new(SubgraphTriggerProcessor {}),
                                deployment_status_metric,
                            )
                            .await?;

                        self.start_subgraph_inner(logger, loc, runner).await
                    }
                    BlockchainKind::Near => {
                        let runner = instance_manager
                            .build_subgraph_runner::<graph_chain_near::Chain>(
                                logger.clone(),
                                self.env_vars.cheap_clone(),
                                loc.clone(),
                                manifest,
                                stop_block,
                                Box::new(SubgraphTriggerProcessor {}),
                                deployment_status_metric,
                            )
                            .await?;

                        self.start_subgraph_inner(logger, loc, runner).await
                    }
                    BlockchainKind::Substreams => {
                        let runner = instance_manager
                            .build_subgraph_runner::<graph_chain_substreams::Chain>(
                                logger.clone(),
                                self.env_vars.cheap_clone(),
                                loc.cheap_clone(),
                                manifest,
                                stop_block,
                                Box::new(graph_chain_substreams::TriggerProcessor::new(
                                    loc.clone(),
                                )),
                                deployment_status_metric,
                            )
                            .await?;

                        self.start_subgraph_inner(logger, loc, runner).await
                    }
                }
            }
        };

        // Perform the actual work of starting the subgraph in a separate
        // task. If the subgraph is a graft or a copy, starting it will
        // perform the actual work of grafting/copying, which can take
        // hours. Running it in the background makes sure the instance
        // manager does not hang because of that work.
        graph::spawn(async move {
            match subgraph_start_future.await {
                Ok(()) => {}
                Err(err) => {
                    deployment_status_metric.failed();

                    error!(
                        err_logger,
                        "Failed to start subgraph";
                        "error" => format!("{:#}", err),
                        "code" => LogCode::SubgraphStartFailure
                    );
                }
            }
        });
    }

    async fn stop_subgraph(&self, loc: DeploymentLocator) {
        let logger = self.logger_factory.subgraph_logger(&loc);

        match self.subgraph_store.stop_subgraph(&loc).await {
            Ok(()) => debug!(logger, "Stopped subgraph writer"),
            Err(err) => {
                error!(logger, "Error stopping subgraph writer"; "error" => format!("{:#}", err))
            }
        }

        self.instances.remove(&loc.id);

        info!(logger, "Stopped subgraph");
    }
}

impl<S: SubgraphStore> SubgraphInstanceManager<S> {
    pub fn new(
        logger_factory: &LoggerFactory,
        env_vars: Arc<EnvVars>,
        subgraph_store: Arc<S>,
        chains: Arc<BlockchainMap>,
        sg_metrics: Arc<SubgraphCountMetric>,
        metrics_registry: Arc<MetricsRegistry>,
        link_resolver: Arc<dyn LinkResolver>,
        ipfs_service: IpfsService,
        arweave_service: ArweaveService,
        static_filters: bool,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphInstanceManager", None);
        let logger_factory = logger_factory.with_parent(logger.clone());

        SubgraphInstanceManager {
            logger_factory,
            subgraph_store,
            chains,
            metrics_registry: metrics_registry.cheap_clone(),
            instances: SubgraphKeepAlive::new(sg_metrics),
            link_resolver,
            ipfs_service,
            static_filters,
            env_vars,
            arweave_service,
            subgraph_start_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub async fn get_sourceable_stores<C: Blockchain>(
        &self,
        hashes: Vec<DeploymentHash>,
        is_runner_test: bool,
    ) -> anyhow::Result<Vec<Arc<dyn SourceableStore>>> {
        if is_runner_test {
            return Ok(Vec::new());
        }

        let mut sourceable_stores = Vec::new();
        let subgraph_store = self.subgraph_store.clone();

        for hash in hashes {
            let loc = subgraph_store
                .active_locator(&hash)?
                .ok_or_else(|| anyhow!("no active deployment for hash {}", hash))?;

            let sourceable_store = subgraph_store.clone().sourceable(loc.id.clone()).await?;
            sourceable_stores.push(sourceable_store);
        }

        Ok(sourceable_stores)
    }

    pub async fn build_subgraph_runner<C>(
        &self,
        logger: Logger,
        env_vars: Arc<EnvVars>,
        deployment: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
        tp: Box<dyn TriggerProcessor<C, RuntimeHostBuilder<C>>>,
        deployment_status_metric: DeploymentStatusMetric,
    ) -> anyhow::Result<SubgraphRunner<C, RuntimeHostBuilder<C>>>
    where
        C: Blockchain,
        <C as Blockchain>::MappingTrigger: ToAscPtr,
    {
        self.build_subgraph_runner_inner(
            logger,
            env_vars,
            deployment,
            manifest,
            stop_block,
            tp,
            deployment_status_metric,
            false,
        )
        .await
    }

    pub async fn build_subgraph_runner_inner<C>(
        &self,
        logger: Logger,
        env_vars: Arc<EnvVars>,
        deployment: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
        tp: Box<dyn TriggerProcessor<C, RuntimeHostBuilder<C>>>,
        deployment_status_metric: DeploymentStatusMetric,
        is_runner_test: bool,
    ) -> anyhow::Result<SubgraphRunner<C, RuntimeHostBuilder<C>>>
    where
        C: Blockchain,
        <C as Blockchain>::MappingTrigger: ToAscPtr,
    {
        let subgraph_store = self.subgraph_store.cheap_clone();
        let registry = self.metrics_registry.cheap_clone();

        let raw_yaml = serde_yaml::to_string(&manifest).unwrap();
        let manifest = UnresolvedSubgraphManifest::parse(deployment.hash.cheap_clone(), manifest)?;

        // Allow for infinite retries for subgraph definition files.
        let link_resolver = Arc::from(
            self.link_resolver
                .for_manifest(&deployment.hash.to_string())
                .map_err(SubgraphRegistrarError::Unknown)?
                .with_retries(),
        );

        // Make sure the `raw_yaml` is present on both this subgraph and the graft base.
        self.subgraph_store
            .set_manifest_raw_yaml(&deployment.hash, raw_yaml)
            .await?;
        if let Some(graft) = &manifest.graft {
            if self.subgraph_store.is_deployed(&graft.base)? {
                let file_bytes = self
                    .link_resolver
                    .cat(&logger, &graft.base.to_ipfs_link())
                    .await?;
                let yaml = String::from_utf8(file_bytes)?;

                self.subgraph_store
                    .set_manifest_raw_yaml(&graft.base, yaml)
                    .await?;
            }
        }

        info!(logger, "Resolve subgraph files using IPFS";
            "n_data_sources" => manifest.data_sources.len(),
            "n_templates" => manifest.templates.len(),
        );

        let manifest = manifest
            .resolve(&link_resolver, &logger, ENV_VARS.max_spec_version.clone())
            .await?;

        {
            let features = if manifest.features.is_empty() {
                "ø".to_string()
            } else {
                manifest
                    .features
                    .iter()
                    .map(|f| f.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            info!(logger, "Successfully resolved subgraph files using IPFS";
                "n_data_sources" => manifest.data_sources.len(),
                "n_templates" => manifest.templates.len(),
                "features" => features
            );
        }

        let store = self
            .subgraph_store
            .cheap_clone()
            .writable(
                logger.clone(),
                deployment.id,
                Arc::new(manifest.template_idx_and_name().collect()),
            )
            .await?;

        // Create deployment features from the manifest
        // Write it to the database
        let deployment_features = manifest.deployment_features();
        self.subgraph_store
            .create_subgraph_features(deployment_features)?;

        // Start the subgraph deployment before reading dynamic data
        // sources; if the subgraph is a graft or a copy, starting it will
        // do the copying and dynamic data sources won't show up until after
        // that is done
        store.start_subgraph_deployment(&logger).await?;

        let dynamic_data_sources =
            load_dynamic_data_sources(store.clone(), logger.clone(), &manifest)
                .await
                .context("Failed to load dynamic data sources")?;

        // Combine the data sources from the manifest with the dynamic data sources
        let mut data_sources = manifest.data_sources.clone();
        data_sources.extend(dynamic_data_sources);

        info!(logger, "Data source count at start: {}", data_sources.len());

        let onchain_data_sources = data_sources
            .iter()
            .filter_map(|d| d.as_onchain().cloned())
            .collect::<Vec<_>>();

        let subgraph_data_sources = data_sources
            .iter()
            .filter_map(|d| d.as_subgraph())
            .collect::<Vec<_>>();

        let subgraph_ds_source_deployments = subgraph_data_sources
            .iter()
            .map(|d| d.source.address())
            .collect::<Vec<_>>();

        let required_capabilities = C::NodeCapabilities::from_data_sources(&onchain_data_sources);
        let network: Word = manifest.network_name().into();

        let chain = self
            .chains
            .get::<C>(network.clone())
            .with_context(|| format!("no chain configured for network {}", network))?
            .clone();

        let start_blocks: Vec<BlockNumber> = data_sources
            .iter()
            .filter_map(|d| d.start_block())
            .collect();

        let end_blocks: BTreeSet<BlockNumber> = manifest
            .data_sources
            .iter()
            .filter_map(|d| {
                d.as_onchain()
                    .map(|d: &C::DataSource| d.end_block())
                    .flatten()
            })
            .collect();

        // We can set `max_end_block` to the maximum of `end_blocks` and stop the subgraph
        // only when there are no dynamic data sources and no offchain data sources present. This is because:
        // - Dynamic data sources do not have a defined `end_block`, so we can't determine
        //   when to stop processing them.
        // - Offchain data sources might require processing beyond the end block of
        //   onchain data sources, so the subgraph needs to continue.
        let max_end_block: Option<BlockNumber> = if data_sources.len() == end_blocks.len() {
            end_blocks.iter().max().cloned()
        } else {
            None
        };

        let templates = Arc::new(manifest.templates.clone());

        // Obtain the debug fork from the subgraph store
        let debug_fork = self
            .subgraph_store
            .debug_fork(&deployment.hash, logger.clone())?;

        // Create a subgraph instance from the manifest; this moves
        // ownership of the manifest and host builder into the new instance
        let stopwatch_metrics = StopwatchMetrics::new(
            logger.clone(),
            deployment.hash.clone(),
            "process",
            self.metrics_registry.clone(),
            store.shard().to_string(),
        );

        let gas_metrics = GasMetrics::new(deployment.hash.clone(), self.metrics_registry.clone());

        let unified_mapping_api_version = manifest.unified_mapping_api_version()?;
        let triggers_adapter = chain.triggers_adapter(&deployment, &required_capabilities, unified_mapping_api_version).map_err(|e|
                anyhow!(
                "expected triggers adapter that matches deployment {} with required capabilities: {}: {}",
                &deployment,
                &required_capabilities, e))?.clone();

        let host_metrics = Arc::new(HostMetrics::new(
            registry.cheap_clone(),
            deployment.hash.as_str(),
            stopwatch_metrics.clone(),
            gas_metrics.clone(),
        ));

        let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
            registry.cheap_clone(),
            deployment.hash.as_str(),
            stopwatch_metrics.clone(),
            deployment_status_metric,
        ));

        let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
            registry.cheap_clone(),
            &deployment.hash,
            manifest.network_name(),
            store.shard().to_string(),
            stopwatch_metrics,
        ));

        let offchain_monitor = OffchainMonitor::new(
            logger.cheap_clone(),
            registry.cheap_clone(),
            &manifest.id,
            self.ipfs_service.clone(),
            self.arweave_service.clone(),
        );

        // Initialize deployment_head with current deployment head. Any sort of trouble in
        // getting the deployment head ptr leads to initializing with 0
        let deployment_head = store.block_ptr().map(|ptr| ptr.number).unwrap_or(0) as f64;
        block_stream_metrics.deployment_head.set(deployment_head);

        let (runtime_adapter, decoder_hook) = chain.runtime()?;
        let host_builder = graph_runtime_wasm::RuntimeHostBuilder::new(
            runtime_adapter,
            self.link_resolver.cheap_clone(),
            subgraph_store.ens_lookup(),
        );

        let features = manifest.features.clone();
        let unified_api_version = manifest.unified_mapping_api_version()?;
        let poi_version = if manifest.spec_version.ge(&SPEC_VERSION_0_0_6) {
            ProofOfIndexingVersion::Fast
        } else {
            ProofOfIndexingVersion::Legacy
        };

        let causality_region_seq =
            CausalityRegionSeq::from_current(store.causality_region_curr_val().await?);

        let instrument = self.subgraph_store.instrument(&deployment)?;

        let decoder = Box::new(Decoder::new(decoder_hook));

        let subgraph_data_source_stores = self
            .get_sourceable_stores::<C>(subgraph_ds_source_deployments, is_runner_test)
            .await?;

        let triggers_adapter = Arc::new(TriggersAdapterWrapper::new(
            triggers_adapter,
            subgraph_data_source_stores.clone(),
        ));

        let inputs = IndexingInputs {
            deployment: deployment.clone(),
            features,
            start_blocks,
            end_blocks,
            source_subgraph_stores: subgraph_data_source_stores,
            stop_block,
            max_end_block,
            store,
            debug_fork,
            triggers_adapter,
            chain,
            templates,
            unified_api_version,
            static_filters: self.static_filters,
            poi_version,
            network: network.to_string(),
            instrument,
        };

        // Initialize the indexing context, including both static and dynamic data sources.
        // The order of inclusion is the order of processing when a same trigger matches
        // multiple data sources.
        let ctx = {
            let mut ctx = IndexingContext::new(
                manifest,
                host_builder,
                host_metrics.clone(),
                causality_region_seq,
                self.instances.cheap_clone(),
                offchain_monitor,
                tp,
                decoder,
            );
            for data_source in data_sources {
                ctx.add_dynamic_data_source(&logger, data_source)?;
            }
            ctx
        };

        let metrics = RunnerMetrics {
            subgraph: subgraph_metrics,
            host: host_metrics,
            stream: block_stream_metrics,
        };

        Ok(SubgraphRunner::new(
            inputs,
            ctx,
            logger.cheap_clone(),
            metrics,
            env_vars,
        ))
    }

    async fn start_subgraph_inner<C: Blockchain>(
        &self,
        logger: Logger,
        deployment: DeploymentLocator,
        runner: SubgraphRunner<C, RuntimeHostBuilder<C>>,
    ) -> Result<(), Error>
    where
        <C as Blockchain>::MappingTrigger: ToAscPtr,
    {
        let registry = self.metrics_registry.cheap_clone();
        let subgraph_metrics = runner.metrics.subgraph.cheap_clone();

        // Keep restarting the subgraph until it terminates. The subgraph
        // will usually only run once, but is restarted whenever a block
        // creates dynamic data sources. This allows us to recreate the
        // block stream and include events for the new data sources going
        // forward; this is easier than updating the existing block stream.
        //
        // This is a long-running and unfortunately a blocking future (see #905), so it is run in
        // its own thread. It is also run with `task::unconstrained` because we have seen deadlocks
        // occur without it, possibly caused by our use of legacy futures and tokio versions in the
        // codebase and dependencies, which may not play well with the tokio 1.0 cooperative
        // scheduling. It is also logical in terms of performance to run this with `unconstrained`,
        // it has a dedicated OS thread so the OS will handle the preemption. See
        // https://github.com/tokio-rs/tokio/issues/3493.
        graph::spawn_thread(deployment.to_string(), move || {
            match graph::block_on(task::unconstrained(runner.run())) {
                Ok(()) => {
                    subgraph_metrics.deployment_status.stopped();
                }
                Err(SubgraphRunnerError::Duplicate) => {
                    // We do not need to unregister metrics because they are unique per subgraph
                    // and another runner is still active.
                    return;
                }
                Err(err) => {
                    error!(&logger, "Subgraph instance failed to run: {:#}", err);
                    subgraph_metrics.deployment_status.failed();
                }
            }

            subgraph_metrics.unregister(registry);
        });

        Ok(())
    }

    pub fn new_deployment_status_metric(
        &self,
        deployment: &DeploymentLocator,
    ) -> DeploymentStatusMetric {
        DeploymentStatusMetric::register(&self.metrics_registry, deployment)
    }
}
