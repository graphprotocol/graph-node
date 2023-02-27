use crate::polling_monitor::IpfsService;
use crate::subgraph::context::{IndexingContext, SubgraphKeepAlive};
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::loader::load_dynamic_data_sources;

use crate::subgraph::runner::SubgraphRunner;
use graph::blockchain::block_stream::BlockStreamMetrics;
use graph::blockchain::Blockchain;
use graph::blockchain::NodeCapabilities;
use graph::blockchain::{BlockchainKind, TriggerFilter};
use graph::components::subgraph::ProofOfIndexingVersion;
use graph::data::subgraph::{UnresolvedSubgraphManifest, SPEC_VERSION_0_0_6};
use graph::data_source::causality_region::CausalityRegionSeq;
use graph::env::EnvVars;
use graph::prelude::{SubgraphInstanceManager as SubgraphInstanceManagerTrait, *};
use graph::{blockchain::BlockchainMap, components::store::DeploymentLocator};
use graph_runtime_wasm::module::ToAscPtr;
use graph_runtime_wasm::RuntimeHostBuilder;
use tokio::task;

use super::context::OffchainMonitor;
use super::SubgraphTriggerProcessor;

#[derive(Clone)]
pub struct SubgraphInstanceManager<S: SubgraphStore> {
    logger_factory: LoggerFactory,
    subgraph_store: Arc<S>,
    chains: Arc<BlockchainMap>,
    metrics_registry: Arc<dyn MetricsRegistry>,
    instances: SubgraphKeepAlive,
    link_resolver: Arc<dyn LinkResolver>,
    ipfs_service: IpfsService,
    static_filters: bool,
    env_vars: Arc<EnvVars>,
}

#[async_trait]
impl<S: SubgraphStore> SubgraphInstanceManagerTrait for SubgraphInstanceManager<S> {
    async fn start_subgraph(
        self: Arc<Self>,
        loc: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
    ) {
        let logger = self.logger_factory.subgraph_logger(&loc);
        let err_logger = logger.clone();
        let instance_manager = self.cheap_clone();

        let subgraph_start_future = async move {
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
                        )
                        .await?;

                    self.start_subgraph_inner(logger, loc, runner).await
                }
                BlockchainKind::Cosmos => {
                    let runner = instance_manager
                        .build_subgraph_runner::<graph_chain_cosmos::Chain>(
                            logger.clone(),
                            self.env_vars.cheap_clone(),
                            loc.clone(),
                            manifest,
                            stop_block,
                            Box::new(SubgraphTriggerProcessor {}),
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
                            Box::new(graph_chain_substreams::TriggerProcessor::new(loc.clone())),
                        )
                        .await?;

                    self.start_subgraph_inner(logger, loc, runner).await
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
                Err(err) => error!(
                    err_logger,
                    "Failed to start subgraph";
                    "error" => format!("{:#}", err),
                    "code" => LogCode::SubgraphStartFailure
                ),
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
        metrics_registry: Arc<dyn MetricsRegistry>,
        link_resolver: Arc<dyn LinkResolver>,
        ipfs_service: IpfsService,
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
        }
    }

    pub async fn build_subgraph_runner<C>(
        &self,
        logger: Logger,
        env_vars: Arc<EnvVars>,
        deployment: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
        tp: Box<dyn TriggerProcessor<C, RuntimeHostBuilder<C>>>,
    ) -> anyhow::Result<SubgraphRunner<C, RuntimeHostBuilder<C>>>
    where
        C: Blockchain,
        <C as Blockchain>::MappingTrigger: ToAscPtr,
    {
        let subgraph_store = self.subgraph_store.cheap_clone();
        let registry = self.metrics_registry.cheap_clone();

        let store = self
            .subgraph_store
            .cheap_clone()
            .writable(logger.clone(), deployment.id)
            .await?;

        let raw_yaml = serde_yaml::to_string(&manifest).unwrap();
        let manifest = UnresolvedSubgraphManifest::parse(deployment.hash.cheap_clone(), manifest)?;

        // Allow for infinite retries for subgraph definition files.
        let link_resolver = Arc::from(self.link_resolver.with_retries());

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

        info!(logger, "Resolve subgraph files using IPFS");

        let mut manifest = manifest
            .resolve(&link_resolver, &logger, ENV_VARS.max_spec_version.clone())
            .await?;

        info!(logger, "Successfully resolved subgraph files using IPFS");

        let manifest_idx_and_name: Vec<(u32, String)> = manifest.template_idx_and_name().collect();

        // Start the subgraph deployment before reading dynamic data
        // sources; if the subgraph is a graft or a copy, starting it will
        // do the copying and dynamic data sources won't show up until after
        // that is done
        store.start_subgraph_deployment(&logger).await?;

        // Dynamic data sources are loaded by appending them to the manifest.
        //
        // Refactor: Preferrably we'd avoid any mutation of the manifest.
        let (manifest, static_data_sources) = {
            let data_sources = load_dynamic_data_sources(store.clone(), logger.clone(), &manifest)
                .await
                .context("Failed to load dynamic data sources")?;

            let static_data_sources = manifest.data_sources.clone();

            // Add dynamic data sources to the subgraph
            manifest.data_sources.extend(data_sources);

            info!(
                logger,
                "Data source count at start: {}",
                manifest.data_sources.len()
            );

            (manifest, static_data_sources)
        };

        let static_filters =
            self.static_filters || manifest.data_sources.len() >= ENV_VARS.static_filters_threshold;

        let onchain_data_sources = manifest
            .data_sources
            .iter()
            .filter_map(|d| d.as_onchain().cloned())
            .collect::<Vec<_>>();
        let required_capabilities = C::NodeCapabilities::from_data_sources(&onchain_data_sources);
        let network = manifest.network_name();

        let chain = self
            .chains
            .get::<C>(network.clone())
            .with_context(|| format!("no chain configured for network {}", network))?
            .clone();

        // if static_filters is enabled, build a minimal filter with the static data sources and
        // add the necessary filters based on templates.
        // if not enabled we just stick to the filter based on all the data sources.
        // This specifically removes dynamic data sources based filters because these can be derived
        // from templates AND this reduces the cost of egress traffic by making the payloads smaller.
        let filter = if static_filters {
            if !self.static_filters {
                info!(logger, "forcing subgraph to use static filters.")
            }

            let onchain_data_sources = static_data_sources.iter().filter_map(|d| d.as_onchain());

            let mut filter = C::TriggerFilter::from_data_sources(onchain_data_sources);

            filter.extend_with_template(
                manifest
                    .templates
                    .iter()
                    .filter_map(|ds| ds.as_onchain())
                    .cloned(),
            );
            filter
        } else {
            C::TriggerFilter::from_data_sources(onchain_data_sources.iter())
        };

        let start_blocks = manifest.start_blocks();

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
        );

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
        ));

        let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
            registry.cheap_clone(),
            deployment.hash.as_str(),
            stopwatch_metrics.clone(),
        ));

        let block_stream_metrics = Arc::new(BlockStreamMetrics::new(
            registry.cheap_clone(),
            &deployment.hash,
            manifest.network_name(),
            store.shard().to_string(),
            stopwatch_metrics,
        ));

        let mut offchain_monitor = OffchainMonitor::new(
            logger.cheap_clone(),
            registry.cheap_clone(),
            &manifest.id,
            self.ipfs_service.clone(),
        );

        // Initialize deployment_head with current deployment head. Any sort of trouble in
        // getting the deployment head ptr leads to initializing with 0
        let deployment_head = store.block_ptr().map(|ptr| ptr.number).unwrap_or(0) as f64;
        block_stream_metrics.deployment_head.set(deployment_head);

        let host_builder = graph_runtime_wasm::RuntimeHostBuilder::new(
            chain.runtime_adapter(),
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

        let instance = super::context::instance::SubgraphInstance::from_manifest(
            &logger,
            manifest,
            host_builder,
            host_metrics.clone(),
            &mut offchain_monitor,
            causality_region_seq,
        )?;

        let inputs = IndexingInputs {
            deployment: deployment.clone(),
            features,
            start_blocks,
            stop_block,
            store,
            debug_fork,
            triggers_adapter,
            chain,
            templates,
            unified_api_version,
            static_filters,
            manifest_idx_and_name,
            poi_version,
            network,
        };

        // The subgraph state tracks the state of the subgraph instance over time
        let ctx = IndexingContext::new(
            instance,
            self.instances.cheap_clone(),
            filter,
            offchain_monitor,
            tp,
        );

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
        let subgraph_metrics_unregister = runner.metrics.subgraph.cheap_clone();

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
            if let Err(e) = graph::block_on(task::unconstrained(runner.run())) {
                error!(
                    &logger,
                    "Subgraph instance failed to run: {}",
                    format!("{:#}", e)
                );
            }
            subgraph_metrics_unregister.unregister(registry);
        });

        Ok(())
    }
}
