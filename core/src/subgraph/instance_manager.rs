use crate::polling_monitor::ipfs_service::IpfsService;
use crate::subgraph::context::{IndexingContext, SharedInstanceKeepAliveMap};
use crate::subgraph::inputs::IndexingInputs;
use crate::subgraph::loader::load_dynamic_data_sources;
use crate::subgraph::runner::SubgraphRunner;
use graph::blockchain::block_stream::BlockStreamMetrics;
use graph::blockchain::Blockchain;
use graph::blockchain::NodeCapabilities;
use graph::blockchain::{BlockchainKind, TriggerFilter};
use graph::components::subgraph::ProofOfIndexingVersion;
use graph::data::subgraph::SPEC_VERSION_0_0_6;
use graph::prelude::{SubgraphInstanceManager as SubgraphInstanceManagerTrait, *};
use graph::{blockchain::BlockchainMap, components::store::DeploymentLocator};
use graph_runtime_wasm::module::ToAscPtr;
use graph_runtime_wasm::RuntimeHostBuilder;
use tokio::task;

use super::context::OffchainMonitor;
use super::SubgraphTriggerProcessor;

pub struct SubgraphInstanceManager<S: SubgraphStore> {
    logger_factory: LoggerFactory,
    subgraph_store: Arc<S>,
    chains: Arc<BlockchainMap>,
    metrics_registry: Arc<dyn MetricsRegistry>,
    manager_metrics: SubgraphInstanceManagerMetrics,
    instances: SharedInstanceKeepAliveMap,
    link_resolver: Arc<dyn LinkResolver>,
    ipfs_service: IpfsService,
    static_filters: bool,
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
                    instance_manager
                        .start_subgraph_inner::<graph_chain_arweave::Chain>(
                            logger,
                            loc,
                            manifest,
                            stop_block,
                            Box::new(SubgraphTriggerProcessor {}),
                        )
                        .await
                }
                BlockchainKind::Ethereum => {
                    instance_manager
                        .start_subgraph_inner::<graph_chain_ethereum::Chain>(
                            logger,
                            loc,
                            manifest,
                            stop_block,
                            Box::new(SubgraphTriggerProcessor {}),
                        )
                        .await
                }
                BlockchainKind::Near => {
                    instance_manager
                        .start_subgraph_inner::<graph_chain_near::Chain>(
                            logger,
                            loc,
                            manifest,
                            stop_block,
                            Box::new(SubgraphTriggerProcessor {}),
                        )
                        .await
                }
                BlockchainKind::Cosmos => {
                    instance_manager
                        .start_subgraph_inner::<graph_chain_cosmos::Chain>(
                            logger,
                            loc,
                            manifest,
                            stop_block,
                            Box::new(SubgraphTriggerProcessor {}),
                        )
                        .await
                }
                BlockchainKind::Substreams => {
                    instance_manager
                        .start_subgraph_inner::<graph_chain_substreams::Chain>(
                            logger,
                            loc.cheap_clone(),
                            manifest,
                            stop_block,
                            Box::new(graph_chain_substreams::TriggerProcessor::new(loc)),
                        )
                        .await
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
                Ok(()) => self.manager_metrics.subgraph_count.inc(),
                Err(err) => error!(
                    err_logger,
                    "Failed to start subgraph";
                    "error" => format!("{:#}", err),
                    "code" => LogCode::SubgraphStartFailure
                ),
            }
        });
    }

    fn stop_subgraph(&self, loc: DeploymentLocator) {
        let logger = self.logger_factory.subgraph_logger(&loc);
        info!(logger, "Stop subgraph");

        // Drop the cancel guard to shut down the subgraph now
        let mut instances = self.instances.write().unwrap();
        instances.remove(&loc.id);

        self.manager_metrics.subgraph_count.dec();
    }
}

impl<S: SubgraphStore> SubgraphInstanceManager<S> {
    pub fn new(
        logger_factory: &LoggerFactory,
        subgraph_store: Arc<S>,
        chains: Arc<BlockchainMap>,
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
            manager_metrics: SubgraphInstanceManagerMetrics::new(metrics_registry.cheap_clone()),
            metrics_registry,
            instances: SharedInstanceKeepAliveMap::default(),
            link_resolver,
            ipfs_service,
            static_filters,
        }
    }

    async fn start_subgraph_inner<C: Blockchain>(
        self: Arc<Self>,
        logger: Logger,
        deployment: DeploymentLocator,
        manifest: serde_yaml::Mapping,
        stop_block: Option<BlockNumber>,
        tp: Box<dyn TriggerProcessor<C, RuntimeHostBuilder<C>>>,
    ) -> Result<(), Error>
    where
        <C as Blockchain>::MappingTrigger: ToAscPtr,
    {
        let subgraph_store = self.subgraph_store.cheap_clone();
        let registry = self.metrics_registry.cheap_clone();
        let store = self
            .subgraph_store
            .cheap_clone()
            .writable(logger.clone(), deployment.id)
            .await?;

        // Start the subgraph deployment before reading dynamic data
        // sources; if the subgraph is a graft or a copy, starting it will
        // do the copying and dynamic data sources won't show up until after
        // that is done
        store.start_subgraph_deployment(&logger).await?;

        let (manifest, manifest_idx_and_name) = {
            info!(logger, "Resolve subgraph files using IPFS");

            let mut manifest = SubgraphManifest::resolve_from_raw(
                deployment.hash.cheap_clone(),
                manifest,
                // Allow for infinite retries for subgraph definition files.
                &Arc::from(self.link_resolver.with_retries()),
                &logger,
                ENV_VARS.max_spec_version.clone(),
            )
            .await
            .context("Failed to resolve subgraph from IPFS")?;

            // We cannot include static data sources in the map because a static data source and a
            // template may have the same name in the manifest.
            let ds_len = manifest.data_sources.len() as u32;
            let manifest_idx_and_name: Vec<(u32, String)> = manifest
                .templates
                .iter()
                .map(|t| t.name().to_owned())
                .enumerate()
                .map(|(idx, name)| (ds_len + idx as u32, name))
                .collect();

            let data_sources = load_dynamic_data_sources(
                store.clone(),
                logger.clone(),
                &manifest,
                manifest_idx_and_name.clone(),
            )
            .await
            .context("Failed to load dynamic data sources")?;

            info!(logger, "Successfully resolved subgraph files using IPFS");

            // Add dynamic data sources to the subgraph
            manifest.data_sources.extend(data_sources);

            info!(
                logger,
                "Data source count at start: {}",
                manifest.data_sources.len()
            );

            (manifest, manifest_idx_and_name)
        };

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

        // Obtain filters from the manifest
        let mut filter = C::TriggerFilter::from_data_sources(onchain_data_sources.iter());

        if self.static_filters {
            filter.extend_with_template(
                manifest
                    .templates
                    .iter()
                    .filter_map(|ds| ds.as_onchain())
                    .cloned(),
            );
        }

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

        let subgraph_metrics = Arc::new(SubgraphInstanceMetrics::new(
            registry.cheap_clone(),
            deployment.hash.as_str(),
        ));
        let subgraph_metrics_unregister = subgraph_metrics.clone();
        let host_metrics = Arc::new(HostMetrics::new(
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
            self.ipfs_service.cheap_clone(),
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

        let instance = super::context::instance::SubgraphInstance::from_manifest(
            &logger,
            manifest,
            host_builder,
            host_metrics.clone(),
            &mut offchain_monitor,
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
            static_filters: self.static_filters,
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
            let runner = SubgraphRunner::new(inputs, ctx, logger.cheap_clone(), metrics);
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
