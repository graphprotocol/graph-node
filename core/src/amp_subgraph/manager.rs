use std::collections::HashMap;
use std::sync::Arc;

use alloy::primitives::BlockNumber;
use anyhow::Context;
use async_trait::async_trait;
use graph::{
    amp,
    components::{
        link_resolver::{LinkResolver, LinkResolverContext},
        metrics::MetricsRegistry,
        network_provider::{AmpChainConfig, AmpClients},
        store::{DeploymentLocator, SubgraphStore},
        subgraph::SubgraphInstanceManager,
    },
    env::EnvVars,
    log::factory::LoggerFactory,
    prelude::CheapClone,
};
use slog::{debug, error};
use tokio_util::sync::CancellationToken;

use super::{runner, Metrics, Monitor};

/// Manages Amp subgraph runner futures.
///
/// Creates and schedules Amp subgraph runner futures for execution on demand.
/// Also handles stopping previously started Amp subgraph runners.
pub struct Manager<SS, NC> {
    logger_factory: LoggerFactory,
    metrics_registry: Arc<MetricsRegistry>,
    env_vars: Arc<EnvVars>,
    monitor: Monitor,
    subgraph_store: Arc<SS>,
    link_resolver: Arc<dyn LinkResolver>,
    amp_clients: AmpClients<NC>,
    amp_chain_configs: HashMap<String, AmpChainConfig>,
}

impl<SS, NC> Manager<SS, NC>
where
    SS: SubgraphStore,
    NC: amp::Client,
{
    /// Creates a new Amp subgraph manager.
    pub fn new(
        logger_factory: &LoggerFactory,
        metrics_registry: Arc<MetricsRegistry>,
        env_vars: Arc<EnvVars>,
        cancel_token: &CancellationToken,
        subgraph_store: Arc<SS>,
        link_resolver: Arc<dyn LinkResolver>,
        amp_clients: AmpClients<NC>,
        amp_chain_configs: HashMap<String, AmpChainConfig>,
    ) -> Self {
        let logger = logger_factory.component_logger("AmpSubgraphManager", None);
        let logger_factory = logger_factory.with_parent(logger);

        let monitor = Monitor::new(&logger_factory, cancel_token);

        Self {
            logger_factory,
            metrics_registry,
            env_vars,
            monitor,
            subgraph_store,
            link_resolver,
            amp_clients,
            amp_chain_configs,
        }
    }
}

#[async_trait]
impl<SS, NC> SubgraphInstanceManager for Manager<SS, NC>
where
    SS: SubgraphStore,
    NC: amp::Client + Send + Sync + 'static,
{
    async fn start_subgraph(
        self: Arc<Self>,
        deployment: DeploymentLocator,
        stop_block: Option<i32>,
    ) {
        let manager = self.cheap_clone();

        self.monitor.start(
            deployment.cheap_clone(),
            Box::new(move |cancel_token| {
                Box::pin(async move {
                    let logger = manager.logger_factory.subgraph_logger(&deployment);

                    let store = manager
                        .subgraph_store
                        .cheap_clone()
                        .writable(logger.cheap_clone(), deployment.id, Vec::new().into())
                        .await
                        .context("failed to create writable store")?;

                    let metrics = Metrics::new(
                        &logger,
                        manager.metrics_registry.cheap_clone(),
                        store.cheap_clone(),
                        deployment.hash.cheap_clone(),
                    );

                    let link_resolver = manager
                        .link_resolver
                        .for_manifest(&deployment.hash.to_string())
                        .context("failed to create link resolver")?;

                    let manifest_bytes = link_resolver
                        .cat(
                            &LinkResolverContext::new(&deployment.hash, &logger),
                            &deployment.hash.to_ipfs_link(),
                        )
                        .await
                        .context("failed to load subgraph manifest")?;

                    let raw_manifest: serde_yaml::Mapping = serde_yaml::from_slice(&manifest_bytes)
                        .context("failed to parse subgraph manifest")?;

                    // Extract the network name from the raw manifest to look
                    // up the per-chain Amp client.
                    let network_name = raw_manifest
                        .get(serde_yaml::Value::String("dataSources".to_owned()))
                        .and_then(|ds| ds.as_sequence())
                        .and_then(|ds| ds.first())
                        .and_then(|ds| ds.as_mapping())
                        .and_then(|ds| ds.get(serde_yaml::Value::String("network".to_owned())))
                        .and_then(|n| n.as_str())
                        .map(|s| s.to_owned());

                    let amp_client = match &network_name {
                        Some(network) => match manager.amp_clients.get(network) {
                            Some(client) => client,
                            None => {
                                anyhow::bail!(
                                    "Amp is not configured for chain '{}'; \
                                     cannot start Amp subgraph '{}'",
                                    network,
                                    deployment.hash
                                );
                            }
                        },
                        None => {
                            anyhow::bail!(
                                "no network name found in manifest for Amp subgraph '{}'",
                                deployment.hash
                            );
                        }
                    };

                    let amp_context = network_name.as_deref().and_then(|chain| {
                        manager
                            .amp_chain_configs
                            .get(chain)
                            .map(|cfg| (cfg.context_dataset.clone(), cfg.context_table.clone()))
                    });

                    let mut manifest = amp::Manifest::resolve::<graph_chain_ethereum::Chain, _>(
                        &logger,
                        manager.link_resolver.cheap_clone(),
                        amp_client.cheap_clone(),
                        manager.env_vars.max_spec_version.cheap_clone(),
                        deployment.hash.cheap_clone(),
                        raw_manifest,
                        amp_context,
                    )
                    .await?;

                    if let Some(stop_block) = stop_block {
                        for data_source in manifest.data_sources.iter_mut() {
                            data_source.source.end_block = stop_block as BlockNumber;
                        }
                    }

                    store
                        .start_subgraph_deployment(&logger)
                        .await
                        .context("failed to start subgraph deployment")?;

                    let runner_context = runner::Context::new(
                        &logger,
                        &manager.env_vars.amp,
                        amp_client,
                        store,
                        deployment.hash.cheap_clone(),
                        manifest,
                        metrics,
                    );

                    let runner_result = runner::new_runner(runner_context, cancel_token).await;

                    match manager.subgraph_store.stop_subgraph(&deployment).await {
                        Ok(()) => {
                            debug!(logger, "Subgraph writer stopped");
                        }
                        Err(e) => {
                            error!(logger, "Failed to stop subgraph writer";
                                "e" => ?e
                            );
                        }
                    }

                    runner_result
                })
            }),
        );
    }

    async fn stop_subgraph(&self, deployment: DeploymentLocator) {
        self.monitor.stop(deployment);
    }
}
