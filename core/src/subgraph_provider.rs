use std::{collections::HashMap, sync::Arc, time::Instant};

use graph::{
    amp,
    cheap_clone::CheapClone as _,
    components::{
        link_resolver::{LinkResolver, LinkResolverContext},
        metrics::subgraph::SubgraphCountMetric,
        store::DeploymentLocator,
        subgraph::SubgraphInstanceManager,
    },
    log::factory::LoggerFactory,
};
use itertools::Itertools as _;
use parking_lot::RwLock;
use slog::{debug, error};
use tokio_util::sync::CancellationToken;

/// Starts and stops subgraph deployments.
///
/// For each subgraph deployment, checks the subgraph processing kind
/// and finds the appropriate subgraph instance manager to handle the
/// processing of the subgraph deployment.
///
/// This is required to support both trigger-based subgraphs and Amp-powered subgraphs,
/// which have separate runners.
pub struct SubgraphProvider {
    logger_factory: LoggerFactory,
    count_metrics: Arc<SubgraphCountMetric>,
    link_resolver: Arc<dyn LinkResolver>,

    /// Stops active subgraph start request tasks.
    ///
    /// When a subgraph deployment start request is processed, a background task is created
    /// to load the subgraph manifest and determine the subgraph processing kind. The processing
    /// kind is then used to find the appropriate subgraph instance manager. This token stops
    /// all tasks that are still loading manifests or waiting for subgraphs to start.
    cancel_token: CancellationToken,

    /// Contains the enabled subgraph instance managers.
    ///
    /// Only subgraphs for which there is an appropriate instance manager will be started.
    instance_managers: SubgraphInstanceManagers,

    /// Maintains a list of started subgraphs with their processing kinds.
    ///
    /// Used to forward subgraph deployment stop requests to the appropriate subgraph instance manager.
    assignments: SubgraphAssignments,
}

impl SubgraphProvider {
    /// Creates a new subgraph provider.
    ///
    /// # Arguments
    /// - `logger_factory`: Creates loggers for each subgraph deployment start/stop request
    /// - `count_metrics`: Tracks the number of started subgraph deployments
    /// - `link_resolver`: Loads subgraph manifests to determine the subgraph processing kinds
    /// - `cancel_token`: Stops active subgraph start request tasks
    /// - `instance_managers`: Contains the enabled subgraph instance managers
    pub fn new(
        logger_factory: &LoggerFactory,
        count_metrics: Arc<SubgraphCountMetric>,
        link_resolver: Arc<dyn LinkResolver>,
        cancel_token: CancellationToken,
        instance_managers: SubgraphInstanceManagers,
    ) -> Self {
        let logger = logger_factory.component_logger("SubgraphProvider", None);
        let logger_factory = logger_factory.with_parent(logger.cheap_clone());

        debug!(logger, "Creating subgraph provider";
            "enabled_subgraph_processing_kinds" => instance_managers.0.keys().join(", ")
        );

        Self {
            logger_factory,
            count_metrics,
            link_resolver,
            cancel_token,
            instance_managers,
            assignments: SubgraphAssignments::new(),
        }
    }

    /// Starts a subgraph deployment with the appropriate subgraph instance manager.
    ///
    /// Loads the subgraph manifest for the specified deployment locator, determines
    /// the subgraph processing kind, finds the required instance manager, and forwards
    /// the start request to that instance manager. Keeps the subgraph processing kind
    /// in memory for handling the stop requests.
    async fn assign_and_start_subgraph(
        &self,
        loc: DeploymentLocator,
        stop_block: Option<i32>,
    ) -> Result<(), Error> {
        let logger = self.logger_factory.subgraph_logger(&loc);

        let link_resolver = self
            .link_resolver
            .for_manifest(&loc.hash.to_string())
            .map_err(|e| Error::CreateLinkResolver {
                loc: loc.cheap_clone(),
                source: e,
            })?;

        let file_bytes = link_resolver
            .cat(
                &LinkResolverContext::new(&loc.hash, &logger),
                &loc.hash.to_ipfs_link(),
            )
            .await
            .map_err(|e| Error::LoadManifest {
                loc: loc.cheap_clone(),
                source: e,
            })?;

        let raw_manifest: serde_yaml::Mapping =
            serde_yaml::from_slice(&file_bytes).map_err(|e| Error::ParseManifest {
                loc: loc.cheap_clone(),
                source: e,
            })?;

        let subgraph_kind = SubgraphProcessingKind::from_manifest(&raw_manifest);
        self.assignments.set_subgraph_kind(&loc, subgraph_kind);

        let Some(instance_manager) = self.instance_managers.get(&subgraph_kind) else {
            return Err(Error::GetManager { loc, subgraph_kind });
        };

        instance_manager.start_subgraph(loc, stop_block).await;
        Ok(())
    }
}

#[async_trait::async_trait]
impl SubgraphInstanceManager for SubgraphProvider {
    async fn start_subgraph(self: Arc<Self>, loc: DeploymentLocator, stop_block: Option<i32>) {
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(slog::o!("method" => "start_subgraph"));

        if self.assignments.is_assigned(&loc) {
            debug!(logger, "Subgraph is already started");
            return;
        }

        self.count_metrics.deployment_count.inc();

        let handle = tokio::spawn({
            let provider = self.cheap_clone();
            let loc = loc.cheap_clone();
            let start_instant = Instant::now();

            async move {
                debug!(logger, "Starting subgraph");

                let fut = provider.assign_and_start_subgraph(loc, stop_block);
                match provider.cancel_token.run_until_cancelled(fut).await {
                    Some(Ok(())) => {
                        debug!(logger, "Subgraph started";
                            "duration_ms" => start_instant.elapsed().as_millis()
                        );
                    }
                    Some(Err(e)) => {
                        error!(logger, "Subgraph failed to start";
                            "e" => ?e
                        );
                    }
                    None => {
                        debug!(logger, "Subgraph start cancelled");
                    }
                }
            }
        });

        self.assignments.add(
            loc,
            SubgraphAssignment {
                handle,
                subgraph_kind: None,
            },
        )
    }

    async fn stop_subgraph(&self, loc: DeploymentLocator) {
        let logger = self
            .logger_factory
            .subgraph_logger(&loc)
            .new(slog::o!("method" => "stop_subgraph"));

        debug!(logger, "Stopping subgraph");

        let Some(SubgraphAssignment {
            handle,
            subgraph_kind,
        }) = self.assignments.take(&loc)
        else {
            debug!(logger, "Subgraph is not started");
            return;
        };

        handle.abort();
        self.count_metrics.deployment_count.dec();

        let Some(subgraph_kind) = subgraph_kind else {
            debug!(logger, "Unknown subgraph kind");
            return;
        };

        let Some(instance_manager) = self.instance_managers.get(&subgraph_kind) else {
            debug!(logger, "Missing instance manager");
            return;
        };

        instance_manager.stop_subgraph(loc).await;
        debug!(logger, "Subgraph stopped");
    }
}

/// Enumerates all possible errors of the subgraph provider.
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("failed to create link resolver for '{loc}': {source:#}")]
    CreateLinkResolver {
        loc: DeploymentLocator,
        source: anyhow::Error,
    },

    #[error("failed to load manifest for '{loc}': {source:#}")]
    LoadManifest {
        loc: DeploymentLocator,
        source: anyhow::Error,
    },

    #[error("failed to parse manifest for '{loc}': {source:#}")]
    ParseManifest {
        loc: DeploymentLocator,
        source: serde_yaml::Error,
    },

    #[error("failed to get instance manager for '{loc}' with kind '{subgraph_kind}'")]
    GetManager {
        loc: DeploymentLocator,
        subgraph_kind: SubgraphProcessingKind,
    },
}

/// Contains a mapping of enabled subgraph instance managers by subgraph processing kinds.
///
/// Before starting a subgraph, its processing kind is determined from the subgraph manifest.
/// Then, the appropriate instance manager is loaded from this mapping.
pub struct SubgraphInstanceManagers(
    HashMap<SubgraphProcessingKind, Arc<dyn SubgraphInstanceManager>>,
);

impl SubgraphInstanceManagers {
    /// Creates a new empty subgraph instance manager mapping.
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Adds a new subgraph instance manager for all subgraphs of the specified processing kind.
    pub fn add(
        &mut self,
        subgraph_kind: SubgraphProcessingKind,
        instance_manager: Arc<dyn SubgraphInstanceManager>,
    ) {
        self.0.insert(subgraph_kind, instance_manager);
    }

    /// Returns the subgraph instance manager for the specified processing kind.
    pub fn get(
        &self,
        subgraph_kind: &SubgraphProcessingKind,
    ) -> Option<Arc<dyn SubgraphInstanceManager>> {
        self.0
            .get(subgraph_kind)
            .map(|instance_manager| instance_manager.cheap_clone())
    }
}

/// Enumerates the supported subgraph processing kinds.
///
/// Subgraphs may have different processing requirements, and this enum helps to map them
/// to the appropriate instance managers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::Display)]
#[strum(serialize_all = "snake_case")]
pub enum SubgraphProcessingKind {
    /// Represents trigger-based subgraphs.
    Trigger,

    /// Represents Amp-powered subgraphs.
    Amp,
}

impl SubgraphProcessingKind {
    /// Determines the subgraph processing kind from the subgraph manifest.
    fn from_manifest(raw_manifest: &serde_yaml::Mapping) -> Self {
        use serde_yaml::Value;

        let is_amp_manifest = raw_manifest
            .get("dataSources")
            .and_then(Value::as_sequence)
            .and_then(|seq| {
                seq.iter()
                    .filter_map(Value::as_mapping)
                    .filter_map(|map| map.get("kind"))
                    .filter_map(Value::as_str)
                    .filter(|kind| *kind == amp::manifest::DataSource::KIND)
                    .next()
            })
            .is_some();

        if is_amp_manifest {
            return Self::Amp;
        }

        Self::Trigger
    }
}

/// Maintains a list of started subgraph deployments with details required for stopping them.
struct SubgraphAssignments(RwLock<HashMap<DeploymentLocator, SubgraphAssignment>>);

impl SubgraphAssignments {
    /// Creates a new empty list of started subgraph deployments.
    fn new() -> Self {
        Self(RwLock::new(HashMap::new()))
    }

    /// Adds a new subgraph deployment to the list of started subgraph deployments.
    fn add(&self, loc: DeploymentLocator, subgraph_assignment: SubgraphAssignment) {
        self.0.write().insert(loc, subgraph_assignment);
    }

    /// Updates the started subgraph deployment with the specified subgraph processing kind.
    fn set_subgraph_kind(&self, loc: &DeploymentLocator, subgraph_kind: SubgraphProcessingKind) {
        if let Some(subgraph_assignment) = self.0.write().get_mut(loc) {
            subgraph_assignment.subgraph_kind = Some(subgraph_kind);
        }
    }

    /// Checks if the subgraph deployment is started.
    fn is_assigned(&self, loc: &DeploymentLocator) -> bool {
        self.0.read().contains_key(loc)
    }

    /// Removes the subgraph deployment from the list of started subgraph deployments and returns its details.
    fn take(&self, loc: &DeploymentLocator) -> Option<SubgraphAssignment> {
        self.0.write().remove(loc)
    }
}

/// Contains the details of a started subgraph deployment.
struct SubgraphAssignment {
    /// The handle to the background task that starts this subgraph deployment.
    handle: tokio::task::JoinHandle<()>,

    /// The subgraph processing kind of this subgraph deployment.
    ///
    /// Used to get the appropriate subgraph instance manager to forward the stop request to.
    ///
    /// Set to `None` until the subgraph manifest is loaded and parsed.
    subgraph_kind: Option<SubgraphProcessingKind>,
}
