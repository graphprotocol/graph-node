use async_trait::async_trait;

use crate::prelude::*;

/// Events emitted by [DeploymentController](trait.DeploymentController.html)s.
#[derive(Debug, PartialEq)]
pub enum DeploymentControllerEvent {
    /// A subgraph with the given manifest should start indexing.
    Start(SubgraphManifest),
    /// The subgraph with the given ID should stop indexing.
    Stop(SubgraphDeploymentId),
}

#[derive(Fail, Debug)]
pub enum DeploymentControllerError {
    #[fail(display = "Subgraph resolve error: {}", _0)]
    ResolveError(SubgraphManifestResolveError),
    #[fail(display = "Failed to load dynamic data sources: {}", _0)]
    DynamicDataSourcesError(Error),
    /// Occurs when attempting to remove a subgraph that's not hosted.
    #[fail(display = "Subgraph with ID {} already running", _0)]
    AlreadyRunning(SubgraphDeploymentId),
    #[fail(display = "Subgraph with ID {} is not running", _0)]
    NotRunning(SubgraphDeploymentId),
    /// Occurs when a subgraph's GraphQL schema is invalid.
    #[fail(display = "GraphQL schema error: {}", _0)]
    SchemaValidationError(Error),
    #[fail(
        display = "Error building index for subgraph {}, entity {} and attribute {}",
        _0, _1, _2
    )]
    BuildIndexesError(String, String, String),
    #[fail(display = "Subgraph provider error: {}", _0)]
    Unknown(Error),
}

impl From<Error> for DeploymentControllerError {
    fn from(e: Error) -> Self {
        DeploymentControllerError::Unknown(e)
    }
}

impl From<::diesel::result::Error> for DeploymentControllerError {
    fn from(e: ::diesel::result::Error) -> Self {
        DeploymentControllerError::Unknown(e.into())
    }
}

/// Common trait for deployment controllers.
#[async_trait]
pub trait DeploymentController:
    EventProducer<DeploymentControllerEvent> + Send + Sync + 'static
{
    async fn start(&self, id: &SubgraphDeploymentId) -> Result<(), DeploymentControllerError>;
    async fn stop(&self, id: &SubgraphDeploymentId) -> Result<(), DeploymentControllerError>;
}
