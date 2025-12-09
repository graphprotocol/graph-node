use std::sync::Arc;

use anyhow::anyhow;
use graph::components::store::DeploymentLocator;
use graph::components::store::StoreEvent;
use graph::prelude::AssignmentChange;
use graph::prelude::NodeId;
use graph_store_postgres::command_support::catalog;
use graph_store_postgres::command_support::catalog::Site;
use graph_store_postgres::ConnectionPool;
use graph_store_postgres::NotificationSender;
use thiserror::Error;

use crate::deployment::DeploymentSelector;
use crate::deployment::DeploymentVersionSelector;
use crate::GraphmanError;

pub struct Deployment {
    locator: DeploymentLocator,
    site: Site,
}

impl Deployment {
    pub fn locator(&self) -> &DeploymentLocator {
        &self.locator
    }

    pub async fn assigned_node(
        &self,
        primary_pool: ConnectionPool,
    ) -> Result<Option<NodeId>, GraphmanError> {
        let primary_conn = primary_pool
            .get_permitted()
            .await
            .map_err(GraphmanError::from)?;
        let mut catalog_conn = catalog::Connection::new(primary_conn);
        let node = catalog_conn
            .assigned_node(&self.site)
            .await
            .map_err(GraphmanError::from)?;
        Ok(node)
    }
}

#[derive(Debug, Error)]
pub enum ReassignDeploymentError {
    #[error("deployment '{0}' is already assigned to '{1}'")]
    AlreadyAssigned(String, String),

    #[error(transparent)]
    Common(#[from] GraphmanError),
}

#[derive(Clone, Debug)]
pub enum ReassignResult {
    Ok,
    CompletedWithWarnings(Vec<String>),
}

pub async fn load_deployment(
    primary_pool: ConnectionPool,
    deployment: &DeploymentSelector,
) -> Result<Deployment, ReassignDeploymentError> {
    let mut primary_conn = primary_pool
        .get_permitted()
        .await
        .map_err(GraphmanError::from)?;

    let locator = crate::deployment::load_deployment_locator(
        &mut primary_conn,
        deployment,
        &DeploymentVersionSelector::All,
    )
    .await?;

    let mut catalog_conn = catalog::Connection::new(primary_conn);

    let site = catalog_conn
        .locate_site(locator.clone())
        .await
        .map_err(GraphmanError::from)?
        .ok_or_else(|| {
            GraphmanError::Store(anyhow!("deployment site not found for '{locator}'"))
        })?;

    Ok(Deployment { locator, site })
}

pub async fn reassign_deployment(
    primary_pool: ConnectionPool,
    notification_sender: Arc<NotificationSender>,
    deployment: &Deployment,
    node: &NodeId,
    curr_node: Option<NodeId>,
) -> Result<ReassignResult, ReassignDeploymentError> {
    let primary_conn = primary_pool
        .get_permitted()
        .await
        .map_err(GraphmanError::from)?;
    let mut catalog_conn = catalog::Connection::new(primary_conn);
    let changes: Vec<AssignmentChange> = match &curr_node {
        Some(curr) => {
            if &curr == &node {
                vec![]
            } else {
                catalog_conn
                    .reassign_subgraph(&deployment.site, &node)
                    .await
                    .map_err(GraphmanError::from)?
            }
        }
        None => catalog_conn
            .assign_subgraph(&deployment.site, &node)
            .await
            .map_err(GraphmanError::from)?,
    };

    if changes.is_empty() {
        return Err(ReassignDeploymentError::AlreadyAssigned(
            deployment.locator.to_string(),
            node.to_string(),
        ));
    }

    catalog_conn
        .send_store_event(&notification_sender, &StoreEvent::new(changes))
        .await
        .map_err(GraphmanError::from)?;

    let mirror = catalog::Mirror::primary_only(primary_pool);
    let count = mirror
        .assignments(&node)
        .await
        .map_err(GraphmanError::from)?
        .len();
    if count == 1 {
        let warning_msg = format!("This is the only deployment assigned to '{}'. Please make sure that the node ID is spelled correctly.",node.as_str());
        Ok(ReassignResult::CompletedWithWarnings(vec![warning_msg]))
    } else {
        Ok(ReassignResult::Ok)
    }
}
