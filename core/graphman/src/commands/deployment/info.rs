use std::collections::HashMap;
use std::sync::Arc;

use anyhow::anyhow;
use graph::blockchain::BlockPtr;
use graph::components::store::BlockNumber;
use graph::components::store::DeploymentId;
use graph::components::store::StatusStore;
use graph::data::subgraph::schema::SubgraphHealth;
use graph_store_postgres::connection_pool::ConnectionPool;
use graph_store_postgres::Store;
use itertools::Itertools;

use crate::deployment::Deployment;
use crate::deployment::DeploymentSelector;
use crate::deployment::DeploymentVersionSelector;
use crate::GraphmanError;

#[derive(Clone, Debug)]
pub struct DeploymentStatus {
    pub is_paused: Option<bool>,
    pub is_synced: bool,
    pub health: SubgraphHealth,
    pub earliest_block_number: BlockNumber,
    pub latest_block: Option<BlockPtr>,
    pub chain_head_block: Option<BlockPtr>,
}

pub fn load_deployments(
    primary_pool: ConnectionPool,
    deployment: &DeploymentSelector,
    version: &DeploymentVersionSelector,
) -> Result<Vec<Deployment>, GraphmanError> {
    let mut primary_conn = primary_pool.get()?;

    crate::deployment::load_deployments(&mut primary_conn, &deployment, &version)
}

pub fn load_deployment_statuses(
    store: Arc<Store>,
    deployments: &[Deployment],
) -> Result<HashMap<i32, DeploymentStatus>, GraphmanError> {
    use graph::data::subgraph::status::Filter;

    let deployment_ids = deployments
        .iter()
        .map(|deployment| DeploymentId::new(deployment.id))
        .collect_vec();

    let deployment_statuses = store
        .status(Filter::DeploymentIds(deployment_ids))?
        .into_iter()
        .map(|status| {
            let id = status.id.0;

            let chain = status
                .chains
                .get(0)
                .ok_or_else(|| {
                    GraphmanError::Store(anyhow!(
                        "deployment status has no chains on deployment '{id}'"
                    ))
                })?
                .to_owned();

            Ok((
                id,
                DeploymentStatus {
                    is_paused: status.paused,
                    is_synced: status.synced,
                    health: status.health,
                    earliest_block_number: chain.earliest_block_number.to_owned(),
                    latest_block: chain.latest_block.map(|x| x.to_ptr()),
                    chain_head_block: chain.chain_head_block.map(|x| x.to_ptr()),
                },
            ))
        })
        .collect::<Result<_, GraphmanError>>()?;

    Ok(deployment_statuses)
}
