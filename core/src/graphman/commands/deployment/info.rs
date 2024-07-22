use std::collections::HashMap;

use graph::blockchain::BlockPtr;
use graph::components::store::{BlockNumber, StatusStore};
use graph::data::subgraph::schema::SubgraphHealth;
use graph::data::subgraph::status::Info as SubgraphStatus;

use crate::graphman::deployment_search::{Deployment, DeploymentSearch};
use crate::graphman::deployment_search::{DeploymentFilters, DeploymentSelector};
use crate::graphman::{GraphmanContext, GraphmanError};
use crate::graphman_primitives::{BoxedFuture, ExtensibleGraphmanCommand, GraphmanCommand};

#[derive(Clone, Debug)]
pub struct DeploymentInfoCommand {
    pub deployment: DeploymentSelector,
    pub filters: DeploymentFilters,
    pub status: DeploymentStatusSelector,
}

#[derive(Clone, Debug, Default)]
pub enum DeploymentStatusSelector {
    #[default]
    Skip,

    Load,
}

#[derive(Clone, Debug)]
pub struct DeploymentInfo {
    pub id: i32,
    pub hash: String,
    pub namespace: String,
    pub name: String,
    pub node_id: Option<String>,
    pub shard: String,
    pub chain: String,
    pub version_status: String,
    pub is_active: bool,
    pub status: Option<DeploymentStatus>,
}

#[derive(Clone, Debug)]
pub struct DeploymentStatus {
    pub is_paused: Option<bool>,
    pub is_synced: bool,
    pub health: SubgraphHealth,
    pub earliest_block_number: BlockNumber,
    pub latest_block: Option<BlockPtr>,
    pub chain_head_block: Option<BlockPtr>,
}

impl DeploymentInfoCommand {
    fn try_execute(self, ctx: &GraphmanContext) -> Result<Vec<DeploymentInfo>, GraphmanError> {
        let Self {
            deployment,
            filters,
            status,
        } = self;

        let search = DeploymentSearch::new(ctx.pool.clone());

        let deployments = search.by_deployment_and_filters(deployment, filters)?;

        let deployment_ids = deployments.iter().map(|d| d.locator().id).collect();

        let statuses = match status {
            DeploymentStatusSelector::Skip => HashMap::new(),
            DeploymentStatusSelector::Load => {
                use graph::data::subgraph::status::Filter;

                ctx.store
                    .status(Filter::DeploymentIds(deployment_ids))?
                    .into_iter()
                    .map(|status| (status.id.0, status))
                    .collect()
            }
        };

        let deployments_info = deployments
            .into_iter()
            .map(|deployment| {
                let status = Self::make_deployment_status(statuses.get(&deployment.id));

                Self::make_deployment_info(deployment, status)
            })
            .collect();

        Ok(deployments_info)
    }

    fn make_deployment_info(info: Deployment, status: Option<DeploymentStatus>) -> DeploymentInfo {
        let Deployment {
            id,
            hash,
            namespace,
            name,
            node_id,
            shard,
            chain,
            version_status,
            is_active,
        } = info;

        DeploymentInfo {
            id,
            hash,
            namespace,
            name,
            node_id,
            shard,
            chain,
            version_status,
            is_active,
            status,
        }
    }

    fn make_deployment_status(status: Option<&SubgraphStatus>) -> Option<DeploymentStatus> {
        let status = status?;
        let chain = status.chains.get(0)?.to_owned();

        Some(DeploymentStatus {
            is_paused: status.paused,
            is_synced: status.synced,
            health: status.health,
            earliest_block_number: chain.earliest_block_number,
            latest_block: chain.latest_block.map(|x| x.to_ptr()),
            chain_head_block: chain.chain_head_block.map(|x| x.to_ptr()),
        })
    }
}

impl<Ctx> GraphmanCommand<Ctx> for DeploymentInfoCommand
where
    Ctx: AsRef<GraphmanContext> + Send + 'static,
{
    type Output = Vec<DeploymentInfo>;
    type Error = GraphmanError;
    type Future = BoxedFuture<Self::Output, Self::Error>;

    fn execute(self, ctx: Ctx) -> Self::Future {
        Box::pin(async move { self.try_execute(ctx.as_ref()) })
    }
}

impl ExtensibleGraphmanCommand for DeploymentInfoCommand {}
