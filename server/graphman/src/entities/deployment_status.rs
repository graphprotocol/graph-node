use async_graphql::SimpleObject;

use crate::entities::BlockNumber;
use crate::entities::BlockPtr;
use crate::entities::SubgraphHealth;

#[derive(Clone, Debug, SimpleObject)]
pub struct DeploymentStatus {
    pub is_paused: Option<bool>,
    pub is_synced: bool,
    pub health: SubgraphHealth,
    pub earliest_block_number: BlockNumber,
    pub latest_block: Option<BlockPtr>,
    pub chain_head_block: Option<BlockPtr>,
}

impl From<graphman::commands::deployment::info::DeploymentStatus> for DeploymentStatus {
    fn from(status: graphman::commands::deployment::info::DeploymentStatus) -> Self {
        let graphman::commands::deployment::info::DeploymentStatus {
            is_paused,
            is_synced,
            health,
            earliest_block_number,
            latest_block,
            chain_head_block,
        } = status;

        Self {
            is_paused,
            is_synced,
            health: health.into(),
            earliest_block_number: earliest_block_number.into(),
            latest_block: latest_block.map(Into::into),
            chain_head_block: chain_head_block.map(Into::into),
        }
    }
}
