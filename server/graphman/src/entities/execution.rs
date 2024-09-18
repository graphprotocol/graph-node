use anyhow::Result;
use async_graphql::Enum;
use async_graphql::SimpleObject;
use chrono::DateTime;
use chrono::Utc;

use crate::entities::CommandKind;
use crate::entities::ExecutionId;

/// Data stored about a command execution.
#[derive(Clone, Debug, SimpleObject)]
pub struct Execution {
    pub id: ExecutionId,
    pub kind: CommandKind,
    pub status: ExecutionStatus,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// All possible states of a command execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Enum)]
#[graphql(remote = "graphman_store::ExecutionStatus")]
pub enum ExecutionStatus {
    Initializing,
    Running,
    Failed,
    Succeeded,
}

impl TryFrom<graphman_store::Execution> for Execution {
    type Error = anyhow::Error;

    fn try_from(execution: graphman_store::Execution) -> Result<Self> {
        let graphman_store::Execution {
            id,
            kind,
            status,
            error_message,
            created_at,
            updated_at,
            completed_at,
        } = execution;

        Ok(Self {
            id: id.into(),
            kind: kind.into(),
            status: status.into(),
            error_message,
            created_at,
            updated_at,
            completed_at,
        })
    }
}
