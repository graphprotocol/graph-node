use anyhow::Result;
use async_trait::async_trait;
use chrono::Utc;
use diesel::prelude::*;
use diesel_async::RunQueryDsl;
use graphman_store::CommandKind;
use graphman_store::Execution;
use graphman_store::ExecutionId;
use graphman_store::ExecutionStatus;

use crate::ConnectionPool;

mod schema;

use self::schema::graphman_command_executions as gce;

#[derive(Clone)]
pub struct GraphmanStore {
    primary_pool: ConnectionPool,
}

impl GraphmanStore {
    pub fn new(primary_pool: ConnectionPool) -> Self {
        Self { primary_pool }
    }
}

#[async_trait]
impl graphman_store::GraphmanStore for GraphmanStore {
    async fn new_execution(&self, kind: CommandKind) -> Result<ExecutionId> {
        let mut conn = self.primary_pool.get().await?;

        let id: i64 = diesel::insert_into(gce::table)
            .values((
                gce::kind.eq(kind),
                gce::status.eq(ExecutionStatus::Initializing),
                gce::created_at.eq(Utc::now()),
            ))
            .returning(gce::id)
            .get_result(&mut conn)
            .await?;

        Ok(ExecutionId(id))
    }

    async fn load_execution(&self, id: ExecutionId) -> Result<Execution> {
        let mut conn = self.primary_pool.get().await?;
        let execution = gce::table.find(id).first(&mut conn).await?;

        Ok(execution)
    }

    async fn mark_execution_as_running(&self, id: ExecutionId) -> Result<()> {
        let mut conn = self.primary_pool.get().await?;

        diesel::update(gce::table)
            .set((
                gce::status.eq(ExecutionStatus::Running),
                gce::updated_at.eq(Utc::now()),
            ))
            .filter(gce::id.eq(id))
            .filter(gce::completed_at.is_null())
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    async fn mark_execution_as_failed(&self, id: ExecutionId, error_message: String) -> Result<()> {
        let mut conn = self.primary_pool.get().await?;

        diesel::update(gce::table)
            .set((
                gce::status.eq(ExecutionStatus::Failed),
                gce::error_message.eq(error_message),
                gce::completed_at.eq(Utc::now()),
            ))
            .filter(gce::id.eq(id))
            .execute(&mut conn)
            .await?;

        Ok(())
    }

    async fn mark_execution_as_succeeded(&self, id: ExecutionId) -> Result<()> {
        let mut conn = self.primary_pool.get().await?;

        diesel::update(gce::table)
            .set((
                gce::status.eq(ExecutionStatus::Succeeded),
                gce::completed_at.eq(Utc::now()),
            ))
            .filter(gce::id.eq(id))
            .execute(&mut conn)
            .await?;

        Ok(())
    }
}
