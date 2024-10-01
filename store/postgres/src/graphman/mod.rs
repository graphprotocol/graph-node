use anyhow::Result;
use chrono::Utc;
use diesel::prelude::*;
use graphman_store::CommandKind;
use graphman_store::Execution;
use graphman_store::ExecutionId;
use graphman_store::ExecutionStatus;

use crate::connection_pool::ConnectionPool;

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

impl graphman_store::GraphmanStore for GraphmanStore {
    fn new_execution(&self, kind: CommandKind) -> Result<ExecutionId> {
        let mut conn = self.primary_pool.get()?;

        let id: i64 = diesel::insert_into(gce::table)
            .values((
                gce::kind.eq(kind),
                gce::status.eq(ExecutionStatus::Initializing),
                gce::created_at.eq(Utc::now()),
            ))
            .returning(gce::id)
            .get_result(&mut conn)?;

        Ok(ExecutionId(id))
    }

    fn load_execution(&self, id: ExecutionId) -> Result<Execution> {
        let mut conn = self.primary_pool.get()?;
        let execution = gce::table.find(id).first(&mut conn)?;

        Ok(execution)
    }

    fn mark_execution_as_running(&self, id: ExecutionId) -> Result<()> {
        let mut conn = self.primary_pool.get()?;

        diesel::update(gce::table)
            .set((
                gce::status.eq(ExecutionStatus::Running),
                gce::updated_at.eq(Utc::now()),
            ))
            .filter(gce::id.eq(id))
            .filter(gce::completed_at.is_null())
            .execute(&mut conn)?;

        Ok(())
    }

    fn mark_execution_as_failed(&self, id: ExecutionId, error_message: String) -> Result<()> {
        let mut conn = self.primary_pool.get()?;

        diesel::update(gce::table)
            .set((
                gce::status.eq(ExecutionStatus::Failed),
                gce::error_message.eq(error_message),
                gce::completed_at.eq(Utc::now()),
            ))
            .filter(gce::id.eq(id))
            .execute(&mut conn)?;

        Ok(())
    }

    fn mark_execution_as_succeeded(&self, id: ExecutionId) -> Result<()> {
        let mut conn = self.primary_pool.get()?;

        diesel::update(gce::table)
            .set((
                gce::status.eq(ExecutionStatus::Succeeded),
                gce::completed_at.eq(Utc::now()),
            ))
            .filter(gce::id.eq(id))
            .execute(&mut conn)?;

        Ok(())
    }
}
