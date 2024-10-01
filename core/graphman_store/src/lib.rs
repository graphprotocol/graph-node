//! This crate allows graphman commands to store data in a persistent storage.
//!
//! Note: The trait is extracted as a separate crate to avoid cyclic dependencies between graphman
//!       commands and store implementations.

use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use diesel::deserialize::FromSql;
use diesel::pg::Pg;
use diesel::pg::PgValue;
use diesel::serialize::Output;
use diesel::serialize::ToSql;
use diesel::sql_types::BigSerial;
use diesel::sql_types::Varchar;
use diesel::AsExpression;
use diesel::FromSqlRow;
use diesel::Queryable;
use strum::Display;
use strum::EnumString;
use strum::IntoStaticStr;

/// Describes all the capabilities that graphman commands need from a persistent storage.
///
/// The primary use case for this is background execution of commands.
pub trait GraphmanStore {
    /// Creates a new pending execution of the specified type.
    /// The implementation is expected to manage execution IDs and return unique IDs on each call.
    ///
    /// Creating a new execution does not mean that a command is actually running or will run.
    fn new_execution(&self, kind: CommandKind) -> Result<ExecutionId>;

    /// Returns all stored execution data.
    fn load_execution(&self, id: ExecutionId) -> Result<Execution>;

    /// When an execution begins to make progress, this method is used to update its status.
    ///
    /// For long-running commands, it is expected that this method will be called at some interval
    /// to show that the execution is still making progress.
    ///
    /// The implementation is expected to not allow updating the status of completed executions.
    fn mark_execution_as_running(&self, id: ExecutionId) -> Result<()>;

    /// This is a finalizing operation and is expected to be called only once,
    /// when an execution fails.
    ///
    /// The implementation is not expected to prevent overriding the final state of an execution.
    fn mark_execution_as_failed(&self, id: ExecutionId, error_message: String) -> Result<()>;

    /// This is a finalizing operation and is expected to be called only once,
    /// when an execution succeeds.
    ///
    /// The implementation is not expected to prevent overriding the final state of an execution.
    fn mark_execution_as_succeeded(&self, id: ExecutionId) -> Result<()>;
}

/// Data stored about a command execution.
#[derive(Clone, Debug, Queryable)]
pub struct Execution {
    pub id: ExecutionId,
    pub kind: CommandKind,
    pub status: ExecutionStatus,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// A unique ID of a command execution.
#[derive(Clone, Copy, Debug, AsExpression, FromSqlRow)]
#[diesel(sql_type = BigSerial)]
pub struct ExecutionId(pub i64);

/// Types of commands that can store data about their execution.
#[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Display, IntoStaticStr, EnumString)]
#[diesel(sql_type = Varchar)]
#[strum(serialize_all = "snake_case")]
pub enum CommandKind {
    RestartDeployment,
}

/// All possible states of a command execution.
#[derive(Clone, Copy, Debug, AsExpression, FromSqlRow, Display, IntoStaticStr, EnumString)]
#[diesel(sql_type = Varchar)]
#[strum(serialize_all = "snake_case")]
pub enum ExecutionStatus {
    Initializing,
    Running,
    Failed,
    Succeeded,
}

impl FromSql<BigSerial, Pg> for ExecutionId {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        Ok(ExecutionId(i64::from_sql(bytes)?))
    }
}

impl ToSql<BigSerial, Pg> for ExecutionId {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        <i64 as ToSql<BigSerial, Pg>>::to_sql(&self.0, &mut out.reborrow())
    }
}

impl FromSql<Varchar, Pg> for CommandKind {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        Ok(std::str::from_utf8(bytes.as_bytes())?.parse()?)
    }
}

impl ToSql<Varchar, Pg> for CommandKind {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        <str as ToSql<Varchar, Pg>>::to_sql(self.into(), &mut out.reborrow())
    }
}

impl FromSql<Varchar, Pg> for ExecutionStatus {
    fn from_sql(bytes: PgValue) -> diesel::deserialize::Result<Self> {
        Ok(std::str::from_utf8(bytes.as_bytes())?.parse()?)
    }
}

impl ToSql<Varchar, Pg> for ExecutionStatus {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> diesel::serialize::Result {
        <str as ToSql<Varchar, Pg>>::to_sql(self.into(), &mut out.reborrow())
    }
}
