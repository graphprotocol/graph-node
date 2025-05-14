use super::{BlockNumber, DeploymentSchemaVersion};
use crate::prelude::DeploymentHash;
use crate::prelude::QueryExecutionError;

use anyhow::{anyhow, Error};
use diesel::result::Error as DieselError;
use thiserror::Error;
use tokio::task::JoinError;

pub type StoreResult<T> = Result<T, StoreError>;

#[derive(Error, Debug)]
pub enum StoreError {
    #[error("store error: {0:#}")]
    Unknown(Error),
    #[error(
        "tried to set entity of type `{0}` with ID \"{1}\" but an entity of type `{2}`, \
         which has an interface in common with `{0}`, exists with the same ID"
    )]
    ConflictingId(String, String, String), // (entity, id, conflicting_entity)
    #[error("table '{0}' does not have a field '{1}'")]
    UnknownField(String, String),
    #[error("unknown table '{0}'")]
    UnknownTable(String),
    #[error("entity type '{0}' does not have an attribute '{0}'")]
    UnknownAttribute(String, String),
    #[error("query execution failed: {0}")]
    QueryExecutionError(String),
    #[error("Child filter nesting not supported by value `{0}`: `{1}`")]
    ChildFilterNestingNotSupportedError(String, String),
    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),
    #[error(
        "subgraph `{0}` has already processed block `{1}`; \
         there are most likely two (or more) nodes indexing this subgraph"
    )]
    DuplicateBlockProcessing(DeploymentHash, BlockNumber),
    /// An internal error where we expected the application logic to enforce
    /// some constraint, e.g., that subgraph names are unique, but found that
    /// constraint to not hold
    #[error("internal error: {0}")]
    InternalError(String),
    #[error("deployment not found: {0}")]
    DeploymentNotFound(String),
    #[error("shard not found: {0} (this usually indicates a misconfiguration)")]
    UnknownShard(String),
    #[error("Fulltext search not yet deterministic")]
    FulltextSearchNonDeterministic,
    #[error("Fulltext search column missing configuration")]
    FulltextColumnMissingConfig,
    #[error("operation was canceled")]
    Canceled,
    #[error("database unavailable")]
    DatabaseUnavailable,
    #[error("subgraph forking failed: {0}")]
    ForkFailure(String),
    #[error("subgraph writer poisoned by previous error")]
    Poisoned,
    #[error("panic in subgraph writer: {0}")]
    WriterPanic(JoinError),
    #[error(
        "found schema version {0} but this graph node only supports versions up to {latest}. \
         Did you downgrade Graph Node?",
        latest = DeploymentSchemaVersion::LATEST
    )]
    UnsupportedDeploymentSchemaVersion(i32),
    #[error("pruning failed: {0}")]
    PruneFailure(String),
    #[error("unsupported filter `{0}` for value `{1}`")]
    UnsupportedFilter(String, String),
    #[error("writing {0} entities at block {1} failed: {2} Query: {3}")]
    WriteFailure(String, BlockNumber, String, String),
    #[error("database query timed out")]
    StatementTimeout,
    #[error("database constraint violated: {0}")]
    ConstraintViolation(String),
}

// Convenience to report an internal error
#[macro_export]
macro_rules! internal_error {
    ($msg:expr) => {{
        $crate::prelude::StoreError::InternalError(format!("{}", $msg))
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        $crate::prelude::StoreError::InternalError(format!($fmt, $($arg)*))
    }}
}

/// We can't derive `Clone` because some variants use non-cloneable data.
/// For those cases, produce an `Unknown` error with some details about the
/// original error
impl Clone for StoreError {
    fn clone(&self) -> Self {
        match self {
            Self::Unknown(arg0) => Self::Unknown(anyhow!("{}", arg0)),
            Self::ConflictingId(arg0, arg1, arg2) => {
                Self::ConflictingId(arg0.clone(), arg1.clone(), arg2.clone())
            }
            Self::UnknownField(arg0, arg1) => Self::UnknownField(arg0.clone(), arg1.clone()),
            Self::UnknownTable(arg0) => Self::UnknownTable(arg0.clone()),
            Self::UnknownAttribute(arg0, arg1) => {
                Self::UnknownAttribute(arg0.clone(), arg1.clone())
            }
            Self::QueryExecutionError(arg0) => Self::QueryExecutionError(arg0.clone()),
            Self::ChildFilterNestingNotSupportedError(arg0, arg1) => {
                Self::ChildFilterNestingNotSupportedError(arg0.clone(), arg1.clone())
            }
            Self::InvalidIdentifier(arg0) => Self::InvalidIdentifier(arg0.clone()),
            Self::DuplicateBlockProcessing(arg0, arg1) => {
                Self::DuplicateBlockProcessing(arg0.clone(), arg1.clone())
            }
            Self::InternalError(arg0) => Self::InternalError(arg0.clone()),
            Self::DeploymentNotFound(arg0) => Self::DeploymentNotFound(arg0.clone()),
            Self::UnknownShard(arg0) => Self::UnknownShard(arg0.clone()),
            Self::FulltextSearchNonDeterministic => Self::FulltextSearchNonDeterministic,
            Self::FulltextColumnMissingConfig => Self::FulltextColumnMissingConfig,
            Self::Canceled => Self::Canceled,
            Self::DatabaseUnavailable => Self::DatabaseUnavailable,
            Self::ForkFailure(arg0) => Self::ForkFailure(arg0.clone()),
            Self::Poisoned => Self::Poisoned,
            Self::WriterPanic(arg0) => Self::Unknown(anyhow!("writer panic: {}", arg0)),
            Self::UnsupportedDeploymentSchemaVersion(arg0) => {
                Self::UnsupportedDeploymentSchemaVersion(arg0.clone())
            }
            Self::PruneFailure(arg0) => Self::PruneFailure(arg0.clone()),
            Self::UnsupportedFilter(arg0, arg1) => {
                Self::UnsupportedFilter(arg0.clone(), arg1.clone())
            }
            Self::WriteFailure(arg0, arg1, arg2, arg3) => {
                Self::WriteFailure(arg0.clone(), arg1.clone(), arg2.clone(), arg3.clone())
            }
            Self::StatementTimeout => Self::StatementTimeout,
            Self::ConstraintViolation(arg0) => Self::ConstraintViolation(arg0.clone()),
        }
    }
}

impl StoreError {
    pub fn from_diesel_error(e: &DieselError) -> Option<Self> {
        const CONN_CLOSE: &str = "server closed the connection unexpectedly";
        const STMT_TIMEOUT: &str = "canceling statement due to statement timeout";
        const UNIQUE_CONSTR: &str = "duplicate key value violates unique constraint";
        let DieselError::DatabaseError(_, info) = e else {
            return None;
        };
        if info.message().contains(CONN_CLOSE) {
            // When the error is caused by a closed connection, treat the error
            // as 'database unavailable'. When this happens during indexing, the
            // indexing machinery will retry in that case rather than fail the
            // subgraph
            Some(StoreError::DatabaseUnavailable)
        } else if info.message().contains(STMT_TIMEOUT) {
            Some(StoreError::StatementTimeout)
        } else if info.message().contains(UNIQUE_CONSTR) {
            let msg = match info.details() {
                Some(details) => format!("{}: {}", info.message(), details.replace('\n', " ")),
                None => info.message().to_string(),
            };
            Some(StoreError::ConstraintViolation(msg))
        } else {
            None
        }
    }

    pub fn write_failure(
        error: DieselError,
        entity: &str,
        block: BlockNumber,
        query: String,
    ) -> Self {
        Self::from_diesel_error(&error).unwrap_or_else(|| {
            StoreError::WriteFailure(entity.to_string(), block, error.to_string(), query)
        })
    }

    pub fn is_deterministic(&self) -> bool {
        use StoreError::*;

        // This classification tries to err on the side of caution. If in doubt,
        // assume the error is non-deterministic.
        match self {
            // deterministic errors
            ConflictingId(_, _, _)
            | UnknownField(_, _)
            | UnknownTable(_)
            | UnknownAttribute(_, _)
            | InvalidIdentifier(_)
            | UnsupportedFilter(_, _)
            | ConstraintViolation(_) => true,

            // non-deterministic errors
            Unknown(_)
            | QueryExecutionError(_)
            | ChildFilterNestingNotSupportedError(_, _)
            | DuplicateBlockProcessing(_, _)
            | InternalError(_)
            | DeploymentNotFound(_)
            | UnknownShard(_)
            | FulltextSearchNonDeterministic
            | FulltextColumnMissingConfig
            | Canceled
            | DatabaseUnavailable
            | ForkFailure(_)
            | Poisoned
            | WriterPanic(_)
            | UnsupportedDeploymentSchemaVersion(_)
            | PruneFailure(_)
            | WriteFailure(_, _, _, _)
            | StatementTimeout => false,
        }
    }
}

impl From<DieselError> for StoreError {
    fn from(e: DieselError) -> Self {
        Self::from_diesel_error(&e).unwrap_or_else(|| StoreError::Unknown(e.into()))
    }
}

impl From<::diesel::r2d2::PoolError> for StoreError {
    fn from(e: ::diesel::r2d2::PoolError) -> Self {
        StoreError::Unknown(e.into())
    }
}

impl From<Error> for StoreError {
    fn from(e: Error) -> Self {
        StoreError::Unknown(e)
    }
}

impl From<serde_json::Error> for StoreError {
    fn from(e: serde_json::Error) -> Self {
        StoreError::Unknown(e.into())
    }
}

impl From<QueryExecutionError> for StoreError {
    fn from(e: QueryExecutionError) -> Self {
        StoreError::QueryExecutionError(e.to_string())
    }
}

impl From<std::fmt::Error> for StoreError {
    fn from(e: std::fmt::Error) -> Self {
        StoreError::Unknown(anyhow!("{}", e.to_string()))
    }
}
