use graph::data::subgraph::schema::SubgraphError;
use graph::prelude::{thiserror, Error, StoreError};

pub trait DeterministicError: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static {}

impl DeterministicError for SubgraphError {}

/// An error happened during processing and we need to classify errors into
/// deterministic and non-deterministic errors. This struct holds the result
/// of that classification
#[derive(thiserror::Error, Debug)]
pub enum ProcessingError {
    #[error("{0:#}")]
    Unknown(Error),

    // The error had a deterministic cause but, for a possibly non-deterministic reason, we chose to
    // halt processing due to the error.
    #[error("{0}")]
    Deterministic(Box<dyn DeterministicError>),

    #[error("subgraph stopped while processing triggers")]
    Canceled,
}

impl ProcessingError {
    pub fn is_deterministic(&self) -> bool {
        matches!(self, ProcessingError::Deterministic(_))
    }
}
/// Implement this for errors that are always non-deterministic.
pub(crate) trait NonDeterministicErrorHelper<T, E> {
    fn non_deterministic(self: Self) -> Result<T, ProcessingError>;
}

impl<T> NonDeterministicErrorHelper<T, anyhow::Error> for Result<T, anyhow::Error> {
    fn non_deterministic(self) -> Result<T, ProcessingError> {
        self.map_err(|e| ProcessingError::Unknown(e))
    }
}

impl<T> NonDeterministicErrorHelper<T, StoreError> for Result<T, StoreError> {
    fn non_deterministic(self) -> Result<T, ProcessingError> {
        self.map_err(|e| ProcessingError::Unknown(Error::from(e)))
    }
}
