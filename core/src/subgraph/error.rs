use graph::data::subgraph::schema::SubgraphError;
use graph::prelude::{thiserror, Error, StoreError};

#[derive(thiserror::Error, Debug)]
pub enum BlockProcessingError {
    #[error("{0:#}")]
    Unknown(Error),

    // The error had a deterministic cause but, for a possibly non-deterministic reason, we chose to
    // halt processing due to the error.
    #[error("{0}")]
    Deterministic(SubgraphError),

    #[error("subgraph stopped while processing triggers")]
    Canceled,
}

impl BlockProcessingError {
    pub fn is_deterministic(&self) -> bool {
        matches!(self, BlockProcessingError::Deterministic(_))
    }
}

pub(crate) trait ErrorHelper<T, E> {
    fn non_deterministic(self: Self) -> Result<T, BlockProcessingError>;
}

impl<T> ErrorHelper<T, anyhow::Error> for Result<T, anyhow::Error> {
    fn non_deterministic(self) -> Result<T, BlockProcessingError> {
        self.map_err(|e| BlockProcessingError::Unknown(e))
    }
}

impl<T> ErrorHelper<T, StoreError> for Result<T, StoreError> {
    fn non_deterministic(self) -> Result<T, BlockProcessingError> {
        self.map_err(|e| BlockProcessingError::Unknown(Error::from(e)))
    }
}
