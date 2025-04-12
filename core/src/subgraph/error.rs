use graph::data::subgraph::schema::SubgraphError;
use graph::env::ENV_VARS;
use graph::prelude::{anyhow, thiserror, Error, StoreError};

pub trait DeterministicError: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static {}

impl DeterministicError for SubgraphError {}

impl DeterministicError for StoreError {}

impl DeterministicError for anyhow::Error {}

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

    pub fn detail(self, ctx: &str) -> ProcessingError {
        match self {
            ProcessingError::Unknown(e) => {
                let x = e.context(ctx.to_string());
                ProcessingError::Unknown(x)
            }
            ProcessingError::Deterministic(e) => {
                ProcessingError::Deterministic(Box::new(anyhow!("{e}").context(ctx.to_string())))
            }
            ProcessingError::Canceled => ProcessingError::Canceled,
        }
    }
}

/// Similar to `anyhow::Context`, but for `Result<T, ProcessingError>`. We
/// call the method `detail` to avoid ambiguity with anyhow's `context`
/// method
pub trait DetailHelper<T, E> {
    fn detail(self: Self, ctx: &str) -> Result<T, ProcessingError>;
}

impl<T> DetailHelper<T, ProcessingError> for Result<T, ProcessingError> {
    fn detail(self, ctx: &str) -> Result<T, ProcessingError> {
        self.map_err(|e| e.detail(ctx))
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

/// Implement this for errors where it depends on the details whether they
/// are deterministic or not.
pub(crate) trait ClassifyErrorHelper<T, E> {
    fn classify(self: Self) -> Result<T, ProcessingError>;
}

impl<T> ClassifyErrorHelper<T, StoreError> for Result<T, StoreError> {
    fn classify(self) -> Result<T, ProcessingError> {
        self.map_err(|e| {
            if ENV_VARS.mappings.store_errors_are_nondeterministic {
                // Old behavior, just in case the new behavior causes issues
                ProcessingError::Unknown(Error::from(e))
            } else {
                if e.is_deterministic() {
                    ProcessingError::Deterministic(Box::new(e))
                } else {
                    ProcessingError::Unknown(Error::from(e))
                }
            }
        })
    }
}
