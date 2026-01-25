use graph::data::subgraph::schema::SubgraphError;
use graph::env::ENV_VARS;
use graph::prelude::{anyhow, thiserror, Error, StoreError};

pub trait DeterministicError: std::fmt::Debug + std::fmt::Display + Send + Sync + 'static {}

impl DeterministicError for SubgraphError {}

impl DeterministicError for StoreError {}

impl DeterministicError for anyhow::Error {}

/// Classification of processing errors for unified error handling.
///
/// This enum provides a consistent way to categorize errors and determine
/// the appropriate response. The error handling invariants are:
///
/// - **Deterministic**: Stop processing the current block, persist PoI only.
///   The subgraph will be marked as failed. These errors are reproducible
///   and indicate a bug in the subgraph or a permanent data issue.
///
/// - **NonDeterministic**: Retry with exponential backoff. These errors are
///   transient (network issues, temporary database problems) and may succeed
///   on retry.
///
/// - **PossibleReorg**: Restart the block stream cleanly without persisting.
///   The block stream needs to be restarted to detect and handle a potential
///   blockchain reorganization.
///
/// - **Canceled**: The subgraph was canceled (unassigned or shut down).
///   No error should be recorded; this is a clean shutdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingErrorKind {
    /// Error is deterministic - stop processing, persist PoI only
    Deterministic,
    /// Error is non-deterministic - retry with backoff
    NonDeterministic,
    /// Possible blockchain reorg detected - restart block stream cleanly
    PossibleReorg,
    /// Processing was canceled - clean shutdown
    Canceled,
}

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

    /// A possible blockchain reorganization was detected.
    /// The block stream should be restarted to detect and handle the reorg.
    #[error("possible reorg detected: {0:#}")]
    PossibleReorg(Error),

    #[error("subgraph stopped while processing triggers")]
    Canceled,
}

impl ProcessingError {
    /// Classify the error into one of the defined error kinds.
    ///
    /// This method provides a unified way to determine how to handle an error:
    /// - `Deterministic`: Stop processing, persist PoI only
    /// - `NonDeterministic`: Retry with backoff
    /// - `PossibleReorg`: Restart block stream cleanly
    /// - `Canceled`: Clean shutdown, no error recording
    pub fn kind(&self) -> ProcessingErrorKind {
        match self {
            ProcessingError::Unknown(_) => ProcessingErrorKind::NonDeterministic,
            ProcessingError::Deterministic(_) => ProcessingErrorKind::Deterministic,
            ProcessingError::PossibleReorg(_) => ProcessingErrorKind::PossibleReorg,
            ProcessingError::Canceled => ProcessingErrorKind::Canceled,
        }
    }

    #[allow(dead_code)]
    pub fn is_deterministic(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::Deterministic)
    }

    /// Returns true if this error should stop processing the current block.
    ///
    /// Deterministic errors stop processing because continuing would produce
    /// incorrect results. The PoI is still persisted for debugging purposes.
    #[allow(dead_code)]
    pub fn should_stop_processing(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::Deterministic)
    }

    /// Returns true if this error requires a clean restart of the block stream.
    ///
    /// Possible reorgs require restarting to allow the block stream to detect
    /// and properly handle the reorganization. No state should be persisted
    /// in this case.
    #[allow(dead_code)]
    pub fn should_restart(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::PossibleReorg)
    }

    /// Returns true if this error is retryable with exponential backoff.
    ///
    /// Non-deterministic errors (network issues, temporary failures) may
    /// succeed on retry and should not immediately fail the subgraph.
    #[allow(dead_code)]
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::NonDeterministic)
    }

    /// Returns true if processing was canceled (clean shutdown).
    ///
    /// Canceled errors indicate the subgraph was unassigned or shut down
    /// intentionally and should not be treated as failures.
    #[allow(dead_code)]
    pub fn is_canceled(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::Canceled)
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
            ProcessingError::PossibleReorg(e) => {
                ProcessingError::PossibleReorg(e.context(ctx.to_string()))
            }
            ProcessingError::Canceled => ProcessingError::Canceled,
        }
    }
}

/// Similar to `anyhow::Context`, but for `Result<T, ProcessingError>`. We
/// call the method `detail` to avoid ambiguity with anyhow's `context`
/// method
pub trait DetailHelper<T, E> {
    fn detail(self, ctx: &str) -> Result<T, ProcessingError>;
}

impl<T> DetailHelper<T, ProcessingError> for Result<T, ProcessingError> {
    fn detail(self, ctx: &str) -> Result<T, ProcessingError> {
        self.map_err(|e| e.detail(ctx))
    }
}

/// Implement this for errors that are always non-deterministic.
pub(crate) trait NonDeterministicErrorHelper<T, E> {
    fn non_deterministic(self) -> Result<T, ProcessingError>;
}

impl<T> NonDeterministicErrorHelper<T, anyhow::Error> for Result<T, anyhow::Error> {
    fn non_deterministic(self) -> Result<T, ProcessingError> {
        self.map_err(ProcessingError::Unknown)
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
    fn classify(self) -> Result<T, ProcessingError>;
}

impl<T> ClassifyErrorHelper<T, StoreError> for Result<T, StoreError> {
    fn classify(self) -> Result<T, ProcessingError> {
        self.map_err(|e| {
            if ENV_VARS.mappings.store_errors_are_nondeterministic {
                // Old behavior, just in case the new behavior causes issues
                ProcessingError::Unknown(Error::from(e))
            } else if e.is_deterministic() {
                ProcessingError::Deterministic(Box::new(e))
            } else {
                ProcessingError::Unknown(Error::from(e))
            }
        })
    }
}
