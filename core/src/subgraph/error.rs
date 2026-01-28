//! Error types for subgraph processing.
//!
//! This module provides error classification for block processing, enabling
//! consistent error handling across the runner. See [`ProcessingErrorKind`]
//! for the error classification system and its invariants.
//!
//! # Error Handling Invariants
//!
//! These invariants MUST be preserved throughout the codebase:
//!
//! | Error Kind      | Response                                          |
//! |-----------------|---------------------------------------------------|
//! | Deterministic   | Stop processing, persist PoI only, fail subgraph  |
//! | NonDeterministic| Retry with exponential backoff                    |
//! | PossibleReorg   | Restart block stream cleanly (don't persist)      |

use graph::components::subgraph::MappingError;
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

    /// A possible reorg was detected during processing.
    ///
    /// This indicates that the data being processed may be inconsistent due to a
    /// blockchain reorganization. The correct response is to restart the block stream
    /// cleanly without persisting any changes, allowing it to detect and handle the reorg.
    #[error("possible reorg detected: {0:#}")]
    PossibleReorg(Error),
}

/// Classification of processing errors for determining the appropriate response.
///
/// # Error Handling Invariants
///
/// The following invariants MUST be preserved when handling errors:
///
/// - **Deterministic**: Stop processing the current block, persist only the PoI entity,
///   and fail the subgraph. These errors are reproducible and indicate a bug in the
///   subgraph mapping or an invalid state.
///
/// - **NonDeterministic**: Retry with exponential backoff. These errors are transient
///   (network issues, temporary unavailability) and may succeed on retry.
///
/// - **PossibleReorg**: Restart the block stream cleanly without persisting any changes.
///   The block stream will detect the reorg and provide the correct blocks to process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingErrorKind {
    /// Stop processing, persist PoI only, fail the subgraph
    Deterministic,
    /// Retry with backoff, attempt to unfail
    NonDeterministic,
    /// Restart block stream cleanly (don't persist)
    PossibleReorg,
}

impl ProcessingError {
    pub fn is_deterministic(&self) -> bool {
        matches!(self, ProcessingError::Deterministic(_))
    }

    /// Classify this error to determine the appropriate response.
    ///
    /// See [`ProcessingErrorKind`] for the semantics of each classification.
    pub fn kind(&self) -> ProcessingErrorKind {
        match self {
            ProcessingError::Deterministic(_) => ProcessingErrorKind::Deterministic,
            ProcessingError::Unknown(_) => ProcessingErrorKind::NonDeterministic,
            ProcessingError::PossibleReorg(_) => ProcessingErrorKind::PossibleReorg,
            // Canceled is treated as non-deterministic for classification purposes,
            // but it's typically handled specially (clean shutdown).
            ProcessingError::Canceled => ProcessingErrorKind::NonDeterministic,
        }
    }

    /// Whether this error should stop processing the current block.
    ///
    /// Returns `true` for deterministic errors, which indicate a bug in the
    /// subgraph mapping that will reproduce on retry.
    #[allow(dead_code)] // Part of public error classification API
    pub fn should_stop_processing(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::Deterministic)
    }

    /// Whether this error requires a clean restart of the block stream.
    ///
    /// Returns `true` for possible reorg errors, where we need to restart
    /// without persisting any changes so the block stream can detect and
    /// handle the reorganization.
    pub fn should_restart(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::PossibleReorg)
    }

    /// Whether this error is retryable with backoff.
    ///
    /// Returns `true` for non-deterministic errors, which are transient
    /// and may succeed on retry (network issues, temporary unavailability).
    #[allow(dead_code)] // Part of public error classification API
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::NonDeterministic)
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

impl From<MappingError> for ProcessingError {
    fn from(e: MappingError) -> Self {
        match e {
            MappingError::PossibleReorg(e) => ProcessingError::PossibleReorg(e),
            MappingError::Unknown(e) => ProcessingError::Unknown(e),
        }
    }
}

/// Helper trait for converting `MappingError` results to `ProcessingError` results.
///
/// This preserves the `PossibleReorg` variant for proper error handling.
pub(crate) trait MappingErrorHelper<T> {
    fn into_processing_error(self) -> Result<T, ProcessingError>;
}

impl<T> MappingErrorHelper<T> for Result<T, MappingError> {
    fn into_processing_error(self) -> Result<T, ProcessingError> {
        self.map_err(ProcessingError::from)
    }
}
