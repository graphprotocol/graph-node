use slog::Logger;

use crate::ipfs::error::IpfsError;
use crate::util::futures::retry;
use crate::util::futures::RetryConfigNoTimeout;

/// This is a safety mechanism to prevent infinite spamming of IPFS servers
/// in the event of logical or unhandled deterministic errors.
const DEFAULT_MAX_ATTEMPTS: usize = 10_0000;

/// Describes retry behavior when IPFS requests fail.
#[derive(Clone, Copy, Debug)]
pub enum RetryPolicy {
    /// At the first error, immediately stops execution and returns the error.
    None,

    /// Retries the request if the error is related to the network connection.
    Networking,

    /// Retries the request if the error is related to the network connection,
    /// and for any error that may be resolved by sending another request.
    NonDeterministic,
}

impl RetryPolicy {
    /// Creates a retry policy for every request sent to IPFS servers.
    ///
    /// Note: It is expected that retries will be wrapped in timeouts
    ///       when necessary to make them more flexible.
    pub(super) fn create<O: Send + Sync + 'static>(
        self,
        operation_name: impl ToString,
        logger: &Logger,
    ) -> RetryConfigNoTimeout<O, IpfsError> {
        retry(operation_name, logger)
            .limit(DEFAULT_MAX_ATTEMPTS)
            .when(move |result: &Result<O, IpfsError>| match result {
                Ok(_) => false,
                Err(err) => match self {
                    Self::None => false,
                    Self::Networking => err.is_networking(),
                    Self::NonDeterministic => !err.is_deterministic(),
                },
            })
            .no_timeout()
    }
}
