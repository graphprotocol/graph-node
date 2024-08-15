use slog::Logger;

use crate::ipfs::error::IpfsError;
use crate::util::futures::retry;
use crate::util::futures::RetryConfigNoTimeout;

const DEFAULT_MAX_ATTEMPTS: usize = 100;

/// Creates a retry policy for each request sent by IPFS clients.
///
/// Note: It is expected that timeouts will be set on the requests.
pub fn retry_policy<O: Send + Sync + 'static>(
    operation_name: &'static str,
    logger: &Logger,
) -> RetryConfigNoTimeout<O, IpfsError> {
    retry(operation_name, logger)
        .limit(DEFAULT_MAX_ATTEMPTS)
        .when(|result: &Result<O, IpfsError>| match result {
            Ok(_) => false,
            Err(IpfsError::RequestFailed(err)) => !err.is_timeout() && err.is_retriable(),
            Err(_) => false,
        })
        .no_timeout()
}
