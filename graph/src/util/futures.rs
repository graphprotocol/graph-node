use slog::Logger;
use std::time::Duration;
use tokio::prelude::*;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Error as RetryError;
use tokio_retry::Retry;

pub fn with_retry<F, R, I, E>(
    logger: Logger,
    operation_name: String,
    try_it: F,
) -> impl Future<Item = I, Error = E> + Send
where
    F: Fn() -> R + Send,
    R: Future<Item = I, Error = E> + Send,
    I: Send,
    E: Send,
{
    trace!(logger, "with_retry: {}", operation_name);

    let max_attempts = 10;
    let retry_strategy = ExponentialBackoff::from_millis(250)
        .max_delay(Duration::from_millis(1000))
        .map(jitter)
        .take(max_attempts);

    let mut attempt_count = 0;
    Retry::spawn(retry_strategy, move || {
        let operation_name = operation_name.clone();
        let logger = logger.clone();

        attempt_count += 1;

        try_it().map_err(move |e| {
            if attempt_count < max_attempts {
                warn!(
                    logger,
                    "Trying again after {} failed (attempt {}/{})",
                    &operation_name,
                    attempt_count + 1,
                    max_attempts
                );
            }

            e
        })
    }).map_err(|e| match e {
        RetryError::OperationError(e) => e,
        RetryError::TimerError(e) => panic!("tokio timer error: {}", e),
    })
}
