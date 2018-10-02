use slog::Logger;
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

    let retry_strategy = ExponentialBackoff::from_millis(2).map(jitter);

    let mut attempt_count = 0;
    Retry::spawn(retry_strategy, move || {
        let operation_name = operation_name.clone();
        let logger = logger.clone();

        attempt_count += 1;
        let delay_ms = 1 << attempt_count;

        try_it().map_err(move |e| {
            warn!(
                logger,
                "Trying again in ~{} seconds after {} failed (attempt #{})",
                (delay_ms as f64 / 1000.0).round(),
                &operation_name,
                attempt_count + 1,
            );

            e
        })
    }).map_err(|e| match e {
        RetryError::OperationError(e) => e,
        RetryError::TimerError(e) => panic!("tokio timer error: {}", e),
    })
}

pub fn with_retry_no_logging<F, R, I, E>(try_it: F) -> impl Future<Item = I, Error = E> + Send
where
    F: Fn() -> R + Send,
    R: Future<Item = I, Error = E> + Send,
    I: Send,
    E: Send,
{
    let retry_strategy = ExponentialBackoff::from_millis(2).map(jitter);

    Retry::spawn(retry_strategy, try_it).map_err(|e| match e {
        RetryError::OperationError(e) => e,
        RetryError::TimerError(e) => panic!("tokio timer error: {}", e),
    })
}
