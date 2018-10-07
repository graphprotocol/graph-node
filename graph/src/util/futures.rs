use slog::Logger;
use std::time::Duration;
use tokio::prelude::*;
use tokio_retry::strategy::{jitter, ExponentialBackoff};
use tokio_retry::Error as RetryError;
use tokio_retry::Retry;

/// Repeatedly invoke `try_it` until the future it returns resolves to an `Ok`.
/// `operation_name` is used in log messages.
/// Warnings will be logged after failed attempts.
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
    // Start logging immediately after first failed attempt
    with_retry_log_after(logger, operation_name, 1, try_it)
}

/// Repeatedly invoke `try_it` until the future it returns resolves to an `Ok`.
/// `operation_name` is used in log messages.
/// Warnings will be logged after failed attempts, but only after at least
/// `log_after` failed attempts.
pub fn with_retry_log_after<F, R, I, E>(
    logger: Logger,
    operation_name: String,
    log_after: u64,
    try_it: F,
) -> impl Future<Item = I, Error = E> + Send
where
    F: Fn() -> R + Send,
    R: Future<Item = I, Error = E> + Send,
    I: Send,
    E: Send,
{
    trace!(logger, "with_retry: {}", operation_name);

    let max_delay_ms = 30_000;
    let retry_strategy = ExponentialBackoff::from_millis(2)
        .max_delay(Duration::from_millis(max_delay_ms))
        .map(jitter);

    let mut attempt_count = 0;
    Retry::spawn(retry_strategy, move || {
        let operation_name = operation_name.clone();
        let logger = logger.clone();

        attempt_count += 1;
        let delay_ms = (1 << attempt_count).min(max_delay_ms);

        try_it().map_err(move |e| {
            if attempt_count >= log_after {
                debug!(
                    logger,
                    "Trying again in ~{} seconds after {} failed (attempt #{})",
                    (delay_ms as f64 / 1000.0).round(),
                    &operation_name,
                    attempt_count + 1,
                );
            }

            e
        })
    }).map_err(|e| match e {
        RetryError::OperationError(e) => e,
        RetryError::TimerError(e) => panic!("tokio timer error: {}", e),
    })
}
