use slog::Logger;
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::timer::DeadlineError;
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
) -> impl Future<Item = I, Error = DeadlineError<E>> + Send
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
) -> impl Future<Item = I, Error = DeadlineError<E>> + Send
where
    F: Fn() -> R + Send,
    R: Future<Item = I, Error = E> + Send,
    I: Send,
    E: Send,
{
    trace!(logger, "with_retry_log_after: {}", operation_name);

    let max_delay_ms = 30_000;
    let retry_strategy = ExponentialBackoff::from_millis(2)
        .max_delay(Duration::from_millis(max_delay_ms))
        .map(jitter);

    let mut attempt_count = 0;
    Retry::spawn(retry_strategy, move || {
        let operation_name = operation_name.clone();
        let logger = logger.clone();

        attempt_count += 1;

        try_it()
            .deadline(Instant::now() + Duration::from_secs(60))
            .map_err(move |e| {
                if e.is_elapsed() {
                    if attempt_count >= log_after {
                        debug!(
                            logger,
                            "Trying again after {} timed out (attempt #{})",
                            &operation_name,
                            attempt_count + 1,
                        );
                    }

                    e
                } else {
                    if attempt_count >= log_after {
                        debug!(
                            logger,
                            "Trying again after {} failed (attempt #{})",
                            &operation_name,
                            attempt_count + 1,
                        );
                    }

                    e
                }
            })
    }).map_err(|e| match e {
        RetryError::OperationError(e) => e,
        RetryError::TimerError(e) => panic!("tokio timer error: {}", e),
    })
}

/// Repeatedly invoke `try_it` until the future it returns resolves to an `Ok`,
/// or `max_retry` consecutive invocations all returned futures that resolved to an `Err`.
pub fn with_retry_max_retry<F, R, I, E>(
    max_retry_count: usize,
    try_it: F,
) -> impl Future<Item = I, Error = DeadlineError<E>> + Send
where
    F: Fn() -> R + Send,
    R: Future<Item = I, Error = E> + Send,
    I: Send,
    E: Send,
{
    let max_delay_ms = 30_000;
    let retry_strategy = ExponentialBackoff::from_millis(2)
        .max_delay(Duration::from_millis(max_delay_ms))
        .map(jitter)
        .take(max_retry_count);

    Retry::spawn(retry_strategy, move || {
        try_it().deadline(Instant::now() + Duration::from_secs(60))
    }).map_err(|e| match e {
        RetryError::OperationError(e) => e,
        RetryError::TimerError(e) => panic!("tokio timer error: {}", e),
    })
}
