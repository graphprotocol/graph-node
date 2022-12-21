use slog::{o, Drain, Duplicate, Logger};
use slog_async;
use std::fmt::Debug;

/// Creates an async slog logger that writes to two drains in parallel.
pub fn split_logger<D1, D2>(drain1: D1, drain2: D2) -> Logger
where
    D1: Drain + Send + Debug + 'static,
    D2: Drain + Send + Debug + 'static,
    D1::Err: Debug,
    D2::Err: Debug,
{
    let split_drain = Duplicate::new(drain1.fuse(), drain2.fuse()).fuse();
    let async_drain = slog_async::Async::new(split_drain)
        .chan_size(20000)
        .build()
        .fuse();
    Logger::root(async_drain, o!())
}
