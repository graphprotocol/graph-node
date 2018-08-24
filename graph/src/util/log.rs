use slog;
use slog_async;
use slog_term;

use slog::Drain;

pub fn logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

pub fn guarded_logger() -> (slog::Logger, slog_async::AsyncGuard) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let (drain, guard) = slog_async::Async::new(drain).build_with_guard();
    (slog::Logger::root(drain.fuse(), o!()), guard)
}
