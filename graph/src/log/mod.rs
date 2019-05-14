use slog::{o, Drain, FilterLevel, Logger};
use slog_async;
use slog_envlogger;
use slog_term;
use std::env;

pub mod codes;
pub mod elastic;
pub mod split;

pub fn logger(show_debug: bool) -> Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::LogBuilder::new(drain)
        .filter(
            None,
            if show_debug {
                FilterLevel::Debug
            } else {
                FilterLevel::Info
            },
        )
        .parse(
            env::var_os("GRAPH_LOG")
                .unwrap_or_else(|| "".into())
                .to_str()
                .unwrap(),
        )
        .build();
    let drain = slog_async::Async::new(drain).build().fuse();
    Logger::root(drain, o!())
}
