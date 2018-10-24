use backtrace::Backtrace;
use slog;
use slog_async;
use slog_envlogger;
use slog_term;
use std::{env, panic};

use slog::{Drain, FilterLevel};

pub fn logger(show_debug: bool) -> slog::Logger {
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
        ).parse(
            env::var_os("GRAPH_LOG")
                .unwrap_or("".into())
                .to_str()
                .unwrap(),
        ).build();
    let drain = slog_async::Async::new(drain).build().fuse();
    slog::Logger::root(drain, o!())
}

pub fn guarded_logger() -> (slog::Logger, slog_async::AsyncGuard) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let (drain, guard) = slog_async::Async::new(drain).build_with_guard();
    (slog::Logger::root(drain.fuse(), o!()), guard)
}

pub fn register_panic_hook(panic_logger: slog::Logger) {
    panic::set_hook(Box::new(move |panic_info| {
        let panic_payload = panic_info
            .payload()
            .downcast_ref::<String>()
            .cloned()
            .or(panic_info
                .payload()
                .downcast_ref::<&str>()
                .map(|s| s.to_string()));

        let panic_location = if let Some(location) = panic_info.location() {
            format!("{}:{}", location.file(), location.line().to_string())
        } else {
            "NA".to_string()
        };

        match env::var_os("RUST_BACKTRACE") {
            Some(ref val) if val != "0" => {
                crit!(
                    panic_logger, "{}", panic_payload.unwrap();
                    "location" => &panic_location,
                    "backtrace" => format!("{:?}", Backtrace::new()),
                );
            }
            _ => {
                crit!(
                    panic_logger, "{}", panic_payload.unwrap();
                    "location" => &panic_location,
                );
            }
        };
    }));
}
