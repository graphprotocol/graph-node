use backtrace::Backtrace;
use slog;
use slog_async;
use slog_envlogger;
use slog_term;
use std::{env, panic};

use slog::Drain;

pub fn logger() -> slog::Logger {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_envlogger::new(drain);
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
        let panic_payload = panic_info.payload().downcast_ref::<String>();
        let panic_location = if let Some(location) = panic_info.location() {
            format!("{}:{}", location.file(), location.line().to_string())
        } else {
            "NA".to_string()
        };
        crit!(panic_logger, "Node error";
            "error" => panic_payload.clone(),
            "location" => panic_location.clone()
           );
        match env::var_os("RUST_BACKTRACE") {
            Some(ref val) if val != "0" => {
                crit!(panic_logger, "Backtrace";
                    "trace" => format!("{:?}", Backtrace::new()));
            }
            _ => (),
        }
    }));
}
