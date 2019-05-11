use backtrace::Backtrace;
use futures::sync::oneshot;
use slog::{crit, debug, o, Drain, FilterLevel, Logger};
use slog_async;
use slog_envlogger;
use slog_term;
use std::sync::Mutex;
use std::time::Duration;
use std::{env, panic, process, thread};

pub mod codes;
pub mod elastic;
pub mod split;

pub const MAPPING_THREAD_PREFIX: &str = "mapping-thread";

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

pub fn guarded_logger() -> (Logger, slog_async::AsyncGuard) {
    let decorator = slog_term::TermDecorator::new().build();
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let (drain, guard) = slog_async::Async::new(drain).build_with_guard();
    (Logger::root(drain.fuse(), o!()), guard)
}

pub fn register_panic_hook(panic_logger: Logger, shutdown_sender: oneshot::Sender<()>) {
    let shutdown_mutex = Mutex::new(Some(shutdown_sender));
    panic::set_hook(Box::new(move |panic_info| {
        let panic_payload = panic_info
            .payload()
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                panic_info
                    .payload()
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
            });

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

        // Don't kill the process when a mapping thread panics.
        if thread::current()
            .name()
            .filter(|name| name.starts_with(MAPPING_THREAD_PREFIX))
            .is_some()
        {
            return;
        }

        // Send a shutdown signal to main which will attempt to cleanly shutdown the runtime
        // After sending shutdown, the thread sleeps for 3 seconds then forces the process to
        // exit because the shutdown is not always able to cleanly exit all workers
        match shutdown_mutex.lock().unwrap().take() {
            Some(sender) => sender
                .send(())
                .map(|_| ())
                .map_err(|_| {
                    crit!(panic_logger, "Failed to send shutdown signal");
                    ()
                })
                .unwrap_or(()),
            None => debug!(panic_logger, "Shutdown signal already sent"),
        }
        thread::sleep(Duration::from_millis(3000));
        process::exit(1);
    }));
}
