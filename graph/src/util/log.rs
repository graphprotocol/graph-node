use backtrace::Backtrace;
use futures::sync::oneshot;
use slog::{crit, o, Drain, FilterLevel, Logger};
use slog_async;
use slog_envlogger;
use slog_term;
use std::sync::Mutex;
use std::time::{Duration, Instant};
use std::{env, panic, process, thread};

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

pub fn register_panic_hook(
    panic_logger: Logger,
    shutdown_sender: Mutex<Option<oneshot::Sender<()>>>,
) {
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

        // Send a shutdown signal to main which will attempt to cleanly shutdown the runtime
        // After sending shutdown, the thread sleeps for 3 seconds then forces the process to
        // exit because the shutdown is not always able to cleanly exit all workers
        shutdown_sender
            .lock()
            .unwrap()
            .take()
            .expect("Shutdown signal already sent")
            .send(())
            .map(|_| ())
            .map_err(|_| ())
            .expect("Failed to signal shutdown");
        thread::sleep(Duration::from_millis(3000));
        process::exit(1);
    }));
}
