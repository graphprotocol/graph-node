//! Output configuration for runner tests.
//!
//! When running locally, verbose output (slog logs, command stdout/stderr) is redirected
//! to `tests/runner-tests.log` while progress messages appear on the console.
//! In CI (detected via `GITHUB_ACTIONS` env var), all output goes to the console.

use slog::{o, Drain, Logger};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

/// Log file name relative to the `tests` crate root.
const LOG_FILE_NAME: &str = "runner-tests.log";

/// Global output configuration, initialized once.
static OUTPUT_CONFIG: OnceLock<OutputConfig> = OnceLock::new();

/// Output configuration for runner tests.
pub struct OutputConfig {
    /// Log file handle (None in CI mode).
    log_file: Option<Mutex<File>>,
    /// Absolute path to log file (None in CI mode).
    log_file_path: Option<PathBuf>,
    /// Whether running in CI.
    is_ci: bool,
}

impl OutputConfig {
    /// Initialize the global output configuration.
    ///
    /// In CI (when `GITHUB_ACTIONS` is set), output goes to stdout.
    /// Locally, verbose output is redirected to the log file.
    ///
    /// Prints the log file path at startup (local only).
    pub fn init() -> &'static Self {
        OUTPUT_CONFIG.get_or_init(|| {
            let is_ci = std::env::var("GITHUB_ACTIONS").is_ok();

            if is_ci {
                OutputConfig {
                    log_file: None,
                    log_file_path: None,
                    is_ci,
                }
            } else {
                let cwd = std::env::current_dir()
                    .expect("Failed to get current directory")
                    .canonicalize()
                    .expect("Failed to canonicalize current directory");
                let log_file_path = cwd.join(LOG_FILE_NAME);

                let file = File::create(&log_file_path).unwrap_or_else(|e| {
                    panic!(
                        "Failed to create log file {}: {}",
                        log_file_path.display(),
                        e
                    )
                });

                let config = OutputConfig {
                    log_file: Some(Mutex::new(file)),
                    log_file_path: Some(log_file_path),
                    is_ci,
                };

                // Print log file path at startup
                config.print_log_file_info();

                config
            }
        })
    }

    /// Get the global output configuration, initializing if needed.
    pub fn get() -> &'static Self {
        Self::init()
    }

    /// Print the log file path to console.
    pub fn print_log_file_info(&self) {
        if let Some(ref path) = self.log_file_path {
            println!("Runner test logs: {}", path.display());
        }
    }

    /// Returns true if running in CI.
    pub fn is_ci(&self) -> bool {
        self.is_ci
    }
}

impl Write for &OutputConfig {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if let Some(ref file_mutex) = self.log_file {
            file_mutex.lock().unwrap().write(buf)
        } else {
            io::stdout().write(buf)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(ref file_mutex) = self.log_file {
            file_mutex.lock().unwrap().flush()
        } else {
            io::stdout().flush()
        }
    }
}

/// Create a slog logger for a test, respecting the output configuration.
///
/// In CI, logs go to stdout. Locally, logs go to the log file.
pub fn test_logger(test_name: &str) -> Logger {
    let output = OutputConfig::get();

    if output.is_ci {
        // CI: use default logger that writes to stdout
        graph::log::logger(true).new(o!("test" => test_name.to_string()))
    } else {
        // Local: write to log file
        let decorator = slog_term::PlainDecorator::new(LogFileWriter);
        let drain = slog_term::FullFormat::new(decorator).build().fuse();
        let drain = slog_async::Async::new(drain).build().fuse();
        Logger::root(drain, o!("test" => test_name.to_string()))
    }
}

/// A writer that forwards to the OutputConfig log file.
struct LogFileWriter;

impl io::Write for LogFileWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut output = OutputConfig::get();
        output.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut output = OutputConfig::get();
        output.flush()
    }
}
