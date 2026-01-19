//! Spinner and progress output utilities.
//!
//! This module provides spinner functionality matching the output format of
//! the TypeScript graph-cli, which uses gluegun's print.spin() (based on ora).
//!
//! # Examples
//!
//! ```ignore
//! use gnd::output::{with_spinner, Spinner};
//!
//! // Simple usage with closure
//! let result = with_spinner(
//!     "Loading data",
//!     "Failed to load data",
//!     "Data loaded with warnings",
//!     |spinner| {
//!         // Do work...
//!         Ok::<_, anyhow::Error>(42)
//!     }
//! )?;
//!
//! // Or use Spinner directly for more control
//! let mut spinner = Spinner::new("Processing...");
//! spinner.step("Step 1", Some("details"));
//! spinner.succeed("Done!");
//! ```

use std::fmt::Display;
use std::time::Duration;

use console::{style, Term};
use indicatif::{ProgressBar, ProgressStyle};

/// The checkmark symbol used for successful steps (matches TS CLI)
pub const SUCCESS_SYMBOL: &str = "✔";
/// The cross symbol used for failed steps (matches TS CLI)
pub const FAILURE_SYMBOL: &str = "✖";
/// The warning symbol used for warnings
pub const WARNING_SYMBOL: &str = "⚠";

/// A command-line spinner for showing progress on long-running operations.
///
/// This matches the behavior of gluegun's `print.spin()` which is based on ora.
pub struct Spinner {
    progress: ProgressBar,
    term: Term,
}

impl Spinner {
    /// Create a new spinner with the given initial message.
    pub fn new(message: impl Into<String>) -> Self {
        let progress = ProgressBar::new_spinner();
        progress.set_style(
            ProgressStyle::default_spinner()
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏")
                .template("{spinner:.cyan} {msg}")
                .expect("Invalid spinner template"),
        );
        progress.set_message(message.into());
        progress.enable_steady_tick(Duration::from_millis(80));

        Self {
            progress,
            term: Term::stderr(),
        }
    }

    /// Update the spinner message.
    pub fn set_message(&self, message: impl Into<String>) {
        self.progress.set_message(message.into());
    }

    /// Stop the spinner and show a step with optional details.
    ///
    /// The spinner will continue after showing the step (matches TS CLI behavior).
    pub fn step(&self, subject: impl Display, text: Option<impl Display>) {
        self.progress.suspend(|| {
            let msg = if let Some(text) = text {
                format!("{} {}", style(subject).dim(), style(text).dim())
            } else {
                format!("{}", style(subject).dim())
            };
            let _ = self.term.write_line(&msg);
        });
    }

    /// Stop the spinner with a success checkmark and message.
    pub fn succeed(self, message: impl Display) {
        self.progress.finish_and_clear();
        let _ = self
            .term
            .write_line(&format!("{} {}", style(SUCCESS_SYMBOL).green(), message));
    }

    /// Stop the spinner with a failure X and message.
    pub fn fail(self, message: impl Display) {
        self.progress.finish_and_clear();
        let _ = self
            .term
            .write_line(&format!("{} {}", style(FAILURE_SYMBOL).red(), message));
    }

    /// Stop the spinner with a warning symbol and message.
    pub fn warn(self, message: impl Display) {
        self.progress.finish_and_clear();
        let _ = self
            .term
            .write_line(&format!("{} {}", style(WARNING_SYMBOL).yellow(), message));
    }

    /// Stop the spinner, clearing the line without any message.
    pub fn stop(self) {
        self.progress.finish_and_clear();
    }

    /// Stop the spinner and persist with a custom symbol and text.
    pub fn stop_and_persist(self, symbol: impl Display, message: impl Display) {
        self.progress.finish_and_clear();
        let _ = self.term.write_line(&format!("{} {}", symbol, message));
    }
}

/// Result type for spinner operations that can have warnings.
#[derive(Debug)]
pub enum SpinnerResult<T> {
    /// Operation succeeded without warnings
    Ok(T),
    /// Operation succeeded with a warning
    Warning { result: T, warning: String },
    /// Operation failed with an error message
    Error { result: Option<T>, error: String },
}

impl<T> SpinnerResult<T> {
    /// Create a successful result
    pub fn ok(result: T) -> Self {
        SpinnerResult::Ok(result)
    }

    /// Create a result with a warning
    pub fn warning(result: T, warning: impl Into<String>) -> Self {
        SpinnerResult::Warning {
            result,
            warning: warning.into(),
        }
    }

    /// Create an error result
    pub fn error(error: impl Into<String>) -> Self {
        SpinnerResult::Error {
            result: None,
            error: error.into(),
        }
    }

    /// Create an error result with a partial result
    pub fn error_with_result(result: T, error: impl Into<String>) -> Self {
        SpinnerResult::Error {
            result: Some(result),
            error: error.into(),
        }
    }
}

/// Execute a function with a spinner, showing progress and handling errors.
///
/// This matches the TS CLI's `withSpinner` function behavior:
/// - Shows `text` while the operation is in progress
/// - On success, shows `text` with a checkmark
/// - On error, shows `error_text: <error message>` with an X
/// - If the function returns `SpinnerResult::Warning`, shows the warning but still succeeds
///
/// # Arguments
///
/// * `text` - The text to show while the operation is in progress (and on success)
/// * `error_text` - The prefix for error messages
/// * `warning_text` - The prefix for warning messages
/// * `f` - The function to execute
///
/// # Returns
///
/// The result of the function, or an error if the function failed.
///
/// # Examples
///
/// ```ignore
/// let result = with_spinner(
///     "Uploading files",
///     "Failed to upload",
///     "Uploaded with warnings",
///     |spinner| {
///         spinner.step("Preparing", Some("files"));
///         // Do work...
///         Ok::<_, anyhow::Error>(42)
///     }
/// )?;
/// ```
pub fn with_spinner<T, E>(
    text: impl Into<String>,
    error_text: impl Into<String>,
    _warning_text: impl Into<String>,
    f: impl FnOnce(&Spinner) -> Result<T, E>,
) -> Result<T, E>
where
    E: Display,
{
    let text = text.into();
    let error_text = error_text.into();

    let spinner = Spinner::new(&text);

    match f(&spinner) {
        Ok(result) => {
            spinner.succeed(&text);
            Ok(result)
        }
        Err(e) => {
            spinner.fail(format!("{}: {}", error_text, e));
            Err(e)
        }
    }
}

/// Execute a step as part of a spinner, showing the step description.
///
/// This matches the TS CLI's `step` function which shows a muted step
/// description while continuing the spinner.
pub fn step(spinner: &Spinner, subject: impl Display, text: Option<impl Display>) {
    spinner.step(subject, text);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_spinner_result_ok() {
        let result: SpinnerResult<i32> = SpinnerResult::ok(42);
        match result {
            SpinnerResult::Ok(v) => assert_eq!(v, 42),
            _ => panic!("Expected Ok"),
        }
    }

    #[test]
    fn test_spinner_result_warning() {
        let result: SpinnerResult<i32> = SpinnerResult::warning(42, "some warning");
        match result {
            SpinnerResult::Warning { result, warning } => {
                assert_eq!(result, 42);
                assert_eq!(warning, "some warning");
            }
            _ => panic!("Expected Warning"),
        }
    }

    #[test]
    fn test_spinner_result_error() {
        let result: SpinnerResult<i32> = SpinnerResult::error("some error");
        match result {
            SpinnerResult::Error { result, error } => {
                assert!(result.is_none());
                assert_eq!(error, "some error");
            }
            _ => panic!("Expected Error"),
        }
    }

    #[test]
    fn test_spinner_result_error_with_result() {
        let result: SpinnerResult<i32> = SpinnerResult::error_with_result(42, "partial error");
        match result {
            SpinnerResult::Error { result, error } => {
                assert_eq!(result, Some(42));
                assert_eq!(error, "partial error");
            }
            _ => panic!("Expected Error"),
        }
    }

    #[test]
    fn test_with_spinner_success() {
        let result: Result<i32, &str> =
            with_spinner("Testing", "Test failed", "Test warning", |_spinner| {
                Ok::<_, &str>(42)
            });
        assert_eq!(result.unwrap(), 42);
    }

    #[test]
    fn test_with_spinner_error() {
        let result: Result<i32, &str> =
            with_spinner("Testing", "Test failed", "Test warning", |_spinner| {
                Err::<i32, _>("error occurred")
            });
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "error occurred");
    }
}
