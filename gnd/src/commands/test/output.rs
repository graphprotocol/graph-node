//! Console output formatting for test results.
//!
//! Formats test results with colored pass/fail indicators and detailed
//! assertion failure diffs showing expected vs actual JSON values.

use console::style;

use super::schema::{AssertionFailure, TestResult};
use crate::output::{step, Step};

/// Print the header line when starting a test file.
pub fn print_test_start(path: &std::path::Path) {
    step(Step::Load, &format!("Running {}", path.display()));
}

/// Print the result of a single test case (pass or fail with details).
pub fn print_test_result(name: &str, result: &TestResult) {
    match result {
        TestResult::Passed => {
            println!("  {} {}", style("✔").green(), name);
        }
        TestResult::Failed {
            handler_error,
            assertion_failures,
        } => {
            println!("  {} {}", style("✘").red(), name);
            if let Some(err) = handler_error {
                println!("    {} {}", style("Handler error:").red(), err);
            }
            for failure in assertion_failures {
                print_assertion_failure(failure);
            }
        }
    }
}

/// Print a detailed assertion failure showing query, expected, and actual values.
fn print_assertion_failure(failure: &AssertionFailure) {
    println!("    {} {}", style("Query:").yellow(), failure.query);
    println!(
        "    {} {}",
        style("Expected:").green(),
        serde_json::to_string_pretty(&failure.expected).unwrap_or_default()
    );
    println!(
        "    {} {}",
        style("Actual:").red(),
        serde_json::to_string_pretty(&failure.actual).unwrap_or_default()
    );
}

/// Print the final summary line with total pass/fail counts.
pub fn print_summary(passed: usize, failed: usize) {
    println!();
    if failed == 0 {
        println!(
            "{}",
            style(format!("Tests: {} passed, {} failed", passed, failed)).green()
        );
    } else {
        println!(
            "{}",
            style(format!("Tests: {} passed, {} failed", passed, failed)).red()
        );
    }
}
