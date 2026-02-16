//! Console output formatting for test results.
//!
//! Formats test results with colored pass/fail indicators per query and
//! detailed assertion failure diffs collected at the end of the run.

use console::style;
use similar::{ChangeTag, TextDiff};

use super::assertion::align_for_diff;
use super::schema::{AssertionFailure, AssertionOutcome, TestResult};
use crate::output::{step, Step};

/// Print the header line when starting a test file.
pub fn print_test_start(path: &std::path::Path) {
    step(Step::Load, &format!("Running {}", path.display()));
}

/// Print the result of a single test case with per-query pass/fail indicators.
///
/// Shows ✔/✘ for the test name, then ✔/✘ for each individual assertion query.
/// Detailed diffs are NOT printed here — they are collected and printed at the end
/// via [`print_failure_details`].
pub fn print_test_result(name: &str, result: &TestResult) {
    if result.is_passed() {
        println!("  {} {}", style("✔").green(), name);
    } else {
        println!("  {} {}", style("✘").red(), name);
    }

    if let Some(err) = result.handler_error() {
        println!("    {} {}", style("Handler error:").red(), err);
    }

    for outcome in result.assertions() {
        match outcome {
            AssertionOutcome::Passed { query } => {
                println!("    {} {}", style("✔").green(), style(query).dim());
            }
            AssertionOutcome::Failed(failure) => {
                println!("    {} {}", style("✘").red(), failure.query);
            }
        }
    }
}

/// Collected failure info for deferred output.
pub struct FailureDetail {
    /// Name of the test that failed.
    pub test_name: String,
    /// The assertion failure details.
    pub failure: AssertionFailure,
}

/// Collect assertion failures from a test result for deferred display.
pub fn collect_failures(test_name: &str, result: &TestResult) -> Vec<FailureDetail> {
    result
        .assertions()
        .iter()
        .filter_map(|outcome| match outcome {
            AssertionOutcome::Passed { .. } => None,
            AssertionOutcome::Failed(failure) => Some(FailureDetail {
                test_name: test_name.to_string(),
                failure: AssertionFailure {
                    query: failure.query.clone(),
                    expected: failure.expected.clone(),
                    actual: failure.actual.clone(),
                },
            }),
        })
        .collect()
}

/// Print all collected failure details at the end of the test run.
pub fn print_failure_details(details: &[FailureDetail]) {
    if details.is_empty() {
        return;
    }

    println!();
    println!("{}", style("Failures:").red().bold());

    for detail in details {
        println!();
        println!(
            "  {} {} {}",
            style("●").red(),
            style(&detail.test_name).bold(),
            style("→").dim(),
        );
        println!("    {} {}", style("Query:").yellow(), detail.failure.query);

        let expected = serde_json::to_string_pretty(&detail.failure.expected).unwrap_or_default();
        // Align actual arrays to expected's element ordering so the diff
        // highlights real value differences instead of showing every line
        // as changed due to non-deterministic GraphQL collection ordering.
        let aligned_actual = align_for_diff(&detail.failure.expected, &detail.failure.actual);
        let actual = serde_json::to_string_pretty(&aligned_actual).unwrap_or_default();

        println!(
            "    {} {} expected {} actual",
            style("Diff:").yellow(),
            style("(-)").red(),
            style("(+)").green(),
        );

        let diff = TextDiff::from_lines(&expected, &actual);
        for change in diff.iter_all_changes() {
            let text = change.value().trim_end_matches('\n');
            match change.tag() {
                ChangeTag::Delete => println!("      {}", style(format!("- {text}")).red()),
                ChangeTag::Insert => println!("      {}", style(format!("+ {text}")).green()),
                ChangeTag::Equal => println!("        {text}"),
            }
        }
    }
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
