//! Console output formatting for test results.

use console::style;
use similar::{ChangeTag, TextDiff};

use super::assertion::align_for_diff;
use super::schema::{AssertionFailure, AssertionOutcome, TestResult};
use crate::output::{step, Step};

pub fn print_test_start(path: &std::path::Path) {
    step(Step::Load, &format!("Running {}", path.display()));
}

/// Print pass/fail for a test case. Diffs are deferred to `print_failure_details`.
pub fn print_test_result(name: &str, result: &TestResult) {
    if result.is_passed() {
        println!("  {} {}", style("✔").green(), name);
    } else {
        println!("  {} {}", style("✘").red(), name);
    }

    if let Some(err) = &result.handler_error {
        println!("    {} {}", style("Handler error:").red(), err);
    }

    for outcome in &result.assertions {
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

pub struct FailureDetail {
    pub test_name: String,
    pub failure: AssertionFailure,
}

pub fn collect_failures(test_name: &str, result: &TestResult) -> Vec<FailureDetail> {
    result
        .assertions
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

pub fn print_failure_details(details: &[FailureDetail]) {
    if details.is_empty() {
        return;
    }

    println!("\n{}", style("Failures:").red().bold());

    for detail in details {
        println!(
            "\n  {} {} {}",
            style("●").red(),
            style(&detail.test_name).bold(),
            style("→").dim(),
        );
        println!("    {} {}", style("Query:").yellow(), detail.failure.query);

        let expected = serde_json::to_string_pretty(&detail.failure.expected).unwrap_or_default();
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
