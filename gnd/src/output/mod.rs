mod spinner;

pub use spinner::{step as spinner_step, with_spinner, Spinner, SpinnerResult};

use console::style;

/// Step type for standalone step printing (without spinner)
#[derive(Debug, Clone, Copy)]
pub enum Step {
    /// Loading/reading a file
    Load,
    /// Generating code
    Generate,
    /// Writing a file
    Write,
    /// Skipping a step
    Skip,
    /// Deploying a subgraph
    Deploy,
    /// Done with all steps
    Done,
    /// Warning (non-fatal)
    Warn,
}

/// Print a standalone step without a spinner.
///
/// This is used for code generation steps where we don't need
/// the spinner animation but want consistent output formatting.
pub fn step(step_type: Step, message: &str) {
    let symbol = match step_type {
        Step::Load => style("→").cyan(),
        Step::Generate => style("→").cyan(),
        Step::Write => style("→").cyan(),
        Step::Skip => style("→").dim(),
        Step::Deploy => style("→").cyan(),
        Step::Done => style("✔").green(),
        Step::Warn => style("⚠").yellow(),
    };
    println!("{} {}", symbol, message);
}

/// Print a standalone step result.
pub struct StepResult;

impl StepResult {
    pub fn success(message: &str) {
        println!("{} {}", style("✔").green(), message);
    }

    pub fn error(message: &str) {
        eprintln!("{} {}", style("✖").red(), message);
    }

    pub fn warning(message: &str) {
        println!("{} {}", style("⚠").yellow(), message);
    }
}
