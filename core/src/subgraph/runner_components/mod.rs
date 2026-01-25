//! SubgraphRunner components and state management.
//!
//! This module contains the state machine and helper components for the SubgraphRunner:
//!
//! - [`state`]: Defines [`RunnerState`], [`RestartReason`], and [`StopReason`] enums
//!   that control the runner's lifecycle.
//! - [`trigger_runner`]: Provides [`TriggerRunner`] for unified trigger execution.

mod state;
mod trigger_runner;

pub use state::{RestartReason, RunnerState, StopReason};
pub use trigger_runner::TriggerRunner;
