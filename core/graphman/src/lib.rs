//! This crate contains graphman commands that can be executed via
//! the GraphQL API as well as via the CLI.
//!
//! Each command is broken into small execution steps to allow different interfaces to perform
//! some additional interface-specific operations between steps. An example of this is printing
//! intermediate information to the user in the CLI, or prompting for additional input.

mod error;

pub mod commands;
pub mod deployment;
pub mod execution_tracker;

pub use self::error::GraphmanError;
pub use self::execution_tracker::GraphmanExecutionTracker;
