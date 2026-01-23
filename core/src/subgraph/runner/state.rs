//! Runner state machine types for the subgraph runner.
//!
//! This module defines the explicit state machine for the runner's lifecycle,
//! replacing the implicit state embedded in the nested loop structure.
//!
//! Note: Some variants are currently unused as the state machine is incrementally
//! adopted. They will be used in future phases as the refactor progresses.

// Allow dead code as these types are part of the public API and will be used
// in subsequent phases of the runner refactor.
#![allow(dead_code)]

use graph::blockchain::block_stream::{BlockStream, BlockWithTriggers, FirehoseCursor};
use graph::blockchain::Blockchain;
use graph::ext::futures::Cancelable;
use graph::prelude::BlockPtr;

/// The runner's lifecycle state.
///
/// This enum represents the explicit states the runner can be in during its
/// execution. Transitions between states are handled by the main run loop.
#[derive(Default)]
pub enum RunnerState<C: Blockchain> {
    /// Initial state, ready to start block stream.
    #[default]
    Initializing,

    /// Block stream active, waiting for next event.
    AwaitingBlock {
        block_stream: Cancelable<Box<dyn BlockStream<C>>>,
    },

    /// Processing a block through the pipeline.
    ProcessingBlock {
        block: BlockWithTriggers<C>,
        cursor: FirehoseCursor,
    },

    /// Handling a revert event.
    Reverting {
        to_ptr: BlockPtr,
        cursor: FirehoseCursor,
    },

    /// Restarting block stream (new filters, store restart, etc.).
    Restarting { reason: RestartReason },

    /// Terminal state.
    Stopped { reason: StopReason },
}

/// Reasons why the block stream needs to be restarted.
#[derive(Debug, Clone)]
pub enum RestartReason {
    /// New dynamic data sources were created that require filter updates.
    DynamicDataSourceCreated,
    /// A data source reached its end block.
    DataSourceExpired,
    /// A store error occurred that requires restart.
    StoreError,
    /// A possible reorg was detected.
    PossibleReorg,
}

/// Reasons why the runner stopped.
#[derive(Debug, Clone)]
pub enum StopReason {
    /// The subgraph reached its configured maximum end block.
    MaxEndBlockReached,
    /// The subgraph reached its configured stop block.
    StopBlockReached,
    /// The subgraph was canceled (unassigned).
    Canceled,
    /// The subgraph was unassigned or a newer runner took over.
    Unassigned,
    /// A fatal/deterministic error occurred.
    Fatal,
    /// Block stream ended naturally (e.g., in tests).
    StreamEnded,
    /// For testing: stop when restart would happen.
    BreakOnRestart,
}
