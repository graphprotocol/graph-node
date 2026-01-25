//! State machine types for SubgraphRunner.
//!
//! This module defines the explicit state machine that controls the runner's lifecycle,
//! replacing the previous nested loop structure with clear state transitions.

use graph::blockchain::block_stream::{BlockStream, BlockWithTriggers, FirehoseCursor};
use graph::blockchain::Blockchain;
use graph::ext::futures::Cancelable;
use graph::prelude::BlockPtr;

/// The current state of the SubgraphRunner's lifecycle.
///
/// The runner transitions through these states as it processes blocks,
/// handles reverts, and responds to errors or cancellation signals.
///
/// ## State Transitions
///
/// ```text
/// Initializing ───────────────────────────────────┐
///      │                                          │
///      v                                          │
///  AwaitingBlock ◄────────────────────────────────┤
///      │                                          │
///      ├── ProcessBlock event ──► ProcessingBlock │
///      │                              │           │
///      │                              ├── success ┼──► AwaitingBlock
///      │                              │           │
///      │                              └── restart ┼──► Restarting
///      │                                          │
///      ├── Revert event ──────────► Reverting ────┤
///      │                                          │
///      ├── Error ─────────────────► Restarting ───┤
///      │                                          │
///      └── Cancel/MaxBlock ───────► Stopped       │
///                                                 │
/// Restarting ─────────────────────────────────────┘
/// ```
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
    /// The block stream is kept alive to continue processing after this block.
    ProcessingBlock {
        block_stream: Cancelable<Box<dyn BlockStream<C>>>,
        block: BlockWithTriggers<C>,
        cursor: FirehoseCursor,
    },

    /// Handling a revert event.
    /// The block stream is kept alive to continue processing after the revert.
    Reverting {
        block_stream: Cancelable<Box<dyn BlockStream<C>>>,
        to_ptr: BlockPtr,
        cursor: FirehoseCursor,
    },

    /// Restarting block stream (new filters, store restart, etc.).
    Restarting { reason: RestartReason },

    /// Terminal state.
    Stopped { reason: StopReason },
}

/// Reasons for restarting the block stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartReason {
    /// New dynamic data source was created that requires filter updates.
    DynamicDataSourceCreated,
    /// A data source reached its end block.
    DataSourceExpired,
    /// Store error occurred and store needs to be restarted.
    StoreError,
    /// Possible reorg detected, need to restart to detect it.
    /// NOTE: Currently unused but reserved for future error handling consolidation (Phase 5).
    #[allow(dead_code)]
    PossibleReorg,
}

/// Reasons for stopping the runner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// The maximum end block was reached.
    MaxEndBlockReached,
    /// The runner was canceled (unassigned or shutdown).
    Canceled,
    /// The subgraph was unassigned while this runner was active.
    Unassigned,
    /// The block stream ended (typically in tests).
    StreamEnded,
    /// A deterministic error occurred.
    DeterministicError,
}
