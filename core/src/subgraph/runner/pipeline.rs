//! Block processing pipeline stages for the subgraph runner.
//!
//! This module defines the pipeline stages used to process blocks in a structured,
//! sequential manner. Each stage represents a distinct phase of block processing:
//!
//! 1. **TriggerMatchStage**: Matches block triggers to handlers and decodes them
//! 2. **TriggerExecuteStage**: Executes matched triggers via TriggerRunner
//! 3. **DynamicDataSourceStage**: Processes newly created dynamic data sources
//! 4. **OffchainTriggerStage**: Handles offchain data source triggers
//! 5. **PersistStage**: Persists the accumulated block state to the store
//!
//! The pipeline transforms a `BlockWithTriggers` into persisted entity modifications.

// Allow unused code - these types are scaffolding for future refactoring phases.
// They will be used as the pipeline architecture is further developed.
#![allow(dead_code)]

use graph::blockchain::{BlockTime, Blockchain};
use graph::prelude::{BlockPtr, BlockState};

/// Context for block processing, containing block metadata and PoI information.
pub struct BlockProcessingContext<'a, C: Blockchain> {
    /// The block being processed
    pub block: &'a std::sync::Arc<C::Block>,
    /// Block pointer for the current block
    pub block_ptr: BlockPtr,
    /// Block timestamp
    pub block_time: BlockTime,
    /// Causality region for onchain triggers (network-derived string)
    pub causality_region: String,
}

impl<'a, C: Blockchain> BlockProcessingContext<'a, C> {
    /// Create a new block processing context.
    pub fn new(
        block: &'a std::sync::Arc<C::Block>,
        block_ptr: BlockPtr,
        block_time: BlockTime,
        causality_region: String,
    ) -> Self {
        Self {
            block,
            block_ptr,
            block_time,
            causality_region,
        }
    }
}

/// Result from the dynamic data source processing stage.
///
/// Indicates whether a block stream restart is needed due to newly created
/// on-chain data sources (when static filters are not enabled) or data sources
/// that have reached their end block.
pub struct DynamicDataSourceResult {
    /// Whether the block stream needs to be restarted
    pub needs_restart: bool,
    /// Updated block state after processing dynamic data sources
    pub block_state: BlockState,
}

impl DynamicDataSourceResult {
    /// Create a new result indicating no restart is needed.
    pub fn no_restart(block_state: BlockState) -> Self {
        Self {
            needs_restart: false,
            block_state,
        }
    }

    /// Create a new result indicating a restart is needed.
    pub fn with_restart(block_state: BlockState) -> Self {
        Self {
            needs_restart: true,
            block_state,
        }
    }
}
