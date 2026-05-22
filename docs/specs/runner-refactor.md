# Subgraph Runner Simplification Spec

## Problem Statement

`core/src/subgraph/runner.rs` is complex and hard to modify. Key issues:

1. **Duplicated trigger processing** (lines 616-656 vs 754-790): Nearly identical loops
2. **Control flow confusion**: Nested loops in `run_inner` with 6 exit paths
3. **State management**: Mixed patterns (mutable fields, `std::mem::take`, drains)
4. **`process_block` monolith**: ~260 lines handling triggers, dynamic DS, offchain, persistence

## Design Decisions

| Aspect | Decision |
|--------|----------|
| Control flow | Enum-based FSM for full runner lifecycle |
| Trigger processing | New `TriggerRunner` component |
| Block processing | Pipeline with explicitly defined stages |
| State management | Mutable accumulator with checkpoints |
| Breaking changes | Moderate (internal APIs can change) |

## Target Architecture

### 1. Runner State Machine

Replace nested loops in `run_inner` with an explicit enum FSM covering the full lifecycle:

```rust
enum RunnerState {
    /// Initial state, ready to start block stream
    Initializing,

    /// Block stream active, waiting for next event
    AwaitingBlock {
        block_stream: Cancelable<Box<dyn BlockStream<C>>>,
    },

    /// Processing a block through the pipeline
    ProcessingBlock {
        block: BlockWithTriggers<C>,
        cursor: FirehoseCursor,
    },

    /// Handling a revert event
    Reverting {
        to_ptr: BlockPtr,
        cursor: FirehoseCursor,
    },

    /// Restarting block stream (new filters, store restart, etc.)
    Restarting {
        reason: RestartReason,
    },

    /// Terminal state
    Stopped {
        reason: StopReason,
    },
}

enum RestartReason {
    DynamicDataSourceCreated,
    DataSourceExpired,
    StoreError,
    PossibleReorg,
}

enum StopReason {
    MaxEndBlockReached,
    Canceled,
    Unassigned,
}
```

The main loop becomes:

```rust
async fn run(mut self) -> Result<(), SubgraphRunnerError> {
    loop {
        self.state = match self.state {
            RunnerState::Initializing => self.initialize().await?,
            RunnerState::AwaitingBlock { stream } => self.await_block(stream).await?,
            RunnerState::ProcessingBlock { block, cursor } => {
                self.process_block(block, cursor).await?
            }
            RunnerState::Reverting { to_ptr, cursor } => {
                self.handle_revert(to_ptr, cursor).await?
            }
            RunnerState::Restarting { reason } => self.restart(reason).await?,
            RunnerState::Stopped { reason } => return self.finalize(reason).await,
        };
    }
}
```

### 2. Block Processing Pipeline

Replace the `process_block` monolith with explicit stages:

```rust
/// Pipeline stages for block processing
mod pipeline {
    pub struct TriggerMatchStage;
    pub struct TriggerExecuteStage;
    pub struct DynamicDataSourceStage;
    pub struct OffchainTriggerStage;
    pub struct PersistStage;
}

/// Result of block processing pipeline
pub struct BlockProcessingResult {
    pub action: Action,
    pub block_state: BlockState,
}

impl SubgraphRunner {
    async fn process_block(
        &mut self,
        block: BlockWithTriggers<C>,
        cursor: FirehoseCursor,
    ) -> Result<RunnerState, ProcessingError> {
        let block = Arc::new(block.block);
        let triggers = block.trigger_data;

        // Stage 1: Match triggers to hosts and decode
        let runnables = self.match_triggers(&block, triggers).await?;

        // Stage 2: Execute triggers (unified for initial + dynamic DS)
        let mut block_state = self.execute_triggers(&block, runnables).await?;

        // Checkpoint before dynamic DS processing
        let checkpoint = block_state.checkpoint();

        // Stage 3: Process dynamic data sources (loop until none created)
        block_state = self.process_dynamic_data_sources(&block, &cursor, block_state).await?;

        // Stage 4: Handle offchain triggers
        let offchain_result = self.process_offchain_triggers(&block, &mut block_state).await?;

        // Stage 5: Persist to store
        self.persist_block_state(block_state, offchain_result).await?;

        // Determine next state
        Ok(self.determine_next_state())
    }
}
```

### 3. Error Handling Strategy

Consolidate scattered error handling into explicit classification:

```rust
/// Unified error classification for trigger processing
pub enum ProcessingErrorKind {
    /// Stop processing, persist PoI only
    Deterministic(anyhow::Error),
    /// Retry with backoff, attempt to unfail
    NonDeterministic(anyhow::Error),
    /// Restart block stream cleanly (don't persist)
    PossibleReorg(anyhow::Error),
}

impl ProcessingError {
    /// Classify error once, use classification throughout
    pub fn kind(&self) -> ProcessingErrorKind { ... }

    /// Whether this error should stop processing the current block
    pub fn should_stop_processing(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::Deterministic(_))
    }

    /// Whether this error requires a clean restart
    pub fn should_restart(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::PossibleReorg(_))
    }

    /// Whether this error is retryable with backoff
    pub fn is_retryable(&self) -> bool {
        matches!(self.kind(), ProcessingErrorKind::NonDeterministic(_))
    }
}
```

**Key Invariant (must be preserved):**
```
Deterministic    → Stop processing block, persist PoI only
NonDeterministic → Retry with backoff
PossibleReorg    → Restart cleanly (don't persist)
```

Currently this logic is scattered across:
- `process_block` early return for PossibleReorg (line 664-677)
- Dynamic data sources error mapping (line 792-802)
- `transact_block_state` (line 405-430)
- `handle_offchain_triggers` (line 1180-1190)

Consolidating into helper methods eliminates these scattered special cases.

### 4. TriggerRunner Component

Extract trigger execution into a dedicated component:

```rust
/// Handles matching, decoding, and executing triggers
pub struct TriggerRunner<'a, C: Blockchain, T: RuntimeHostBuilder<C>> {
    decoder: &'a Decoder<C, T>,
    processor: &'a dyn TriggerProcessor<C, T>,
    logger: &'a Logger,
    metrics: &'a SubgraphMetrics,
    debug_fork: &'a Option<Arc<dyn SubgraphFork>>,
    instrument: bool,
}

impl<'a, C, T> TriggerRunner<'a, C, T>
where
    C: Blockchain,
    T: RuntimeHostBuilder<C>,
{
    /// Execute triggers against hosts, accumulating state
    pub async fn execute(
        &self,
        block: &Arc<C::Block>,
        runnables: Vec<RunnableTriggers<'a, C>>,
        mut block_state: BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        causality_region: &PoICausalityRegion,
    ) -> Result<BlockState, MappingError> {
        for runnable in runnables {
            block_state = self.processor
                .process_trigger(
                    self.logger,
                    runnable.hosted_triggers,
                    block,
                    block_state,
                    proof_of_indexing,
                    causality_region,
                    self.debug_fork,
                    self.metrics,
                    self.instrument,
                )
                .await
                .map_err(|e| e.add_trigger_context(&runnable.trigger))?;
        }
        Ok(block_state)
    }
}
```

This eliminates the duplicated loops (lines 616-656 and 754-790).

### 5. State Management with Checkpoints

**Explicit Input/Output Types for Pipeline Stages:**

```rust
/// Input to trigger processing - makes dependencies explicit
struct TriggerProcessingContext<'a> {
    block: &'a Arc<C::Block>,
    proof_of_indexing: &'a SharedProofOfIndexing,
    causality_region: &'a PoICausalityRegion,
}

/// Output from trigger processing - makes results explicit
struct TriggerProcessingResult {
    block_state: BlockState,
    restart_needed: bool,
}
```

**Add checkpoint capability to `BlockState` for rollback scenarios:**

```rust
impl BlockState {
    /// Create a lightweight checkpoint for rollback
    pub fn checkpoint(&self) -> BlockStateCheckpoint {
        BlockStateCheckpoint {
            created_data_sources_count: self.created_data_sources.len(),
            persisted_data_sources_count: self.persisted_data_sources.len(),
            // Note: entity_cache changes cannot be easily checkpointed
            // Rollback clears the cache (acceptable per current behavior)
        }
    }

    /// Restore state to checkpoint (partial rollback)
    pub fn restore(&mut self, checkpoint: BlockStateCheckpoint) {
        self.created_data_sources.truncate(checkpoint.created_data_sources_count);
        self.persisted_data_sources.truncate(checkpoint.persisted_data_sources_count);
        // Entity cache is cleared on rollback (matches current behavior)
    }
}
```

### 6. File Structure

```
core/src/subgraph/
├── runner.rs              # Main SubgraphRunner with FSM
├── runner/
│   ├── mod.rs
│   ├── state.rs           # RunnerState enum and transitions
│   ├── pipeline.rs        # Pipeline stage definitions
│   └── trigger_runner.rs  # TriggerRunner component
├── context.rs             # IndexingContext (unchanged)
├── inputs.rs              # IndexingInputs (unchanged)
└── state.rs               # IndexingState (unchanged)
```

## Deferred Concerns

### Fishy Block Refetch (Preserve Behavior)

The TODO at lines 721-729 notes unclear behavior around block refetching in the dynamic DS loop. The restructure preserves this behavior without attempting to fix it. Investigate separately.

## Key Interfaces

### RunnerState Transitions

```
Initializing ──────────────────────────────────┐
     │                                          │
     v                                          │
AwaitingBlock ◄─────────────────────────────────┤
     │                                          │
     ├── ProcessBlock event ──► ProcessingBlock │
     │                              │           │
     │                              ├── success ┼──► AwaitingBlock
     │                              │           │
     │                              └── restart ┼──► Restarting
     │                                          │
     ├── Revert event ──────────► Reverting ────┤
     │                                          │
     ├── Error ─────────────────► Restarting ───┤
     │                                          │
     └── Cancel/MaxBlock ───────► Stopped       │
                                                │
Restarting ─────────────────────────────────────┘
```

### Pipeline Data Flow

```
BlockWithTriggers
       │
       v
┌──────────────────┐
│ TriggerMatchStage│ ─► Vec<RunnableTriggers>
└──────────────────┘
       │
       v
┌────────────────────┐
│ TriggerExecuteStage│ ─► BlockState (mutated)
└────────────────────┘
       │
       v (loop while has_created_data_sources)
┌─────────────────────────┐
│ DynamicDataSourceStage  │ ─► BlockState (mutated), new hosts added
└─────────────────────────┘
       │
       v
┌─────────────────────┐
│ OffchainTriggerStage│ ─► offchain_mods, processed_ds
└─────────────────────┘
       │
       v
┌─────────────┐
│ PersistStage│ ─► Store transaction
└─────────────┘
```

## Verification

After implementation, verify:

1. **Unit tests pass**: `just test-unit`
2. **Runner tests pass**: `just test-runner`
3. **Lint clean**: `just lint` (zero warnings)
4. **Build succeeds**: `just check --release`

For behavioral verification, the existing runner tests should catch regressions. No new integration tests required for a refactor that preserves behavior.
