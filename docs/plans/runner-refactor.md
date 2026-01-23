# Runner Refactor Implementation Plan

This document outlines the implementation plan for the runner refactor described in [the spec](../specs/runner-refactor.md).

## Overview

The refactor transforms `core/src/subgraph/runner.rs` from a complex nested-loop structure into a cleaner state machine with explicit pipeline stages.

## Git Workflow

**Branch**: All work should be committed to the `runner-refactor` branch.

**Commit discipline**:

- Commit work in small, reviewable chunks
- Each commit should be self-contained and pass all checks
- Prefer many small commits over few large ones
- Each commit message should clearly describe what it does
- Each step in the implementation phases should correspond to one or more commits

**Before each commit**:

```bash
just format
just lint
just test-unit
just test-runner
```

- MANDATORY: Work must be committed, a task is only done when work is committed
- MANDATORY: Make sure to follow the commit discipline above
- IMPORTANT: The runner tests produce output in `tests/runner-tests.log`. Use that to investigate failures.

## Implementation Phases

### Phase 1: Extract TriggerRunner Component

**Goal:** Eliminate duplicated trigger processing code (lines 616-656 vs 754-790).

**Files to modify:**

- `core/src/subgraph/runner.rs` - Extract logic
- Create `core/src/subgraph/runner/trigger_runner.rs`

**Steps:**

1. Create `TriggerRunner` struct with execute method
2. Replace first trigger loop (lines 616-656) with `TriggerRunner::execute()`
3. Replace second trigger loop (lines 754-790) with same call
4. Verify tests pass

**Verification:**

- `just test-unit` passes
- `just test-runner` passes
- No behavioral changes

### Phase 2: Define RunnerState Enum

**Goal:** Introduce explicit state machine types without changing control flow yet.

**Files to modify:**

- Create `core/src/subgraph/runner/state.rs`
- `core/src/subgraph/runner.rs` - Add state field

**Steps:**

1. Define `RunnerState` enum with all variants
2. Define `RestartReason` and `StopReason` enums
3. Add `state: RunnerState` field to `SubgraphRunner`
4. Initialize state in constructor
5. Verify tests pass (no behavioral changes yet)

**Verification:**

- Code compiles
- Tests pass unchanged

### Phase 3: Refactor run_inner to State Machine

**Goal:** Replace nested loops with explicit state transitions.

**Files to modify:**

- `core/src/subgraph/runner.rs` - Rewrite `run_inner`

**Steps:**

1. Extract `initialize()` method for pre-loop setup
2. Extract `await_block()` method for stream event handling
3. Extract `restart()` method for restart logic
4. Extract `finalize()` method for cleanup
5. Rewrite `run_inner` as state machine loop
6. Remove nested loop structure
7. Verify tests pass

**Verification:**

- `just test-unit` passes
- `just test-runner` passes
- Same behavior, cleaner structure

### Phase 4: Define Pipeline Stages

**Goal:** Break `process_block` into explicit stages.

**Files to modify:**

- Create `core/src/subgraph/runner/pipeline.rs`
- `core/src/subgraph/runner.rs` - Refactor `process_block`

**Steps:**

1. Extract `match_triggers()` stage method
2. Extract `execute_triggers()` stage method (uses `TriggerRunner`)
3. Extract `process_dynamic_data_sources()` stage method
4. Extract `process_offchain_triggers()` stage method
5. Extract `persist_block_state()` stage method
6. Rewrite `process_block` to call stages in sequence
7. Verify tests pass

**Verification:**

- `just test-unit` passes
- `just test-runner` passes
- Same behavior, cleaner structure

### Phase 5: Consolidate Error Handling

**Goal:** Unify scattered error handling into explicit classification.

**Files to modify:**

- `graph/src/components/subgraph/error.rs` (or wherever `ProcessingError` lives)
- `core/src/subgraph/runner.rs` - Use new error methods

**Steps:**

1. Add `ProcessingErrorKind` enum with Deterministic/NonDeterministic/PossibleReorg variants
2. Add `kind()` method to `ProcessingError`
3. Add helper methods: `should_stop_processing()`, `should_restart()`, `is_retryable()`
4. Replace scattered error checks in `process_block` with unified logic
5. Replace scattered error checks in dynamic DS handling
6. Replace scattered error checks in `handle_offchain_triggers`
7. Document error handling invariants in code comments
8. Verify tests pass

**Verification:**

- `just test-unit` passes
- `just test-runner` passes
- Error behavior unchanged (same semantics, cleaner code)

### Phase 6: Add BlockState Checkpoints

**Goal:** Enable rollback capability with minimal overhead.

**Files to modify:**

- `graph/src/prelude.rs` or wherever `BlockState` is defined
- `core/src/subgraph/runner.rs` - Use checkpoints

**Steps:**

1. Add `checkpoint()` method to `BlockState`
2. Add `BlockStateCheckpoint` struct
3. Add `restore()` method to `BlockState`
4. Use checkpoint before dynamic DS processing
5. Verify tests pass

**Verification:**

- `just test-unit` passes
- No performance regression (checkpoints are lightweight)

### Phase 7: Module Organization

**Goal:** Organize code into proper module structure.

**Files to create/modify:**

- `core/src/subgraph/runner/mod.rs`
- Move/organize existing extracted modules

**Steps:**

1. Create `runner/` directory
2. Move `state.rs`, `pipeline.rs`, `trigger_runner.rs` into it
3. Update `runner.rs` to re-export from module
4. Update imports in dependent files
5. Verify tests pass

**Verification:**

- `just test-unit` passes
- `just test-runner` passes
- `just lint` shows no warnings

## Completion Criteria

Each phase is complete when:

1. `just format` - Code is formatted
2. `just lint` - Zero warnings
3. `just check --release` - Builds in release mode
4. `just test-unit` - Unit tests pass
5. `just test-runner` - Runner tests pass

## Progress Checklist

### Phase 1: Extract TriggerRunner Component

- [x] Create `TriggerRunner` struct with execute method
- [x] Replace first trigger loop (lines 616-656)
- [x] Replace second trigger loop (lines 754-790)
- [x] Verify tests pass

### Phase 2: Define RunnerState Enum

- [ ] Define `RunnerState` enum with all variants
- [ ] Define `RestartReason` and `StopReason` enums
- [ ] Add `state: RunnerState` field to `SubgraphRunner`
- [ ] Initialize state in constructor
- [ ] Verify tests pass

### Phase 3: Refactor run_inner to State Machine

- [ ] Extract `initialize()` method
- [ ] Extract `await_block()` method
- [ ] Extract `restart()` method
- [ ] Extract `finalize()` method
- [ ] Rewrite `run_inner` as state machine loop
- [ ] Remove nested loop structure
- [ ] Verify tests pass

### Phase 4: Define Pipeline Stages

- [ ] Extract `match_triggers()` stage method
- [ ] Extract `execute_triggers()` stage method
- [ ] Extract `process_dynamic_data_sources()` stage method
- [ ] Extract `process_offchain_triggers()` stage method
- [ ] Extract `persist_block_state()` stage method
- [ ] Rewrite `process_block` to call stages in sequence
- [ ] Verify tests pass

### Phase 5: Consolidate Error Handling

- [ ] Add `ProcessingErrorKind` enum
- [ ] Add `kind()` method to `ProcessingError`
- [ ] Add helper methods (`should_stop_processing()`, `should_restart()`, `is_retryable()`)
- [ ] Replace scattered error checks in `process_block`
- [ ] Replace scattered error checks in dynamic DS handling
- [ ] Replace scattered error checks in `handle_offchain_triggers`
- [ ] Document error handling invariants
- [ ] Verify tests pass

### Phase 6: Add BlockState Checkpoints

- [ ] Add `BlockStateCheckpoint` struct
- [ ] Add `checkpoint()` method to `BlockState`
- [ ] Add `restore()` method to `BlockState`
- [ ] Use checkpoint before dynamic DS processing
- [ ] Verify tests pass

### Phase 7: Module Organization

- [ ] Create `runner/` directory
- [ ] Move `state.rs`, `pipeline.rs`, `trigger_runner.rs` into it
- [ ] Update `runner.rs` to re-export from module
- [ ] Update imports in dependent files
- [ ] Verify tests pass
- [ ] `just lint` shows zero warnings

## Notes

- Each phase should be a separate, reviewable PR
- Phases 1-4 can potentially be combined if changes are small
- Phase 3 (FSM refactor of run_inner) is the most invasive and should be reviewed carefully
- Phase 5 (error handling) can be done earlier if it helps simplify other phases
- Preserve all existing behavior - this is a refactor, not a feature change
