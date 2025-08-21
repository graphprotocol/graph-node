# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Graph Node is a Rust-based decentralized blockchain indexing protocol that enables efficient querying of blockchain data through GraphQL. It's the core component of The Graph protocol, written as a Cargo workspace with multiple crates organized by functionality.

## Essential Development Commands

### Testing Workflow

⚠️ **Only run integration tests when explicitly requested or when changes require full system testing**

Use unit tests for regular development and only run integration tests when:

- Explicitly asked to do so
- Making changes to integration/end-to-end functionality
- Debugging issues that require full system testing
- Preparing releases or major changes

### Unit Tests

Unit tests are inlined with source code.

**Prerequisites:**
1. PostgreSQL running on localhost:5432 (with initialised `graph-test` database)
2. IPFS running on localhost:5001
3. Yarn (v1)
4. Foundry (for smart contract compilation)
5. Environment variable `THEGRAPH_STORE_POSTGRES_DIESEL_URL` set

The environment dependencies and environment setup are operated by the human.

**Running Unit Tests:**
```bash
# REQUIRED: Export database connection string before running unit tests (do this **once** at the beginning of a testing session)
export THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://graph:graph@127.0.0.1:5432/graph-test"

# Run unit tests (excluding integration tests)
cargo test --workspace --exclude graph-tests

# Run specific tests
cargo test -p graph data_source::common::tests
cargo test <specific_test_name>
```

**⚠️ Test Verification Requirements:**
- **ALWAYS verify tests actually ran** - Check the output for "test result: ok. X passed" where X > 0
- **If output shows "0 passed" or "0 tests run"**, the test filter/path was wrong - fix and re-run
- **Never trust exit code 0 alone** - Cargo can exit successfully even when no tests matched your filter
- **For specific test names**, ensure the test name appears in the output as "test {name} ... ok"

### Runner Tests (Integration Tests)

**Prerequisites:**
1. PostgreSQL running on localhost:5432 (with initialised `graph-test` database)
2. IPFS running on localhost:5001
3. Yarn (v1)
4. Foundry (for smart contract compilation)
5. Environment variable `THEGRAPH_STORE_POSTGRES_DIESEL_URL` set

**Running Runner Tests:**
```bash
# REQUIRED: Export database connection string before running unit tests (do this **once** at the beginning of a testing session)
export THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://graph:graph@127.0.0.1:5432/graph-test"

# Run all runner tests
cargo test -p graph-tests --test runner_tests -- --nocapture

# Run specific runner tests
cargo test -p graph-tests --test runner_tests test_name -- --nocapture
```

**⚠️ Test Verification Requirements:**
- **ALWAYS verify tests actually ran** - Check the output for "test result: ok. X passed" where X > 0
- **If output shows "0 passed" or "0 tests run"**, the test filter/path was wrong - fix and re-run
- **Never trust exit code 0 alone** - Cargo can exit successfully even when no tests matched your filter

**Important Notes:**
- Runner tests take moderate time (10-20 seconds)
- Tests automatically reset the database between runs
- Some tests can pass without IPFS, but tests involving file data sources or substreams require it

### Integration Tests

**Prerequisites:**
1. PostgreSQL running on localhost:3011 (with initialised `graph-node` database)
2. IPFS running on localhost:3001
3. Anvil running on localhost:3021
4. Yarn (v1)
5. Foundry (for smart contract compilation)
6. **Built graph-node binary** (integration tests require the compiled binary)

The environment dependencies and environment setup are operated by the human.

**Running Integration Tests:**
```bash
# REQUIRED: Build graph-node binary before running integration tests
cargo build --bin graph-node

# Run all integration tests
cargo test -p graph-tests --test integration_tests -- --nocapture

# Run a specific integration test case (e.g., "grafted" test case)
TEST_CASE=grafted cargo test -p graph-tests --test integration_tests -- --nocapture
```

**⚠️ Test Verification Requirements:**
- **ALWAYS verify tests actually ran** - Check the output for "test result: ok. X passed" where X > 0
- **If output shows "0 passed" or "0 tests run"**, the TEST_CASE variable or filter was wrong - fix and re-run
- **Never trust exit code 0 alone** - Cargo can exit successfully even when no tests matched your filter

**Important Notes:**
- Integration tests take significant time (several minutes)
- Tests automatically reset the database between runs
- Logs are written to `tests/integration-tests/graph-node.log`

### Code Quality
```bash
# 🚨 MANDATORY: Format all code IMMEDIATELY after any .rs file edit
cargo fmt --all

# 🚨 MANDATORY: Check code for warnings and errors - MUST have zero warnings
cargo check

# 🚨 MANDATORY: Build in release mode to catch linking/optimization issues that cargo check misses
cargo check --release
```

🚨 **CRITICAL REQUIREMENTS for ANY implementation**:
- **🚨 MANDATORY**: `cargo fmt --all` MUST be run before any commit
- **🚨 MANDATORY**: `cargo check` MUST show zero warnings before any commit
- **🚨 MANDATORY**: `cargo check --release` MUST complete successfully before any commit
- **🚨 MANDATORY**: The unit test suite MUST pass before any commit

Forgetting any of these means you failed to follow instructions. Before any commit or PR, ALL of the above MUST be satisfied! No exceptions!

## High-Level Architecture

### Core Components
- **`graph/`**: Core abstractions, traits, and shared types
- **`node/`**: Main executable and CLI (graphman)
- **`chain/`**: Blockchain-specific adapters (ethereum, near, substreams)
- **`runtime/`**: WebAssembly runtime for subgraph execution
- **`store/`**: PostgreSQL-based storage layer
- **`graphql/`**: GraphQL query execution engine
- **`server/`**: HTTP/WebSocket APIs

### Data Flow
```
Blockchain → Chain Adapter → Block Stream → Trigger Processing → Runtime → Store → GraphQL API
```

1. **Chain Adapters** connect to blockchain nodes and convert data to standardized formats
2. **Block Streams** provide event-driven streaming of blockchain blocks
3. **Trigger Processing** matches blockchain events to subgraph handlers
4. **Runtime** executes subgraph code in WebAssembly sandbox
5. **Store** persists entities with block-level granularity
6. **GraphQL** processes queries and returns results

### Key Abstractions
- **`Blockchain`** trait: Core blockchain interface
- **`Store`** trait: Storage abstraction with read/write variants
- **`RuntimeHost`**: WASM execution environment
- **`TriggerData`**: Standardized blockchain events
- **`EventConsumer`/`EventProducer`**: Component communication

### Architecture Patterns
- **Event-driven**: Components communicate through async streams and channels
- **Trait-based**: Extensive use of traits for abstraction and modularity
- **Async/await**: Tokio-based async runtime throughout
- **Multi-shard**: Database sharding for scalability
- **Sandboxed execution**: WASM runtime with gas metering

## Development Guidelines

### Commit Convention
Use format: `{crate-name}: {description}`
- Single crate: `store: Support 'Or' filters`
- Multiple crates: `core, graphql: Add event source to store`
- All crates: `all: {description}`

### Git Workflow
- Rebase on master (don't merge master into feature branch)
- Keep commits logical and atomic
- Squash commits to clean up history before merging

## Crate Structure

### Core Crates
- **`graph`**: Shared types, traits, and utilities
- **`node`**: Main binary and component wiring
- **`core`**: Business logic and subgraph management

### Blockchain Integration
- **`chain/ethereum`**: Ethereum chain support
- **`chain/near`**: NEAR protocol support
- **`chain/substreams`**: Substreams data source support

### Infrastructure
- **`store/postgres`**: PostgreSQL storage implementation
- **`runtime/wasm`**: WebAssembly runtime and host functions
- **`graphql`**: Query processing and execution
- **`server/`**: HTTP/WebSocket servers

### Key Dependencies
- **`diesel`**: PostgreSQL ORM
- **`tokio`**: Async runtime
- **`tonic`**: gRPC framework
- **`wasmtime`**: WebAssembly runtime
- **`web3`**: Ethereum interaction

## Test Environment Requirements

### Process Compose Setup (Recommended)

The repository includes a process-compose-flake setup that provides native, declarative service management.

Currently, the human is required to operate the service dependencies as illustrated below.

**Unit Tests:**
```bash
# Human: Start PostgreSQL + IPFS for unit tests in a separate terminal
# PostgreSQL: localhost:5432, IPFS: localhost:5001
nix run .#unit

# Claude: Export the database connection string before running unit tests
export THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://graph:graph@127.0.0.1:5432/graph-test"

# Claude: Run unit tests
cargo test --workspace --exclude graph-tests
```

**Runner Tests:**
```bash
# Human: Start PostgreSQL + IPFS for runner tests in a separate terminal
# PostgreSQL: localhost:5432, IPFS: localhost:5001
nix run .#unit # NOTE: Runner tests are using the same nix services stack as the unit test

# Claude: Export the database connection string before running runner tests
export THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://graph:graph@127.0.0.1:5432/graph-test"

# Claude: Run runner tests
cargo test -p graph-tests --test runner_tests -- --nocapture
```

**Integration Tests:**
```bash
# Human: Start all services for integration tests in a separate terminal
# PostgreSQL: localhost:3011, IPFS: localhost:3001, Anvil: localhost:3021
nix run .#integration

# Claude: Build graph-node binary before running integration tests
cargo build --bin graph-node

# Claude: Run integration tests
cargo test -p graph-tests --test integration_tests
```

**Services Configuration:**
The services are configured to use the test suite default ports for unit- and integration tests respectively.

| Service | Unit Tests Port | Integration Tests Port | Database/Config |
|---------|-----------------|------------------------|-----------------|
| PostgreSQL | 5432 | 3011 | `graph-test` / `graph-node` |
| IPFS | 5001 | 3001 | Data in `./.data/unit` or `./.data/integration` |
| Anvil (Ethereum) | - | 3021 | Deterministic test chain |

**Service Configuration:**
The setup combines built-in services-flake services with custom multiService modules:

**Built-in Services:**
- **PostgreSQL**: Uses services-flake's postgres service with a helper function (`mkPostgresConfig`) that provides graph-specific defaults including required extensions.

**Custom Services** (located in `./nix`):
- `ipfs.nix`: IPFS (kubo) with automatic initialization and configurable ports
- `anvil.nix`: Ethereum test chain with deterministic configuration
