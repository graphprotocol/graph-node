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
3. PNPM
4. Foundry (for smart contract compilation)
5. Environment variable `THEGRAPH_STORE_POSTGRES_DIESEL_URL` set to `postgresql://graph:graph@127.0.0.1:5432/graph-test`

The environment dependencies and environment setup are operated by the human.

**Running Unit Tests:**

```bash
# Run unit tests
just test-unit

# Run specific tests (e.g. `data_source::common::tests`)
just test-unit data_source::common::tests
```

**⚠️ Test Verification Requirements:**
When filtering for specific tests, ensure the intended test name(s) appear in the output.

### Runner Tests (Integration Tests)

**Prerequisites:**

1. PostgreSQL running on localhost:5432 (with initialised `graph-test` database)
2. IPFS running on localhost:5001
3. PNPM
4. Foundry (for smart contract compilation)
5. Environment variable `THEGRAPH_STORE_POSTGRES_DIESEL_URL` set to `postgresql://graph:graph@127.0.0.1:5432/graph-test`

**Running Runner Tests:**

```bash
# Run runner tests.
just test-runner

# Run specific tests (e.g. `block_handlers`)
just test-runner block_handlers
```

**⚠️ Test Verification Requirements:**
When filtering for specific tests, ensure the intended test name(s) appear in the output.

**Important Notes:**

- Runner tests take moderate time (10-20 seconds)
- Tests automatically reset the database between runs
- Some tests can pass without IPFS, but tests involving file data sources require it

### Integration Tests

**Prerequisites:**

1. PostgreSQL running on localhost:3011 (with initialised `graph-node` database)
2. IPFS running on localhost:3001
3. Anvil running on localhost:3021
4. PNPM
5. Foundry (for smart contract compilation)
6. **Built graph-node binary** (integration tests require the compiled binary)

The environment dependencies and environment setup are operated by the human.

**Running Integration Tests:**

```bash
# REQUIRED: Build graph-node binary before running integration tests
just build

# Run all integration tests
just test-integration

# Run a specific integration test case (e.g., "grafted" test case)
TEST_CASE=grafted just test-integration
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
just format

# 🚨 MANDATORY: Check code for warnings and errors - MUST have zero warnings
just lint

# 🚨 MANDATORY: Check in release mode to catch linking/optimization issues that cargo check misses
just check --release
```

🚨 **CRITICAL REQUIREMENTS for ANY implementation**:

- **🚨 MANDATORY**: `cargo fmt --all` MUST be run before any commit
- **🚨 MANDATORY**: `just lint` MUST show zero warnings before any commit
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

# Claude: Run unit tests
just test-unit
```

**Runner Tests:**

```bash
# Human: Start PostgreSQL + IPFS for runner tests in a separate terminal
# PostgreSQL: localhost:5432, IPFS: localhost:5001
nix run .#unit # NOTE: Runner tests are using the same nix services stack as the unit test

# Claude: Run runner tests
just test-runner
```

**Integration Tests:**

```bash
# Human: Start all services for integration tests in a separate terminal
# PostgreSQL: localhost:3011, IPFS: localhost:3001, Anvil: localhost:3021
nix run .#integration

# Claude: Build graph-node binary before running integration tests
just build

# Claude: Run integration tests
just test-integration
```

**Services Configuration:**
The services are configured to use the test suite default ports for unit- and integration tests respectively.

| Service          | Unit Tests Port | Integration Tests Port | Database/Config                                 |
| ---------------- | --------------- | ---------------------- | ----------------------------------------------- |
| PostgreSQL       | 5432            | 3011                   | `graph-test` / `graph-node`                     |
| IPFS             | 5001            | 3001                   | Data in `./.data/unit` or `./.data/integration` |
| Anvil (Ethereum) | -               | 3021                   | Deterministic test chain                        |

**Service Configuration:**
The setup combines built-in services-flake services with custom multiService modules:

**Built-in Services:**

- **PostgreSQL**: Uses services-flake's postgres service with a helper function (`mkPostgresConfig`) that provides graph-specific defaults including required extensions.

**Custom Services** (located in `./nix`):

- `ipfs.nix`: IPFS (kubo) with automatic initialization and configurable ports
- `anvil.nix`: Ethereum test chain with deterministic configuration
