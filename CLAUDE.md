# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

This document assumes that your human has already set up all database configuration required for the test suite to run.

## Project Overview

Graph Node is a Rust-based decentralized blockchain indexing protocol that enables efficient querying of blockchain data through GraphQL. It's the core component of The Graph protocol, written as a Cargo workspace with multiple crates organized by functionality.

## Essential Development Commands

### Testing

```bash
# Run unit tests (integration tests are excluded due to missing environment dependencies)
# Claude should set the database URL before running tests:
export THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://graph:graph@127.0.0.1:5432/graph-test"
cargo test --workspace --exclude graph-tests

# Or run inline:
THEGRAPH_STORE_POSTGRES_DIESEL_URL="postgresql://graph:graph@127.0.0.1:5432/graph-test" cargo test <test_name>
```

### Code Quality
```bash
# Format all code
cargo fmt --all

# Check code without building
cargo check
```

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
- Use `git rebase -i` to clean up history before merging

### Testing Requirements
- Unit tests inline with source code
- Integration tests require Docker Compose setup and additional environment dependencies
- Claude cannot run integration tests due to missing environment dependencies
- Claude must set `THEGRAPH_STORE_POSTGRES_DIESEL_URL` before running any tests

### Environment Variables
- `GRAPH_LOG=debug`: Enable debug logging
- `THEGRAPH_STORE_POSTGRES_DIESEL_URL`: Required for most tests - Claude must set this to `postgresql://graph:graph@127.0.0.1:5432/graph-test`
- `GRAPH_NODE_TEST_CONFIG`: Points to test configuration file

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
Most tests require external services to be running:
- **IPFS**: Required for distributed storage functionality
- **PostgreSQL**: Required for database operations and storage tests
- Your human should ensure these services are running before test execution
