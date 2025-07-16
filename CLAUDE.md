# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Graph Node is a Rust-based decentralized blockchain indexing protocol that enables efficient querying of blockchain data through GraphQL. It's the core component of The Graph protocol, written as a Cargo workspace with multiple crates organized by functionality.

## Essential Development Commands

### Build and Run
```bash
# Build and run Graph Node
cargo run -p graph-node --release -- \
  --postgres-url $POSTGRES_URL \
  --ethereum-rpc NETWORK_NAME:[CAPABILITIES]:URL \
  --ipfs 127.0.0.1:5001

# Development with auto-reload (requires cargo-watch)
cargo watch \
  -x "fmt --all" \
  -x check \
  -x "test -- --test-threads=1" \
  -x "doc --no-deps"
```

### Testing
```bash
# Run all tests (requires test config)
GRAPH_NODE_TEST_CONFIG=`pwd`/store/test-store/config.simple.toml cargo test --workspace --exclude graph-tests

# Run a single test
cargo test <test_name>

# Test setup using Docker Compose
./store/test-store/devel/up.sh

# Reset test databases
./store/test-store/devel/db-reset
```

### Code Quality
```bash
# Format all code
cargo fmt --all

# Check code without building
cargo check

# Build documentation
cargo doc --no-deps
```

### Database Setup
```bash
# Create test database
psql -U <SUPERUSER> <<EOF
create user graph with password '<password>';
create database "graph-node" with owner=graph template=template0 encoding='UTF8' locale='C';
create extension pg_trgm;
create extension btree_gist;
create extension postgres_fdw;
grant usage on foreign data wrapper postgres_fdw to graph;
EOF

export POSTGRES_URL=postgresql://graph:<password>@localhost:5432/graph-node
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
- Integration tests require Docker Compose setup
- Tests should run with `--test-threads=1` for consistency
- Use `GRAPH_NODE_TEST_CONFIG` environment variable for test configuration

### Environment Variables
- `POSTGRES_URL`: Database connection string
- `GRAPH_LOG=debug`: Enable debug logging
- `THEGRAPH_STORE_POSTGRES_DIESEL_URL`: For Diesel/Postgres store tests
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

## Common Development Tasks

### Adding New Blockchain Support
1. Create new crate under `chain/`
2. Implement `Blockchain` trait
3. Add chain-specific `TriggerData` types
4. Implement block streaming and event parsing
5. Add integration tests

### Modifying Storage Layer
- Storage interfaces defined in `graph/src/components/store/`
- PostgreSQL implementation in `store/postgres/`
- Use migrations for schema changes
- Consider multi-shard implications

### GraphQL Changes
- Schema parsing in `graphql/src/schema/`
- Query execution in `graphql/src/execution/`
- Resolver implementations handle entity fetching
- Consider query complexity and performance

### Runtime Modifications
- Host functions in `runtime/wasm/src/host.rs`
- AssemblyScript bindings in `runtime/wasm/src/asc_abi/`
- Gas metering in `runtime/wasm/src/gas/`
- Security considerations for sandboxing