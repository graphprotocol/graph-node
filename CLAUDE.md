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
- Some tests can pass without IPFS, but tests involving file data sources or substreams require it

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
just check

# 🚨 MANDATORY: Check in release mode to catch linking/optimization issues that cargo check misses
just check --release

# 🚨 MANDATORY: Run clippy to catch common mistakes and improve code quality
just clippy
```

🚨 **CRITICAL REQUIREMENTS for ANY implementation**:

- **🚨 MANDATORY**: `cargo fmt --all` MUST be run before any commit
- **🚨 MANDATORY**: `cargo check` MUST show zero warnings before any commit
- **🚨 MANDATORY**: `cargo check --release` MUST complete successfully before any commit
- **🚨 MANDATORY**: `cargo clippy` MUST show zero warnings before any commit
- **🚨 MANDATORY**: The unit test suite MUST pass before any commit

Forgetting any of these means you failed to follow instructions. Before any commit or PR, ALL of the above MUST be satisfied! No exceptions!

## Coding Standards

### Error Handling

```rust
// ALWAYS use Result<T, E> for fallible operations
pub fn process_block(block: Block) -> Result<ProcessedBlock, ProcessingError> {
    // Implementation
}

// Define meaningful error types using thiserror
#[derive(Debug, thiserror::Error)]
pub enum ProcessingError {
    #[error("Invalid block data: {0}")]
    InvalidData(String),

    #[error("Database error")]
    Database(#[from] diesel::result::Error),

    #[error("Timeout processing block {block_number}")]
    Timeout { block_number: u64 },
}

// Use ? operator for error propagation
fn complex_operation() -> Result<Data, Box<dyn Error>> {
    let config = read_config()?;
    let connection = establish_connection(&config)?;
    process_data(&connection)?;
    Ok(data)
}

// Only use unwrap/expect in tests or with clear justification
// BAD:
let value = some_option.unwrap();

// GOOD:
let value = some_option.expect("value must exist after validation in line 42");

// BETTER:
let value = some_option.ok_or(ProcessingError::MissingValue)?;
```

### Memory and Performance Patterns

```rust
// Prefer borrowing over cloning
// BAD:
fn process(data: Vec<u8>) { ... }  // Takes ownership unnecessarily

// GOOD:
fn process(data: &[u8]) { ... }    // Borrows data

// Use Cow for conditional cloning
use std::borrow::Cow;
fn process_text(input: &str) -> Cow<str> {
    if needs_modification(input) {
        Cow::Owned(modify(input))
    } else {
        Cow::Borrowed(input)
    }
}

// Preallocate collections when size is known
let mut results = Vec::with_capacity(items.len());

// Use iterators instead of manual loops
// BAD:
let mut sum = 0;
for i in 0..numbers.len() {
    if numbers[i] > 0 {
        sum += numbers[i] * 2;
    }
}

// GOOD:
let sum: i32 = numbers.iter()
    .filter(|&&x| x > 0)
    .map(|&x| x * 2)
    .sum();
```

### Type System Best Practices

```rust
// Use newtype pattern for domain types
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockNumber(pub u64);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubgraphId(pub String);

// Leverage const generics and phantom types
use std::marker::PhantomData;

struct State<S> {
    data: Vec<u8>,
    _state: PhantomData<S>,
}

struct Unverified;
struct Verified;

impl State<Unverified> {
    fn verify(self) -> Result<State<Verified>, VerificationError> {
        // Verification logic
    }
}

// Use builder pattern for complex configurations
pub struct IndexerConfigBuilder {
    network: Option<String>,
    endpoints: Vec<String>,
    timeout: Duration,
}

impl IndexerConfigBuilder {
    pub fn new() -> Self {
        Self {
            network: None,
            endpoints: Vec::new(),
            timeout: Duration::from_secs(30),
        }
    }

    pub fn network(mut self, network: impl Into<String>) -> Self {
        self.network = Some(network.into());
        self
    }

    pub fn build(self) -> Result<IndexerConfig, ConfigError> {
        Ok(IndexerConfig {
            network: self.network.ok_or(ConfigError::MissingNetwork)?,
            endpoints: self.endpoints,
            timeout: self.timeout,
        })
    }
}
```

### Async Patterns (Tokio-specific)

```rust
// Use tokio::select! for concurrent operations
use tokio::select;

async fn process_with_timeout(data: Data) -> Result<Output, Error> {
    let processing = process_data(data);
    let timeout = tokio::time::sleep(Duration::from_secs(30));

    select! {
        result = processing => result,
        _ = timeout => Err(Error::Timeout),
    }
}

// Properly handle cancellation
async fn cancellable_operation(
    mut shutdown: tokio::sync::watch::Receiver<bool>
) -> Result<(), Error> {
    loop {
        select! {
            _ = shutdown.changed() => {
                info!("Shutting down gracefully");
                return Ok(());
            }
            result = do_work() => {
                result?;
            }
        }
    }
}

// Use bounded channels to prevent memory issues
let (tx, rx) = tokio::sync::mpsc::channel(1000); // Bounded to 1000 messages
```

### Testing Patterns

```rust
#[cfg(test)]
mod tests {
    use super::*;

    // Use descriptive test names
    #[test]
    fn block_processor_handles_empty_block() {
        // Test implementation
    }

    // Use test fixtures
    fn create_test_block() -> Block {
        Block {
            number: 12345,
            hash: "0xdeadbeef".to_string(),
            // ...
        }
    }

    // Test error cases
    #[test]
    fn invalid_block_data_returns_error() {
        let invalid_block = Block { number: 0, ..Default::default() };
        let result = process_block(invalid_block);

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            ProcessingError::InvalidData(_)
        ));
    }

    // Use proptest for property-based testing
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn serialization_roundtrip(block: Block) {
            let serialized = serde_json::to_string(&block).unwrap();
            let deserialized: Block = serde_json::from_str(&serialized).unwrap();
            assert_eq!(block, deserialized);
        }
    }
}
```

### Documentation Standards

```rust
/// Processes a blockchain block and extracts relevant events.
///
/// This function performs the following steps:
/// 1. Validates block structure
/// 2. Extracts and filters events
/// 3. Persists processed data to the store
///
/// # Arguments
///
/// * `block` - The block to process
/// * `store` - Storage backend for persistence
///
/// # Returns
///
/// Returns `Ok(ProcessedBlock)` on success, or an error if:
/// - Block validation fails
/// - Database operations fail
/// - Processing timeout occurs
///
/// # Example
///
/// ```rust
/// let block = fetch_block(12345).await?;
/// let processed = process_block(block, &store).await?;
/// println!("Processed {} events", processed.event_count);
/// ```
pub async fn process_block(
    block: Block,
    store: &Store,
) -> Result<ProcessedBlock, ProcessingError> {
    // Implementation
}

// Document invariants and safety requirements
/// SAFETY: This function assumes that `ptr` is valid and properly aligned.
/// The caller must ensure that no other references to this data exist.
unsafe fn raw_data_access(ptr: *const u8, len: usize) -> &[u8] {
    std::slice::from_raw_parts(ptr, len)
}
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

## Performance Optimization Guidelines

### Database Performance

```rust
// Use prepared statements for repeated queries
use diesel::sql_query;
use diesel::sql_types::{Integer, Text};

#[derive(QueryableByName)]
struct BlockData {
    #[sql_type = "Integer"]
    number: i32,
    #[sql_type = "Text"]
    hash: String,
}

let results = sql_query("SELECT number, hash FROM blocks WHERE number > $1")
    .bind::<Integer, _>(start_block)
    .load::<BlockData>(&conn)?;

// Batch database operations
let values: Vec<_> = entities.iter()
    .map(|e| (
        entity::id.eq(&e.id),
        entity::data.eq(&e.data),
    ))
    .collect();

diesel::insert_into(entity::table)
    .values(&values)
    .on_conflict_do_nothing()
    .execute(&conn)?;
```

### Memory Optimization

```rust
// Use Arc for shared immutable data
use std::sync::Arc;

struct SharedConfig {
    data: Arc<ConfigData>,
}

// Stream large datasets instead of loading into memory
use futures::stream::{self, StreamExt};

async fn process_large_dataset() -> Result<(), Error> {
    let mut stream = fetch_blocks_stream();

    while let Some(block) = stream.next().await {
        process_single_block(block?).await?;
    }

    Ok(())
}

// Use SmallVec for small collections
use smallvec::SmallVec;

// Stack-allocated for up to 8 items
type EventList = SmallVec<[Event; 8]>;
```

## Security Considerations

### Input Validation

```rust
// Always validate external input
pub fn parse_block_number(input: &str) -> Result<BlockNumber, ValidationError> {
    let number = input.parse::<u64>()
        .map_err(|_| ValidationError::InvalidBlockNumber)?;

    if number == 0 {
        return Err(ValidationError::BlockNumberZero);
    }

    Ok(BlockNumber(number))
}

// Use strong typing to prevent confusion
pub struct Wei(U256);
pub struct Gwei(U256);

impl From<Wei> for Gwei {
    fn from(wei: Wei) -> Self {
        Gwei(wei.0 / 1_000_000_000)
    }
}
```

### Safe Defaults

```rust
// Use secure defaults in builders
impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_retries: 3,
            enable_tls: true,  // Secure by default
            verify_certificates: true,
        }
    }
}
```

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
