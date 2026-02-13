//! JSON schema types for test files and result types.
//!
//! Test files are JSON documents that describe a sequence of mock blockchain
//! blocks with triggers (log events) and GraphQL assertions to validate the
//! resulting entity state after indexing. Block triggers are auto-injected
//! for every block (both `Start` and `End` types) so block handlers with any
//! filter (`once`, `polling`, or none) fire correctly without explicit config.
//!
//! ## Test file format
//!
//! ```json
//! {
//!   "name": "Transfer creates entity",
//!   "blocks": [
//!     {
//!       "number": 1,
//!       "events": [
//!         {
//!           "address": "0x1234...",
//!           "event": "Transfer(address indexed from, address indexed to, uint256 value)",
//!           "params": { "from": "0xaaaa...", "to": "0xbbbb...", "value": "1000" }
//!         }
//!       ]
//!     }
//!   ],
//!   "assertions": [
//!     {
//!       "query": "{ transfer(id: \"1\") { from to value } }",
//!       "expected": { "transfer": { "from": "0xaaaa...", "to": "0xbbbb...", "value": "1000" } }
//!     }
//!   ]
//! }
//! ```

use serde::Deserialize;
use serde_json::Value;
use std::path::{Path, PathBuf};

// ============ JSON Input Types ============

/// Top-level test file structure. Each file represents one named test case
/// with a sequence of blocks to index and assertions to check afterward.
#[derive(Debug, Clone, Deserialize)]
pub struct TestFile {
    /// Human-readable name for this test case (shown in output).
    pub name: String,

    /// Ordered sequence of blocks to feed through the indexer.
    /// Blocks are processed sequentially; triggers within each block are
    /// sorted by graph-node's standard trigger ordering logic.
    #[serde(default)]
    pub blocks: Vec<TestBlock>,

    /// GraphQL assertions to run after all blocks have been indexed.
    /// Each assertion queries the subgraph and compares the result to an expected value.
    #[serde(default)]
    pub assertions: Vec<Assertion>,
}

/// A mock blockchain block containing zero or more events.
#[derive(Debug, Clone, Deserialize)]
pub struct TestBlock {
    /// Block number. If omitted, auto-increments starting from `start_block`
    /// (default 1). Explicit numbers allow gaps (e.g., blocks 1, 5, 100).
    #[serde(default)]
    pub number: Option<u64>,

    /// Block hash as hex string (e.g., "0xabc..."). If omitted, generated
    /// deterministically as `keccak256(block_number)`.
    #[serde(default)]
    pub hash: Option<String>,

    /// Unix timestamp. If omitted, defaults to `block_number * 12`
    /// (simulating 12-second block times).
    #[serde(default)]
    pub timestamp: Option<u64>,

    /// Base fee per gas (EIP-1559). If omitted, defaults to None (pre-EIP-1559 blocks).
    /// Specified as a decimal string to handle large values (e.g., "15000000000").
    #[serde(default, rename = "baseFeePerGas")]
    pub base_fee_per_gas: Option<String>,

    /// Log events within this block. Block triggers are auto-injected.
    /// Multiple events per block are supported and will be sorted by
    /// graph-node's trigger ordering (block start -> events by logIndex -> block end).
    #[serde(default)]
    pub events: Vec<LogEvent>,

    /// Mock contract call responses for this specific block.
    /// These are pre-cached in the database before the test runs so that
    /// `ethereum.call()` invocations in handlers return the mocked values.
    #[serde(default, rename = "ethCalls")]
    pub eth_calls: Vec<MockEthCall>,
}

/// A mock Ethereum event log.
///
/// The event signature is parsed and parameters are ABI-encoded into the
/// proper topics (indexed params) and data (non-indexed params) format
/// that graph-node expects.
///
/// JSON example:
/// ```json
/// { "address": "0x...", "event": "Transfer(...)", "params": {...} }
/// ```
#[derive(Debug, Clone, Deserialize)]
pub struct LogEvent {
    /// Contract address that emitted the event (checksummed or lowercase hex).
    pub address: String,

    /// Full event signature including parameter names and `indexed` keywords.
    /// Example: `"Transfer(address indexed from, address indexed to, uint256 value)"`
    ///
    /// The signature is parsed to determine:
    /// - topic0 (keccak256 hash of the canonical signature)
    /// - Which parameters are indexed (become topics) vs non-indexed (become data)
    pub event: String,

    /// Event parameter values keyed by name. Values are JSON strings/numbers
    /// that get converted to the appropriate Solidity type:
    /// - Addresses: hex string `"0x1234..."`
    /// - Integers: string `"1000000000000000000"` or number `1000`
    /// - Booleans: `true` / `false`
    /// - Bytes: hex string `"0xdeadbeef"`
    #[serde(default)]
    pub params: serde_json::Map<String, Value>,

    /// Transaction hash. If omitted, generated deterministically as
    /// `keccak256(block_number || log_index)`.
    #[serde(default)]
    pub tx_hash: Option<String>,
}

/// A mock contract call response that will be pre-cached for a specific block.
///
/// When a subgraph handler calls `ethereum.call()` during indexing, graph-node
/// looks up the result in its call cache. By pre-populating this cache with
/// mock responses, tests can control what contract calls return without needing
/// a real Ethereum node.
#[derive(Debug, Clone, Deserialize)]
pub struct MockEthCall {
    /// Contract address to mock (checksummed or lowercase hex).
    pub address: String,

    /// Function signature to mock.
    /// Example: `"balanceOf(address):(uint256)"`
    pub function: String,

    /// Input parameters for the function call.
    pub params: Vec<Value>,

    /// Return values for the function call.
    pub returns: Vec<Value>,

    /// If true, the call will revert instead of returning values.
    #[serde(default)]
    pub reverts: bool,
}

/// A GraphQL assertion to validate indexed entity state.
#[derive(Debug, Clone, Deserialize)]
#[allow(dead_code)]
pub struct Assertion {
    /// GraphQL query string. Example: `"{ transfer(id: \"1\") { from to value } }"`
    pub query: String,

    /// Expected JSON result. Compared against the actual query response.
    /// Object key order doesn't matter. String-vs-number coercion is applied
    /// to handle GraphQL's BigInt/BigDecimal string representation.
    pub expected: Value,
}

// ============ Result Types ============

/// Outcome of running a single test case.
#[derive(Debug)]
pub enum TestResult {
    /// All assertions passed and no handler errors occurred.
    Passed {
        /// Per-assertion outcomes (all passed).
        assertions: Vec<AssertionOutcome>,
    },
    /// The test failed due to handler errors and/or assertion mismatches.
    Failed {
        /// If the subgraph handler threw a fatal error during indexing,
        /// this contains the error message. The test fails immediately
        /// without running assertions.
        handler_error: Option<String>,
        /// Per-assertion outcomes (mix of passed and failed).
        assertions: Vec<AssertionOutcome>,
    },
}

impl TestResult {
    pub fn is_passed(&self) -> bool {
        matches!(self, TestResult::Passed { .. })
    }

    pub fn assertions(&self) -> &[AssertionOutcome] {
        match self {
            TestResult::Passed { assertions } | TestResult::Failed { assertions, .. } => assertions,
        }
    }

    pub fn handler_error(&self) -> Option<&str> {
        match self {
            TestResult::Failed {
                handler_error: Some(e),
                ..
            } => Some(e),
            _ => None,
        }
    }
}

/// Outcome of a single assertion query.
#[derive(Debug)]
pub enum AssertionOutcome {
    /// The assertion passed — actual matched expected.
    Passed {
        /// The GraphQL query that was executed.
        query: String,
    },
    /// The assertion failed — actual did not match expected.
    Failed(AssertionFailure),
}

/// Details about a single failed assertion.
#[derive(Debug)]
pub struct AssertionFailure {
    /// The GraphQL query that was executed.
    pub query: String,
    /// What the test expected to get back.
    pub expected: Value,
    /// What the query actually returned.
    pub actual: Value,
}

// ============ Parsing ============

/// Parse a JSON test file from disk into a [`TestFile`].
pub fn parse_test_file(path: &Path) -> anyhow::Result<TestFile> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read test file {}: {}", path.display(), e))?;
    serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse test file {}: {}", path.display(), e))
}

/// Discover test files in a directory.
///
/// Matches `*.json` and `*.test.json` files (non-recursive).
/// Returns paths sorted alphabetically for deterministic execution order.
pub fn discover_test_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if !dir.exists() {
        return Ok(files);
    }

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_file() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if (name.ends_with(".test.json") || name.ends_with(".json"))
                    && !name.starts_with('.')
                {
                    files.push(path);
                }
            }
        }
    }

    files.sort();
    Ok(files)
}
