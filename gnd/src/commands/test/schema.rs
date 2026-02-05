//! JSON schema types for test files and result types.
//!
//! Test files are JSON documents that describe a sequence of mock blockchain
//! blocks with triggers (log events, block events) and GraphQL assertions to
//! validate the resulting entity state after indexing.
//!
//! ## Test file format
//!
//! ```json
//! {
//!   "name": "Transfer creates entity",
//!   "blocks": [
//!     {
//!       "number": 1,
//!       "triggers": [
//!         {
//!           "type": "log",
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

/// A mock blockchain block containing zero or more triggers.
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

    /// Triggers within this block (log events, block events).
    /// Multiple triggers per block are supported and will be sorted by
    /// graph-node's trigger ordering (block start -> events by logIndex -> block end).
    #[serde(default)]
    pub triggers: Vec<TestTrigger>,
}

/// A trigger within a block. The `type` field determines the variant.
///
/// JSON example for a log trigger:
/// ```json
/// { "type": "log", "address": "0x...", "event": "Transfer(...)", "params": {...} }
/// ```
///
/// JSON example for a block trigger:
/// ```json
/// { "type": "block" }
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TestTrigger {
    /// An Ethereum log (event) trigger. This is the most common trigger type.
    Log(LogTrigger),
    /// A block-level trigger that fires at the end of block processing.
    Block(BlockTrigger),
}

/// A mock Ethereum event log trigger.
///
/// The event signature is parsed and parameters are ABI-encoded into the
/// proper topics (indexed params) and data (non-indexed params) format
/// that graph-node expects.
#[derive(Debug, Clone, Deserialize)]
pub struct LogTrigger {
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

/// A block-level trigger. Fires as `EthereumBlockTriggerType::End`,
/// meaning it runs after all event handlers in the block.
/// No additional fields needed — the block data comes from the parent TestBlock.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BlockTrigger {}

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
    Passed,
    /// The test failed due to handler errors and/or assertion mismatches.
    Failed {
        /// If the subgraph handler threw a fatal error during indexing,
        /// this contains the error message. The test fails immediately
        /// without running assertions.
        handler_error: Option<String>,
        /// List of assertions where actual != expected.
        assertion_failures: Vec<AssertionFailure>,
    },
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
                if name.ends_with(".test.json") || name.ends_with(".json") {
                    files.push(path);
                }
            }
        }
    }

    files.sort();
    Ok(files)
}
