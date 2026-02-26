//! JSON schema types for test files and result types.
//!
//! Test files are JSON documents that describe a sequence of mock blockchain
//! blocks with triggers (log events) and GraphQL assertions to validate the
//! resulting entity state after indexing. Block triggers are auto-injected
//! for every block (both `Start` and `End` types) so block handlers with any
//! filter (`once`, `polling`, or none) fire correctly without explicit config.
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

/// Top-level test file. A named test case with mock blocks and GraphQL assertions.
#[derive(Debug, Clone, Deserialize)]
pub struct TestFile {
    pub name: String,

    /// Ordered sequence of mock blocks to index.
    #[serde(default)]
    pub blocks: Vec<TestBlock>,

    /// GraphQL assertions to run after indexing.
    #[serde(default)]
    pub assertions: Vec<Assertion>,
}

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

    /// Unix timestamp in seconds. If omitted, defaults to the block number
    /// (monotonically increasing, chain-agnostic).
    #[serde(default)]
    pub timestamp: Option<u64>,

    /// Log events within this block. Block triggers (Start/End) are auto-injected.
    #[serde(default)]
    pub events: Vec<LogEvent>,

    /// Mock contract call responses pre-cached before the test runs.
    #[serde(default, rename = "ethCalls")]
    pub eth_calls: Vec<MockEthCall>,
}

/// A mock Ethereum event log.
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

    /// Explicit tx hash, or generated as `keccak256(block_number || log_index)`.
    #[serde(default)]
    pub tx_hash: Option<String>,
}

/// A mock contract call response pre-cached for a specific block.
#[derive(Debug, Clone, Deserialize)]
pub struct MockEthCall {
    pub address: String,
    pub function: String,
    pub params: Vec<Value>,
    pub returns: Vec<Value>,

    #[serde(default)]
    pub reverts: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Assertion {
    pub query: String,

    /// Expected JSON result. String/number coercion is applied (BigInt/BigDecimal).
    pub expected: Value,
}

#[derive(Debug)]
pub struct TestResult {
    pub handler_error: Option<String>,
    pub assertions: Vec<AssertionOutcome>,
}

impl TestResult {
    pub fn is_passed(&self) -> bool {
        self.handler_error.is_none()
            && self
                .assertions
                .iter()
                .all(|a| matches!(a, AssertionOutcome::Passed { .. }))
    }
}

#[derive(Debug)]
pub enum AssertionOutcome {
    Passed { query: String },
    Failed(AssertionFailure),
}

#[derive(Debug)]
pub struct AssertionFailure {
    pub query: String,
    pub expected: Value,
    pub actual: Value,
}

/// Parse a JSON test file. NOTE: Only validates JSON schema, not semantic correctness.
pub fn parse_test_file(path: &Path) -> anyhow::Result<TestFile> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("Failed to read test file {}: {}", path.display(), e))?;
    serde_json::from_str(&content)
        .map_err(|e| anyhow::anyhow!("Failed to parse test file {}: {}", path.display(), e))
}

/// Discover `*.json` / `*.test.json` test files in a directory (recursive). Skips entries starting with non-alphanumeric characters.
pub fn discover_test_files(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();

    if !dir.exists() {
        return Ok(files);
    }

    discover_recursive(dir, &mut files)?;
    files.sort();
    Ok(files)
}

fn discover_recursive(dir: &Path, files: &mut Vec<PathBuf>) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = match path.file_name().and_then(|n| n.to_str()) {
            Some(n) => n,
            None => continue,
        };

        // Skip entries whose name starts with a non-alphanumeric character.
        if !name.starts_with(|c: char| c.is_alphanumeric()) {
            continue;
        }

        if path.is_dir() {
            discover_recursive(&path, files)?;
        } else if path.is_file() && (name.ends_with(".test.json") || name.ends_with(".json")) {
            files.push(path);
        }
    }

    Ok(())
}
