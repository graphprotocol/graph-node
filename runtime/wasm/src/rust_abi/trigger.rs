//! Trigger serialization for Rust WASM modules.
//!
//! Provides the `ToRustBytes` trait for serializing blockchain triggers
//! to the TLV format expected by Graphite SDK handlers.

use super::types::ToRustWasm;
use std::io::{self, Write};

/// Trait for serializing trigger data to Rust WASM format.
///
/// Implemented by chain-specific trigger types (e.g., Ethereum MappingTrigger).
/// The serialized format matches what Graphite SDK's `FromWasmBytes` expects.
pub trait ToRustBytes {
    /// Serialize to TLV bytes for Rust handlers.
    fn to_rust_bytes(&self) -> Vec<u8>;
}

/// Log trigger data in a format suitable for serialization.
///
/// This struct provides a chain-agnostic representation of a log trigger
/// that can be serialized for Rust WASM handlers.
#[derive(Debug, Clone)]
pub struct RustLogTrigger {
    /// Contract address that emitted the log (20 bytes)
    pub address: [u8; 20],
    /// Transaction hash (32 bytes)
    pub tx_hash: [u8; 32],
    /// Log index within the block
    pub log_index: u64,
    /// Block number
    pub block_number: u64,
    /// Block timestamp (Unix seconds)
    pub block_timestamp: u64,
    /// Log topics (event selector + indexed params)
    pub topics: Vec<[u8; 32]>,
    /// ABI-encoded non-indexed event data
    pub data: Vec<u8>,
}

impl ToRustBytes for RustLogTrigger {
    fn to_rust_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf).expect("write to vec cannot fail");
        buf
    }
}

impl ToRustWasm for RustLogTrigger {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Fixed-size fields first (no length prefix)
        writer.write_all(&self.address)?;        // 20 bytes
        writer.write_all(&self.tx_hash)?;        // 32 bytes
        writer.write_all(&self.log_index.to_le_bytes())?;      // 8 bytes
        writer.write_all(&self.block_number.to_le_bytes())?;   // 8 bytes
        writer.write_all(&self.block_timestamp.to_le_bytes())?; // 8 bytes

        // Topics array: count + data
        writer.write_all(&(self.topics.len() as u32).to_le_bytes())?;
        for topic in &self.topics {
            writer.write_all(topic)?;  // 32 bytes each
        }

        // Data: length + bytes
        writer.write_all(&(self.data.len() as u32).to_le_bytes())?;
        writer.write_all(&self.data)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize_log_trigger() {
        let trigger = RustLogTrigger {
            address: [0xde; 20],
            tx_hash: [0xab; 32],
            log_index: 42,
            block_number: 12345678,
            block_timestamp: 1700000000,
            topics: vec![[0x11; 32], [0x22; 32]],
            data: vec![1, 2, 3, 4],
        };

        let bytes = trigger.to_rust_bytes();

        // Verify structure:
        // 20 (address) + 32 (tx_hash) + 8*3 (log_index, block_number, timestamp)
        // + 4 (topics count) + 64 (2 topics) + 4 (data len) + 4 (data)
        assert_eq!(bytes.len(), 20 + 32 + 24 + 4 + 64 + 4 + 4);

        // Check address
        assert_eq!(&bytes[0..20], &[0xde; 20]);

        // Check topics count
        let topics_offset = 20 + 32 + 24;
        let topics_count = u32::from_le_bytes(bytes[topics_offset..topics_offset+4].try_into().unwrap());
        assert_eq!(topics_count, 2);
    }
}
