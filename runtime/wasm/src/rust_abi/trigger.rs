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

/// Call trigger data for Rust handlers.
#[derive(Debug, Clone)]
pub struct RustCallTrigger {
    /// Contract address being called (20 bytes)
    pub to: [u8; 20],
    /// Caller address (20 bytes)
    pub from: [u8; 20],
    /// Transaction hash (32 bytes)
    pub tx_hash: [u8; 32],
    /// Block number
    pub block_number: u64,
    /// Block timestamp (Unix seconds)
    pub block_timestamp: u64,
    /// Block hash (32 bytes)
    pub block_hash: [u8; 32],
    /// Call input data
    pub input: Vec<u8>,
    /// Call output data
    pub output: Vec<u8>,
}

impl ToRustBytes for RustCallTrigger {
    fn to_rust_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf).expect("write to vec cannot fail");
        buf
    }
}

impl ToRustWasm for RustCallTrigger {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Fixed-size fields
        writer.write_all(&self.to)?;                              // 20 bytes
        writer.write_all(&self.from)?;                            // 20 bytes
        writer.write_all(&self.tx_hash)?;                         // 32 bytes
        writer.write_all(&self.block_number.to_le_bytes())?;      // 8 bytes
        writer.write_all(&self.block_timestamp.to_le_bytes())?;   // 8 bytes
        writer.write_all(&self.block_hash)?;                      // 32 bytes

        // Input: length + bytes
        writer.write_all(&(self.input.len() as u32).to_le_bytes())?;
        writer.write_all(&self.input)?;

        // Output: length + bytes
        writer.write_all(&(self.output.len() as u32).to_le_bytes())?;
        writer.write_all(&self.output)?;

        Ok(())
    }
}

/// Block trigger data for Rust handlers.
#[derive(Debug, Clone)]
pub struct RustBlockTrigger {
    /// Block hash (32 bytes)
    pub hash: [u8; 32],
    /// Parent block hash (32 bytes)
    pub parent_hash: [u8; 32],
    /// Block number
    pub number: u64,
    /// Block timestamp (Unix seconds)
    pub timestamp: u64,
    /// Block author/miner address (20 bytes)
    pub author: [u8; 20],
    /// Gas used in the block
    pub gas_used: u64,
    /// Gas limit for the block
    pub gas_limit: u64,
    /// Block difficulty (32 bytes, big-endian U256)
    pub difficulty: [u8; 32],
    /// Block base fee per gas (0 if pre-EIP-1559)
    pub base_fee_per_gas: u64,
}

impl ToRustBytes for RustBlockTrigger {
    fn to_rust_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf).expect("write to vec cannot fail");
        buf
    }
}

impl ToRustWasm for RustBlockTrigger {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.hash)?;                            // 32 bytes
        writer.write_all(&self.parent_hash)?;                     // 32 bytes
        writer.write_all(&self.number.to_le_bytes())?;            // 8 bytes
        writer.write_all(&self.timestamp.to_le_bytes())?;         // 8 bytes
        writer.write_all(&self.author)?;                          // 20 bytes
        writer.write_all(&self.gas_used.to_le_bytes())?;          // 8 bytes
        writer.write_all(&self.gas_limit.to_le_bytes())?;         // 8 bytes
        writer.write_all(&self.difficulty)?;                      // 32 bytes
        writer.write_all(&self.base_fee_per_gas.to_le_bytes())?;  // 8 bytes
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

    #[test]
    fn serialize_call_trigger() {
        let trigger = RustCallTrigger {
            to: [0xaa; 20],
            from: [0xbb; 20],
            tx_hash: [0xcc; 32],
            block_number: 100,
            block_timestamp: 1700000000,
            block_hash: [0xdd; 32],
            input: vec![0x12, 0x34, 0x56, 0x78],
            output: vec![0xab, 0xcd],
        };

        let bytes = trigger.to_rust_bytes();

        // Fixed: 20+20+32+8+8+32 = 120, plus 4+4 + 4+2 = 14
        assert_eq!(bytes.len(), 120 + 14);

        // Check addresses
        assert_eq!(&bytes[0..20], &[0xaa; 20]);
        assert_eq!(&bytes[20..40], &[0xbb; 20]);
    }

    #[test]
    fn serialize_block_trigger() {
        let trigger = RustBlockTrigger {
            hash: [0x11; 32],
            parent_hash: [0x22; 32],
            number: 12345678,
            timestamp: 1700000000,
            author: [0x33; 20],
            gas_used: 21000,
            gas_limit: 30000000,
            difficulty: [0x00; 32],
            base_fee_per_gas: 1000000000,
        };

        let bytes = trigger.to_rust_bytes();

        // 32+32+8+8+20+8+8+32+8 = 156 bytes
        assert_eq!(bytes.len(), 156);

        // Check hash
        assert_eq!(&bytes[0..32], &[0x11; 32]);
        assert_eq!(&bytes[32..64], &[0x22; 32]);
    }
}
