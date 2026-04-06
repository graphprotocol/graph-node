//! Rust WASM serialization types.
//!
//! Defines traits and constants for serializing data between
//! graph-node and Rust WASM modules.

use graph::prelude::*;
use std::io::{self, Read, Write};
use std::str::FromStr;

/// Canonical TLV value tag bytes.
///
/// These are the single source of truth for the tag byte of each `Value`
/// variant on the wire. They match the table in
/// `docs/rust-abi-spec.md` section 4.6 one-for-one. Changing any value in
/// this module is a breaking ABI change and requires an `apiVersion` bump.
pub mod tags {
    pub const NULL: u8 = 0x00;
    pub const STRING: u8 = 0x01;
    pub const INT: u8 = 0x02;
    pub const INT8: u8 = 0x03;
    pub const BIG_INT: u8 = 0x04;
    pub const BIG_DECIMAL: u8 = 0x05;
    pub const BOOL: u8 = 0x06;
    pub const BYTES: u8 = 0x07;
    pub const ADDRESS: u8 = 0x08;
    pub const ARRAY: u8 = 0x09;
}

/// Value type tags for TLV serialization.
///
/// Discriminants are pulled from the [`tags`] module so there is a single
/// authoritative definition of each on-wire byte.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueTag {
    Null = tags::NULL,
    String = tags::STRING,
    Int = tags::INT,
    Int8 = tags::INT8,
    BigInt = tags::BIG_INT,
    BigDecimal = tags::BIG_DECIMAL,
    Bool = tags::BOOL,
    Bytes = tags::BYTES,
    Address = tags::ADDRESS,
    Array = tags::ARRAY,
}

impl ValueTag {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            tags::NULL => Some(Self::Null),
            tags::STRING => Some(Self::String),
            tags::INT => Some(Self::Int),
            tags::INT8 => Some(Self::Int8),
            tags::BIG_INT => Some(Self::BigInt),
            tags::BIG_DECIMAL => Some(Self::BigDecimal),
            tags::BOOL => Some(Self::Bool),
            tags::BYTES => Some(Self::Bytes),
            tags::ADDRESS => Some(Self::Address),
            tags::ARRAY => Some(Self::Array),
            _ => None,
        }
    }
}

/// Trait for types that can be serialized to Rust WASM format.
pub trait ToRustWasm {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()>;

    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf).expect("write to vec cannot fail");
        buf
    }
}

/// Trait for types that can be deserialized from Rust WASM format.
pub trait FromRustWasm: Sized {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self>;

    fn from_bytes(bytes: &[u8]) -> io::Result<Self> {
        Self::read_from(&mut &bytes[..])
    }
}

// ============================================================================
// Primitive implementations
// ============================================================================

impl ToRustWasm for String {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let bytes = self.as_bytes();
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(bytes)
    }
}

impl FromRustWasm for String {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;

        String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl ToRustWasm for i32 {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }
}

impl FromRustWasm for i32 {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; 4];
        reader.read_exact(&mut buf)?;
        Ok(i32::from_le_bytes(buf))
    }
}

impl ToRustWasm for i64 {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&self.to_le_bytes())
    }
}

impl FromRustWasm for i64 {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; 8];
        reader.read_exact(&mut buf)?;
        Ok(i64::from_le_bytes(buf))
    }
}

impl ToRustWasm for bool {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&[if *self { 1 } else { 0 }])
    }
}

impl FromRustWasm for bool {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; 1];
        reader.read_exact(&mut buf)?;
        Ok(buf[0] != 0)
    }
}

impl ToRustWasm for Vec<u8> {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&(self.len() as u32).to_le_bytes())?;
        writer.write_all(self)
    }
}

impl FromRustWasm for Vec<u8> {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl ToRustWasm for [u8; 20] {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(self)
    }
}

impl FromRustWasm for [u8; 20] {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; 20];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

impl ToRustWasm for [u8; 32] {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(self)
    }
}

impl FromRustWasm for [u8; 32] {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut buf = [0u8; 32];
        reader.read_exact(&mut buf)?;
        Ok(buf)
    }
}

// ============================================================================
// Graph-specific type implementations
// ============================================================================

impl ToRustWasm for BigInt {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let bytes = self.to_signed_bytes_le();
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(&bytes)
    }
}

impl FromRustWasm for BigInt {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf) as usize;

        let mut buf = vec![0u8; len];
        reader.read_exact(&mut buf)?;
        BigInt::from_signed_bytes_le(&buf)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

impl ToRustWasm for BigDecimal {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Serialize as string for simplicity and accuracy
        let s = self.to_string();
        s.write_to(writer)
    }
}

impl FromRustWasm for BigDecimal {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let s = String::read_from(reader)?;
        BigDecimal::from_str(&s).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn string_roundtrip() {
        let s = "hello world".to_string();
        let bytes = s.to_bytes();
        let recovered = String::from_bytes(&bytes).unwrap();
        assert_eq!(s, recovered);
    }

    #[test]
    fn bigint_roundtrip() {
        let n = BigInt::from(12345678901234567890_u128);
        let bytes = n.to_bytes();
        let recovered = BigInt::from_bytes(&bytes).unwrap();
        assert_eq!(n, recovered);
    }

    #[test]
    fn negative_bigint_roundtrip() {
        let n = BigInt::from(-999999999999_i64);
        let bytes = n.to_bytes();
        let recovered = BigInt::from_bytes(&bytes).unwrap();
        assert_eq!(n, recovered);
    }
}
