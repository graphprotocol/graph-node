//! Rust WASM serialization types.
//!
//! Defines traits and constants for serializing data between
//! graph-node and Rust WASM modules.

use graph::prelude::*;
use std::io::{self, Read, Write};
use std::str::FromStr;

/// Value type tags for TLV serialization.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueTag {
    Null = 0x00,
    String = 0x01,
    Int = 0x02,
    Int8 = 0x03,
    BigInt = 0x04,
    BigDecimal = 0x05,
    Bool = 0x06,
    Bytes = 0x07,
    Address = 0x08,
    Array = 0x09,
}

impl ValueTag {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x00 => Some(Self::Null),
            0x01 => Some(Self::String),
            0x02 => Some(Self::Int),
            0x03 => Some(Self::Int8),
            0x04 => Some(Self::BigInt),
            0x05 => Some(Self::BigDecimal),
            0x06 => Some(Self::Bool),
            0x07 => Some(Self::Bytes),
            0x08 => Some(Self::Address),
            0x09 => Some(Self::Array),
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
