//! Entity serialization for Rust WASM modules.
//!
//! Serializes entity data to/from the TLV format used by Rust subgraphs.
//! Works with `HashMap<String, Value>` for deserialization since graph-node's
//! `Entity` type requires schema context for construction.

use super::types::{FromRustWasm, ToRustWasm, ValueTag};
use graph::data::store::scalar::Bytes;
use graph::data::store::{Entity, Value};
use graph::prelude::*;
use std::collections::HashMap;
use std::io::{self, Read, Write};

/// A map of field names to values, representing entity data.
/// This is what we deserialize from WASM before passing to host_exports.
pub type EntityData = HashMap<String, Value>;

/// Serialize an Entity to Rust WASM format.
///
/// Format:
/// ```text
/// field_count: u32
/// for each field:
///   key_len: u32
///   key: [u8; key_len]
///   value: Value (tagged)
/// ```
pub fn serialize_entity(entity: &Entity) -> Vec<u8> {
    let mut buf = Vec::new();
    entity.write_to(&mut buf).expect("write to vec cannot fail");
    buf
}

/// Deserialize entity data from Rust WASM format.
///
/// Returns a HashMap that can be passed to host_exports.store_set().
pub fn deserialize_entity_data(bytes: &[u8]) -> io::Result<EntityData> {
    EntityData::read_from(&mut &bytes[..])
}

impl ToRustWasm for Entity {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let fields: Vec<_> = self.into_iter().collect();
        writer.write_all(&(fields.len() as u32).to_le_bytes())?;

        for (key, value) in fields {
            // Write key (as String)
            let key_bytes = key.as_bytes();
            writer.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
            writer.write_all(key_bytes)?;
            // Write value
            value.write_to(writer)?;
        }

        Ok(())
    }
}

impl ToRustWasm for EntityData {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        writer.write_all(&(self.len() as u32).to_le_bytes())?;

        for (key, value) in self {
            // Write key
            key.as_str().write_to(writer)?;
            // Write value
            value.write_to(writer)?;
        }

        Ok(())
    }
}

impl FromRustWasm for EntityData {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let count = u32::from_le_bytes(len_buf) as usize;

        let mut fields = HashMap::with_capacity(count);
        for _ in 0..count {
            let key = String::read_from(reader)?;
            let value = Value::read_from(reader)?;
            fields.insert(key, value);
        }

        Ok(fields)
    }
}

impl ToRustWasm for Value {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        match self {
            Value::Null => {
                writer.write_all(&[ValueTag::Null as u8])?;
            }
            Value::String(s) => {
                writer.write_all(&[ValueTag::String as u8])?;
                s.as_str().write_to(writer)?;
            }
            Value::Int(n) => {
                writer.write_all(&[ValueTag::Int as u8])?;
                n.write_to(writer)?;
            }
            Value::Int8(n) => {
                writer.write_all(&[ValueTag::Int8 as u8])?;
                n.write_to(writer)?;
            }
            Value::BigInt(n) => {
                writer.write_all(&[ValueTag::BigInt as u8])?;
                n.write_to(writer)?;
            }
            Value::BigDecimal(n) => {
                writer.write_all(&[ValueTag::BigDecimal as u8])?;
                n.write_to(writer)?;
            }
            Value::Bool(b) => {
                writer.write_all(&[ValueTag::Bool as u8])?;
                b.write_to(writer)?;
            }
            Value::Bytes(b) => {
                writer.write_all(&[ValueTag::Bytes as u8])?;
                b.as_slice().to_vec().write_to(writer)?;
            }
            Value::List(arr) => {
                writer.write_all(&[ValueTag::Array as u8])?;
                writer.write_all(&(arr.len() as u32).to_le_bytes())?;
                for v in arr {
                    v.write_to(writer)?;
                }
            }
            Value::Timestamp(ts) => {
                // Serialize timestamp as BigInt (microseconds since epoch)
                writer.write_all(&[ValueTag::BigInt as u8])?;
                BigInt::from(ts.as_microseconds_since_epoch()).write_to(writer)?;
            }
        }
        Ok(())
    }
}

impl FromRustWasm for Value {
    fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        let mut tag_buf = [0u8; 1];
        reader.read_exact(&mut tag_buf)?;

        let tag = ValueTag::from_u8(tag_buf[0])
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "unknown value tag"))?;

        let value = match tag {
            ValueTag::Null => Value::Null,
            ValueTag::String => Value::String(String::read_from(reader)?),
            ValueTag::Int => Value::Int(i32::read_from(reader)?),
            ValueTag::Int8 => Value::Int8(i64::read_from(reader)?),
            ValueTag::BigInt => Value::BigInt(BigInt::read_from(reader)?),
            ValueTag::BigDecimal => Value::BigDecimal(BigDecimal::read_from(reader)?),
            ValueTag::Bool => Value::Bool(bool::read_from(reader)?),
            ValueTag::Bytes => {
                let bytes = Vec::<u8>::read_from(reader)?;
                Value::Bytes(Bytes::from(bytes))
            }
            ValueTag::Address => {
                let addr = <[u8; 20]>::read_from(reader)?;
                Value::Bytes(Bytes::from(addr.as_slice()))
            }
            ValueTag::Array => {
                let mut len_buf = [0u8; 4];
                reader.read_exact(&mut len_buf)?;
                let len = u32::from_le_bytes(len_buf) as usize;

                let mut arr = Vec::with_capacity(len);
                for _ in 0..len {
                    arr.push(Value::read_from(reader)?);
                }
                Value::List(arr)
            }
        };

        Ok(value)
    }
}

/// Add ToRustWasm impl for &str so we can serialize string slices directly.
impl ToRustWasm for &str {
    fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        let bytes = self.as_bytes();
        writer.write_all(&(bytes.len() as u32).to_le_bytes())?;
        writer.write_all(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_null_roundtrip() {
        let v = Value::Null;
        let bytes = v.to_bytes();
        let recovered = Value::from_bytes(&bytes).unwrap();
        assert_eq!(v, recovered);
    }

    #[test]
    fn value_string_roundtrip() {
        let v = Value::String("hello world".to_string());
        let bytes = v.to_bytes();
        let recovered = Value::from_bytes(&bytes).unwrap();
        assert_eq!(v, recovered);
    }

    #[test]
    fn value_int_roundtrip() {
        let v = Value::Int(42);
        let bytes = v.to_bytes();
        let recovered = Value::from_bytes(&bytes).unwrap();
        assert_eq!(v, recovered);
    }

    #[test]
    fn value_bool_roundtrip() {
        let v = Value::Bool(true);
        let bytes = v.to_bytes();
        let recovered = Value::from_bytes(&bytes).unwrap();
        assert_eq!(v, recovered);
    }

    #[test]
    fn value_array_roundtrip() {
        let v = Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::String("three".into()),
        ]);
        let bytes = v.to_bytes();
        let recovered = Value::from_bytes(&bytes).unwrap();
        assert_eq!(v, recovered);
    }

    #[test]
    fn entity_data_roundtrip() {
        let mut data = EntityData::new();
        data.insert("id".to_string(), Value::String("test-123".to_string()));
        data.insert("count".to_string(), Value::Int(42));
        data.insert(
            "balance".to_string(),
            Value::BigInt(BigInt::from(1000000000000_u64)),
        );
        data.insert("active".to_string(), Value::Bool(true));

        let bytes = data.to_bytes();
        let recovered = EntityData::from_bytes(&bytes).unwrap();

        assert_eq!(data.get("id"), recovered.get("id"));
        assert_eq!(data.get("count"), recovered.get("count"));
        assert_eq!(data.get("balance"), recovered.get("balance"));
        assert_eq!(data.get("active"), recovered.get("active"));
    }
}
