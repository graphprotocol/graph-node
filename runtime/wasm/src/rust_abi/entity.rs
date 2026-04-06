//! Entity serialization for Rust WASM modules.
//!
//! Serializes entity data to/from the TLV format used by Rust subgraphs.
//! Works with `HashMap<String, Value>` for deserialization since graph-node's
//! `Entity` type requires schema context for construction.

use super::types::{tags, FromRustWasm, ToRustWasm, ValueTag};
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
                writer.write_all(&[tags::NULL])?;
            }
            Value::String(s) => {
                writer.write_all(&[tags::STRING])?;
                s.as_str().write_to(writer)?;
            }
            Value::Int(n) => {
                writer.write_all(&[tags::INT])?;
                n.write_to(writer)?;
            }
            Value::Int8(n) => {
                writer.write_all(&[tags::INT8])?;
                n.write_to(writer)?;
            }
            Value::BigInt(n) => {
                writer.write_all(&[tags::BIG_INT])?;
                n.write_to(writer)?;
            }
            Value::BigDecimal(n) => {
                writer.write_all(&[tags::BIG_DECIMAL])?;
                n.write_to(writer)?;
            }
            Value::Bool(b) => {
                writer.write_all(&[tags::BOOL])?;
                b.write_to(writer)?;
            }
            Value::Bytes(b) => {
                writer.write_all(&[tags::BYTES])?;
                b.as_slice().to_vec().write_to(writer)?;
            }
            Value::List(arr) => {
                writer.write_all(&[tags::ARRAY])?;
                writer.write_all(&(arr.len() as u32).to_le_bytes())?;
                for v in arr {
                    v.write_to(writer)?;
                }
            }
            Value::Timestamp(ts) => {
                // Serialize timestamp as BigInt (microseconds since epoch)
                writer.write_all(&[tags::BIG_INT])?;
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

    // -------------------------------------------------------------------------
    // ABI cross-validation vectors.
    //
    // These tests encode known byte sequences by hand and assert that
    // graph-node's ToRustWasm/FromRustWasm impls produce identical bytes.
    // The same raw bytes are validated against the Graphite SDK in
    // graphite/src/abi_vectors_tests.rs.
    // -------------------------------------------------------------------------

    fn le32(n: u32) -> [u8; 4] {
        n.to_le_bytes()
    }

    // -- Null (tag 0x00) --

    #[test]
    fn abi_vec_null_encodes_to_single_byte() {
        let bytes = Value::Null.to_bytes();
        assert_eq!(bytes, [0x00u8]);
    }

    #[test]
    fn abi_vec_null_decode() {
        let v = Value::from_bytes(&[0x00u8]).unwrap();
        assert_eq!(v, Value::Null);
    }

    // -- String (tag 0x01, len:u32 LE, utf-8 bytes) --

    #[test]
    fn abi_vec_string_known_bytes() {
        // "hi" → [0x01, 0x02 0x00 0x00 0x00, 0x68 0x69]
        let v = Value::String("hi".to_string());
        let bytes = v.to_bytes();
        let mut expected = vec![0x01u8];
        expected.extend_from_slice(&le32(2));
        expected.extend_from_slice(b"hi");
        assert_eq!(bytes, expected);
    }

    #[test]
    fn abi_vec_string_decode_known_bytes() {
        let mut raw = vec![0x01u8];
        raw.extend_from_slice(&le32(2));
        raw.extend_from_slice(b"hi");
        let v = Value::from_bytes(&raw).unwrap();
        assert_eq!(v, Value::String("hi".to_string()));
    }

    // -- Int (tag 0x02, i32 LE 4 bytes) --

    #[test]
    fn abi_vec_int_known_bytes() {
        let v = Value::Int(42);
        let bytes = v.to_bytes();
        let mut expected = vec![0x02u8];
        expected.extend_from_slice(&42i32.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    #[test]
    fn abi_vec_int_negative() {
        let v = Value::Int(-1);
        let bytes = v.to_bytes();
        let mut expected = vec![0x02u8];
        expected.extend_from_slice(&(-1i32).to_le_bytes());
        assert_eq!(bytes, expected);
    }

    // -- Int8 (tag 0x03, i64 LE 8 bytes) --

    #[test]
    fn abi_vec_int8_known_bytes() {
        let v = Value::Int8(i64::MAX);
        let bytes = v.to_bytes();
        let mut expected = vec![0x03u8];
        expected.extend_from_slice(&i64::MAX.to_le_bytes());
        assert_eq!(bytes, expected);
    }

    // -- BigInt (tag 0x04, len:u32 LE, signed-LE bytes) --

    #[test]
    fn abi_vec_bigint_uses_signed_le() {
        // 1000 in signed-LE is [0xe8, 0x03]
        let n = BigInt::from(1000u64);
        let le = n.to_signed_bytes_le();
        assert_eq!(le, vec![0xe8u8, 0x03]);

        let v = Value::BigInt(n);
        let bytes = v.to_bytes();
        assert_eq!(bytes[0], 0x04);
        let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        assert_eq!(len, 2);
        assert_eq!(&bytes[5..], &[0xe8u8, 0x03]);
    }

    #[test]
    fn abi_vec_bigint_decode_known_le_bytes() {
        // hand-encoded 1000 as signed-LE
        let mut raw = vec![0x04u8];
        raw.extend_from_slice(&le32(2));
        raw.push(0xe8);
        raw.push(0x03);
        let v = Value::from_bytes(&raw).unwrap();
        assert_eq!(v, Value::BigInt(BigInt::from(1000u64)));
    }

    #[test]
    fn abi_vec_bigint_zero_len_zero() {
        let v = Value::BigInt(BigInt::from(0i32));
        let bytes = v.to_bytes();
        // zero may encode as empty or as [0x00]; either is valid as long as decode round-trips
        let recovered = Value::from_bytes(&bytes).unwrap();
        assert_eq!(recovered, Value::BigInt(BigInt::from(0i32)));
    }

    // -- BigDecimal (tag 0x05, len:u32 LE, UTF-8 string) --

    #[test]
    fn abi_vec_bigdecimal_encodes_as_string() {
        use std::str::FromStr;
        let d = graph::prelude::BigDecimal::from_str("3.14").unwrap();
        let v = Value::BigDecimal(d);
        let bytes = v.to_bytes();
        assert_eq!(bytes[0], 0x05);
        let len = u32::from_le_bytes(bytes[1..5].try_into().unwrap()) as usize;
        let s = std::str::from_utf8(&bytes[5..5 + len]).unwrap();
        // must be a valid decimal string representation
        assert!(s.contains('.') || s.chars().all(|c| c.is_ascii_digit() || c == '-' || c == 'E' || c == 'e'),
            "expected decimal string, got: {}", s);
    }

    #[test]
    fn abi_vec_bigdecimal_decode_known_bytes() {
        use std::str::FromStr;
        let s = b"3.14";
        let mut raw = vec![0x05u8];
        raw.extend_from_slice(&le32(s.len() as u32));
        raw.extend_from_slice(s);
        let v = Value::from_bytes(&raw).unwrap();
        let expected = Value::BigDecimal(graph::prelude::BigDecimal::from_str("3.14").unwrap());
        assert_eq!(v, expected);
    }

    // -- Bool (tag 0x06) --

    #[test]
    fn abi_vec_bool_true_known_bytes() {
        assert_eq!(Value::Bool(true).to_bytes(), [0x06u8, 0x01]);
    }

    #[test]
    fn abi_vec_bool_false_known_bytes() {
        assert_eq!(Value::Bool(false).to_bytes(), [0x06u8, 0x00]);
    }

    // -- Bytes (tag 0x07, len:u32 LE, raw bytes) --

    #[test]
    fn abi_vec_bytes_known_bytes() {
        use graph::data::store::scalar::Bytes as StoreBytes;
        let payload = vec![0xde, 0xad, 0xbe, 0xef];
        let v = Value::Bytes(StoreBytes::from(payload.clone()));
        let encoded = v.to_bytes();
        let mut expected = vec![0x07u8];
        expected.extend_from_slice(&le32(4));
        expected.extend_from_slice(&payload);
        assert_eq!(encoded, expected);
    }

    // -- Address (tag 0x08, 20 raw bytes, NO length prefix) --

    #[test]
    fn abi_vec_address_decode_tag_0x08() {
        // The SDK writes Address as tag 0x08 + 20 raw bytes (no length prefix).
        // graph-node decodes tag 0x08 as Value::Bytes(20 bytes).
        // Note: graph-node re-encodes Bytes as tag 0x07 (length-prefixed); the
        // asymmetry is intentional — 0x08 is a SDK-write/host-read optimisation.
        let addr = [0xabu8; 20];
        let mut raw = vec![0x08u8];
        raw.extend_from_slice(&addr);
        assert_eq!(raw.len(), 21); // tag + 20 bytes, no length prefix

        // graph-node must decode SDK-written 0x08 as Bytes carrying the 20-byte address
        let v = Value::from_bytes(&raw).unwrap();
        if let Value::Bytes(b) = &v {
            assert_eq!(b.as_slice(), &addr);
        } else {
            panic!("expected Bytes for Address tag, got {:?}", v);
        }
    }

    // -- Array (tag 0x09, len:u32 LE, tagged Values) --

    #[test]
    fn abi_vec_array_empty() {
        let v = Value::List(vec![]);
        let bytes = v.to_bytes();
        let mut expected = vec![0x09u8];
        expected.extend_from_slice(&le32(0));
        assert_eq!(bytes, expected);
    }

    #[test]
    fn abi_vec_array_known_bytes() {
        // [Int(1), Bool(true)]
        let v = Value::List(vec![Value::Int(1), Value::Bool(true)]);
        let bytes = v.to_bytes();
        assert_eq!(bytes[0], 0x09);
        let count = u32::from_le_bytes(bytes[1..5].try_into().unwrap());
        assert_eq!(count, 2);
    }

    #[test]
    fn abi_vec_array_decode_known_bytes() {
        let mut raw = vec![0x09u8];
        raw.extend_from_slice(&le32(2));
        raw.push(0x02);
        raw.extend_from_slice(&1i32.to_le_bytes());
        raw.push(0x06);
        raw.push(0x01);
        let v = Value::from_bytes(&raw).unwrap();
        assert_eq!(v, Value::List(vec![Value::Int(1), Value::Bool(true)]));
    }

    // -- Cross-wire: spec worked example --

    #[test]
    fn abi_vec_spec_worked_example() {
        // { id: "tx-1", value: 42, active: true }  (spec section 4.6.1)
        // field_count: 3, field order unspecified but all fields must survive
        let mut data = EntityData::new();
        data.insert("id".to_string(), Value::String("tx-1".to_string()));
        data.insert("value".to_string(), Value::Int(42));
        data.insert("active".to_string(), Value::Bool(true));

        let bytes = data.to_bytes();

        // field count must be 3
        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(count, 3);

        let recovered = EntityData::from_bytes(&bytes).unwrap();
        assert_eq!(recovered.get("id"), Some(&Value::String("tx-1".to_string())));
        assert_eq!(recovered.get("value"), Some(&Value::Int(42)));
        assert_eq!(recovered.get("active"), Some(&Value::Bool(true)));
    }
}
