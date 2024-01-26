use std::{
    fmt::{Debug, Formatter},
    str::FromStr,
};

use graph::anyhow;
use serde::{de::Visitor, Deserialize};

/// Represents the primitive `FieldElement` type used in Starknet. Each `FieldElement` is 252-bit
/// in size.
#[derive(Clone, PartialEq, Eq)]
pub struct Felt([u8; 32]);

struct FeltVisitor;

impl Debug for Felt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "0x{}", hex::encode(self.0))
    }
}

impl From<[u8; 32]> for Felt {
    fn from(value: [u8; 32]) -> Self {
        Self(value)
    }
}

impl AsRef<[u8]> for Felt {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl FromStr for Felt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let hex_str = s.trim_start_matches("0x");
        if hex_str.len() % 2 == 0 {
            Ok(Felt(decode_even_hex_str(hex_str)?))
        } else {
            // We need to manually pad it as the `hex` crate does not allow odd hex length
            let mut padded_string = String::from("0");
            padded_string.push_str(hex_str);

            Ok(Felt(decode_even_hex_str(&padded_string)?))
        }
    }
}

impl<'de> Deserialize<'de> for Felt {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(FeltVisitor)
    }
}

impl<'de> Visitor<'de> for FeltVisitor {
    type Value = Felt;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(formatter, "string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Felt::from_str(v).map_err(|_| {
            serde::de::Error::invalid_value(serde::de::Unexpected::Str(v), &"valid Felt value")
        })
    }
}

/// Attempts to decode a even-length hex string into a padded 32-byte array.
pub fn decode_even_hex_str(hex_str: &str) -> anyhow::Result<[u8; 32]> {
    let byte_len = hex_str.len() / 2;
    if byte_len > 32 {
        anyhow::bail!("length exceeds 32 bytes");
    }

    let mut buffer = [0u8; 32];
    hex::decode_to_slice(hex_str, &mut buffer[(32 - byte_len)..])?;

    Ok(buffer)
}
