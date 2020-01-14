use hex;
use num_bigint;
use serde::{self, Deserialize, Serialize};
use web3::types::*;

use std::fmt::{self, Display, Formatter};
use std::ops::{Add, Div, Mul, Rem, Sub};
use std::str::FromStr;

pub use num_bigint::Sign as BigIntSign;

// Caveat: The exponent is currently an i64 and may overflow.
// See https://github.com/akubera/bigdecimal-rs/issues/54.
pub type BigDecimal = bigdecimal::BigDecimal;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct BigInt(num_bigint::BigInt);

impl BigInt {
    pub fn from_unsigned_bytes_le(bytes: &[u8]) -> Self {
        BigInt(num_bigint::BigInt::from_bytes_le(
            num_bigint::Sign::Plus,
            bytes,
        ))
    }

    pub fn from_signed_bytes_le(bytes: &[u8]) -> Self {
        BigInt(num_bigint::BigInt::from_signed_bytes_le(bytes))
    }

    pub fn to_bytes_le(&self) -> (BigIntSign, Vec<u8>) {
        self.0.to_bytes_le()
    }

    pub fn to_bytes_be(&self) -> (BigIntSign, Vec<u8>) {
        self.0.to_bytes_be()
    }

    pub fn to_signed_bytes_le(&self) -> Vec<u8> {
        self.0.to_signed_bytes_le()
    }

    pub fn to_u64(&self) -> u64 {
        let (sign, bytes) = self.to_bytes_le();

        if sign == num_bigint::Sign::Minus {
            panic!("cannot convert negative BigInt into u64");
        }

        if bytes.len() > 8 {
            panic!("BigInt value is too large for a u64");
        }

        // Replace this with u64::from_le_bytes when stabilized
        let mut n = 0u64;
        let mut shift_dist = 0;
        for b in bytes {
            n = ((b as u64) << shift_dist) | n;
            shift_dist += 8;
        }
        n
    }

    pub fn from_unsigned_u256(n: &U256) -> Self {
        let mut bytes: [u8; 32] = [0; 32];
        n.to_little_endian(&mut bytes);
        BigInt::from_unsigned_bytes_le(&bytes)
    }

    pub fn from_signed_u256(n: &U256) -> Self {
        let mut bytes: [u8; 32] = [0; 32];
        n.to_little_endian(&mut bytes);
        BigInt::from_signed_bytes_le(&bytes)
    }

    pub fn to_signed_u256(&self) -> U256 {
        let bytes = self.to_signed_bytes_le();
        if self < &BigInt::from(0) {
            assert!(
                bytes.len() <= 32,
                "BigInt value does not fit into signed U256"
            );
            let mut i_bytes: [u8; 32] = [255; 32];
            i_bytes[..bytes.len()].copy_from_slice(&bytes);
            U256::from_little_endian(&i_bytes)
        } else {
            U256::from_little_endian(&bytes)
        }
    }

    pub fn to_unsigned_u256(&self) -> U256 {
        let (sign, bytes) = self.to_bytes_le();
        assert!(
            sign == BigIntSign::NoSign || sign == BigIntSign::Plus,
            "negative value encountered for U256: {}",
            self
        );
        U256::from_little_endian(&bytes)
    }

    pub fn to_big_decimal(self, exp: BigInt) -> BigDecimal {
        let bytes = exp.to_signed_bytes_le();

        // The hope here is that bigdecimal switches to BigInt exponents. Until
        // then, a panic is fine since this is only used in mappings.
        if bytes.len() > 8 {
            panic!("big decimal exponent does not fit in i64")
        }
        let mut byte_array = if exp >= 0.into() { [0; 8] } else { [255; 8] };
        byte_array[..bytes.len()].copy_from_slice(&bytes);
        BigDecimal::new(self.0, -i64::from_le_bytes(byte_array))
    }

    pub fn pow(self, exponent: u8) -> Self {
        use num_traits::pow::Pow;

        BigInt(self.0.pow(&exponent))
    }

    pub fn bits(&self) -> u64 {
        self.0.bits() as u64
    }
}

impl Display for BigInt {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl From<num_bigint::BigInt> for BigInt {
    fn from(big_int: num_bigint::BigInt) -> BigInt {
        BigInt(big_int)
    }
}

impl From<i32> for BigInt {
    fn from(i: i32) -> BigInt {
        BigInt(i.into())
    }
}

impl From<u64> for BigInt {
    fn from(i: u64) -> BigInt {
        BigInt(i.into())
    }
}

impl From<i64> for BigInt {
    fn from(i: i64) -> BigInt {
        BigInt(i.into())
    }
}

impl From<U64> for BigInt {
    /// This implementation assumes that U64 represents an unsigned U64,
    /// and not a signed U64 (aka int64 in Solidity). Right now, this is
    /// all we need (for block numbers). If it ever becomes necessary to
    /// handle signed U64s, we should add the same
    /// `{to,from}_{signed,unsigned}_u64` methods that we have for U64.
    fn from(n: U64) -> BigInt {
        let mut bytes: [u8; 16] = [0; 16];
        n.to_little_endian(&mut bytes);
        BigInt::from_unsigned_bytes_le(&bytes)
    }
}

impl From<U128> for BigInt {
    /// This implementation assumes that U128 represents an unsigned U128,
    /// and not a signed U128 (aka int128 in Solidity). Right now, this is
    /// all we need (for block numbers). If it ever becomes necessary to
    /// handle signed U128s, we should add the same
    /// `{to,from}_{signed,unsigned}_u128` methods that we have for U256.
    fn from(n: U128) -> BigInt {
        let mut bytes: [u8; 16] = [0; 16];
        n.to_little_endian(&mut bytes);
        BigInt::from_unsigned_bytes_le(&bytes)
    }
}

impl FromStr for BigInt {
    type Err = <num_bigint::BigInt as FromStr>::Err;

    fn from_str(s: &str) -> Result<BigInt, Self::Err> {
        num_bigint::BigInt::from_str(s).map(BigInt)
    }
}

impl Serialize for BigInt {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for BigInt {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let decimal_string = <String>::deserialize(deserializer)?;
        BigInt::from_str(&decimal_string).map_err(D::Error::custom)
    }
}

impl Add for BigInt {
    type Output = BigInt;

    fn add(self, other: BigInt) -> BigInt {
        BigInt(self.0.add(other.0))
    }
}

impl Sub for BigInt {
    type Output = BigInt;

    fn sub(self, other: BigInt) -> BigInt {
        BigInt(self.0.sub(other.0))
    }
}

impl Mul for BigInt {
    type Output = BigInt;

    fn mul(self, other: BigInt) -> BigInt {
        BigInt(self.0.mul(other.0))
    }
}

impl Div for BigInt {
    type Output = BigInt;

    fn div(self, other: BigInt) -> BigInt {
        if other == BigInt::from(0) {
            panic!("Cannot divide by zero-valued `BigInt`!")
        }

        BigInt(self.0.div(other.0))
    }
}

impl Rem for BigInt {
    type Output = BigInt;

    fn rem(self, other: BigInt) -> BigInt {
        BigInt(self.0.rem(other.0))
    }
}

/// A byte array that's serialized as a hex string prefixed by `0x`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bytes(Box<[u8]>);

impl Bytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl Display for Bytes {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl FromStr for Bytes {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Bytes, Self::Err> {
        hex::decode(s.trim_start_matches("0x")).map(|x| Bytes(x.into()))
    }
}

impl<'a> From<&'a [u8]> for Bytes {
    fn from(array: &[u8]) -> Self {
        Bytes(array.into())
    }
}

impl From<Address> for Bytes {
    fn from(address: Address) -> Bytes {
        Bytes::from(address.as_ref())
    }
}

impl Serialize for Bytes {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.to_string().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let hex_string = <String>::deserialize(deserializer)?;
        Bytes::from_str(&hex_string).map_err(D::Error::custom)
    }
}
