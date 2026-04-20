use alloy::primitives::I256;
use anyhow::bail;
use num_bigint;
use serde::{self, Deserialize, Serialize};
use stable_hash::StableHash;
use stable_hash::utils::AsInt;
use thiserror::Error;

use crate::prelude::alloy::primitives::{U64, U128, U256};
use std::convert::{TryFrom, TryInto};
use std::fmt;
use std::ops::{Add, BitAnd, BitOr, Div, Mul, Rem, Shl, Shr, Sub};
use std::str::FromStr;

pub use num_bigint::Sign as BigIntSign;

use crate::runtime::gas::{Gas, GasSizeOf, SaturatingInto};

// Use a private module to ensure a constructor is used.
pub use big_int::BigInt;
mod big_int {
    use std::{
        f32::consts::LOG2_10,
        fmt::{self, Display, Formatter},
    };

    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct BigInt(num_bigint::BigInt);

    impl Display for BigInt {
        fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
            self.0.fmt(f)
        }
    }

    impl BigInt {
        // Postgres `numeric` has a limit documented here [https://www.postgresql.org/docs/current/datatype-numeric.htm]:
        // "Up to 131072 digits before the decimal point; up to 16383 digits after the decimal point"
        // So based on this we adopt a limit of 131072 decimal digits for big int, converted here to bits.
        pub const MAX_BITS: u32 = (131072.0 * LOG2_10) as u32 + 1; // 435_412

        pub fn new(inner: num_bigint::BigInt) -> Result<Self, anyhow::Error> {
            // `inner.bits()` won't include the sign bit, so we add 1 to account for it.
            let bits = inner.bits() + 1;
            if bits > Self::MAX_BITS as u64 {
                anyhow::bail!(
                    "BigInt is too big, total bits {} (max {})",
                    bits,
                    Self::MAX_BITS
                );
            }
            Ok(Self(inner))
        }

        /// Creates a BigInt without checking the digit limit.
        pub(in super::super) fn unchecked_new(inner: num_bigint::BigInt) -> Self {
            Self(inner)
        }

        pub fn sign(&self) -> num_bigint::Sign {
            self.0.sign()
        }

        pub fn to_bytes_le(&self) -> (super::BigIntSign, Vec<u8>) {
            self.0.to_bytes_le()
        }

        pub fn to_bytes_be(&self) -> (super::BigIntSign, Vec<u8>) {
            self.0.to_bytes_be()
        }

        pub fn to_signed_bytes_le(&self) -> Vec<u8> {
            self.0.to_signed_bytes_le()
        }

        pub fn bits(&self) -> usize {
            self.0.bits() as usize
        }

        pub(in super::super) fn inner(self) -> num_bigint::BigInt {
            self.0
        }
    }
}

impl stable_hash_legacy::StableHash for BigInt {
    #[inline]
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        sequence_number: H::Seq,
        state: &mut H,
    ) {
        stable_hash_legacy::utils::AsInt {
            is_negative: self.sign() == BigIntSign::Minus,
            little_endian: &self.to_bytes_le().1,
        }
        .stable_hash(sequence_number, state)
    }
}

impl StableHash for BigInt {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        AsInt {
            is_negative: self.sign() == BigIntSign::Minus,
            little_endian: &self.to_bytes_le().1,
        }
        .stable_hash(field_address, state)
    }
}

#[derive(Error, Debug)]
pub enum BigIntOutOfRangeError {
    #[error("Cannot convert negative BigInt into type")]
    Negative,
    #[error("BigInt value is too large for type")]
    Overflow,
}

impl<'a> TryFrom<&'a BigInt> for u64 {
    type Error = BigIntOutOfRangeError;
    fn try_from(value: &'a BigInt) -> Result<u64, BigIntOutOfRangeError> {
        let (sign, bytes) = value.to_bytes_le();

        if sign == num_bigint::Sign::Minus {
            return Err(BigIntOutOfRangeError::Negative);
        }

        if bytes.len() > 8 {
            return Err(BigIntOutOfRangeError::Overflow);
        }

        // Replace this with u64::from_le_bytes when stabilized
        let mut n = 0u64;
        let mut shift_dist = 0;
        for b in bytes {
            n |= (b as u64) << shift_dist;
            shift_dist += 8;
        }
        Ok(n)
    }
}

impl TryFrom<BigInt> for u64 {
    type Error = BigIntOutOfRangeError;
    fn try_from(value: BigInt) -> Result<u64, BigIntOutOfRangeError> {
        (&value).try_into()
    }
}

impl fmt::Debug for BigInt {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BigInt({})", self)
    }
}

impl BigInt {
    pub fn from_unsigned_bytes_le(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        BigInt::new(num_bigint::BigInt::from_bytes_le(
            num_bigint::Sign::Plus,
            bytes,
        ))
    }

    pub fn from_signed_bytes_le(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        BigInt::new(num_bigint::BigInt::from_signed_bytes_le(bytes))
    }

    pub fn from_signed_bytes_be(bytes: &[u8]) -> Result<Self, anyhow::Error> {
        BigInt::new(num_bigint::BigInt::from_signed_bytes_be(bytes))
    }

    /// Deprecated. Use try_into instead
    pub fn to_u64(&self) -> u64 {
        self.try_into().unwrap()
    }

    pub fn from_unsigned_u128(n: U128) -> Self {
        let bytes: [u8; U128::BYTES] = n.to_le_bytes();
        // Unwrap: 128 bits is much less than BigInt::MAX_BITS
        BigInt::from_unsigned_bytes_le(&bytes).unwrap()
    }

    pub fn from_unsigned_u256(n: &U256) -> Self {
        let bytes: [u8; U256::BYTES] = n.to_le_bytes();
        // Unwrap: 256 bits is much less than BigInt::MAX_BITS
        BigInt::from_unsigned_bytes_le(&bytes).unwrap()
    }

    pub fn from_i256(n: &I256) -> Self {
        let bytes: [u8; I256::BYTES] = n.to_le_bytes();
        // Unwrap: 256 bits is much less than BigInt::MAX_BITS
        BigInt::from_signed_bytes_le(&bytes).unwrap()
    }

    pub fn to_i256(&self) -> Result<I256, anyhow::Error> {
        let bytes = self.to_signed_bytes_le();
        anyhow::ensure!(
            bytes.len() <= I256::BYTES,
            "BigInt value `{}` does not fit into int256",
            self
        );
        let fill: u8 = if self.sign() == BigIntSign::Minus {
            0xFF
        } else {
            0x00
        };
        let mut buf = [fill; I256::BYTES];
        buf[..bytes.len()].copy_from_slice(&bytes);
        Ok(I256::from_le_bytes(buf))
    }

    pub fn to_unsigned_u256(&self) -> Result<U256, anyhow::Error> {
        let (sign, bytes) = self.to_bytes_le();
        if sign != BigIntSign::Plus && sign != BigIntSign::NoSign {
            bail!(
                "BigInt value is negative, cannot convert to unsigned U256: {}",
                self
            );
        }
        anyhow::ensure!(
            bytes.len() <= U256::BYTES,
            "BigInt value `{}` does not fit into uint256",
            self
        );
        Ok(U256::from_le_slice(&bytes))
    }

    pub fn pow(self, exponent: u8) -> Result<BigInt, anyhow::Error> {
        use num_traits::pow::Pow;

        BigInt::new(self.inner().pow(&exponent))
    }
}

impl From<i8> for BigInt {
    fn from(i: i8) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<i16> for BigInt {
    fn from(i: i16) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<i32> for BigInt {
    fn from(i: i32) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<i64> for BigInt {
    fn from(i: i64) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<i128> for BigInt {
    fn from(i: i128) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<u128> for BigInt {
    fn from(i: u128) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<u8> for BigInt {
    fn from(i: u8) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<u16> for BigInt {
    fn from(i: u16) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<u32> for BigInt {
    fn from(i: u32) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<u64> for BigInt {
    fn from(i: u64) -> BigInt {
        BigInt::unchecked_new(i.into())
    }
}

impl From<U64> for BigInt {
    /// This implementation assumes that U64 represents an unsigned U64,
    /// and not a signed U64 (aka int64 in Solidity). Right now, this is
    /// all we need (for block numbers). If it ever becomes necessary to
    /// handle signed U64s, we should add the same
    /// `{to,from}_{signed,unsigned}_u64` methods that we have for U64.
    fn from(n: U64) -> BigInt {
        BigInt::from(n.to::<u64>())
    }
}

impl FromStr for BigInt {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<BigInt, Self::Err> {
        num_bigint::BigInt::from_str(s)
            .map_err(anyhow::Error::from)
            .and_then(BigInt::new)
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
        BigInt::unchecked_new(self.inner().add(other.inner()))
    }
}

impl Sub for BigInt {
    type Output = BigInt;

    fn sub(self, other: BigInt) -> BigInt {
        BigInt::unchecked_new(self.inner().sub(other.inner()))
    }
}

impl Mul for BigInt {
    type Output = BigInt;

    fn mul(self, other: BigInt) -> BigInt {
        BigInt::unchecked_new(self.inner().mul(other.inner()))
    }
}

impl Div for BigInt {
    type Output = BigInt;

    fn div(self, other: BigInt) -> BigInt {
        if other == BigInt::from(0) {
            panic!("Cannot divide by zero-valued `BigInt`!")
        }

        BigInt::unchecked_new(self.inner().div(other.inner()))
    }
}

impl Rem for BigInt {
    type Output = BigInt;

    fn rem(self, other: BigInt) -> BigInt {
        BigInt::unchecked_new(self.inner().rem(other.inner()))
    }
}

impl BitOr for BigInt {
    type Output = Self;

    fn bitor(self, other: Self) -> Self {
        BigInt::unchecked_new(self.inner().bitor(other.inner()))
    }
}

impl BitAnd for BigInt {
    type Output = Self;

    fn bitand(self, other: Self) -> Self {
        BigInt::unchecked_new(self.inner().bitand(other.inner()))
    }
}

impl Shl<u8> for BigInt {
    type Output = Self;

    fn shl(self, bits: u8) -> Self {
        BigInt::unchecked_new(self.inner().shl(usize::from(bits)))
    }
}

impl Shr<u8> for BigInt {
    type Output = Self;

    fn shr(self, bits: u8) -> Self {
        BigInt::unchecked_new(self.inner().shr(usize::from(bits)))
    }
}

impl GasSizeOf for BigInt {
    fn gas_size_of(&self) -> Gas {
        // Add one to always have an upper bound on the number of bytes required to represent the
        // number, and so that `0` has a size of 1.
        let n_bytes = self.bits() / 8 + 1;
        n_bytes.saturating_into()
    }
}

#[cfg(test)]
mod test {
    use alloy::primitives::U64;

    use super::{super::test::same_stable_hash, BigInt};

    /// Compute 2^n via repeated doubling so we can build values larger than
    /// `BigInt::pow`'s u8 exponent limit.
    fn pow2(n: u32) -> BigInt {
        let mut acc = BigInt::from(1u64);
        for _ in 0..n {
            acc = acc * BigInt::from(2u64);
        }
        acc
    }

    #[test]
    fn to_i256_succeeds_at_boundaries() {
        let one = BigInt::from(1u64);
        let i256_max = pow2(255) - one.clone();
        let i256_min = BigInt::from(0) - pow2(255);

        for v in &[
            BigInt::from(0),
            BigInt::from(1),
            BigInt::from(-1),
            i256_max.clone(),
            i256_min.clone(),
        ] {
            let i = v.to_i256().expect("in-range value should convert");
            let back = BigInt::from_i256(&i);
            assert_eq!(&back, v, "round-trip failed for {}", v);
        }
    }

    #[test]
    fn to_i256_errors_outside_range() {
        let one = BigInt::from(1u64);
        let just_above_max = pow2(255);
        let just_below_min = BigInt::from(0) - pow2(255) - one;
        let way_above = pow2(300);
        let way_below = BigInt::from(0) - pow2(300);

        for v in &[just_above_max, just_below_min, way_above, way_below] {
            assert!(
                v.to_i256().is_err(),
                "out-of-range value {} should error, not panic",
                v
            );
        }
    }

    #[test]
    fn to_unsigned_u256_errors_outside_range() {
        let just_above_max = pow2(256);
        let way_above = pow2(300);

        for v in &[just_above_max, way_above] {
            assert!(
                v.to_unsigned_u256().is_err(),
                "value {} above u256::MAX should error, not panic",
                v
            );
        }

        assert!(
            BigInt::from(-1).to_unsigned_u256().is_err(),
            "negative value should error"
        );
    }

    #[test]
    fn bigint_to_from_u64() {
        for n in 0..100 {
            let u = U64::from(n);
            let bn = BigInt::from(u);
            assert_eq!(n, bn.to_u64());
        }
    }

    #[test]
    fn big_int_stable_hash_same_as_int() {
        same_stable_hash(0, BigInt::from(0u64));
        same_stable_hash(1, BigInt::from(1u64));
        same_stable_hash(1u64 << 20, BigInt::from(1u64 << 20));

        same_stable_hash(
            -1,
            BigInt::from_signed_bytes_le(&(-1i32).to_le_bytes()).unwrap(),
        );
    }
}
