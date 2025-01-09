use diesel::deserialize::FromSqlRow;
use diesel::expression::AsExpression;
use num_bigint;
use num_traits::FromPrimitive;
use serde::{self, Deserialize, Serialize};
use stable_hash::{FieldAddress, StableHash};
use stable_hash_legacy::SequenceNumber;

use std::fmt::{self, Display, Formatter};
use std::ops::{Add, Div, Mul, Sub};
use std::str::FromStr;

use crate::runtime::gas::{Gas, GasSizeOf};

use old_bigdecimal::BigDecimal as OldBigDecimal;
pub use old_bigdecimal::ToPrimitive;

use super::BigInt;

/// All operations on `BigDecimal` return a normalized value.
// Caveat: The exponent is currently an i64 and may overflow. See
// https://github.com/akubera/bigdecimal-rs/issues/54.
// Using `#[serde(from = "BigDecimal"]` makes sure deserialization calls `BigDecimal::new()`.
#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, AsExpression, FromSqlRow,
)]
#[serde(from = "OldBigDecimal")]
#[diesel(sql_type = diesel::sql_types::Numeric)]
pub struct BigDecimal(OldBigDecimal);

impl From<OldBigDecimal> for BigDecimal {
    fn from(big_decimal: OldBigDecimal) -> Self {
        BigDecimal(big_decimal).normalized()
    }
}

impl BigDecimal {
    /// These are the limits of IEEE-754 decimal128, a format we may want to switch to. See
    /// https://en.wikipedia.org/wiki/Decimal128_floating-point_format.
    pub const MIN_EXP: i32 = -6143;
    pub const MAX_EXP: i32 = 6144;
    pub const MAX_SIGNFICANT_DIGITS: i32 = 34;

    pub fn new(digits: BigInt, exp: i64) -> Self {
        // bigdecimal uses `scale` as the opposite of the power of ten, so negate `exp`.
        Self::from(OldBigDecimal::new(digits.inner(), -exp))
    }

    pub fn parse_bytes(bytes: &[u8]) -> Option<Self> {
        OldBigDecimal::parse_bytes(bytes, 10).map(Self)
    }

    pub fn zero() -> BigDecimal {
        use old_bigdecimal::Zero;

        BigDecimal(OldBigDecimal::zero())
    }

    pub fn as_bigint_and_exponent(&self) -> (num_bigint::BigInt, i64) {
        self.0.as_bigint_and_exponent()
    }

    pub fn digits(&self) -> u64 {
        self.0.digits()
    }

    // Copy-pasted from `OldBigDecimal::normalize`. We can use the upstream version once it
    // is included in a released version supported by Diesel.
    #[must_use]
    pub fn normalized(&self) -> BigDecimal {
        if self == &BigDecimal::zero() {
            return BigDecimal::zero();
        }

        // Round to the maximum significant digits.
        let big_decimal = self.0.with_prec(Self::MAX_SIGNFICANT_DIGITS as u64);

        let (bigint, exp) = big_decimal.as_bigint_and_exponent();
        let (sign, mut digits) = bigint.to_radix_be(10);
        let trailing_count = digits.iter().rev().take_while(|i| **i == 0).count();
        digits.truncate(digits.len() - trailing_count);
        let int_val = num_bigint::BigInt::from_radix_be(sign, &digits, 10).unwrap();
        let scale = exp - trailing_count as i64;

        BigDecimal(OldBigDecimal::new(int_val, scale))
    }
}

impl Display for BigDecimal {
    fn fmt(&self, f: &mut Formatter) -> Result<(), fmt::Error> {
        self.0.fmt(f)
    }
}

impl fmt::Debug for BigDecimal {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "BigDecimal({})", self.0)
    }
}

impl FromStr for BigDecimal {
    type Err = <OldBigDecimal as FromStr>::Err;

    fn from_str(s: &str) -> Result<BigDecimal, Self::Err> {
        Ok(Self::from(OldBigDecimal::from_str(s)?))
    }
}

impl From<i32> for BigDecimal {
    fn from(n: i32) -> Self {
        Self::from(OldBigDecimal::from(n))
    }
}

impl From<i64> for BigDecimal {
    fn from(n: i64) -> Self {
        Self::from(OldBigDecimal::from(n))
    }
}

impl From<u64> for BigDecimal {
    fn from(n: u64) -> Self {
        Self::from(OldBigDecimal::from(n))
    }
}

impl From<f64> for BigDecimal {
    fn from(n: f64) -> Self {
        Self::from(OldBigDecimal::from_f64(n).unwrap_or_default())
    }
}

impl Add for BigDecimal {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        Self::from(self.0.add(other.0))
    }
}

impl Sub for BigDecimal {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self::from(self.0.sub(other.0))
    }
}

impl Mul for BigDecimal {
    type Output = Self;

    fn mul(self, other: Self) -> Self {
        Self::from(self.0.mul(other.0))
    }
}

impl Div for BigDecimal {
    type Output = Self;

    fn div(self, other: Self) -> Self {
        if other == BigDecimal::from(0) {
            panic!("Cannot divide by zero-valued `BigDecimal`!")
        }

        Self::from(self.0.div(other.0))
    }
}

impl old_bigdecimal::ToPrimitive for BigDecimal {
    fn to_i64(&self) -> Option<i64> {
        self.0.to_i64()
    }
    fn to_u64(&self) -> Option<u64> {
        self.0.to_u64()
    }
}

impl stable_hash_legacy::StableHash for BigDecimal {
    fn stable_hash<H: stable_hash_legacy::StableHasher>(
        &self,
        mut sequence_number: H::Seq,
        state: &mut H,
    ) {
        let (int, exp) = self.as_bigint_and_exponent();
        // This only allows for backward compatible changes between
        // BigDecimal and unsigned ints
        stable_hash_legacy::StableHash::stable_hash(&exp, sequence_number.next_child(), state);
        stable_hash_legacy::StableHash::stable_hash(
            &BigInt::unchecked_new(int),
            sequence_number,
            state,
        );
    }
}

impl StableHash for BigDecimal {
    fn stable_hash<H: stable_hash::StableHasher>(&self, field_address: H::Addr, state: &mut H) {
        // This implementation allows for backward compatible changes from integers (signed or unsigned)
        // when the exponent is zero.
        let (int, exp) = self.as_bigint_and_exponent();
        StableHash::stable_hash(&exp, field_address.child(1), state);
        // Normally it would be a red flag to pass field_address in after having used a child slot.
        // But, we know the implemecntation of StableHash for BigInt will not use child(1) and that
        // it will not in the future due to having no forward schema evolutions for ints and the
        // stability guarantee.
        //
        // For reference, ints use child(0) for the sign and write the little endian bytes to the parent slot.
        BigInt::unchecked_new(int).stable_hash(field_address, state);
    }
}

impl GasSizeOf for BigDecimal {
    fn gas_size_of(&self) -> Gas {
        let (int, _) = self.as_bigint_and_exponent();
        BigInt::unchecked_new(int).gas_size_of()
    }
}

// This code was copied from diesel. Unfortunately, we need to reimplement
// it here because any change to diesel's version of bigdecimal will cause
// the build to break as our old_bigdecimal::BigDecimal and diesel's
// bigdecimal::BigDecimal will then become distinct types, and we can't
// update our old_bigdecimal because updating causes PoI divergences.
//
// The code was taken from diesel-2.1.4/src/pg/types/numeric.rs
mod pg {
    use std::error::Error;

    use diesel::deserialize::FromSql;
    use diesel::pg::{Pg, PgValue};
    use diesel::serialize::{self, Output, ToSql};
    use diesel::sql_types::Numeric;
    use diesel::{data_types::PgNumeric, deserialize};
    use num_bigint::{BigInt, BigUint, Sign};
    use num_integer::Integer;
    use num_traits::{Signed, ToPrimitive, Zero};

    use super::super::BigIntSign;
    use super::{BigDecimal, OldBigDecimal};

    /// Iterator over the digits of a big uint in base 10k.
    /// The digits will be returned in little endian order.
    struct ToBase10000(Option<BigUint>);

    impl Iterator for ToBase10000 {
        type Item = i16;

        fn next(&mut self) -> Option<Self::Item> {
            self.0.take().map(|v| {
                let (div, rem) = v.div_rem(&BigUint::from(10_000u16));
                if !div.is_zero() {
                    self.0 = Some(div);
                }
                rem.to_i16().expect("10000 always fits in an i16")
            })
        }
    }

    impl<'a> TryFrom<&'a PgNumeric> for BigDecimal {
        type Error = Box<dyn Error + Send + Sync>;

        fn try_from(numeric: &'a PgNumeric) -> deserialize::Result<Self> {
            let (sign, weight, scale, digits) = match *numeric {
                PgNumeric::Positive {
                    weight,
                    scale,
                    ref digits,
                } => (BigIntSign::Plus, weight, scale, digits),
                PgNumeric::Negative {
                    weight,
                    scale,
                    ref digits,
                } => (Sign::Minus, weight, scale, digits),
                PgNumeric::NaN => {
                    return Err(Box::from("NaN is not (yet) supported in BigDecimal"))
                }
            };

            let mut result = BigUint::default();
            let count = digits.len() as i64;
            for digit in digits {
                result *= BigUint::from(10_000u64);
                result += BigUint::from(*digit as u64);
            }
            // First digit got factor 10_000^(digits.len() - 1), but should get 10_000^weight
            let correction_exp = 4 * (i64::from(weight) - count + 1);
            let result = OldBigDecimal::new(BigInt::from_biguint(sign, result), -correction_exp)
                .with_scale(i64::from(scale));
            Ok(BigDecimal(result))
        }
    }

    impl TryFrom<PgNumeric> for BigDecimal {
        type Error = Box<dyn Error + Send + Sync>;

        fn try_from(numeric: PgNumeric) -> deserialize::Result<Self> {
            (&numeric).try_into()
        }
    }

    impl<'a> From<&'a BigDecimal> for PgNumeric {
        // NOTE(clippy): No `std::ops::MulAssign` impl for `BigInt`
        // NOTE(clippy): Clippy suggests to replace the `.take_while(|i| i.is_zero())`
        // with `.take_while(Zero::is_zero)`, but that's a false positive.
        // The closure gets an `&&i16` due to autoderef `<i16 as Zero>::is_zero(&self) -> bool`
        // is called. There is no impl for `&i16` that would work with this closure.
        #[allow(clippy::assign_op_pattern, clippy::redundant_closure)]
        fn from(decimal: &'a BigDecimal) -> Self {
            let (mut integer, scale) = decimal.as_bigint_and_exponent();

            // Handling of negative scale
            let scale = if scale < 0 {
                for _ in 0..(-scale) {
                    integer = integer * 10;
                }
                0
            } else {
                scale as u16
            };

            integer = integer.abs();

            // Ensure that the decimal will always lie on a digit boundary
            for _ in 0..(4 - scale % 4) {
                integer = integer * 10;
            }
            let integer = integer.to_biguint().expect("integer is always positive");

            let mut digits = ToBase10000(Some(integer)).collect::<Vec<_>>();
            digits.reverse();
            let digits_after_decimal = scale / 4 + 1;
            let weight = digits.len() as i16 - digits_after_decimal as i16 - 1;

            let unnecessary_zeroes = digits.iter().rev().take_while(|i| i.is_zero()).count();

            let relevant_digits = digits.len() - unnecessary_zeroes;
            digits.truncate(relevant_digits);

            match decimal.0.sign() {
                Sign::Plus => PgNumeric::Positive {
                    digits,
                    scale,
                    weight,
                },
                Sign::Minus => PgNumeric::Negative {
                    digits,
                    scale,
                    weight,
                },
                Sign::NoSign => PgNumeric::Positive {
                    digits: vec![0],
                    scale: 0,
                    weight: 0,
                },
            }
        }
    }

    impl From<BigDecimal> for PgNumeric {
        fn from(bigdecimal: BigDecimal) -> Self {
            (&bigdecimal).into()
        }
    }

    impl ToSql<Numeric, Pg> for BigDecimal {
        fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
            let numeric = PgNumeric::from(self);
            ToSql::<Numeric, Pg>::to_sql(&numeric, &mut out.reborrow())
        }
    }

    impl FromSql<Numeric, Pg> for BigDecimal {
        fn from_sql(numeric: PgValue<'_>) -> deserialize::Result<Self> {
            PgNumeric::from_sql(numeric)?.try_into()
        }
    }

    #[cfg(test)]
    mod tests {
        // The tests are exactly the same as Diesel's tests, but we use our
        // BigDecimal instead of bigdecimal::BigDecimal. In a few places, we
        // have to construct the BigDecimal directly as
        // `BigDecimal(OldBigDecimal...)` because BigDecimal::new inverts
        // the sign of the exponent
        use diesel::data_types::PgNumeric;

        use super::super::{BigDecimal, OldBigDecimal};
        use std::str::FromStr;

        #[test]
        fn bigdecimal_to_pgnumeric_converts_digits_to_base_10000() {
            let decimal = BigDecimal::from_str("1").unwrap();
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 0,
                digits: vec![1],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("10").unwrap();
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 0,
                digits: vec![10],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("10000").unwrap();
            let expected = PgNumeric::Positive {
                weight: 1,
                scale: 0,
                digits: vec![1],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("10001").unwrap();
            let expected = PgNumeric::Positive {
                weight: 1,
                scale: 0,
                digits: vec![1, 1],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("100000000").unwrap();
            let expected = PgNumeric::Positive {
                weight: 2,
                scale: 0,
                digits: vec![1],
            };
            assert_eq!(expected, decimal.into());
        }

        #[test]
        fn bigdecimal_to_pg_numeric_properly_adjusts_scale() {
            let decimal = BigDecimal::from_str("1").unwrap();
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 0,
                digits: vec![1],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal(OldBigDecimal::from_str("1.0").unwrap());
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 1,
                digits: vec![1],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("1.1").unwrap();
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 1,
                digits: vec![1, 1000],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal(OldBigDecimal::from_str("1.10").unwrap());
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 2,
                digits: vec![1, 1000],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("100000000.0001").unwrap();
            let expected = PgNumeric::Positive {
                weight: 2,
                scale: 4,
                digits: vec![1, 0, 0, 1],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("0.1").unwrap();
            let expected = PgNumeric::Positive {
                weight: -1,
                scale: 1,
                digits: vec![1000],
            };
            assert_eq!(expected, decimal.into());
        }

        #[test]
        fn bigdecimal_to_pg_numeric_retains_sign() {
            let decimal = BigDecimal::from_str("123.456").unwrap();
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 3,
                digits: vec![123, 4560],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("-123.456").unwrap();
            let expected = PgNumeric::Negative {
                weight: 0,
                scale: 3,
                digits: vec![123, 4560],
            };
            assert_eq!(expected, decimal.into());
        }

        #[test]
        fn bigdecimal_with_negative_scale_to_pg_numeric_works() {
            let decimal = BigDecimal(OldBigDecimal::new(50.into(), -2));
            let expected = PgNumeric::Positive {
                weight: 0,
                scale: 0,
                digits: vec![5000],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal(OldBigDecimal::new(1.into(), -4));
            let expected = PgNumeric::Positive {
                weight: 1,
                scale: 0,
                digits: vec![1],
            };
            assert_eq!(expected, decimal.into());
        }

        #[test]
        fn bigdecimal_with_negative_weight_to_pg_numeric_works() {
            let decimal = BigDecimal(OldBigDecimal::from_str("0.1000000000000000").unwrap());
            let expected = PgNumeric::Positive {
                weight: -1,
                scale: 16,
                digits: vec![1000],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal::from_str("0.00315937").unwrap();
            let expected = PgNumeric::Positive {
                weight: -1,
                scale: 8,
                digits: vec![31, 5937],
            };
            assert_eq!(expected, decimal.into());

            let decimal = BigDecimal(OldBigDecimal::from_str("0.003159370000000000").unwrap());
            let expected = PgNumeric::Positive {
                weight: -1,
                scale: 18,
                digits: vec![31, 5937],
            };
            assert_eq!(expected, decimal.into());
        }

        #[test]
        fn pg_numeric_to_bigdecimal_works() {
            let expected = BigDecimal::from_str("123.456").unwrap();
            let pg_numeric = PgNumeric::Positive {
                weight: 0,
                scale: 3,
                digits: vec![123, 4560],
            };
            let res: BigDecimal = pg_numeric.try_into().unwrap();
            assert_eq!(res, expected);

            let expected = BigDecimal::from_str("-56.78").unwrap();
            let pg_numeric = PgNumeric::Negative {
                weight: 0,
                scale: 2,
                digits: vec![56, 7800],
            };
            let res: BigDecimal = pg_numeric.try_into().unwrap();
            assert_eq!(res, expected);
        }
    }
}

#[cfg(test)]
mod test {
    use super::{
        super::test::{crypto_stable_hash, same_stable_hash},
        super::Bytes,
        BigDecimal, BigInt, OldBigDecimal,
    };
    use std::str::FromStr;

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

    #[test]
    fn big_decimal_stable_hash_same_as_uint() {
        same_stable_hash(0, BigDecimal::from(0u64));
        same_stable_hash(4, BigDecimal::from(4i64));
        same_stable_hash(1u64 << 21, BigDecimal::from(1u64 << 21));
    }

    #[test]
    fn big_decimal_stable() {
        let cases = vec![
            (
                "28b09c9c3f3e2fe037631b7fbccdf65c37594073016d8bf4bb0708b3fda8066a",
                "0.1",
            ),
            (
                "74fb39f038d2f1c8975740bf2651a5ac0403330ee7e9367f9563cbd7d21086bd",
                "-0.1",
            ),
            (
                "1d79e0476bc5d6fe6074fb54636b04fd3bc207053c767d9cb5e710ba5f002441",
                "198.98765544",
            ),
            (
                "e63f6ad2c65f193aa9eba18dd7e1043faa2d6183597ba84c67765aaa95c95351",
                "0.00000093937698",
            ),
            (
                "6b06b34cc714810072988dc46c493c66a6b6c2c2dd0030271aa3adf3b3f21c20",
                "98765587998098786876.0",
            ),
        ];
        for (hash, s) in cases.iter() {
            let dec = BigDecimal::from_str(s).unwrap();
            assert_eq!(*hash, hex::encode(crypto_stable_hash(dec)));
        }
    }

    #[test]
    fn test_normalize() {
        let vals = vec![
            (
                BigDecimal::new(BigInt::from(10), -2),
                BigDecimal(OldBigDecimal::new(1.into(), 1)),
                "0.1",
            ),
            (
                BigDecimal::new(BigInt::from(132400), 4),
                BigDecimal(OldBigDecimal::new(1324.into(), -6)),
                "1324000000",
            ),
            (
                BigDecimal::new(BigInt::from(1_900_000), -3),
                BigDecimal(OldBigDecimal::new(19.into(), -2)),
                "1900",
            ),
            (BigDecimal::new(0.into(), 3), BigDecimal::zero(), "0"),
            (BigDecimal::new(0.into(), -5), BigDecimal::zero(), "0"),
        ];

        for (not_normalized, normalized, string) in vals {
            assert_eq!(not_normalized.normalized(), normalized);
            assert_eq!(not_normalized.normalized().to_string(), string);
            assert_eq!(normalized.to_string(), string);
        }
    }

    #[test]
    fn fmt_debug() {
        let bi = BigInt::from(-17);
        let bd = BigDecimal::new(bi.clone(), -2);
        let bytes = Bytes::from([222, 173, 190, 239].as_slice());
        assert_eq!("BigInt(-17)", format!("{:?}", bi));
        assert_eq!("BigDecimal(-0.17)", format!("{:?}", bd));
        assert_eq!("Bytes(0xdeadbeef)", format!("{:?}", bytes));
    }
}
