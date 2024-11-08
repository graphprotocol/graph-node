use alloy::dyn_abi::DynSolType;
use alloy::dyn_abi::DynSolValue;
use anyhow::anyhow;
use anyhow::Result;
use itertools::Itertools;

pub trait DynSolValueExt {
    /// Creates a fixed-byte decoded value from a slice.
    ///
    /// Fails if the source slice exceeds 32 bytes.
    fn fixed_bytes_from_slice(s: &[u8]) -> Result<DynSolValue>;

    /// Returns the decoded value as a string.
    ///
    /// The resulting string contains no type information.
    fn to_string(&self) -> String;

    /// Checks whether the value is of the specified type.
    ///
    /// For types with additional size information, returns true if the size of the value is less
    /// than or equal to the size of the specified type.
    #[must_use]
    fn type_check(&self, ty: &DynSolType) -> bool;
}

impl DynSolValueExt for DynSolValue {
    fn fixed_bytes_from_slice(s: &[u8]) -> Result<Self> {
        let num_bytes = s.len();

        if num_bytes > 32 {
            return Err(anyhow!(
                "input slice must contain a maximum of 32 bytes, got {num_bytes}"
            ));
        }

        let mut bytes = [0u8; 32];

        // Access: If `x` is of type `bytesI`, then `x[k]` for `0 <= k < I` returns the `k`th byte.
        // Ref: <https://docs.soliditylang.org/en/v0.8.28/types.html#fixed-size-byte-arrays>
        bytes[..num_bytes].copy_from_slice(s);

        Ok(Self::FixedBytes(bytes.into(), num_bytes))
    }

    fn to_string(&self) -> String {
        let s = |v: &[Self]| v.iter().map(|x| x.to_string()).collect_vec().join(",");

        // Output format is taken from `ethabi`;
        // See: <https://docs.rs/ethabi/18.0.0/ethabi/enum.Token.html#impl-Display-for-Token>
        match self {
            Self::Bool(v) => v.to_string(),
            Self::Int(v, _) => format!("{v:x}"),
            Self::Uint(v, _) => format!("{v:x}"),
            Self::FixedBytes(v, _) => hex::encode(v),
            Self::Address(v) => format!("{v:x}"),
            Self::Function(v) => format!("{v:x}"),
            Self::Bytes(v) => hex::encode(v),
            Self::String(v) => v.to_owned(),
            Self::Array(v) => format!("[{}]", s(v)),
            Self::FixedArray(v) => format!("[{}]", s(v)),
            Self::Tuple(v) => format!("({})", s(v)),
        }
    }

    fn type_check(&self, ty: &DynSolType) -> bool {
        match self {
            Self::Bool(_) => *ty == DynSolType::Bool,
            Self::Int(_, a) => {
                if let DynSolType::Int(b) = ty {
                    b >= a
                } else {
                    false
                }
            }
            Self::Uint(_, a) => {
                if let DynSolType::Uint(b) = ty {
                    b >= a
                } else {
                    false
                }
            }
            Self::FixedBytes(_, a) => {
                if let DynSolType::FixedBytes(b) = ty {
                    b >= a
                } else {
                    false
                }
            }
            Self::Address(_) => *ty == DynSolType::Address,
            Self::Function(_) => *ty == DynSolType::Function,
            Self::Bytes(_) => *ty == DynSolType::Bytes,
            Self::String(_) => *ty == DynSolType::String,
            Self::Array(values) => {
                if let DynSolType::Array(ty) = ty {
                    values.iter().all(|x| x.type_check(ty))
                } else {
                    false
                }
            }
            Self::FixedArray(values) => {
                if let DynSolType::FixedArray(ty, size) = ty {
                    *size == values.len() && values.iter().all(|x| x.type_check(ty))
                } else {
                    false
                }
            }
            Self::Tuple(values) => {
                if let DynSolType::Tuple(types) = ty {
                    types.len() == values.len()
                        && values
                            .iter()
                            .enumerate()
                            .all(|(i, x)| x.type_check(&types[i]))
                } else {
                    false
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::I256;
    use alloy::primitives::U256;

    use super::*;

    #[test]
    fn fixed_bytes_from_slice_empty_slice() {
        let val = DynSolValue::fixed_bytes_from_slice(&[]).unwrap();
        let bytes = [0; 32];

        assert_eq!(val, DynSolValue::FixedBytes(bytes.into(), 0));
    }

    #[test]
    fn fixed_bytes_from_slice_one_byte() {
        let val = DynSolValue::fixed_bytes_from_slice(&[10]).unwrap();
        let mut bytes = [0; 32];
        bytes[0] = 10;

        assert_eq!(val, DynSolValue::FixedBytes(bytes.into(), 1));
    }

    #[test]
    fn fixed_bytes_from_slice_multiple_bytes() {
        let val = DynSolValue::fixed_bytes_from_slice(&[10, 20, 30]).unwrap();
        let mut bytes = [0; 32];
        bytes[0] = 10;
        bytes[1] = 20;
        bytes[2] = 30;

        assert_eq!(val, DynSolValue::FixedBytes(bytes.into(), 3));
    }

    #[test]
    fn fixed_bytes_from_slice_max_bytes() {
        let val = DynSolValue::fixed_bytes_from_slice(&[10; 32]).unwrap();
        let bytes = [10; 32];

        assert_eq!(val, DynSolValue::FixedBytes(bytes.into(), 32));
    }

    #[test]
    fn fixed_bytes_from_slice_too_many_bytes() {
        DynSolValue::fixed_bytes_from_slice(&[10; 33]).unwrap_err();
    }

    #[test]
    fn to_string() {
        use DynSolValue::*;

        assert_eq!(Bool(false).to_string(), "false");
        assert_eq!(Bool(true).to_string(), "true");

        assert_eq!(
            Int(I256::try_from(-10).unwrap(), 256).to_string(),
            "fffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff6",
        );

        assert_eq!(Uint(U256::from(10), 256).to_string(), "a");

        assert_eq!(
            FixedBytes([10; 32].into(), 32).to_string(),
            "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
        );

        assert_eq!(
            Address([10; 20].into()).to_string(),
            "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
        );

        assert_eq!(
            Function([10; 24].into()).to_string(),
            "0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a",
        );

        assert_eq!(Bytes(vec![10, 20, 30]).to_string(), "0a141e");

        assert_eq!(
            String("one two three".to_owned()).to_string(),
            "one two three"
        );

        assert_eq!(
            Array(vec![String("one".to_owned()), String("two".to_owned())]).to_string(),
            "[one,two]",
        );

        assert_eq!(
            FixedArray(vec![String("one".to_owned()), String("two".to_owned())]).to_string(),
            "[one,two]"
        );

        assert_eq!(
            Tuple(vec![String("one".to_owned()), String("two".to_owned())]).to_string(),
            "(one,two)"
        );
    }

    #[test]
    fn type_check() {
        use DynSolType as T;
        use DynSolValue::*;

        assert!(Bool(true).type_check(&T::Bool));
        assert!(!Bool(true).type_check(&T::Int(256)));

        assert!(!Int(I256::try_from(-10).unwrap(), 32).type_check(&T::Int(24)));
        assert!(Int(I256::try_from(-10).unwrap(), 32).type_check(&T::Int(32)));
        assert!(Int(I256::try_from(-10).unwrap(), 32).type_check(&T::Int(256)));
        assert!(!Int(I256::try_from(-10).unwrap(), 32).type_check(&T::Uint(256)));

        assert!(!Uint(U256::from(10), 32).type_check(&T::Uint(24)));
        assert!(Uint(U256::from(10), 32).type_check(&T::Uint(32)));
        assert!(Uint(U256::from(10), 32).type_check(&T::Uint(256)));
        assert!(!Uint(U256::from(10), 32).type_check(&T::FixedBytes(32)));

        assert!(!FixedBytes([0; 32].into(), 16).type_check(&T::FixedBytes(8)));
        assert!(FixedBytes([0; 32].into(), 16).type_check(&T::FixedBytes(16)));
        assert!(FixedBytes([0; 32].into(), 16).type_check(&T::FixedBytes(32)));
        assert!(!FixedBytes([0; 32].into(), 32).type_check(&T::Address));

        assert!(Address([0; 20].into()).type_check(&T::Address));
        assert!(!Address([0; 20].into()).type_check(&T::Function));

        assert!(Function([0; 24].into()).type_check(&T::Function));
        assert!(!Function([0; 24].into()).type_check(&T::Bytes));

        assert!(Bytes(vec![0, 0, 0]).type_check(&T::Bytes));
        assert!(!Bytes(vec![0, 0, 0]).type_check(&T::String));

        assert!(String("".to_owned()).type_check(&T::String));
        assert!(!String("".to_owned()).type_check(&T::Array(Box::new(T::Bool))));

        assert!(Array(vec![Bool(true)]).type_check(&T::Array(Box::new(T::Bool))));
        assert!(!Array(vec![Bool(true)]).type_check(&T::Array(Box::new(T::String))));
        assert!(!Array(vec![Bool(true)]).type_check(&T::FixedArray(Box::new(T::Bool), 1)));

        assert!(!FixedArray(vec![String("".to_owned())])
            .type_check(&T::FixedArray(Box::new(T::Bool), 1)));
        assert!(!FixedArray(vec![Bool(true), Bool(false)])
            .type_check(&T::FixedArray(Box::new(T::Bool), 1)));
        assert!(FixedArray(vec![Bool(true), Bool(false)])
            .type_check(&T::FixedArray(Box::new(T::Bool), 2)));
        assert!(!FixedArray(vec![Bool(true), Bool(false)])
            .type_check(&T::FixedArray(Box::new(T::Bool), 3)));
        assert!(!FixedArray(vec![Bool(true), Bool(false)])
            .type_check(&T::Tuple(vec![T::Bool, T::Bool])));

        assert!(!Tuple(vec![Bool(true), Bool(false)]).type_check(&T::Tuple(vec![T::Bool])));
        assert!(Tuple(vec![Bool(true), Bool(false)]).type_check(&T::Tuple(vec![T::Bool, T::Bool])));
        assert!(!Tuple(vec![Bool(true)]).type_check(&T::Tuple(vec![T::Bool, T::Bool])));
        assert!(!Tuple(vec![Bool(true)]).type_check(&T::Bool));
    }
}
