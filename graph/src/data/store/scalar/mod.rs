mod bigdecimal;
mod bigint;
mod bytes;
mod timestamp;

pub use bigdecimal::BigDecimal;
pub use bigint::{BigInt, BigIntSign};
pub use bytes::Bytes;
pub use old_bigdecimal::ToPrimitive;
pub use timestamp::Timestamp;

// Test helpers for BigInt and BigDecimal tests
#[cfg(test)]
mod test {
    use stable_hash_legacy::crypto::SetHasher;
    use stable_hash_legacy::prelude::*;
    use stable_hash_legacy::utils::stable_hash;

    pub(super) fn crypto_stable_hash(value: impl StableHash) -> <SetHasher as StableHasher>::Out {
        stable_hash::<SetHasher, _>(&value)
    }

    pub(super) fn same_stable_hash(left: impl StableHash, right: impl StableHash) {
        let left = crypto_stable_hash(left);
        let right = crypto_stable_hash(right);
        assert_eq!(left, right);
    }
}
