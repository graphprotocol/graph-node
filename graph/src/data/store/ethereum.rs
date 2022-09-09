use super::scalar;
use crate::prelude::*;
use web3::types::{Address, Bytes, H2048, H256, H64, U128, U256, U64};

impl From<U128> for Value {
    fn from(n: U128) -> Value {
        Value::BigInt(scalar::BigInt::from_signed_u256(&n.into()))
    }
}

impl From<Address> for Value {
    fn from(address: Address) -> Value {
        Value::Bytes(scalar::Bytes::from(address.as_ref()))
    }
}

impl From<H64> for Value {
    fn from(hash: H64) -> Value {
        Value::Bytes(scalar::Bytes::from(hash.as_ref()))
    }
}

impl From<H256> for Value {
    fn from(hash: H256) -> Value {
        Value::Bytes(scalar::Bytes::from(hash.as_ref()))
    }
}

impl From<H2048> for Value {
    fn from(hash: H2048) -> Value {
        Value::Bytes(scalar::Bytes::from(hash.as_ref()))
    }
}

impl From<Bytes> for Value {
    fn from(bytes: Bytes) -> Value {
        Value::Bytes(scalar::Bytes::from(bytes.0.as_slice()))
    }
}

impl From<U64> for Value {
    fn from(n: U64) -> Value {
        Value::BigInt(BigInt::from(n))
    }
}

impl From<U256> for Value {
    fn from(n: U256) -> Value {
        Value::BigInt(BigInt::from_unsigned_u256(&n))
    }
}
