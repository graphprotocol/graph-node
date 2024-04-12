use super::scalar;
use crate::derive::CheapClone;
use crate::prelude::*;
use web3::types::{Address, Bytes, H2048, H256, H64, U64};

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

/// Helper structs for dealing with ethereum calls
pub mod call {
    use std::sync::Arc;

    use crate::data::store::scalar::Bytes;

    use super::CheapClone;

    /// The return value of an ethereum call. `Null` indicates that we made
    /// the call but didn't get a value back (including when we get the
    /// error 'call reverted')
    #[derive(Debug, Clone, PartialEq)]
    pub enum Retval {
        Null,
        Value(Bytes),
    }

    impl Retval {
        pub fn unwrap(self) -> Bytes {
            use Retval::*;
            match self {
                Value(val) => val,
                Null => panic!("called `call::Retval::unwrap()` on a `Null` value"),
            }
        }
    }

    /// Indication of where the result of an ethereum call comes from. We
    /// unfortunately need that so we can avoid double-counting declared calls
    /// as they are accessed as normal eth calls and we'd count them twice
    /// without this.
    #[derive(Debug, Clone, Copy, PartialEq)]
    pub enum Source {
        Memory,
        Store,
        Rpc,
    }

    impl Source {
        /// Return `true` if calls from this source should be observed,
        /// i.e., counted as actual calls
        pub fn observe(&self) -> bool {
            matches!(self, Source::Rpc | Source::Store)
        }
    }

    impl std::fmt::Display for Source {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            match self {
                Source::Memory => write!(f, "memory"),
                Source::Store => write!(f, "store"),
                Source::Rpc => write!(f, "rpc"),
            }
        }
    }

    /// The address and encoded name and parms for an `eth_call`, the raw
    /// ingredients to make an `eth_call` request. Because we cache this, it
    /// gets cloned a lot and needs to remain cheap to clone.
    ///
    /// For equality and hashing, we only consider the address and the
    /// encoded call as the index is set by the caller and has no influence
    /// on the call's return value
    #[derive(Debug, Clone, CheapClone)]
    pub struct Request {
        pub address: ethabi::Address,
        pub encoded_call: Arc<Bytes>,
        /// The index is set by the caller and is used to identify the
        /// request in related data structures that the caller might have
        pub index: u32,
    }

    impl Request {
        pub fn new(address: ethabi::Address, encoded_call: Vec<u8>, index: u32) -> Self {
            Request {
                address,
                encoded_call: Arc::new(Bytes::from(encoded_call)),
                index,
            }
        }

        /// Create a response struct for this request
        pub fn response(self, retval: Retval, source: Source) -> Response {
            Response {
                req: self,
                retval,
                source,
            }
        }
    }

    impl PartialEq for Request {
        fn eq(&self, other: &Self) -> bool {
            self.address == other.address
                && self.encoded_call.as_ref() == other.encoded_call.as_ref()
        }
    }

    impl Eq for Request {}

    impl std::hash::Hash for Request {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.address.hash(state);
            self.encoded_call.as_ref().hash(state);
        }
    }

    #[derive(Debug, PartialEq)]
    pub struct Response {
        pub req: Request,
        pub retval: Retval,
        pub source: Source,
    }
}
