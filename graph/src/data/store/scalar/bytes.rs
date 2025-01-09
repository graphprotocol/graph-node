use diesel::serialize::ToSql;
use hex;
use serde::{self, Deserialize, Serialize};
use web3::types::*;

use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

use crate::blockchain::BlockHash;
use crate::derive::CacheWeight;
use crate::util::stable_hash_glue::{impl_stable_hash, AsBytes};

/// A byte array that's serialized as a hex string prefixed by `0x`.
#[derive(Clone, CacheWeight, PartialEq, Eq, PartialOrd, Ord)]
pub struct Bytes(Box<[u8]>);

impl Deref for Bytes {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Debug for Bytes {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Bytes(0x{})", hex::encode(&self.0))
    }
}

impl_stable_hash!(Bytes(transparent: AsBytes));

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

impl From<web3::types::Bytes> for Bytes {
    fn from(bytes: web3::types::Bytes) -> Bytes {
        Bytes::from(bytes.0.as_slice())
    }
}

impl From<BlockHash> for Bytes {
    fn from(hash: BlockHash) -> Self {
        Bytes(hash.0)
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

impl<const N: usize> From<[u8; N]> for Bytes {
    fn from(array: [u8; N]) -> Bytes {
        Bytes(array.into())
    }
}

impl From<Vec<u8>> for Bytes {
    fn from(vec: Vec<u8>) -> Self {
        Bytes(vec.into())
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl ToSql<diesel::sql_types::Binary, diesel::pg::Pg> for Bytes {
    fn to_sql<'b>(
        &'b self,
        out: &mut diesel::serialize::Output<'b, '_, diesel::pg::Pg>,
    ) -> diesel::serialize::Result {
        <_ as ToSql<diesel::sql_types::Binary, _>>::to_sql(self.as_slice(), &mut out.reborrow())
    }
}
