use anyhow::anyhow;
use chrono::{DateTime, Utc};
use diesel::pg::Pg;
use diesel::serialize::Output;
use diesel::sql_types::Timestamptz;
use diesel::sql_types::{Bytea, Nullable, Text};
use diesel::types::FromSql;
use diesel::types::ToSql;
use diesel_derives::{AsExpression, FromSqlRow};
use std::convert::TryFrom;
use std::io::Write;
use std::time::Duration;
use std::{fmt, str::FromStr};
use web3::types::{Block, H256};

use crate::data::graphql::IntoValue;
use crate::object;
use crate::prelude::{r, BigInt, TryFromValue, Value, ValueMap};
use crate::util::stable_hash_glue::{impl_stable_hash, AsBytes};
use crate::{cheap_clone::CheapClone, components::store::BlockNumber};

/// A simple marker for byte arrays that are really block hashes
#[derive(Clone, Default, PartialEq, Eq, Hash, AsExpression, FromSqlRow)]
pub struct BlockHash(pub Box<[u8]>);

impl_stable_hash!(BlockHash(transparent: AsBytes));

impl BlockHash {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Encodes the block hash into a hexadecimal string **without** a "0x"
    /// prefix. Hashes are stored in the database in this format when the
    /// schema uses `text` columns, which is a legacy and such columns
    /// should be changed to use `bytea`
    pub fn hash_hex(&self) -> String {
        hex::encode(&self.0)
    }

    pub fn zero() -> Self {
        Self::from(H256::zero())
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl fmt::Debug for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "0x{}", hex::encode(&self.0))
    }
}

impl fmt::LowerHex for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

impl CheapClone for BlockHash {}

impl From<H256> for BlockHash {
    fn from(hash: H256) -> Self {
        BlockHash(hash.as_bytes().into())
    }
}

impl From<Vec<u8>> for BlockHash {
    fn from(bytes: Vec<u8>) -> Self {
        BlockHash(bytes.as_slice().into())
    }
}

impl TryFrom<&str> for BlockHash {
    type Error = anyhow::Error;

    fn try_from(hash: &str) -> Result<Self, Self::Error> {
        let hash = hash.trim_start_matches("0x");
        let hash = hex::decode(hash)?;

        Ok(BlockHash(hash.as_slice().into()))
    }
}

impl FromStr for BlockHash {
    type Err = anyhow::Error;

    fn from_str(hash: &str) -> Result<Self, Self::Err> {
        Self::try_from(hash)
    }
}

impl FromSql<Nullable<Text>, Pg> for BlockHash {
    fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        BlockHash::try_from(s.as_str())
            .map_err(|e| format!("invalid block hash `{}`: {}", s, e).into())
    }
}

impl FromSql<Text, Pg> for BlockHash {
    fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
        let s = <String as FromSql<Text, Pg>>::from_sql(bytes)?;
        BlockHash::try_from(s.as_str())
            .map_err(|e| format!("invalid block hash `{}`: {}", s, e).into())
    }
}

impl FromSql<Bytea, Pg> for BlockHash {
    fn from_sql(bytes: Option<&[u8]>) -> diesel::deserialize::Result<Self> {
        let bytes = <Vec<u8> as FromSql<Bytea, Pg>>::from_sql(bytes)?;
        Ok(BlockHash::from(bytes))
    }
}

/// A block hash and block number from a specific Ethereum block.
///
/// Block numbers are signed 32 bit integers
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BlockPtr {
    pub hash: BlockHash,
    pub number: BlockNumber,
}

impl CheapClone for BlockPtr {}

impl_stable_hash!(BlockPtr { hash, number });

impl BlockPtr {
    pub fn new(hash: BlockHash, number: BlockNumber) -> Self {
        Self { hash, number }
    }

    /// Encodes the block hash into a hexadecimal string **without** a "0x" prefix.
    /// Hashes are stored in the database in this format.
    pub fn hash_hex(&self) -> String {
        self.hash.hash_hex()
    }

    /// Block number to be passed into the store. Panics if it does not fit in an i32.
    pub fn block_number(&self) -> BlockNumber {
        self.number
    }

    // FIXME:
    //
    // workaround for arweave
    pub fn hash_as_h256(&self) -> H256 {
        H256::from_slice(&self.hash_slice()[..32])
    }

    pub fn hash_slice(&self) -> &[u8] {
        self.hash.0.as_ref()
    }
}

impl fmt::Display for BlockPtr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({})", self.number, self.hash_hex())
    }
}

impl fmt::Debug for BlockPtr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({})", self.number, self.hash_hex())
    }
}

impl slog::Value for BlockPtr {
    fn serialize(
        &self,
        record: &slog::Record,
        key: slog::Key,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::Value::serialize(&self.to_string(), record, key, serializer)
    }
}

impl<T> From<Block<T>> for BlockPtr {
    fn from(b: Block<T>) -> BlockPtr {
        BlockPtr::from((b.hash.unwrap(), b.number.unwrap().as_u64()))
    }
}

impl<'a, T> From<&'a Block<T>> for BlockPtr {
    fn from(b: &'a Block<T>) -> BlockPtr {
        BlockPtr::from((b.hash.unwrap(), b.number.unwrap().as_u64()))
    }
}

impl From<(Vec<u8>, i32)> for BlockPtr {
    fn from((bytes, number): (Vec<u8>, i32)) -> Self {
        BlockPtr {
            hash: BlockHash::from(bytes),
            number,
        }
    }
}

impl From<(H256, i32)> for BlockPtr {
    fn from((hash, number): (H256, i32)) -> BlockPtr {
        BlockPtr {
            hash: hash.into(),
            number,
        }
    }
}

impl From<(Vec<u8>, u64)> for BlockPtr {
    fn from((bytes, number): (Vec<u8>, u64)) -> Self {
        let number = i32::try_from(number).unwrap();
        BlockPtr {
            hash: BlockHash::from(bytes),
            number,
        }
    }
}

impl From<(H256, u64)> for BlockPtr {
    fn from((hash, number): (H256, u64)) -> BlockPtr {
        let number = i32::try_from(number).unwrap();

        BlockPtr::from((hash, number))
    }
}

impl From<(H256, i64)> for BlockPtr {
    fn from((hash, number): (H256, i64)) -> BlockPtr {
        if number < 0 {
            panic!("block number out of range: {}", number);
        }

        BlockPtr::from((hash, number as u64))
    }
}

impl TryFrom<(&str, i64)> for BlockPtr {
    type Error = anyhow::Error;

    fn try_from((hash, number): (&str, i64)) -> Result<Self, Self::Error> {
        let hash = hash.trim_start_matches("0x");
        let hash = BlockHash::from_str(hash)?;

        Ok(BlockPtr::new(hash, number as i32))
    }
}

impl TryFrom<(&[u8], i64)> for BlockPtr {
    type Error = anyhow::Error;

    fn try_from((bytes, number): (&[u8], i64)) -> Result<Self, Self::Error> {
        let hash = if bytes.len() == H256::len_bytes() {
            H256::from_slice(bytes)
        } else {
            return Err(anyhow!(
                "invalid H256 value `{}` has {} bytes instead of {}",
                hex::encode(bytes),
                bytes.len(),
                H256::len_bytes()
            ));
        };
        Ok(BlockPtr::from((hash, number)))
    }
}

impl TryFromValue for BlockPtr {
    fn try_from_value(value: &r::Value) -> Result<Self, anyhow::Error> {
        match value {
            r::Value::Object(o) => {
                let number = o.get_required::<BigInt>("number")?.to_u64() as BlockNumber;
                let hash = o.get_required::<BlockHash>("hash")?;

                Ok(BlockPtr::new(hash, number))
            }
            _ => Err(anyhow!(
                "failed to parse non-object value into BlockPtr: {:?}",
                value
            )),
        }
    }
}

impl IntoValue for BlockPtr {
    fn into_value(self) -> r::Value {
        object! {
            __typename: "Block",
            hash: self.hash_hex(),
            number: format!("{}", self.number),
        }
    }
}

impl From<BlockPtr> for H256 {
    fn from(ptr: BlockPtr) -> Self {
        ptr.hash_as_h256()
    }
}

impl From<BlockPtr> for BlockNumber {
    fn from(ptr: BlockPtr) -> Self {
        ptr.number
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
/// A collection of attributes that (kind of) uniquely identify a blockchain.
pub struct ChainIdentifier {
    pub net_version: String,
    pub genesis_block_hash: BlockHash,
}

impl fmt::Display for ChainIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "net_version: {}, genesis_block_hash: {}",
            self.net_version, self.genesis_block_hash
        )
    }
}

/// The timestamp associated with a block. This is used whenever a time
/// needs to be connected to data within the block
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlockTime(DateTime<Utc>);

impl BlockTime {
    /// A timestamp from a long long time ago used to indicate that we don't
    /// have a timestamp
    pub const NONE: Self = Self(DateTime::<Utc>::MIN_UTC);

    pub const MAX: Self = Self(DateTime::<Utc>::MAX_UTC);

    pub const MIN: Self = Self(DateTime::<Utc>::MIN_UTC);

    /// Construct a block time that is the given number of seconds and
    /// nanoseconds after the Unix epoch
    pub fn since_epoch(secs: i64, nanos: u32) -> Self {
        Self(
            DateTime::from_timestamp(secs, nanos)
                .ok_or_else(|| anyhow!("invalid block time: {}s {}ns", secs, nanos))
                .unwrap(),
        )
    }

    /// Construct a block time for tests where blocks are exactly 45 minutes
    /// apart. We use that big a timespan to make it easier to trigger
    /// hourly rollups in tests
    #[cfg(debug_assertions)]
    pub fn for_test(ptr: &BlockPtr) -> Self {
        Self::since_epoch(ptr.number as i64 * 45 * 60, 0)
    }

    pub fn as_secs_since_epoch(&self) -> i64 {
        self.0.timestamp()
    }

    /// Return the number of the last bucket that starts before `self`
    /// assuming buckets have the given `length`
    pub(crate) fn bucket(&self, length: Duration) -> usize {
        // Treat any time before the epoch as zero, i.e., the epoch; in
        // practice, we will only deal with block times that are pretty far
        // after the epoch
        let ts = self.0.timestamp_millis().max(0);
        let nr = ts as u128 / length.as_millis();
        usize::try_from(nr).unwrap()
    }
}

impl From<Duration> for BlockTime {
    fn from(d: Duration) -> Self {
        Self::since_epoch(i64::try_from(d.as_secs()).unwrap(), d.subsec_nanos())
    }
}

impl From<BlockTime> for Value {
    fn from(block_time: BlockTime) -> Self {
        Value::Int8(block_time.as_secs_since_epoch())
    }
}

impl TryFrom<&Value> for BlockTime {
    type Error = anyhow::Error;

    fn try_from(value: &Value) -> Result<Self, Self::Error> {
        match value {
            Value::Int8(ts) => Ok(BlockTime::since_epoch(*ts, 0)),
            _ => Err(anyhow!("invalid block time: {:?}", value)),
        }
    }
}

impl ToSql<Timestamptz, Pg> for BlockTime {
    fn to_sql<W: Write>(&self, out: &mut Output<W, Pg>) -> diesel::serialize::Result {
        <DateTime<Utc> as ToSql<Timestamptz, Pg>>::to_sql(&self.0, out)
    }
}
