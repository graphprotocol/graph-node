use anyhow::anyhow;
use stable_hash::prelude::*;
use stable_hash::utils::AsBytes;
use std::convert::TryFrom;
use std::fmt::Write;
use std::{fmt, str::FromStr};
use web3::types::{Block, H256};

use crate::{cheap_clone::CheapClone, components::store::BlockNumber};

/// A simple marker for byte arrays that are really block hashes
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct BlockHash(pub Box<[u8]>);

impl BlockHash {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for BlockHash {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "0x{}", hex::encode(&self.0))
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

/// A block hash and block number from a specific Ethereum block.
///
/// Block numbers are signed 32 bit integers
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct BlockPtr {
    pub hash: BlockHash,
    pub number: BlockNumber,
}

impl CheapClone for BlockPtr {}

impl StableHash for BlockPtr {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        AsBytes(self.hash.0.as_ref()).stable_hash(sequence_number.next_child(), state);
        self.number.stable_hash(sequence_number.next_child(), state);
    }
}

impl BlockPtr {
    /// Encodes the block hash into a hexadecimal string **without** a "0x" prefix.
    /// Hashes are stored in the database in this format.
    pub fn hash_hex(&self) -> String {
        let mut s = String::with_capacity(self.hash.0.len() * 2);
        for b in self.hash.0.iter() {
            write!(s, "{:02x}", b).unwrap();
        }
        s
    }

    /// Block number to be passed into the store. Panics if it does not fit in an i32.
    pub fn block_number(&self) -> BlockNumber {
        self.number
    }

    pub fn hash_as_h256(&self) -> H256 {
        H256::from_slice(self.hash_slice())
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
        let hash = H256::from_str(hash)
            .map_err(|e| anyhow!("Cannot parse H256 value from string `{}`: {}", hash, e))?;

        Ok(BlockPtr::from((hash, number)))
    }
}

impl TryFrom<(&[u8], i64)> for BlockPtr {
    type Error = anyhow::Error;

    fn try_from((bytes, number): (&[u8], i64)) -> Result<Self, Self::Error> {
        let hash = if bytes.len() == H256::len_bytes() {
            H256::from_slice(bytes)
        } else {
            return Err(anyhow!(
                "invalid H256 value `{}` has {} bytes instead of {}"
            ));
        };
        Ok(BlockPtr::from((hash, number)))
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
