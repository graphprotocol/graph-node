use std::fmt;

use async_trait::async_trait;

use crate::data::store::scalar::Bytes;
use crate::prelude::{Error, Logger};

#[derive(Clone, Debug, PartialEq)]
pub struct BlockPointer {
    pub number: u64,
    pub hash: Bytes,
}

impl fmt::Display for BlockPointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({})", self.number, self.hash.as_hex_string())
    }
}

impl Into<u64> for BlockPointer {
    fn into(self) -> u64 {
        self.number
    }
}

impl Into<Bytes> for BlockPointer {
    fn into(self) -> Bytes {
        self.hash
    }
}

pub trait ToBlockPointer: Sized {
    fn to_block_pointer(&self) -> BlockPointer;
    fn into_block_pointer(self) -> BlockPointer {
        self.to_block_pointer()
    }
}

impl ToBlockPointer for BlockPointer {
    fn to_block_pointer(&self) -> BlockPointer {
        self.clone()
    }
}

#[async_trait]
pub trait Blockchain: Send + Sync + 'static {
    async fn latest_block_pointer(&self, logger: &Logger) -> Result<BlockPointer, Error>;

    async fn block_pointer_by_number(
        &self,
        logger: &Logger,
        n: u64,
    ) -> Result<Option<BlockPointer>, Error>;

    async fn block_pointer_by_hash(
        &self,
        logger: &Logger,
        hash: Bytes,
    ) -> Result<Option<BlockPointer>, Error>;

    async fn parent_block_pointer(
        &self,
        logger: &Logger,
        ptr: &BlockPointer,
    ) -> Result<Option<BlockPointer>, Error>;
}
