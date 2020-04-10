use std::fmt;

use async_trait::async_trait;

use crate::data::store::scalar::Bytes;
use crate::prelude::{Error, Logger};

#[derive(Debug, PartialEq)]
pub struct BlockPointer {
    pub number: u64,
    pub hash: Bytes,
}

impl fmt::Display for BlockPointer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "#{} ({})", self.number, self.hash.as_hex_string())
    }
}

trait Block {
    fn number(&self) -> u64;
    fn hash(&self) -> Bytes;
    fn pointer(&self) -> BlockPointer;
    fn parent_pointer(&self) -> Option<BlockPointer>;
}

#[async_trait]
pub trait Blockchain: Send + Sync + 'static {
    async fn latest_block_pointer(&self, logger: &Logger) -> Result<BlockPointer, Error>;

    async fn block_pointer_by_number(&self, logger: &Logger, n: u64)
        -> Result<BlockPointer, Error>;

    async fn block_pointer_by_hash(
        &self,
        logger: &Logger,
        hash: Bytes,
    ) -> Result<BlockPointer, Error>;

    async fn parent_block_pointer(
        &self,
        logger: &Logger,
        ptr: &BlockPointer,
    ) -> Result<Option<BlockPointer>, Error>;
}
