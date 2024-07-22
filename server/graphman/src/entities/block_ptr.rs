use async_graphql::SimpleObject;

use crate::entities::BlockHash;
use crate::entities::BlockNumber;

#[derive(Clone, Debug, SimpleObject)]
pub struct BlockPtr {
    pub hash: BlockHash,
    pub number: BlockNumber,
}

impl From<graph::blockchain::BlockPtr> for BlockPtr {
    fn from(block_ptr: graph::blockchain::BlockPtr) -> Self {
        Self {
            hash: block_ptr.hash.into(),
            number: block_ptr.number.into(),
        }
    }
}
