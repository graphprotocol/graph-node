use std::str::FromStr;
use web3::types::H256;

use graph::data::store::scalar::*;
use graph::prelude::chains::*;
use graph::prelude::*;

pub struct EthereumBlock {
    block: LightEthereumBlock,
    ommers: Vec<EthereumBlock>,
}

struct BlockHash<'a>(&'a H256);

impl<'a> From<BlockHash<'a>> for Bytes {
    fn from(hash: BlockHash<'a>) -> Bytes {
        Bytes::from_str(format!("{:x}", hash.0).as_str()).unwrap()
    }
}

impl Block for EthereumBlock {
    fn number(&self) -> BigInt {
        self.block.number.unwrap().into()
    }

    fn hash(&self) -> Bytes {
        BlockHash(self.block.hash.as_ref().unwrap()).into()
    }

    fn pointer(&self) -> BlockPointer {
        BlockPointer {
            number: self.number(),
            hash: self.hash(),
        }
    }

    fn parent_hash(&self) -> Option<Bytes> {
        if self.block.number.unwrap() == 0.into() {
            None
        } else {
            Some(BlockHash(&self.block.parent_hash).into())
        }
    }
}
