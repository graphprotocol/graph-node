use std::str::FromStr;
use web3::types::H256;

use graph::data::store::scalar::*;
use graph::prelude::blockchain::*;
use graph::prelude::*;

#[derive(Debug)]
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

    fn parent_pointer(&self) -> Option<BlockPointer> {
        let number = self.block.number.unwrap();

        if number == 0.into() {
            None
        } else {
            Some(BlockPointer {
                number: BigInt::from(number - 1),
                hash: BlockHash(&self.block.parent_hash).into(),
            })
        }
    }
}
