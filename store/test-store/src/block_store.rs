use std::{convert::TryFrom, str::FromStr, sync::Arc};

use lazy_static::lazy_static;

use graph::components::store::BlockStore;
use graph::{
    blockchain::Block,
    prelude::{
        serde_json, web3::types::H256, BlockHash, BlockNumber, BlockPtr, EthereumBlock,
        LightEthereumBlock,
    },
};

lazy_static! {
    // Genesis block
    pub static ref GENESIS_BLOCK: FakeBlock = FakeBlock {
        number: super::GENESIS_PTR.number,
        hash: super::GENESIS_PTR.hash_hex(),
        parent_hash: NO_PARENT.to_string()
    };
    pub static ref BLOCK_ONE: FakeBlock = GENESIS_BLOCK
        .make_child("8511fa04b64657581e3f00e14543c1d522d5d7e771b54aa3060b662ade47da13");
    pub static ref BLOCK_ONE_SIBLING: FakeBlock =
        GENESIS_BLOCK.make_child("b98fb783b49de5652097a989414c767824dff7e7fd765a63b493772511db81c1");
    pub static ref BLOCK_ONE_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(
        1,
        "7205bdfcf4521874cf38ce38c879ff967bf3a069941286bfe267109ad275a63d"
    );

    pub static ref BLOCK_TWO: FakeBlock = BLOCK_ONE.make_child("f8ccbd3877eb98c958614f395dd351211afb9abba187bfc1fb4ac414b099c4a6");
    pub static ref BLOCK_TWO_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(2, "3b652b00bff5e168b1218ff47593d516123261c4487629c4175f642ee56113fe");
    pub static ref BLOCK_THREE: FakeBlock = BLOCK_TWO.make_child("7347afe69254df06729e123610b00b8b11f15cfae3241f9366fb113aec07489c");
    pub static ref BLOCK_THREE_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(3, "fa9ebe3f74de4c56908b49f5c4044e85825f7350f3fa08a19151de82a82a7313");
    pub static ref BLOCK_FOUR: FakeBlock = BLOCK_THREE.make_child("7cce080f5a49c2997a6cc65fc1cee9910fd8fc3721b7010c0b5d0873e2ac785e");
    pub static ref BLOCK_FIVE: FakeBlock = BLOCK_FOUR.make_child("7b0ea919e258eb2b119eb32de56b85d12d50ac6a9f7c5909f843d6172c8ba196");
    pub static ref BLOCK_SIX_NO_PARENT: FakeBlock = FakeBlock::make_no_parent(6, "6b834521bb753c132fdcf0e1034803ed9068e324112f8750ba93580b393a986b");
}

// Hash indicating 'no parent'
pub const NO_PARENT: &str = "0000000000000000000000000000000000000000000000000000000000000000";
/// The parts of an Ethereum block that are interesting for these tests:
/// the block number, hash, and the hash of the parent block
#[derive(Clone, Debug, PartialEq)]
pub struct FakeBlock {
    pub number: BlockNumber,
    pub hash: String,
    pub parent_hash: String,
}

impl FakeBlock {
    pub fn make_child(&self, hash: &str) -> Self {
        FakeBlock {
            number: self.number + 1,
            hash: hash.to_owned(),
            parent_hash: self.hash.clone(),
        }
    }

    pub fn make_no_parent(number: BlockNumber, hash: &str) -> Self {
        FakeBlock {
            number,
            hash: hash.to_owned(),
            parent_hash: NO_PARENT.to_string(),
        }
    }

    pub fn block_hash(&self) -> BlockHash {
        BlockHash::from_str(self.hash.as_str()).expect("invalid block hash")
    }

    pub fn block_ptr(&self) -> BlockPtr {
        BlockPtr::new(self.block_hash(), self.number)
    }

    pub fn as_ethereum_block(&self) -> EthereumBlock {
        let parent_hash = H256::from_str(self.parent_hash.as_str()).expect("invalid parent hash");

        let mut block = LightEthereumBlock::default();
        block.number = Some(self.number.into());
        block.parent_hash = parent_hash;
        block.hash = Some(H256(self.block_hash().as_slice().try_into().unwrap()));

        EthereumBlock {
            block: Arc::new(block),
            transaction_receipts: Vec::new(),
        }
    }
}

impl Block for FakeBlock {
    fn ptr(&self) -> BlockPtr {
        self.block_ptr()
    }

    fn parent_ptr(&self) -> Option<BlockPtr> {
        if self.number > 0 {
            Some(
                BlockPtr::try_from((self.parent_hash.as_str(), (self.number - 1) as i64))
                    .expect("can construct parent ptr"),
            )
        } else {
            None
        }
    }

    fn data(&self) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::to_value(self.as_ethereum_block())
    }
}

pub type FakeBlockList<'a> = Vec<&'static FakeBlock>;

/// Store the given chain as the blocks for the `network` set the
/// network's genesis block to `genesis_hash`, and head block to
/// `null`
pub fn set_chain(chain: FakeBlockList, network: &str) {
    let store = crate::store::STORE
        .block_store()
        .chain_store(network)
        .unwrap();
    let chain: Vec<&dyn Block> = chain.iter().map(|block| *block as &dyn Block).collect();
    store.set_chain(&GENESIS_BLOCK.hash, chain);
}
