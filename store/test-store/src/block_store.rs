use lazy_static::lazy_static;

use graph::components::store::BlockStore;
pub use graph_store_postgres::layout_for_tests::chain_support::{
    Chain, FakeBlock, SettableChainStore as _, NO_PARENT,
};

lazy_static! {
    // Genesis block
    pub static ref GENESIS_BLOCK: FakeBlock = FakeBlock {
        number: super::GENESIS_PTR.number,
        hash: format!("{:x}", super::GENESIS_PTR.hash),
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

/// Store the given chain as the blocks for the `network` set the
/// network's genesis block to `genesis_hash`, and head block to
/// `null`
pub fn set_chain(chain: Chain, network: &str) {
    let store = crate::store::STORE
        .block_store()
        .chain_store(network)
        .unwrap();

    store.set_chain(&GENESIS_BLOCK.hash, chain);
}
