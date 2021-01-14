use diesel::prelude::*;
use diesel::PgConnection;
use std::str::FromStr;

use graph::prelude::{serde_json, web3::types::H256, EthereumBlockPointer};
use graph_store_postgres::db_schema_for_tests as db_schema;
use lazy_static::lazy_static;

/// The parts of an Ethereum block that are interesting for these tests:
/// the block number, hash, and the hash of the parent block
#[derive(Clone, Debug, PartialEq)]
pub struct FakeBlock {
    pub number: u64,
    pub hash: String,
    pub parent_hash: String,
}

impl FakeBlock {
    fn make_child(&self, hash: &str) -> Self {
        FakeBlock {
            number: self.number + 1,
            hash: hash.to_owned(),
            parent_hash: self.hash.clone(),
        }
    }

    fn make_no_parent(number: u64, hash: &str) -> Self {
        FakeBlock {
            number,
            hash: hash.to_owned(),
            parent_hash: NO_PARENT.clone(),
        }
    }

    fn insert(&self, conn: &PgConnection) {
        use db_schema::ethereum_blocks as b;

        let data = serde_json::to_value(format!(
            "{{\"hash\":\"{}\", \"number\":{}}}",
            self.hash, self.number
        ))
        .expect("Failed to serialize block");

        let errmsg = format!("Failed to insert block {} ({})", self.number, self.hash);
        diesel::insert_into(b::table)
            .values((
                &b::number.eq(self.number as i64),
                &b::hash.eq(&self.hash),
                &b::parent_hash.eq(&self.parent_hash),
                &b::network_name.eq(super::NETWORK_NAME),
                &b::data.eq(data),
            ))
            .execute(conn)
            .expect(&errmsg);
    }

    pub fn block_hash(&self) -> H256 {
        H256::from_str(self.hash.as_str()).expect("invalid block hash")
    }

    pub fn block_ptr(&self) -> EthereumBlockPointer {
        EthereumBlockPointer {
            number: self.number,
            hash: self.block_hash(),
        }
    }
}

pub type Chain = Vec<&'static FakeBlock>;

lazy_static! {
    // Hash indicating 'no parent'
    pub static ref NO_PARENT: String =
        "0000000000000000000000000000000000000000000000000000000000000000".to_owned();
    // Genesis block
    pub static ref GENESIS_BLOCK: FakeBlock = FakeBlock {
        number: super::GENESIS_PTR.number,
        hash: format!("{:x}", super::GENESIS_PTR.hash),
        parent_hash: NO_PARENT.clone()
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

/// Removes all networks and blocks from the database
pub fn remove() {
    use db_schema::ethereum_blocks as b;
    use db_schema::ethereum_networks as n;

    crate::store::remove_subgraphs();

    let conn = super::PRIMARY_POOL
        .get()
        .expect("Failed to connect to Postgres");

    diesel::delete(b::table)
        .execute(&conn)
        .expect("Failed to delete ethereum_blocks");
    diesel::delete(n::table)
        .execute(&conn)
        .expect("Failed to delete ethereum_networks");
}

// Store the given chain as the blocks for the `network` and set the
// network's genesis block to the hash of `GENESIS_BLOCK`
pub fn insert(chain: Chain, network: &str) {
    let conn = crate::store::PRIMARY_POOL
        .get()
        .expect("Failed to connect to Postgres");

    use db_schema::ethereum_networks as n;
    let hash = format!("{:x}", super::GENESIS_PTR.hash);
    diesel::insert_into(n::table)
        .values((
            &n::name.eq(network),
            &n::genesis_block_hash.eq(&hash),
            &n::net_version.eq(super::NETWORK_VERSION),
        ))
        .execute(&conn)
        .expect("Failed to insert test network");

    for block in chain {
        block.insert(&conn);
    }
}
