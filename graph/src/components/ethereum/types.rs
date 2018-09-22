use ethabi::LogParam;
use web3::types::*;

/// Ethereum block data.
#[derive(Clone, Debug, Serialize)]
pub struct EthereumBlockData {
    pub hash: H256,
    pub parent_hash: H256,
    pub uncles_hash: H256,
    pub author: H160,
    pub state_root: H256,
    pub transactions_root: H256,
    pub receipts_root: H256,
    pub number: U128,
    pub gas_used: U256,
    pub gas_limit: U256,
    pub timestamp: U256,
    pub difficulty: U256,
    pub total_difficulty: U256,
}

impl<'a, T> From<&'a Block<T>> for EthereumBlockData {
    fn from(block: &'a Block<T>) -> EthereumBlockData {
        EthereumBlockData {
            hash: block.hash.clone().unwrap(),
            parent_hash: block.parent_hash.clone(),
            uncles_hash: block.uncles_hash.clone(),
            author: block.author.clone(),
            state_root: block.state_root.clone(),
            transactions_root: block.transactions_root.clone(),
            receipts_root: block.receipts_root.clone(),
            number: block.number.clone().unwrap(),
            gas_used: block.gas_used.clone(),
            gas_limit: block.gas_limit.clone(),
            timestamp: block.timestamp.clone(),
            difficulty: block.difficulty.clone(),
            total_difficulty: block.total_difficulty.clone(),
        }
    }
}

/// Ethereum transaction data.
#[derive(Clone, Debug)]
pub struct EthereumTransactionData {
    pub hash: H256,
    pub block_hash: H256,
    pub block_number: U256,
    pub gas_used: U256,
}

impl<'a> From<&'a Transaction> for EthereumTransactionData {
    fn from(tx: &'a Transaction) -> EthereumTransactionData {
        EthereumTransactionData {
            hash: tx.hash.clone(),
            block_hash: tx.block_hash.clone().unwrap(),
            block_number: tx.block_number.clone().unwrap(),
            gas_used: tx.gas.clone(),
        }
    }
}

/// An Ethereum event logged from a specific contract address and block.
#[derive(Debug)]
pub struct EthereumEventData {
    pub address: Address,
    pub block: EthereumBlockData,
    pub transaction: EthereumTransactionData,
    pub params: Vec<LogParam>,
}

impl Clone for EthereumEventData {
    fn clone(&self) -> Self {
        EthereumEventData {
            address: self.address.clone(),
            block: self.block.clone(),
            transaction: self.transaction.clone(),
            params: self
                .params
                .iter()
                .map(|log_param| LogParam {
                    name: log_param.name.clone(),
                    value: log_param.value.clone(),
                }).collect(),
        }
    }
}

/// A block hash and block number from a specific Ethereum block.
///
/// Maximum block number supported: 2^63 - 1
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EthereumBlockPointer {
    pub hash: H256,
    pub number: u64,
}

impl EthereumBlockPointer {
    /// Creates a pointer to the parent of the specified block.
    pub fn to_parent<T>(b: &Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.parent_hash,
            number: b.number.unwrap().as_u64() - 1,
        }
    }

    /// Encodes the block hash into a hexadecimal string **without** a "0x" prefix.
    /// Hashes are stored in the database in this format.
    ///
    /// This mainly exists because of backwards incompatible changes in how the Web3 library
    /// implements `H256::to_string`.
    pub fn hash_hex(&self) -> String {
        format!("{:x}", self.hash)
    }
}

impl<T> From<Block<T>> for EthereumBlockPointer {
    fn from(b: Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.hash.unwrap(),
            number: b.number.unwrap().as_u64(),
        }
    }
}

impl<'a, T> From<&'a Block<T>> for EthereumBlockPointer {
    fn from(b: &'a Block<T>) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.hash.unwrap(),
            number: b.number.unwrap().as_u64(),
        }
    }
}

impl From<(H256, u64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, u64)) -> EthereumBlockPointer {
        if number >= (1 << 63) {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer { hash, number }
    }
}

impl From<(H256, i64)> for EthereumBlockPointer {
    fn from((hash, number): (H256, i64)) -> EthereumBlockPointer {
        if number < 0 {
            panic!("block number out of range: {}", number);
        }

        EthereumBlockPointer {
            hash,
            number: number as u64,
        }
    }
}
