use ethabi::LogParam;
use web3::types::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EthereumBlock {
    pub block: Block<Transaction>,
    pub transaction_receipts: Vec<TransactionReceipt>,
}

impl EthereumBlock {
    pub fn transaction_for_log(&self, log: &Log) -> Option<Transaction> {
        log.transaction_hash
            .and_then(|hash| self.block.transactions.iter().find(|tx| tx.hash == hash))
            .cloned()
    }
}

// Remove this and derive after a new web3 is released is released.
impl Default for EthereumBlock {
    fn default() -> Self {
        Self {
            block: Block {
                hash: Some(H256::default()),
                parent_hash: H256::default(),
                uncles_hash: H256::default(),
                author: H160::default(),
                state_root: H256::default(),
                transactions_root: H256::default(),
                receipts_root: H256::default(),
                number: None,
                gas_used: U256::default(),
                gas_limit: U256::default(),
                extra_data: Bytes(vec![]),
                logs_bloom: H2048::default(),
                timestamp: U256::default(),
                difficulty: U256::default(),
                total_difficulty: U256::default(),
                seal_fields: vec![],
                uncles: vec![],
                transactions: vec![],
                size: None,
            },
            transaction_receipts: vec![],
        }
    }
}

/// Ethereum block data.
#[derive(Clone, Debug, Default, Serialize)]
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
    pub size: Option<U256>,
}

impl<'a, T> From<&'a Block<T>> for EthereumBlockData {
    fn from(block: &'a Block<T>) -> EthereumBlockData {
        EthereumBlockData {
            hash: block.hash.unwrap(),
            parent_hash: block.parent_hash,
            uncles_hash: block.uncles_hash,
            author: block.author,
            state_root: block.state_root,
            transactions_root: block.transactions_root,
            receipts_root: block.receipts_root,
            number: block.number.unwrap(),
            gas_used: block.gas_used,
            gas_limit: block.gas_limit,
            timestamp: block.timestamp,
            difficulty: block.difficulty,
            total_difficulty: block.total_difficulty,
            size: block.size,
        }
    }
}

/// Ethereum transaction data.
#[derive(Clone, Debug)]
pub struct EthereumTransactionData {
    pub hash: H256,
    pub index: U128,
    pub from: H160,
    pub to: Option<H160>,
    pub value: U256,
    pub gas_used: U256,
    pub gas_price: U256,
}

impl<'a> From<&'a Transaction> for EthereumTransactionData {
    fn from(tx: &'a Transaction) -> EthereumTransactionData {
        EthereumTransactionData {
            hash: tx.hash,
            index: tx.transaction_index.unwrap(),
            from: tx.from,
            to: tx.to,
            value: tx.value,
            gas_used: tx.gas,
            gas_price: tx.gas_price,
        }
    }
}

/// An Ethereum event logged from a specific contract address and block.
#[derive(Debug)]
pub struct EthereumEventData {
    pub address: Address,
    pub log_index: U256,
    pub transaction_log_index: U256,
    pub log_type: Option<String>,
    pub block: EthereumBlockData,
    pub transaction: EthereumTransactionData,
    pub params: Vec<LogParam>,
}

impl Clone for EthereumEventData {
    fn clone(&self) -> Self {
        EthereumEventData {
            address: self.address,
            log_index: self.log_index,
            transaction_log_index: self.transaction_log_index,
            log_type: self.log_type.clone(),
            block: self.block.clone(),
            transaction: self.transaction.clone(),
            params: self
                .params
                .iter()
                .map(|log_param| LogParam {
                    name: log_param.name.clone(),
                    value: log_param.value.clone(),
                })
                .collect(),
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
    pub fn to_parent(b: &EthereumBlock) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.block.parent_hash,
            number: b.block.number.unwrap().as_u64() - 1,
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

impl From<EthereumBlock> for EthereumBlockPointer {
    fn from(b: EthereumBlock) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.block.hash.unwrap(),
            number: b.block.number.unwrap().as_u64(),
        }
    }
}

impl<'a> From<&'a EthereumBlock> for EthereumBlockPointer {
    fn from(b: &'a EthereumBlock) -> EthereumBlockPointer {
        EthereumBlockPointer {
            hash: b.block.hash.unwrap(),
            number: b.block.number.unwrap().as_u64(),
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
