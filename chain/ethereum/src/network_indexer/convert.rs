use graph::prelude::*;
use web3::types::{Block, H160, H256};

use super::*;

/// A value that can be converted to an entity ID.
pub trait ToEntityId {
    fn to_entity_id(&self) -> String;
}

/// A value that can be converted to an entity key.
pub trait ToEntityKey {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey;
}

/// A value that can (maybe) be converted to an entity.
pub trait TryIntoEntity {
    fn try_into_entity(&self) -> Result<Entity, Error>;
}

/**
 * Implementations of these traits for various Ethereum types.
 */

impl ToEntityId for H160 {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self)
    }
}

impl ToEntityId for H256 {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self)
    }
}

impl ToEntityId for Block<H256> {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self.hash.unwrap())
    }
}

impl ToEntityKey for Block<H256> {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", self.hash.unwrap()),
        }
    }
}

impl ToEntityKey for EthereumBlockPointer {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", self.hash),
        }
    }
}

impl ToEntityId for BlockWithUncles {
    fn to_entity_id(&self) -> String {
        (*self).block.block.hash.unwrap().to_entity_id()
    }
}

impl ToEntityKey for &BlockWithUncles {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", (*self).block.block.hash.unwrap()),
        }
    }
}

impl TryIntoEntity for Block<H256> {
    fn try_into_entity(&self) -> Result<Entity, Error> {
        Ok(Entity::from(vec![
            ("id", format!("{:x}", self.hash.unwrap()).into()),
            ("number", self.number.unwrap().into()),
            ("hash", self.hash.unwrap().into()),
            ("parent", self.parent_hash.to_entity_id().into()),
            (
                "nonce",
                self.nonce.map_or(Value::Null, |nonce| nonce.into()),
            ),
            ("transactionsRoot", self.transactions_root.into()),
            ("transactionCount", (self.transactions.len() as i32).into()),
            ("stateRoot", self.state_root.into()),
            ("receiptsRoot", self.receipts_root.into()),
            ("extraData", self.extra_data.clone().into()),
            ("gasLimit", self.gas_limit.into()),
            ("gasUsed", self.gas_used.into()),
            ("timestamp", self.timestamp.into()),
            ("logsBloom", self.logs_bloom.into()),
            ("mixHash", self.mix_hash.into()),
            ("difficulty", self.difficulty.into()),
            ("totalDifficulty", self.total_difficulty.into()),
            ("ommerCount", (self.uncles.len() as i32).into()),
            ("ommerHash", self.uncles_hash.into()),
            (
                "ommers",
                self.uncles
                    .iter()
                    .map(|hash| hash.to_entity_id())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("size", self.size.into()),
            ("sealFields", self.seal_fields.clone().into()),
        ] as Vec<(_, Value)>))
    }
}

impl TryIntoEntity for &BlockWithUncles {
    fn try_into_entity(&self) -> Result<Entity, Error> {
        let inner = self.inner();

        Ok(Entity::from(vec![
            ("id", format!("{:x}", inner.hash.unwrap()).into()),
            ("number", inner.number.unwrap().into()),
            ("hash", inner.hash.unwrap().into()),
            ("parent", inner.parent_hash.to_entity_id().into()),
            (
                "nonce",
                inner.nonce.map_or(Value::Null, |nonce| nonce.into()),
            ),
            ("transactionsRoot", inner.transactions_root.into()),
            ("transactionCount", (inner.transactions.len() as i32).into()),
            ("stateRoot", inner.state_root.into()),
            ("receiptsRoot", inner.receipts_root.into()),
            ("extraData", inner.extra_data.clone().into()),
            ("gasLimit", inner.gas_limit.into()),
            ("gasUsed", inner.gas_used.into()),
            ("timestamp", inner.timestamp.into()),
            ("logsBloom", inner.logs_bloom.into()),
            ("mixHash", inner.mix_hash.into()),
            ("difficulty", inner.difficulty.into()),
            ("totalDifficulty", inner.total_difficulty.into()),
            ("ommerCount", (self.uncles.len() as i32).into()),
            ("ommerHash", inner.uncles_hash.into()),
            (
                "ommers",
                self.uncles
                    .iter()
                    .map(|ommer| {
                        ommer
                            .as_ref()
                            .map_or(Value::Null, |ommer| Value::String(ommer.to_entity_id()))
                    })
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("size", inner.size.into()),
            ("sealFields", inner.seal_fields.clone().into()),
        ] as Vec<(_, Value)>))
    }
}
