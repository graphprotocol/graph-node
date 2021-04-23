use graph::prelude::*;

use super::*;

const BLOCK: &str = "Block";

impl ToEntityId for Ommer {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self.0.hash.unwrap())
    }
}

impl ToEntityKey for Ommer {
    fn to_entity_key(&self, subgraph_id: DeploymentHash) -> EntityKey {
        EntityKey::data(
            subgraph_id,
            BLOCK.to_string(),
            format!("{:x}", self.0.hash.unwrap()),
        )
    }
}

impl ToEntityId for BlockWithOmmers {
    fn to_entity_id(&self) -> String {
        (*self).block.block.hash.unwrap().to_entity_id()
    }
}

impl ToEntityKey for &BlockWithOmmers {
    fn to_entity_key(&self, subgraph_id: DeploymentHash) -> EntityKey {
        EntityKey::data(
            subgraph_id,
            BLOCK.to_string(),
            format!("{:x}", (*self).block.block.hash.unwrap()),
        )
    }
}

impl TryIntoEntity for Ommer {
    fn try_into_entity(self) -> Result<Entity, Error> {
        let inner = &self.0;

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
            ("ommerCount", (inner.uncles.len() as i32).into()),
            ("ommerHash", inner.uncles_hash.into()),
            (
                "ommers",
                inner
                    .uncles
                    .iter()
                    .map(|hash| hash.to_entity_id())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("size", inner.size.into()),
            ("sealFields", inner.seal_fields.clone().into()),
            ("isOmmer", true.into()),
        ] as Vec<(_, Value)>))
    }
}

impl TryIntoEntity for &BlockWithOmmers {
    fn try_into_entity(self) -> Result<Entity, Error> {
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
            ("ommerCount", (self.ommers.len() as i32).into()),
            ("ommerHash", inner.uncles_hash.into()),
            (
                "ommers",
                self.inner()
                    .uncles
                    .iter()
                    .map(|hash| hash.to_entity_id())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("size", inner.size.into()),
            ("sealFields", inner.seal_fields.clone().into()),
            ("isOmmer", false.into()),
        ] as Vec<(_, Value)>))
    }
}
