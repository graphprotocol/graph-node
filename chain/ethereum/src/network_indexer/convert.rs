use graph::prelude::*;

use super::*;

impl ToEntityId for Ommer {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self.0.hash.unwrap())
    }
}

impl ToEntityKey for Ommer {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", self.0.hash.unwrap()),
        }
    }
}

impl ToEntityId for BlockWithOmmers {
    fn to_entity_id(&self) -> String {
        (*self).block.block.hash.unwrap().to_entity_id()
    }
}

impl ToEntityKey for &BlockWithOmmers {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Block".into(),
            entity_id: format!("{:x}", (*self).block.block.hash.unwrap()),
        }
    }
}

impl ToEntityId for LoadedTransaction {
    fn to_entity_id(&self) -> String {
        format!("{:x}", self.transaction.hash)
    }
}

impl ToEntityKey for &LoadedTransaction {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Transaction".into(),
            entity_id: format!("{:x}", self.transaction.hash),
        }
    }
}

impl ToEntityId for Log {
    fn to_entity_id(&self) -> String {
        format!(
            "{:x}-{}",
            self.0.block_hash.unwrap(),
            self.0.log_index.unwrap()
        )
    }
}

impl ToEntityKey for &Log {
    fn to_entity_key(&self, subgraph_id: SubgraphDeploymentId) -> EntityKey {
        EntityKey {
            subgraph_id,
            entity_type: "Log".into(),
            entity_id: format!(
                "{:x}-{}",
                self.0.block_hash.unwrap(),
                self.0.log_index.unwrap()
            ),
        }
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
            ("isOmmer", true.into()),
            ("ommerCount", (inner.uncles.len() as i32).into()),
            (
                "ommers",
                inner
                    .uncles
                    .iter()
                    .map(|hash| hash.to_entity_id())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("ommerHash", inner.uncles_hash.into()),
            (
                "transactions",
                (inner
                    .transactions
                    .iter()
                    .map(move |transaction| transaction.to_entity_id())
                    .collect::<Vec<_>>()
                    .into()),
            ),
            ("size", inner.size.into()),
            ("sealFields", inner.seal_fields.clone().into()),
        ] as Vec<(_, Value)>))
    }
}

impl TryIntoEntity for &BlockWithOmmers {
    fn try_into_entity(self) -> Result<Entity, Error> {
        let inner = self.inner();
        let receipts = &self.block.transaction_receipts;

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
            ("isOmmer", false.into()),
            ("ommerCount", (self.ommers.len() as i32).into()),
            (
                "ommers",
                self.inner()
                    .uncles
                    .iter()
                    .map(|hash| hash.to_entity_id())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("ommerHash", inner.uncles_hash.into()),
            (
                "transactions",
                (inner
                    .transactions
                    .iter()
                    .map(move |transaction| transaction.hash.to_entity_id())
                    .collect::<Vec<_>>()
                    .into()),
            ),
            (
                "logs",
                (receipts
                    .iter()
                    .flat_map(|receipt| receipt.logs.iter())
                    .map(|log| format!("{:x}-{}", log.block_hash.unwrap(), log.log_index.unwrap()))
                    .collect::<Vec<_>>()
                    .into()),
            ),
            ("size", inner.size.into()),
            ("sealFields", inner.seal_fields.clone().into()),
        ] as Vec<(_, Value)>))
    }
}

impl TryIntoEntity for &LoadedTransaction {
    fn try_into_entity(self) -> Result<Entity, Error> {
        let transaction = &self.transaction;
        let receipt = &self.receipt;

        Ok(Entity::from(vec![
            ("id", transaction.hash.to_entity_id().into()),
            ("hash", transaction.hash.into()),
            ("nonce", transaction.nonce.into()),
            ("index", transaction.transaction_index.unwrap().into()),
            ("from", transaction.from.into()),
            ("to", transaction.to.map_or(Value::Null, |to| to.into())),
            ("value", transaction.value.into()),
            ("gasPrice", transaction.gas_price.into()),
            ("gas", transaction.gas.into()),
            ("inputData", transaction.input.clone().into()),
            (
                "block",
                format!("{:x}", transaction.block_hash.unwrap()).into(),
            ),
            (
                "logs",
                receipt
                    .logs
                    .iter()
                    .cloned()
                    .map(|log| Log::from(log).to_entity_id())
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("status", receipt.status.into()),
            ("gasUsed", receipt.gas_used.into()),
            ("cumulativeGasUsed", receipt.cumulative_gas_used.into()),
            (
                "createdContract",
                receipt
                    .contract_address
                    .map_or(Value::Null, |address| address.into()),
            ),
        ] as Vec<(_, Value)>))
    }
}

impl TryIntoEntity for &Log {
    fn try_into_entity(self) -> Result<Entity, Error> {
        let inner = &self.0;

        Ok(Entity::from(vec![
            (
                "id",
                format!(
                    "{:x}-{}",
                    inner.block_hash.unwrap(),
                    inner.log_index.unwrap()
                )
                .into(),
            ),
            ("index", inner.log_index.unwrap().into()),
            ("account", inner.address.into()),
            (
                "topics",
                inner
                    .topics
                    .iter()
                    .cloned()
                    .map(move |topic| topic)
                    .collect::<Vec<_>>()
                    .into(),
            ),
            ("data", inner.data.clone().into()),
            (
                "transaction",
                inner
                    .transaction_hash
                    .map_or(Value::Null, |index| index.to_entity_id().into()),
            ),
        ] as Vec<(_, Value)>))
    }
}
