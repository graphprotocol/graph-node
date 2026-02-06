use graph::prelude::serde_json::{self as json, Value};
use graph::prelude::{EthereumBlock, LightEthereumBlock};

use crate::json_patch;

#[derive(Debug)]
pub struct EthereumJsonBlock(Value);

impl EthereumJsonBlock {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    pub fn is_shallow(&self) -> bool {
        self.0.get("data") == Some(&Value::Null)
    }

    pub fn is_legacy_format(&self) -> bool {
        self.0.get("block").is_none()
    }

    pub fn patch(&mut self) {
        if let Some(block) = self.0.get_mut("block") {
            json_patch::patch_block_transactions(block);
        }
        if let Some(receipts) = self.0.get_mut("transaction_receipts") {
            json_patch::patch_receipts(receipts);
        }
    }

    pub fn into_full_block(mut self) -> Result<EthereumBlock, json::Error> {
        self.patch();
        json::from_value(self.0)
    }

    pub fn into_light_block(mut self) -> Result<LightEthereumBlock, json::Error> {
        let mut inner = self
            .0
            .as_object_mut()
            .and_then(|obj| obj.remove("block"))
            .unwrap_or(self.0);
        json_patch::patch_block_transactions(&mut inner);
        json::from_value(inner)
    }
}

impl From<Value> for EthereumJsonBlock {
    fn from(value: Value) -> Self {
        Self::new(value)
    }
}
