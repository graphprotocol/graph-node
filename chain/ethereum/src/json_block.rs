use graph::prelude::serde_json::{self as json, Value};
use graph::prelude::{EthereumBlock, LightEthereumBlock};

use crate::json_patch;

#[derive(Debug)]
pub struct EthereumJsonBlock(Value);

impl EthereumJsonBlock {
    pub fn new(value: Value) -> Self {
        Self(value)
    }

    /// Returns true if this is a shallow/header-only block (no full block data).
    pub fn is_shallow(&self) -> bool {
        self.0.get("data") == Some(&Value::Null)
    }

    /// Returns true if this block is in the legacy format (direct block JSON
    /// rather than wrapped in a `block` field).
    pub fn is_legacy_format(&self) -> bool {
        self.0.get("block").is_none()
    }

    /// Patches missing `type` fields in transactions and receipts.
    /// Required for alloy compatibility with cached blocks from older graph-node versions.
    pub fn patch(&mut self) {
        if let Some(block) = self.0.get_mut("block") {
            json_patch::patch_block_transactions(block);
        }
        if let Some(receipts) = self.0.get_mut("transaction_receipts") {
            json_patch::patch_receipts(receipts);
        }
    }

    /// Patches and deserializes into a full `EthereumBlock` with receipts.
    pub fn into_full_block(mut self) -> Result<EthereumBlock, json::Error> {
        self.patch();
        json::from_value(self.0)
    }

    /// Extracts and patches the inner block, deserializing into a `LightEthereumBlock`.
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
