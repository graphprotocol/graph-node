use graph::prelude::web3::types::H160;

pub struct ContractIndex {}

impl ContractIndex {
    // Returns the contracts that have address as parent. Will not include the parent itself.
    pub fn addresses_for(&self, address: H160) -> Option<Vec<H160>> {
        None
    }
}
