/// Type conversion utilities between web3 and alloy types
use crate::prelude::web3::types::U256;

// u256 to web3 U256
pub fn alloy_u256_to_web3_u256(_u: alloy::primitives::U256) -> U256 {
    unimplemented!();
}

#[macro_export]
macro_rules! alloy_todo {
    () => {
        todo!()
    };
}
