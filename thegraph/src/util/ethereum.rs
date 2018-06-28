use ethabi::{Contract, Event};
use ethereum_types::H256;
use tiny_keccak::sha3_256;

/// Hashes a string to a H256 hash.
fn string_to_h256(string: &str) -> H256 {
    let bytes = string.as_bytes();
    let hash = sha3_256(bytes);
    H256::from_slice(&hash[0..32])
}

/// Returns the contract event with the given signature, if it exists.
pub fn contract_event_with_signature<'a>(
    contract: &'a Contract,
    signature: &str,
) -> Option<&'a Event> {
    contract
        .events()
        .filter(|event| event.signature() == string_to_h256(signature))
        .next()
}
