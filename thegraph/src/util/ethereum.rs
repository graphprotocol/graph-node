use ethabi::{Contract, Event};
use ethereum_types::H256;
use tiny_keccak::sha3_256;

fn string_to_H256(string: &str) -> H256 {
    let bytes = string.as_bytes();
    let hash = sha3_256(bytes);
    H256::from_slice(&hash[0..32])
}

// Checks if an ethabi Contract contains an event with a particular signature, and returns
// that event, if present.
pub fn get_contract_event_by_signature(contract: &Contract, signature: &str) -> Option<Event> {
    contract
        .events()
        .filter(|event| event.signature() == string_to_H256(signature))
        .next()
        .map(|event| event.clone())
}
