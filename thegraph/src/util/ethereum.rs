use ethabi::{Contract, Event};
use ethereum_types::H256;
use tiny_keccak::Keccak;

/// Hashes a string to a H256 hash.
pub fn string_to_h256(s: &str) -> H256 {
    let mut result = [0u8; 32];

    let data: Vec<u8> = From::from(s);
    let mut sponge = Keccak::new_keccak256();
    sponge.update(&data);
    sponge.finalize(&mut result);

    H256::from_slice(&result)
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
