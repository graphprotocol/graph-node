use ethabi::{Contract, Function};
use tiny_keccak::Keccak;
use web3::types::H256;

/// Hashes a string to a H256 hash.
pub fn string_to_h256(s: &str) -> H256 {
    let mut result = [0u8; 32];
    let data = s.replace(" ", "").into_bytes();
    let mut sponge = Keccak::new_keccak256();
    sponge.update(&data);
    sponge.finalize(&mut result);

    // This was deprecated but the replacement seems to not be available in the
    // version web3 uses.
    #[allow(deprecated)]
    H256::from_slice(&result)
}

pub fn contract_function_with_signature<'a>(
    contract: &'a Contract,
    target_signature: &str,
) -> Option<&'a Function> {
    contract
        .functions()
        .filter(|function| match function.state_mutability {
            ethabi::StateMutability::Payable | ethabi::StateMutability::NonPayable => true,
            ethabi::StateMutability::Pure | ethabi::StateMutability::View => false,
        })
        .find(|function| {
            // Construct the argument function signature:
            // `address,uint256,bool`
            let mut arguments = function
                .inputs
                .iter()
                .map(|input| format!("{}", input.kind))
                .collect::<Vec<String>>()
                .join(",");
            // `address,uint256,bool)
            arguments.push_str(")");
            // `operation(address,uint256,bool)`
            let actual_signature = vec![function.name.clone(), arguments].join("(");
            target_signature == actual_signature
        })
}
