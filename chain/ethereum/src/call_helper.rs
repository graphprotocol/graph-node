use crate::{ContractCallError, ENV_VARS};
use graph::{
    abi,
    data::store::ethereum::call,
    prelude::{
        alloy::transports::{RpcError, TransportErrorKind},
        serde_json, Logger,
    },
    slog::info,
};

// ------------------------------------------------------------------
// Constants and helper utilities used across eth_call handling
// ------------------------------------------------------------------

// Try to check if the call was reverted. The JSON-RPC response for reverts is
// not standardized, so we have ad-hoc checks for each Ethereum client.

// 0xfe is the "designated bad instruction" of the EVM, and Solidity uses it for
// asserts.
const PARITY_BAD_INSTRUCTION_FE: &str = "Bad instruction fe";

// 0xfd is REVERT, but on some contracts, and only on older blocks,
// this happens. Makes sense to consider it a revert as well.
const PARITY_BAD_INSTRUCTION_FD: &str = "Bad instruction fd";

const PARITY_BAD_JUMP_PREFIX: &str = "Bad jump";
const PARITY_STACK_LIMIT_PREFIX: &str = "Out of stack";

// See f0af4ab0-6b7c-4b68-9141-5b79346a5f61.
const PARITY_OUT_OF_GAS: &str = "Out of gas";

// Also covers Nethermind reverts
const PARITY_VM_EXECUTION_ERROR: i64 = -32015;
const PARITY_REVERT_PREFIX: &str = "revert";

const XDAI_REVERT: &str = "revert";

// Deterministic Geth execution errors. We might need to expand this as
// subgraphs come across other errors. See
// https://github.com/ethereum/go-ethereum/blob/cd57d5cd38ef692de8fbedaa56598b4e9fbfbabc/core/vm/errors.go
const GETH_EXECUTION_ERRORS: &[&str] = &[
    // The "revert" substring covers a few known error messages, including:
    // Hardhat: "error: transaction reverted",
    // Ganache and Moonbeam: "vm exception while processing transaction: revert",
    // Geth: "execution reverted"
    // And others.
    "revert",
    "invalid jump destination",
    "invalid opcode",
    // Ethereum says 1024 is the stack sizes limit, so this is deterministic.
    "stack limit reached 1024",
    // See f0af4ab0-6b7c-4b68-9141-5b79346a5f61 for why the gas limit is considered deterministic.
    "out of gas",
    "stack underflow",
];

/// Helper that checks if a geth style RPC error message corresponds to a revert.
fn is_geth_revert_message(message: &str) -> bool {
    let env_geth_call_errors = ENV_VARS.geth_eth_call_errors.iter();
    let mut execution_errors = GETH_EXECUTION_ERRORS
        .iter()
        .copied()
        .chain(env_geth_call_errors.map(|s| s.as_str()));
    execution_errors.any(|e| message.to_lowercase().contains(e))
}

/// Decode a Solidity revert(reason) payload, returning the reason string when possible.
fn as_solidity_revert_reason(bytes: &[u8]) -> Option<String> {
    let selector = &tiny_keccak::keccak256(b"Error(string)")[..4];
    if bytes.len() >= 4 && &bytes[..4] == selector {
        abi::DynSolType::String
            .abi_decode(&bytes[4..])
            .ok()
            .and_then(|val| val.clone().as_str().map(ToOwned::to_owned))
    } else {
        None
    }
}

/// Interpret the error returned by `eth_call`, distinguishing genuine failures from
/// EVM reverts. Returns `Ok(Null)` for reverts or a proper error otherwise.
pub fn interpret_eth_call_error(
    logger: &Logger,
    err: RpcError<TransportErrorKind>,
) -> Result<call::Retval, ContractCallError> {
    fn reverted(logger: &Logger, reason: &str) -> Result<call::Retval, ContractCallError> {
        info!(logger, "Contract call reverted"; "reason" => reason);
        Ok(call::Retval::Null)
    }

    if let RpcError::ErrorResp(rpc_error) = &err {
        if is_geth_revert_message(&rpc_error.message) {
            return reverted(logger, &rpc_error.message);
        }
    }

    if let RpcError::ErrorResp(rpc_error) = &err {
        let code = rpc_error.code;
        let data: Option<String> = rpc_error
            .data
            .as_ref()
            .and_then(|d| serde_json::from_str(d.get()).ok());

        if code == PARITY_VM_EXECUTION_ERROR {
            if let Some(data) = data {
                if is_parity_revert(&data) {
                    return reverted(logger, &parity_revert_reason(&data));
                }
            }
        }
    }

    Err(ContractCallError::AlloyError(err))
}

fn is_parity_revert(data: &str) -> bool {
    data.to_lowercase().starts_with(PARITY_REVERT_PREFIX)
        || data.starts_with(PARITY_BAD_JUMP_PREFIX)
        || data.starts_with(PARITY_STACK_LIMIT_PREFIX)
        || data == PARITY_BAD_INSTRUCTION_FE
        || data == PARITY_BAD_INSTRUCTION_FD
        || data == PARITY_OUT_OF_GAS
        || data == XDAI_REVERT
}

/// Checks if the given data corresponds to a Parity / Nethermind style EVM
/// revert and, if so, tries to extract a human-readable revert reason. Returns `Some`
/// with the reason when the error is identified as a revert, otherwise `None`.
fn parity_revert_reason(data: &str) -> String {
    if data == PARITY_BAD_INSTRUCTION_FE {
        return PARITY_BAD_INSTRUCTION_FE.to_owned();
    }

    // Otherwise try to decode a Solidity revert reason payload.
    let payload = data.trim_start_matches(PARITY_REVERT_PREFIX);
    hex::decode(payload)
        .ok()
        .and_then(|decoded| as_solidity_revert_reason(&decoded))
        .unwrap_or_else(|| "no reason".to_owned())
}
