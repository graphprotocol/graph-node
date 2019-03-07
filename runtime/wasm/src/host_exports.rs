use crate::EventHandlerContext;
use crate::UnresolvedContractCall;
use ethabi::Token;
use futures::sync::oneshot;
use graph::components::ethereum::*;
use graph::components::store::EntityKey;
use graph::data::store::scalar;
use graph::prelude::*;
use graph::serde_json;
use graph::web3::types::H160;
use semver::Version;
use std::collections::HashMap;
use std::fmt;
use std::ops::Deref;
use std::str::FromStr;
use std::time::{Duration, Instant};

pub(crate) const TIMEOUT_ENV_VAR: &str = "GRAPH_EVENT_HANDLER_TIMEOUT";

pub(crate) trait ExportError: fmt::Debug + fmt::Display + Send + Sync + 'static {}
impl<E> ExportError for E where E: fmt::Debug + fmt::Display + Send + Sync + 'static {}

/// Error raised in host functions.
#[derive(Debug)]
pub(crate) struct HostExportError<E>(pub(crate) E);

impl<E: fmt::Display> fmt::Display for HostExportError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) struct HostExports<E, L, S, U> {
    subgraph_id: SubgraphDeploymentId,
    pub api_version: Version,
    abis: Vec<MappingABI>,
    ethereum_adapter: Arc<E>,
    link_resolver: Arc<L>,
    store: Arc<S>,
    task_sink: U,
}

impl<E, L, S, U> HostExports<E, L, S, U>
where
    E: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone,
{
    pub(crate) fn new(
        subgraph_id: SubgraphDeploymentId,
        api_version: Version,
        abis: Vec<MappingABI>,
        ethereum_adapter: Arc<E>,
        link_resolver: Arc<L>,
        store: Arc<S>,
        task_sink: U,
    ) -> Self {
        HostExports {
            subgraph_id,
            api_version,
            abis,
            ethereum_adapter,
            link_resolver,
            store,
            task_sink,
        }
    }

    pub(crate) fn abort(
        &self,
        message: Option<String>,
        file_name: Option<String>,
        line_number: Option<u32>,
        column_number: Option<u32>,
    ) -> Result<(), HostExportError<impl ExportError>> {
        let message = message
            .map(|message| format!("message: {}", message))
            .unwrap_or_else(|| "no message".into());
        let location = match (file_name, line_number, column_number) {
            (None, None, None) => "an unknown location".into(),
            (Some(file_name), None, None) => file_name,
            (Some(file_name), Some(line_number), None) => {
                format!("{}, line {}", file_name, line_number)
            }
            (Some(file_name), Some(line_number), Some(column_number)) => format!(
                "{}, line {}, column {}",
                file_name, line_number, column_number
            ),
            _ => unreachable!(),
        };
        Err(HostExportError(format!(
            "Mapping aborted at {}, with {}",
            location, message
        )))
    }

    pub(crate) fn store_set(
        &self,
        ctx: &mut EventHandlerContext,
        entity_type: String,
        entity_id: String,
        mut data: HashMap<String, Value>,
    ) -> Result<(), HostExportError<impl ExportError>> {
        // Automatically add an "id" value
        match data.insert("id".to_string(), Value::String(entity_id.clone())) {
            Some(ref v) if v != &Value::String(entity_id.clone()) => {
                return Err(HostExportError(format!(
                    "Value of {} attribute 'id' conflicts with ID passed to `store.set()`: \
                     {} != {}",
                    entity_type, v, entity_id,
                )));
            }
            _ => (),
        }

        ctx.entity_operations.push(EntityOperation::Set {
            key: EntityKey {
                subgraph_id: self.subgraph_id.clone(),
                entity_type,
                entity_id,
            },
            data: Entity::from(data),
        });

        Ok(())
    }

    pub(crate) fn store_remove(
        &self,
        ctx: &mut EventHandlerContext,
        entity_type: String,
        entity_id: String,
    ) {
        ctx.entity_operations.push(EntityOperation::Remove {
            key: EntityKey {
                subgraph_id: self.subgraph_id.clone(),
                entity_type,
                entity_id,
            },
        });
    }

    pub(crate) fn store_get(
        &self,
        ctx: &EventHandlerContext,
        entity_type: String,
        entity_id: String,
    ) -> Result<Option<Entity>, HostExportError<impl ExportError>> {
        let store_key = EntityKey {
            subgraph_id: self.subgraph_id.clone(),
            entity_type,
            entity_id,
        };

        // Get all operations for this entity
        let matching_operations: Vec<_> = ctx
            .entity_operations
            .iter()
            .filter(|op| op.matches_entity(&store_key))
            .collect();

        // Shortcut 1: If the latest operation for this entity was a removal,
        // return 0 (= null) to the runtime
        if matching_operations
            .iter()
            .peekable()
            .peek()
            .map(|op| op.is_remove())
            .unwrap_or(false)
        {
            return Ok(None);
        }

        // Shortcut 2: If there is a removal in the operations, the
        // entity will be the result of the operations after that, so we
        // don't have to hit the store for anything
        if matching_operations.iter().any(|op| op.is_remove()) {
            return EntityOperation::apply_all(None, &matching_operations)
                .map_err(QueryExecutionError::StoreError)
                .map_err(HostExportError);
        }

        // No removal in the operations => read the entity from the store, then apply
        // the operations to it to obtain the result
        self.store
            .get(store_key)
            .and_then(|entity| {
                EntityOperation::apply_all(entity, &matching_operations)
                    .map_err(QueryExecutionError::StoreError)
            })
            .map_err(HostExportError)
    }

    pub(crate) fn ethereum_call(
        &self,
        ctx: &EventHandlerContext,
        unresolved_call: UnresolvedContractCall,
    ) -> Result<Vec<Token>, HostExportError<impl ExportError>> {
        debug!(ctx.logger, "Call smart contract";
              "address" => &unresolved_call.contract_address.to_string(),
              "contract" => &unresolved_call.contract_name,
              "function" => &unresolved_call.function_name);

        // Obtain the path to the contract ABI
        let contract = self
            .abis
            .iter()
            .find(|abi| abi.name == unresolved_call.contract_name)
            .ok_or_else(|| {
                HostExportError(format!(
                    "Could not find ABI for contract \"{}\", try adding it to the 'abis' section \
                     of the subgraph manifest",
                    unresolved_call.contract_name
                ))
            })?
            .contract
            .clone();

        let function = contract
            .function(unresolved_call.function_name.as_str())
            .map_err(|e| {
                HostExportError(format!(
                    "Unknown function \"{}::{}\" called from WASM runtime: {}",
                    unresolved_call.contract_name, unresolved_call.function_name, e
                ))
            })?;

        let call = EthereumContractCall {
            address: unresolved_call.contract_address,
            block_ptr: ctx.block.as_ref().deref().into(),
            function: function.clone(),
            args: unresolved_call.function_args.clone(),
        };

        // Run Ethereum call in tokio runtime
        let eth_adapter = self.ethereum_adapter.clone();
        let logger = ctx.logger.clone();
        self.block_on(future::lazy(move || {
            eth_adapter.contract_call(&logger, call).map_err(move |e| {
                HostExportError(format!(
                    "Failed to call function \"{}\" of contract \"{}\": {}",
                    unresolved_call.function_name, unresolved_call.contract_name, e
                ))
            })
        }))
    }

    pub(crate) fn bytes_to_string(
        &self,
        bytes: Vec<u8>,
    ) -> Result<String, HostExportError<impl ExportError>> {
        let s = String::from_utf8(bytes).map_err(HostExportError)?;
        // The string may have been encoded in a fixed length
        // buffer and padded with null characters, so trim
        // trailing nulls.
        Ok(s.trim_end_matches('\u{0000}').to_string())
    }

    /// Converts bytes to a hex string.
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    pub(crate) fn bytes_to_hex(&self, bytes: Vec<u8>) -> String {
        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        format!("0x{}", ::hex::encode(bytes))
    }

    pub(crate) fn big_int_to_string(&self, n: BigInt) -> String {
        format!("{}", n)
    }

    /// Prints the module of `n` in hex.
    /// Integers are encoded using the least amount of digits (no leading zero digits).
    /// Their encoding may be of uneven length. The number zero encodes as "0x0".
    ///
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    pub(crate) fn big_int_to_hex(&self, n: BigInt) -> String {
        if n == 0.into() {
            return "0x0".to_string();
        }

        let bytes = n.to_bytes_be().1;
        format!("0x{}", ::hex::encode(bytes).trim_start_matches('0'))
    }

    pub(crate) fn big_int_to_i32(
        &self,
        n: BigInt,
    ) -> Result<i32, HostExportError<impl ExportError>> {
        if n >= i32::min_value().into() && n <= i32::max_value().into() {
            let n_bytes = n.to_signed_bytes_le();
            let mut i_bytes: [u8; 4] = if n < 0.into() { [255; 4] } else { [0; 4] };
            i_bytes[..n_bytes.len()].copy_from_slice(&n_bytes);
            let i = i32::from_le_bytes(i_bytes);
            Ok(i)
        } else {
            Err(HostExportError(format!(
                "BigInt value does not fit into i32: {}",
                n
            )))
        }
    }

    pub(crate) fn json_from_bytes(
        &self,
        bytes: Vec<u8>,
    ) -> Result<serde_json::Value, HostExportError<impl ExportError>> {
        serde_json::from_reader(&*bytes).map_err(HostExportError)
    }

    pub(crate) fn ipfs_cat(
        &self,
        link: String,
    ) -> Result<Vec<u8>, HostExportError<impl ExportError>> {
        self.block_on(
            self.link_resolver
                .cat(&Link { link })
                .map_err(HostExportError),
        )
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_i64(
        &self,
        json: String,
    ) -> Result<i64, HostExportError<impl ExportError>> {
        i64::from_str(&json)
            .map_err(|_| HostExportError(format!("JSON `{}` cannot be parsed as i64", json)))
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_u64(
        &self,
        json: String,
    ) -> Result<u64, HostExportError<impl ExportError>> {
        u64::from_str(&json)
            .map_err(|_| HostExportError(format!("JSON `{}` cannot be parsed as u64", json)))
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_f64(
        &self,
        json: String,
    ) -> Result<f64, HostExportError<impl ExportError>> {
        f64::from_str(&json)
            .map_err(|_| HostExportError(format!("JSON `{}` cannot be parsed as f64", json)))
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_big_int(
        &self,
        json: String,
    ) -> Result<Vec<u8>, HostExportError<impl ExportError>> {
        let big_int = scalar::BigInt::from_str(&json)
            .map_err(|_| HostExportError(format!("JSON `{}` is not a decimal string", json)))?;
        Ok(big_int.to_signed_bytes_le())
    }

    pub(crate) fn crypto_keccak_256(&self, input: Vec<u8>) -> [u8; 32] {
        ::tiny_keccak::keccak256(&input)
    }

    pub(crate) fn big_int_plus(&self, x: BigInt, y: BigInt) -> BigInt {
        x + y
    }

    pub(crate) fn big_int_minus(&self, x: BigInt, y: BigInt) -> BigInt {
        x - y
    }

    pub(crate) fn big_int_times(&self, x: BigInt, y: BigInt) -> BigInt {
        x * y
    }

    pub(crate) fn big_int_divided_by(&self, x: BigInt, y: BigInt) -> BigInt {
        x / y
    }

    pub(crate) fn big_int_mod(&self, x: BigInt, y: BigInt) -> BigInt {
        x % y
    }

    pub(crate) fn block_on<I: Send + 'static, ER: Send + 'static>(
        &self,
        future: impl Future<Item = I, Error = ER> + Send + 'static,
    ) -> Result<I, ER> {
        let (return_sender, return_receiver) = oneshot::channel();
        self.task_sink
            .clone()
            .send(Box::new(future.then(|res| {
                return_sender.send(res).map_err(|_| unreachable!())
            })))
            .wait()
            .map_err(|_| panic!("task receiver dropped"))
            .unwrap();
        return_receiver.wait().expect("`return_sender` dropped")
    }

    pub(crate) fn check_timeout(
        &self,
        start_time: Instant,
    ) -> Result<(), HostExportError<impl ExportError>> {
        let event_handler_timeout = std::env::var(TIMEOUT_ENV_VAR)
            .ok()
            .and_then(|s| u64::from_str(&s).ok())
            .map(Duration::from_secs);
        if let Some(timeout) = event_handler_timeout {
            if start_time.elapsed() > timeout {
                return Err(HostExportError(format!("Event handler timed out")));
            }
        }
        Ok(())
    }

    /// Useful for IPFS hashes stored as bytes
    pub(crate) fn bytes_to_base58(&self, bytes: Vec<u8>) -> String {
        ::bs58::encode(&bytes).into_string()
    }
}

pub(crate) fn string_to_h160(string: &str) -> Result<H160, HostExportError<impl ExportError>> {
    // `H160::from_str` takes a hex string with no leading `0x`.
    let string = string.trim_start_matches("0x");
    H160::from_str(string)
        .map_err(|e| HostExportError(format!("Failed to convert string to Address/H160: {}", e)))
}

#[test]
fn test_string_to_h160_with_0x() {
    assert_eq!(
        H160::from_str("A16081F360e3847006dB660bae1c6d1b2e17eC2A").unwrap(),
        string_to_h160("0xA16081F360e3847006dB660bae1c6d1b2e17eC2A").unwrap()
    )
}
