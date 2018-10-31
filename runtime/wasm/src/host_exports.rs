use ethabi::Token;
use futures::sync::oneshot;
use graph::components::ethereum::*;
use graph::components::store::StoreKey;
use graph::data::store::scalar;
use graph::data::subgraph::DataSource;
use graph::prelude::*;
use graph::serde_json;
use graph::web3::types::{H160, U256};
use std::collections::HashMap;
use std::fmt;
use std::mem;
use std::ops::Deref;
use std::str::FromStr;
use EventHandlerContext;
use UnresolvedContractCall;

pub(crate) trait ExportError: fmt::Debug + fmt::Display + Send + Sync + 'static {}
impl<E> ExportError for E where E: fmt::Debug + fmt::Display + Send + Sync + 'static {}

/// Error raised in host functions.
#[derive(Debug)]
pub(crate) struct HostExportError<E>(E);

impl<E: fmt::Display> fmt::Display for HostExportError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

pub(crate) struct HostExports<E, L, S, U> {
    logger: Logger,
    subgraph: SubgraphManifest,
    data_source: DataSource,
    ethereum_adapter: Arc<E>,
    link_resolver: Arc<L>,
    store: Arc<S>,
    task_sink: U,
    pub(crate) ctx: Option<EventHandlerContext>,
}

impl<E, L, S, U> HostExports<E, L, S, U>
where
    E: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone,
{
    pub(crate) fn new(
        logger: Logger,
        subgraph: SubgraphManifest,
        data_source: DataSource,
        ethereum_adapter: Arc<E>,
        link_resolver: Arc<L>,
        store: Arc<S>,
        task_sink: U,
        ctx: Option<EventHandlerContext>,
    ) -> Self {
        HostExports {
            logger,
            subgraph,
            data_source,
            ethereum_adapter,
            link_resolver,
            store,
            task_sink,
            ctx,
        }
    }

    pub(crate) fn store_set(
        &mut self,
        entity: String,
        id: String,
        mut data: HashMap<String, Value>,
    ) -> Result<(), HostExportError<impl ExportError>> {
        match data.insert("id".to_string(), Value::String(id.clone())) {
            Some(ref v) if v != &Value::String(id.clone()) => {
                return Err(HostExportError(format!(
                    "Conflicting 'id' value set by mapping for {} entity: {} != {}",
                    entity, v, id,
                )))
            }
            _ => (),
        }

        self.ctx
            .as_mut()
            .map(|ctx| &mut ctx.entity_operations)
            .expect("processing event without context")
            .push(EntityOperation::Set {
                subgraph: self.subgraph.id.clone(),
                entity,
                id,
                data: Entity::from(data),
            });

        Ok(())
    }

    pub(crate) fn store_remove(&mut self, entity: String, id: String) {
        self.ctx
            .as_mut()
            .map(|ctx| &mut ctx.entity_operations)
            .expect("processing event without context")
            .push(EntityOperation::Remove {
                subgraph: self.subgraph.id.clone(),
                entity,
                id,
            });
    }

    pub(crate) fn store_get(
        &self,
        entity: String,
        id: String,
    ) -> Result<Option<Entity>, HostExportError<impl ExportError>> {
        let store_key = StoreKey {
            subgraph: self.subgraph.id.clone(),
            entity,
            id,
        };

        // Get all operations for this entity
        let matching_operations: Vec<_> = self
            .ctx
            .as_ref()
            .map(|ctx| &ctx.entity_operations)
            .expect("processing event without context")
            .clone()
            .iter()
            .cloned()
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
            return Ok(EntityOperation::apply_all(None, &matching_operations));
        }

        // No removal in the operations => read the entity from the store, then apply
        // the operations to it to obtain the result
        Ok(self
            .store
            .get(store_key)
            .map(|entity| EntityOperation::apply_all(entity, &matching_operations))
            .map_err(HostExportError)?)
    }

    pub(crate) fn ethereum_call(
        &self,
        unresolved_call: UnresolvedContractCall,
    ) -> Result<Vec<Token>, HostExportError<impl ExportError>> {
        info!(self.logger, "Call smart contract";
              "address" => &unresolved_call.contract_address.to_string(),
              "contract" => &unresolved_call.contract_name,
              "function" => &unresolved_call.function_name);

        // Obtain the path to the contract ABI
        let contract = self
            .data_source
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == unresolved_call.contract_name)
            .ok_or_else(|| {
                HostExportError(format!(
                    "Unknown contract \"{}\" called from WASM runtime",
                    unresolved_call.contract_name
                ))
            })?.contract
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
            block_ptr: self
                .ctx
                .as_ref()
                .map(|ctx| ctx.block.as_ref())
                .expect("processing event without context")
                .deref()
                .into(),
            function: function.clone(),
            args: unresolved_call.function_args.clone(),
        };

        // Run Ethereum call in tokio runtime
        let eth_adapter = self.ethereum_adapter.clone();
        self.block_on(future::lazy(move || {
            eth_adapter.contract_call(call).map_err(move |e| {
                HostExportError(format!(
                    "Failed to call function \"{}\" of contract \"{}\": {}",
                    unresolved_call.function_name, unresolved_call.contract_name, e
                ))
            })
        }))
    }

    pub(crate) fn convert_bytes_to_string(
        &self,
        bytes: Vec<u8>,
    ) -> Result<String, HostExportError<impl ExportError>> {
        let s = String::from_utf8(bytes).map_err(HostExportError)?;
        // The string may have been encoded in a fixed length
        // buffer and padded with null characters, so trim
        // trailing nulls.
        Ok(s.trim_right_matches('\u{0000}').to_string())
    }

    pub(crate) fn u64_array_to_string(
        &self,
        u64_array: Vec<u64>,
    ) -> Result<String, HostExportError<impl ExportError>> {
        let mut bytes: Vec<u8> = Vec::new();
        for x in u64_array {
            // This is just `x.to_bytes()` which is unstable.
            let x_bytes: [u8; 8] = unsafe { mem::transmute(x) };
            bytes.extend(x_bytes.iter());
        }
        self.convert_bytes_to_string(bytes)
    }

    pub(crate) fn u64_array_to_hex(&self, u64_array: Vec<u64>) -> String {
        let mut bytes: Vec<u8> = Vec::new();
        for x in u64_array {
            // This is just `x.to_bytes()` which is unstable.
            let x_bytes: [u8; 8] = unsafe { mem::transmute(x) };
            bytes.extend(x_bytes.iter());
        }

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        format!("0x{}", ::hex::encode(bytes))
    }

    pub(crate) fn string_to_h160(
        &self,
        string: &str,
    ) -> Result<H160, HostExportError<impl ExportError>> {
        H160::from_str(string).map_err(|e| {
            HostExportError(format!("Failed to convert string to Address/H160: {}", e))
        })
    }

    /// This works for both U256 and I256.
    pub(crate) fn int256_to_big_int(&self, int256: U256) -> [u8; 32] {
        let mut buffer = [0; 32];
        int256.to_little_endian(&mut buffer);
        buffer
    }

    /// FIXME: This should be able to handle size up to 32, rather
    /// than only exactly 32.
    pub(crate) fn big_int_to_int256(
        &self,
        big_int: Vec<u8>,
    ) -> Result<U256, HostExportError<impl ExportError>> {
        let size = big_int.len();
        if size != 32 {
            Err(HostExportError(format!(
                "expected byte array of size 32, found size {}",
                size
            )))
        } else {
            Ok(U256::from_little_endian(&big_int))
        }
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

    pub(crate) fn i64_to_u256(&self, x: i64) -> U256 {
        // Sign extension: pad with 0s or 1s depending on sign of x.
        let mut bytes = if x > 0 { [0; 32] } else { [255; 32] };

        // This is just `x.to_bytes()` which is unstable.
        let x_bytes: [u8; 8] = unsafe { mem::transmute(x) };
        bytes[..8].copy_from_slice(&x_bytes);
        U256::from_little_endian(&bytes)
    }

    pub(crate) fn u256_to_u8(&self, x: U256) -> Result<u8, HostExportError<impl ExportError>> {
        Ok(u256_as_u64(x, u8::max_value().into(), "u8")? as u8)
    }

    pub(crate) fn u256_to_u16(&self, x: U256) -> Result<u16, HostExportError<impl ExportError>> {
        Ok(u256_as_u64(x, u16::max_value().into(), "u16")? as u16)
    }

    pub(crate) fn u256_to_u32(&self, x: U256) -> Result<u32, HostExportError<impl ExportError>> {
        Ok(u256_as_u64(x, u32::max_value().into(), "u32")? as u32)
    }

    pub(crate) fn u256_to_u64(&self, x: U256) -> Result<u64, HostExportError<impl ExportError>> {
        u256_as_u64(x, u64::max_value(), "u64")
    }

    pub(crate) fn u256_to_i8(&self, x: U256) -> Result<i8, HostExportError<impl ExportError>> {
        let bytes = u256_checked_bytes(x, i8::min_value().into(), i8::max_value().into(), "i8")?;

        // This is just `i8::from_bytes` which is unstable.
        let value: i8 = unsafe { mem::transmute(bytes[0]) };
        Ok(value)
    }

    pub(crate) fn u256_to_i16(&self, x: U256) -> Result<i16, HostExportError<impl ExportError>> {
        let bytes = u256_checked_bytes(x, i16::min_value().into(), i16::max_value().into(), "i16")?;

        // This is just `i16::from_bytes` which is unstable.
        let value: i16 = unsafe { mem::transmute([bytes[0], bytes[1]]) };
        Ok(value)
    }

    pub(crate) fn u256_to_i32(&self, x: U256) -> Result<i32, HostExportError<impl ExportError>> {
        let bytes = u256_checked_bytes(x, i32::min_value().into(), i32::max_value().into(), "i32")?;

        // This is just `i32::from_bytes` which is unstable.
        let value: i32 = unsafe { mem::transmute([bytes[0], bytes[1], bytes[2], bytes[3]]) };
        Ok(value)
    }

    pub(crate) fn u256_to_i64(&self, x: U256) -> Result<i64, HostExportError<impl ExportError>> {
        let bytes = u256_checked_bytes(x, i64::min_value(), i64::max_value(), "i64")?;

        // This is just `i64::from_bytes` which is unstable.
        let value: i64 = unsafe {
            mem::transmute([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
            ])
        };
        Ok(value)
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
            }))).wait()
            .map_err(|_| panic!("task receiver dropped"))
            .unwrap();
        return_receiver.wait().expect("`return_sender` dropped")
    }
}

/// Cast U256 to u64, checking that it respects `max_value`.
fn u256_as_u64(
    u256: U256,
    max_value: u64,
    n_name: &str,
) -> Result<u64, HostExportError<impl ExportError>> {
    // Check for overflow.
    let max_value = U256::from(max_value);
    if u256 > max_value {
        return Err(HostExportError(format!(
            "number `{}` is too large for an {}",
            u256, n_name
        )));
    }
    Ok(u64::from(u256))
}

/// Checks that `x >= min_value` and `x <= max_value`
/// and returns the byte representation of `x`.
pub(crate) fn u256_checked_bytes(
    u256: U256,
    min_value: i64,
    max_value: i64,
    n_name: &str,
) -> Result<[u8; 8], HostExportError<impl ExportError>> {
    // The number is negative if the most-significant bit is set.
    let is_negative = u256.bit(255);
    if !is_negative {
        // Check for overflow.
        let max_value = U256::from(max_value);
        if u256 > max_value {
            return Err(HostExportError(format!(
                "number `{}` is too large for an {}",
                u256, n_name
            )));
        }
    } else {
        // Check for underflow.
        // Do two's complement to get the absolute value.
        let u256_abs = !u256 + 1;

        // The absolute value of `T::min_value` is `T::max_value + 1` for a primitive `T`,
        // so we do a math trick to get around that, the `+ 1`s here cancel each other.
        let min_value_abs = U256::from((min_value + 1).abs()) + 1;
        if u256_abs > min_value_abs {
            return Err(HostExportError(format!(
                "number `-{}` is too small for an {}",
                u256_abs, n_name
            )));
        }
    }

    // This is just `u256.low_u64().to_bytes()` which is unstable.
    Ok(unsafe { mem::transmute(u256.low_u64()) })
}
