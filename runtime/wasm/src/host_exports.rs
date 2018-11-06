use ethabi::Token;
use futures::sync::oneshot;
use graph::components::ethereum::*;
use graph::components::store::StoreKey;
use graph::data::store::scalar;
use graph::data::subgraph::DataSource;
use graph::prelude::*;
use graph::serde_json;
use graph::web3::types::H160;
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
        subgraph: SubgraphManifest,
        data_source: DataSource,
        ethereum_adapter: Arc<E>,
        link_resolver: Arc<L>,
        store: Arc<S>,
        task_sink: U,
        ctx: Option<EventHandlerContext>,
    ) -> Self {
        HostExports {
            subgraph,
            data_source,
            ethereum_adapter,
            link_resolver,
            store,
            task_sink,
            ctx,
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
        let ctx = self.ctx.as_ref().expect("processing event without context");

        debug!(ctx.logger, "Call smart contract";
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
                    "Could not find ABI for contract \"{}\", try adding it to the 'abis' section of the subgraph manifest",
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
        Ok(s.trim_right_matches('\u{0000}').to_string())
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
        format!("0x{}", ::hex::encode(bytes).trim_left_matches('0'))
    }

    pub(crate) fn string_to_h160(
        &self,
        string: &str,
    ) -> Result<H160, HostExportError<impl ExportError>> {
        H160::from_str(string).map_err(|e| {
            HostExportError(format!("Failed to convert string to Address/H160: {}", e))
        })
    }

    pub(crate) fn big_int_to_i32(
        &self,
        n: BigInt,
    ) -> Result<i32, HostExportError<impl ExportError>> {
        if n >= i32::min_value().into() && n <= i32::max_value().into() {
            let n_bytes = n.to_signed_bytes_le();
            let mut i_bytes: [u8; 4] = if n < 0.into() { [255; 4] } else { [0; 4] };
            i_bytes[..n_bytes.len()].copy_from_slice(&n_bytes);
            let i: i32 = unsafe { mem::transmute(i_bytes) };
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
