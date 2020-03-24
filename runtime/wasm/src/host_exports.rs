use crate::UnresolvedContractCall;
use bytes::Bytes;
use ethabi::{Address, Token};
use graph::components::arweave::ArweaveAdapter;
use graph::components::ethereum::*;
use graph::components::store::EntityKey;
use graph::components::subgraph::ProofOfIndexingEvent;
use graph::components::three_box::ThreeBoxAdapter;
use graph::data::store;
use graph::prelude::serde_json;
use graph::prelude::{slog::b, slog::record_static, *};
use semver::Version;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::time::{Duration, Instant};
use web3::types::H160;

use graph_graphql::prelude::validate_entity;

use crate::module::WasmiModule;

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

impl From<graph::prelude::Error> for HostExportError<String> {
    fn from(e: graph::prelude::Error) -> Self {
        HostExportError(e.to_string())
    }
}

pub(crate) struct HostExports {
    subgraph_id: SubgraphDeploymentId,
    pub(crate) api_version: Version,
    data_source_name: String,
    data_source_address: Option<Address>,
    data_source_network: String,
    data_source_context: Option<DataSourceContext>,
    /// Some data sources have indeterminism or different notions of time. These
    /// need to be each be stored separately to separate causality between them,
    /// and merge the results later. Right now, this is just the ethereum
    /// networks but will be expanded for ipfs and the availability chain.
    causality_region: String,
    templates: Arc<Vec<DataSourceTemplate>>,
    abis: Vec<MappingABI>,
    ethereum_adapter: Arc<dyn EthereumAdapter>,
    link_resolver: Arc<dyn LinkResolver>,
    call_cache: Arc<dyn EthereumCallCache>,
    store: Arc<dyn crate::RuntimeStore>,
    handler_timeout: Option<Duration>,
    arweave_adapter: Arc<dyn ArweaveAdapter>,
    three_box_adapter: Arc<dyn ThreeBoxAdapter>,
}

// Not meant to be useful, only to allow deriving.
impl std::fmt::Debug for HostExports {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> Result<(), std::fmt::Error> {
        write!(f, "HostExports",)
    }
}

impl HostExports {
    pub(crate) fn new(
        subgraph_id: SubgraphDeploymentId,
        api_version: Version,
        data_source_name: String,
        data_source_address: Option<Address>,
        data_source_network: String,
        data_source_context: Option<DataSourceContext>,
        templates: Arc<Vec<DataSourceTemplate>>,
        abis: Vec<MappingABI>,
        ethereum_adapter: Arc<dyn EthereumAdapter>,
        link_resolver: Arc<dyn LinkResolver>,
        store: Arc<dyn crate::RuntimeStore>,
        call_cache: Arc<dyn EthereumCallCache>,
        handler_timeout: Option<Duration>,
        arweave_adapter: Arc<dyn ArweaveAdapter>,
        three_box_adapter: Arc<dyn ThreeBoxAdapter>,
    ) -> Self {
        let causality_region = format!("ethereum/{}", data_source_network);

        Self {
            subgraph_id,
            api_version,
            data_source_name,
            data_source_address,
            data_source_network,
            data_source_context,
            causality_region,
            templates,
            abis,
            ethereum_adapter,
            link_resolver,
            call_cache,
            store,
            handler_timeout,
            arweave_adapter,
            three_box_adapter,
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
        state: &mut BlockState,
        entity_type: String,
        entity_id: String,
        mut data: HashMap<String, Value>,
    ) -> Result<(), HostExportError<impl ExportError>> {
        state.proof_of_indexing.write(
            &self.causality_region,
            &ProofOfIndexingEvent::SetEntity {
                entity_type: &entity_type,
                id: &entity_id,
                data: &data,
            },
        );

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

        let key = EntityKey {
            subgraph_id: self.subgraph_id.clone(),
            entity_type,
            entity_id,
        };
        let entity = Entity::from(data);
        let schema = self.store.input_schema(&self.subgraph_id)?;
        let is_valid = validate_entity(&schema.document, &key, &entity).is_ok();
        state.entity_cache.set(key.clone(), entity);

        // Validate the changes against the subgraph schema.
        // If the set of fields we have is already valid, avoid hitting the DB.
        if !is_valid && self.store.uses_relational_schema(&self.subgraph_id)? {
            let entity = state
                .entity_cache
                .get(self.store.as_ref(), &key)
                .map_err(|e| HostExportError(e.to_string()))?
                .expect("we just stored this entity");
            validate_entity(&schema.document, &key, &entity)?;
        }
        Ok(())
    }

    pub(crate) fn store_remove(
        &self,
        state: &mut BlockState,
        entity_type: String,
        entity_id: String,
    ) {
        state.proof_of_indexing.write(
            &self.causality_region,
            &ProofOfIndexingEvent::RemoveEntity {
                entity_type: &entity_type,
                id: &entity_id,
            },
        );
        let key = EntityKey {
            subgraph_id: self.subgraph_id.clone(),
            entity_type,
            entity_id,
        };
        state.entity_cache.remove(key);
    }

    pub(crate) fn store_get(
        &self,
        state: &mut BlockState,
        entity_type: String,
        entity_id: String,
    ) -> Result<Option<Entity>, HostExportError<impl ExportError>> {
        let store_key = EntityKey {
            subgraph_id: self.subgraph_id.clone(),
            entity_type: entity_type.clone(),
            entity_id: entity_id.clone(),
        };

        let result = state
            .entity_cache
            .get(self.store.as_ref(), &store_key)
            .map_err(HostExportError)
            .map(|ok| ok.to_owned());

        result
    }

    /// Returns `Ok(None)` if the call was reverted.
    pub(crate) fn ethereum_call(
        &self,
        logger: &Logger,
        block: &LightEthereumBlock,
        unresolved_call: UnresolvedContractCall,
    ) -> Result<Option<Vec<Token>>, HostExportError<impl ExportError>> {
        let start_time = Instant::now();

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

        let function = match unresolved_call.function_signature {
            // Behavior for apiVersion < 0.0.4: look up function by name; for overloaded
            // functions this always picks the same overloaded variant, which is incorrect
            // and may lead to encoding/decoding errors
            None => contract
                .function(unresolved_call.function_name.as_str())
                .map_err(|e| {
                    HostExportError(format!(
                        "Unknown function \"{}::{}\" called from WASM runtime: {}",
                        unresolved_call.contract_name, unresolved_call.function_name, e
                    ))
                })?,

            // Behavior for apiVersion >= 0.0.04: look up function by signature of
            // the form `functionName(uint256,string) returns (bytes32,string)`; this
            // correctly picks the correct variant of an overloaded function
            Some(ref function_signature) => contract
                .functions_by_name(unresolved_call.function_name.as_str())
                .map_err(|e| {
                    HostExportError(format!(
                        "Unknown function \"{}::{}\" called from WASM runtime: {}",
                        unresolved_call.contract_name, unresolved_call.function_name, e
                    ))
                })?
                .iter()
                .find(|f| function_signature == &f.signature())
                .ok_or_else(|| {
                    HostExportError(format!(
                        "Unknown function \"{}::{}\" with signature `{}` \
                         called from WASM runtime",
                        unresolved_call.contract_name,
                        unresolved_call.function_name,
                        function_signature,
                    ))
                })?,
        };

        let call = EthereumContractCall {
            address: unresolved_call.contract_address.clone(),
            block_ptr: block.into(),
            function: function.clone(),
            args: unresolved_call.function_args.clone(),
        };

        // Run Ethereum call in tokio runtime
        let eth_adapter = self.ethereum_adapter.clone();
        let logger1 = logger.clone();
        let call_cache = self.call_cache.clone();
        let result = match block_on(future::lazy(move || {
            eth_adapter.contract_call(&logger1, call, call_cache)
        })) {
            Ok(tokens) => Ok(Some(tokens)),
            Err(EthereumContractCallError::Revert(reason)) => {
                info!(logger, "Contract call reverted"; "reason" => reason);
                Ok(None)
            }
            Err(e) => Err(HostExportError(format!(
                "Failed to call function \"{}\" of contract \"{}\": {}",
                unresolved_call.function_name, unresolved_call.contract_name, e
            ))),
        };

        debug!(logger, "Contract call finished";
              "address" => &unresolved_call.contract_address.to_string(),
              "contract" => &unresolved_call.contract_name,
              "function" => &unresolved_call.function_name,
              "function_signature" => &unresolved_call.function_signature,
              "time" => format!("{}ms", start_time.elapsed().as_millis()));

        result
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
        bytes: &Vec<u8>,
    ) -> Result<serde_json::Value, serde_json::Error> {
        serde_json::from_reader(bytes.as_slice())
    }

    pub(crate) fn ipfs_cat(
        &self,
        logger: &Logger,
        link: String,
    ) -> Result<Vec<u8>, HostExportError<impl ExportError>> {
        block_on03(
            self.link_resolver
                .cat(logger, &Link { link })
                .map_err(HostExportError),
        )
    }

    // Read the IPFS file `link`, split it into JSON objects, and invoke the
    // exported function `callback` on each JSON object. The successful return
    // value contains the block state produced by each callback invocation. Each
    // invocation of `callback` happens in its own instance of a WASM module,
    // which is identical to `module` when it was first started. The signature
    // of the callback must be `callback(JSONValue, Value)`, and the `userData`
    // parameter is passed to the callback without any changes
    pub(crate) fn ipfs_map(
        &self,
        module: &WasmiModule,
        link: String,
        callback: &str,
        user_data: store::Value,
        flags: Vec<String>,
    ) -> Result<Vec<BlockState>, HostExportError<impl ExportError>> {
        const JSON_FLAG: &str = "json";
        if !flags.contains(&JSON_FLAG.to_string()) {
            return Err(HostExportError(format!("Flags must contain 'json'")));
        }

        let host_metrics = module.host_metrics.clone();
        let valid_module = module.valid_module.clone();
        let ctx = module.ctx.clone_with_empty_block_state();
        let callback = callback.to_owned();
        // Create a base error message to avoid borrowing headaches
        let errmsg = format!(
            "ipfs_map: callback '{}' failed when processing file '{}'",
            &*callback, &link
        );

        let start = Instant::now();
        let mut last_log = start;
        let logger = ctx.logger.new(o!("ipfs_map" => link.clone()));

        let result = block_on03(async move {
            let mut stream: JsonValueStream = self
                .link_resolver
                .json_stream(&logger, &Link { link })
                .await?;
            let mut v = Vec::new();
            while let Some(sv) = stream.next().await {
                let sv = sv?;
                let module = WasmiModule::from_valid_module_with_ctx(
                    valid_module.clone(),
                    ctx.clone_with_empty_block_state(),
                    host_metrics.clone(),
                )?;
                let result = module.handle_json_callback(&callback, &sv.value, &user_data)?;
                // Log progress every 15s
                if last_log.elapsed() > Duration::from_secs(15) {
                    debug!(
                        logger,
                        "Processed {} lines in {}s so far",
                        sv.line,
                        start.elapsed().as_secs()
                    );
                    last_log = Instant::now();
                }
                v.push(result)
            }
            Ok(v)
        });
        result.map_err(move |e: Error| HostExportError(format!("{}: {}", errmsg, e.to_string())))
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
        let big_int = BigInt::from_str(&json)
            .map_err(|_| HostExportError(format!("JSON `{}` is not a decimal string", json)))?;
        Ok(big_int.to_signed_bytes_le())
    }

    pub(crate) fn crypto_keccak_256(&self, input: Vec<u8>) -> [u8; 32] {
        tiny_keccak::keccak256(&input)
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

    pub(crate) fn big_int_divided_by(
        &self,
        x: BigInt,
        y: BigInt,
    ) -> Result<BigInt, HostExportError<impl ExportError>> {
        if y == 0.into() {
            return Err(HostExportError(format!(
                "attempted to divide BigInt `{}` by zero",
                x
            )));
        }
        Ok(x / y)
    }

    pub(crate) fn big_int_mod(&self, x: BigInt, y: BigInt) -> BigInt {
        x % y
    }

    /// Limited to a small exponent to avoid creating huge BigInts.
    pub(crate) fn big_int_pow(&self, x: BigInt, exponent: u8) -> BigInt {
        x.pow(exponent)
    }

    pub(crate) fn check_timeout(
        &self,
        start_time: Instant,
    ) -> Result<(), HostExportError<impl ExportError>> {
        if let Some(timeout) = self.handler_timeout {
            if start_time.elapsed() > timeout {
                return Err(HostExportError(format!("Mapping handler timed out")));
            }
        }
        Ok(())
    }

    /// Useful for IPFS hashes stored as bytes
    pub(crate) fn bytes_to_base58(&self, bytes: Vec<u8>) -> String {
        ::bs58::encode(&bytes).into_string()
    }

    pub(crate) fn big_decimal_plus(&self, x: BigDecimal, y: BigDecimal) -> BigDecimal {
        x + y
    }

    pub(crate) fn big_decimal_minus(&self, x: BigDecimal, y: BigDecimal) -> BigDecimal {
        x - y
    }

    pub(crate) fn big_decimal_times(&self, x: BigDecimal, y: BigDecimal) -> BigDecimal {
        x * y
    }

    /// Maximum precision of 100 decimal digits.
    pub(crate) fn big_decimal_divided_by(
        &self,
        x: BigDecimal,
        y: BigDecimal,
    ) -> Result<BigDecimal, HostExportError<impl ExportError>> {
        if y == 0.into() {
            return Err(HostExportError(format!(
                "attempted to divide BigDecimal `{}` by zero",
                x
            )));
        }

        Ok(x / y)
    }

    pub(crate) fn big_decimal_equals(&self, x: BigDecimal, y: BigDecimal) -> bool {
        x == y
    }

    pub(crate) fn big_decimal_to_string(&self, x: BigDecimal) -> String {
        x.to_string()
    }

    pub(crate) fn big_decimal_from_string(
        &self,
        s: String,
    ) -> Result<BigDecimal, HostExportError<impl ExportError>> {
        BigDecimal::from_str(&s)
            .map_err(|e| HostExportError(format!("failed to parse BigDecimal: {}", e)))
    }

    pub(crate) fn data_source_create(
        &self,
        logger: &Logger,
        state: &mut BlockState,
        name: String,
        params: Vec<String>,
        context: Option<DataSourceContext>,
    ) -> Result<(), HostExportError<impl ExportError>> {
        info!(
            logger,
            "Create data source";
            "name" => &name,
            "params" => format!("{}", params.join(","))
        );

        // Resolve the name into the right template
        let template = self
            .templates
            .iter()
            .find(|template| template.name == name)
            .ok_or_else(|| {
                HostExportError(format!(
                    "Failed to create data source from name `{}`: \
                     No template with this name in parent data source `{}`. \
                     Available names: {}.",
                    name,
                    self.data_source_name,
                    self.templates
                        .iter()
                        .map(|template| template.name.clone())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?
            .clone();

        // Remember that we need to create this data source
        state.created_data_sources.push(DataSourceTemplateInfo {
            data_source: self.data_source_name.clone(),
            template,
            params,
            context,
        });

        Ok(())
    }

    pub(crate) fn ens_name_by_hash(
        &self,
        hash: &str,
    ) -> Result<Option<String>, HostExportError<impl ExportError>> {
        self.store.find_ens_name(hash).map_err(HostExportError)
    }

    pub(crate) fn log_log(&self, logger: &Logger, level: slog::Level, msg: String) {
        let rs = record_static!(level, self.data_source_name.as_str());

        logger.log(&slog::Record::new(
            &rs,
            &format_args!("{}", msg),
            b!("data_source" => &self.data_source_name),
        ));

        if level == slog::Level::Critical {
            panic!("Critical error logged in mapping");
        }
    }

    pub(crate) fn data_source_address(&self) -> H160 {
        self.data_source_address.clone().unwrap_or_default()
    }

    pub(crate) fn data_source_network(&self) -> String {
        self.data_source_network.clone()
    }

    pub(crate) fn data_source_context(&self) -> Entity {
        self.data_source_context.clone().unwrap_or_default()
    }

    pub(crate) fn arweave_transaction_data(&self, tx_id: &str) -> Option<Bytes> {
        block_on03(self.arweave_adapter.tx_data(tx_id)).ok()
    }

    pub(crate) fn box_profile(
        &self,
        address: &str,
    ) -> Option<serde_json::Map<String, serde_json::Value>> {
        block_on03(self.three_box_adapter.profile(address)).ok()
    }
}

pub(crate) fn string_to_h160(string: &str) -> Result<H160, HostExportError<impl ExportError>> {
    // `H160::from_str` takes a hex string with no leading `0x`.
    let string = string.trim_start_matches("0x");
    H160::from_str(string)
        .map_err(|e| HostExportError(format!("Failed to convert string to Address/H160: {}", e)))
}

pub(crate) fn bytes_to_string(logger: &Logger, bytes: Vec<u8>) -> String {
    let s = String::from_utf8_lossy(&bytes);

    // If the string was re-allocated, that means it was not UTF8.
    if matches!(s, std::borrow::Cow::Owned(_)) {
        warn!(
            logger,
            "Bytes contain invalid UTF8. This may be caused by attempting \
            to convert a value such as an address that cannot be parsed to a unicode string. \
            You may want to use 'toHexString()' instead. String: '{}'",
            s,
        )
    }

    // The string may have been encoded in a fixed length buffer and padded with null
    // characters, so trim trailing nulls.
    s.trim_end_matches('\u{0000}').to_string()
}

#[test]
fn test_string_to_h160_with_0x() {
    assert_eq!(
        H160::from_str("A16081F360e3847006dB660bae1c6d1b2e17eC2A").unwrap(),
        string_to_h160("0xA16081F360e3847006dB660bae1c6d1b2e17eC2A").unwrap()
    )
}

fn block_on<I, ER>(future: impl Future<Item = I, Error = ER> + Send) -> Result<I, ER> {
    block_on03(future.compat())
}

fn block_on03<T>(future: impl futures03::Future<Output = T> + Send) -> T {
    graph::block_on_allow_panic(future)
}

#[test]
fn bytes_to_string_is_lossy() {
    assert_eq!(
        "Downcoin WETH-USDT",
        bytes_to_string(
            &graph::log::logger(true),
            vec![68, 111, 119, 110, 99, 111, 105, 110, 32, 87, 69, 84, 72, 45, 85, 83, 68, 84]
        )
    );

    assert_eq!(
        "Downcoin WETH-USDT�",
        bytes_to_string(
            &graph::log::logger(true),
            vec![
                68, 111, 119, 110, 99, 111, 105, 110, 32, 87, 69, 84, 72, 45, 85, 83, 68, 84, 160,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ]
        )
    )
}
