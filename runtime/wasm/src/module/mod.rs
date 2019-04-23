use std::fmt;
use std::ops::Deref;
use std::time::Instant;

use semver::Version;
use wasmi::{
    nan_preserving_float::F64, Error, Externals, FuncInstance, FuncRef, HostError, ImportsBuilder,
    MemoryRef, Module, ModuleImportResolver, ModuleInstance, ModuleRef, RuntimeArgs, RuntimeValue,
    Signature, Trap,
};

use crate::host_exports::{self, HostExportError, HostExports};
use crate::MappingContext;
use graph::components::ethereum::*;
use graph::data::store;
use graph::data::subgraph::DataSource;
use graph::ethabi::{LogParam, Param};
use graph::prelude::{Error as FailureError, *};
use graph::web3::types::{Log, U256, Transaction};

use crate::asc_abi::asc_ptr::*;
use crate::asc_abi::class::*;
use crate::asc_abi::*;

#[cfg(test)]
mod test;

// Indexes for exported host functions
const ABORT_FUNC_INDEX: usize = 0;
const STORE_SET_FUNC_INDEX: usize = 1;
const STORE_REMOVE_FUNC_INDEX: usize = 2;
const ETHEREUM_CALL_FUNC_INDEX: usize = 3;
const TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX: usize = 4;
const TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX: usize = 5;
const TYPE_CONVERSION_BIG_INT_TO_STRING_FUNC_INDEX: usize = 6;
const TYPE_CONVERSION_BIG_INT_TO_HEX_FUNC_INDEX: usize = 7;
const TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX: usize = 8;
const TYPE_CONVERSION_I32_TO_BIG_INT_FUNC_INDEX: usize = 9;
const TYPE_CONVERSION_BIG_INT_TO_I32_FUNC_INDEX: usize = 10;
const JSON_FROM_BYTES_FUNC_INDEX: usize = 11;
const JSON_TO_I64_FUNC_INDEX: usize = 12;
const JSON_TO_U64_FUNC_INDEX: usize = 13;
const JSON_TO_F64_FUNC_INDEX: usize = 14;
const JSON_TO_BIG_INT_FUNC_INDEX: usize = 15;
const IPFS_CAT_FUNC_INDEX: usize = 16;
const STORE_GET_FUNC_INDEX: usize = 17;
const CRYPTO_KECCAK_256_INDEX: usize = 18;
const BIG_INT_PLUS: usize = 19;
const BIG_INT_MINUS: usize = 20;
const BIG_INT_TIMES: usize = 21;
const BIG_INT_DIVIDED_BY: usize = 22;
const BIG_INT_MOD: usize = 23;
const GAS_FUNC_INDEX: usize = 24;
const TYPE_CONVERSION_BYTES_TO_BASE_58_INDEX: usize = 25;
const BIG_INT_DIVIDED_BY_DECIMAL: usize = 26;
const BIG_DECIMAL_PLUS: usize = 27;
const BIG_DECIMAL_MINUS: usize = 28;
const BIG_DECIMAL_TIMES: usize = 29;
const BIG_DECIMAL_DIVIDED_BY: usize = 30;
const BIG_DECIMAL_EQUALS: usize = 31;
const BIG_DECIMAL_TO_STRING: usize = 32;
const BIG_DECIMAL_FROM_STRING: usize = 33;
const IPFS_MAP_FUNC_INDEX: usize = 34;
const DATA_SOURCE_CREATE_INDEX: usize = 35;

pub struct WasmiModuleConfig<T, L, S> {
    pub subgraph_id: SubgraphDeploymentId,
    pub data_source: DataSource,
    pub ethereum_adapter: Arc<T>,
    pub link_resolver: Arc<L>,
    pub store: Arc<S>,
}

/// A pre-processed and valid WASM module, ready to be started as a WasmiModule.
pub(crate) struct ValidModule<T, L, S, U> {
    pub logger: Logger,
    pub module: Module,
    host_exports: HostExports<T, L, S, U>,
    user_module: Option<String>,
}

impl<T, L, S, U> ValidModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + Send + Sync + 'static,
{
    /// Pre-process and validate the module.
    pub fn new(
        logger: &Logger,
        config: WasmiModuleConfig<T, L, S>,
        task_sink: U,
    ) -> Result<Self, FailureError> {
        let logger = logger.new(o!("component" => "WasmiModule"));

        // Clone the parsed module so we can create an instance of `Module` from it
        let parsed_module = config.data_source.mapping.runtime.as_ref().clone();

        // Inject metering calls, which are used for checking timeouts.
        let parsed_module = pwasm_utils::inject_gas_counter(parsed_module, &Default::default())
            .map_err(|_| err_msg("failed to inject gas counter"))?;

        // `inject_gas_counter` injects an import so the section must exist.
        let import_section = parsed_module.import_section().unwrap().clone();

        // Hack: AS currently puts all user imports in one module, in addition
        // to the built-in "env" module. The name of that module is not fixed,
        // to able able to infer the name we allow only one module with imports,
        // with "env" being optional.
        let mut user_modules: Vec<_> = import_section
            .entries()
            .into_iter()
            .map(|import| import.module().to_owned())
            .filter(|module| module != "env")
            .collect();
        user_modules.dedup();
        let user_module = match user_modules.len() {
            0 => None,
            1 => Some(user_modules.into_iter().next().unwrap()),
            _ => return Err(err_msg("WASM module has multiple import sections")),
        };

        let name = config.data_source.name.clone();
        let module = Module::from_parity_wasm_module(parsed_module)
            .map_err(|e| format_err!("Invalid module of data source `{}`: {}", name, e))?;

        // Create new instance of externally hosted functions invoker
        let host_exports = HostExports::new(
            config.subgraph_id,
            Version::parse(&config.data_source.mapping.api_version)?,
            config.data_source,
            config.ethereum_adapter.clone(),
            config.link_resolver.clone(),
            config.store.clone(),
            task_sink,
        );

        Ok(ValidModule {
            logger,
            module,
            host_exports,
            user_module,
        })
    }
}

/// A WASM module based on wasmi that powers a subgraph runtime.
pub(crate) struct WasmiModule<T, L, S, U> {
    pub logger: Logger,
    pub module: ModuleRef,
    memory: MemoryRef,

    pub ctx: MappingContext,
    pub valid_module: Arc<ValidModule<T, L, S, U>>,
    host_exports: &'a HostExports<T, L, S, U>,

    // Time when the current handler began processing.
    start_time: Instant,

    // True if `run_start` has not yet been called on the module.
    // This is used to prevent mutating store state in start.
    running_start: bool,
}

impl<T, L, S, U> WasmiModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + Send + Sync + 'static,
{
    /// Creates a new wasmi module
    pub fn from_valid_module_with_ctx(
        valid_module: Arc<ValidModule<T, L, S, U>,
        ctx: MappingContext,
    ) -> Result<Self, FailureError> {
        let logger = valid_module.logger.new(o!("component" => "WasmiModule"));

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &EnvModuleResolver);
        if let Some(user_module) = valid_module.user_module.clone() {
            imports.push_resolver(user_module, &ModuleResolver);
        }

        // Instantiate the runtime module using hosted functions and import resolver
        let module = ModuleInstance::new(&valid_module.module, &imports)
            .map_err(|e| format_err!("Failed to instantiate WASM module: {}", e))?;

        // Provide access to the WASM runtime linear memory
        let not_started_module = module.not_started_instance().clone();
        let memory = not_started_module
            .export_by_name("memory")
            .ok_or_else(|| format_err!("Failed to find memory export in the WASM module"))?
            .as_memory()
            .ok_or_else(|| format_err!("Export \"memory\" has an invalid type"))?
            .clone();

        let mut this = WasmiModule {
            logger,
            module: not_started_module,
            memory,
            ctx,
            valid_module: valid_module.clone(),
            start_time: Instant::now(),
            running_start: true,
        };

        this.module = module
            .run_start(&mut this)
            .map_err(|e| format_err!("Failed to start WASM module instance: {}", e))?;
        this.running_start = false;

        Ok(this)
    }

    pub(crate) fn host_exports(&self) -> &HostExports<T, L, S, U> {
        &self.valid_module.host_exports
    }

    pub(crate) fn handle_ethereum_log(
        mut self,
        handler_name: &str,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
    ) -> Result<BlockState, FailureError> {
        self.start_time = Instant::now();

        let block = self.ctx.block.block.clone();
        let transaction = self.ctx.transaction.clone();

        // Prepare an EthereumEvent for the WASM runtime
        // Decide on the destination type using the mapping
        // api version provided in the subgraph manifest
        let event = if self.valid_module.host_exports.api_version >= Version::new(0, 0, 2) {
            RuntimeValue::from(
                self.asc_new::<AscEthereumEvent<AscEthereumTransaction_0_0_2>, _>(
                    &EthereumEventData {
                        block: EthereumBlockData::from(&block),
                        transaction: EthereumTransactionData::from(transaction.deref()),
                        address: log.address,
                        log_index: log.log_index.unwrap_or(U256::zero()),
                        transaction_log_index: log.transaction_log_index.unwrap_or(U256::zero()),
                        log_type: log.log_type.clone(),
                        params,
                    },
                ),
            )
        } else {
            RuntimeValue::from(self.asc_new::<AscEthereumEvent<AscEthereumTransaction>, _>(
                &EthereumEventData {
                    block: EthereumBlockData::from(&block),
                    transaction: EthereumTransactionData::from(transaction.deref()),
                    address: log.address,
                    log_index: log.log_index.unwrap_or(U256::zero()),
                    transaction_log_index: log.transaction_log_index.unwrap_or(U256::zero()),
                    log_type: log.log_type.clone(),
                    params,
                },
            ))
        };

        // Invoke the event handler
        let result = self
            .module
            .clone()
            .invoke_export(handler_name, &[event], &mut self);

        // Return either the output state (collected entity operations etc.) or an error
        result.map(|_| self.ctx.state).map_err(|e| {
            format_err!(
                "Failed to handle Ethereum event with handler \"{}\": {}",
                handler_name,
                e
            )
        })
    }

    pub(crate) fn handle_json_callback(
        mut self,
        handler_name: &str,
        value: &graph::serde_json::Value,
        user_data: &store::Value,
    ) -> Result<Vec<EntityOperation>, FailureError> {
        let value = RuntimeValue::from(self.asc_new(value));
        let user_data = RuntimeValue::from(self.asc_new(user_data));

        // Invoke the callback
        let result =
            self.module
                .clone()
                .invoke_export(handler_name, &[value, user_data], &mut self);

        // Return either the collected entity operations or an error
        result
            .map(|_| self.ctx.state.entity_operations)
            .map_err(|e| {
                format_err!(
                    "Failed to handle callback with handler \"{}\": {}",
                    handler_name,
                    e
                )
            })
    }

    pub(crate) fn handle_ethereum_call(
        mut self,
        handler_name: &str,
        transaction: Arc<Transaction>,
        call: Arc<EthereumCall>,
        inputs: Vec<LogParam>,
        outputs: Vec<LogParam>,
    ) -> Result<Vec<EntityOperation>, FailureError> {
        self.start_time = Instant::now();
        // Prepare an EthereumCall for the WASM runtime
        let arg = EthereumCallData {
            address: call.to,
            block: EthereumBlockData::from(&self.ctx.block.block),
            transaction: EthereumTransactionData::from(transaction.deref()),
            inputs,
            outputs,
        };
        let result = self.module.clone().invoke_export(
            handler_name,
            &[RuntimeValue::from(self.asc_new(&arg))],
            &mut self,
        );
        result.map(|_| self.ctx.entity_operations)
            .map_err(|err| {
                format_err!(
                    "Failed to handle Ethereum call with handler \"{}\": {}",
                    handler_name,
                    err
                )
            })
    }

    pub(crate) fn handle_ethereum_block(
        mut self,
        handler_name: &str,
    ) -> Result<Vec<EntityOperation>, FailureError> {
        self.start_time = Instant::now();
        // Prepare an EthereumBlock for the WASM runtime
        let arg = EthereumBlockData::from(&self.ctx.block.block);
        let result = self.module.clone().invoke_export(
            handler_name,
            &[RuntimeValue::from(self.asc_new(&arg))],
            &mut self,
        );
        result.map(|_| self.ctx.entity_operations)
            .map_err(|err| {
                format_err!(
                    "Failed to handle Ethereum block with handler \"{}\": {}",
                    handler_name,
                    err
                )
            })
    }
}

impl<T, L, S, U> AscHeap for WasmiModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + Send + Sync + 'static,
{
    fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, Error> {
        let address = self
            .module
            .clone()
            .invoke_export(
                "memory.allocate",
                &[RuntimeValue::I32(bytes.len() as i32)],
                self,
            )
            .expect("Failed to invoke memory allocation function")
            .expect("Function did not return a value")
            .try_into::<u32>()
            .expect("Function did not return u32");

        self.memory.set(address, bytes)?;

        Ok(address)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, Error> {
        self.memory.get(offset, size as usize)
    }
}

impl<E> HostError for HostExportError<E> where E: fmt::Debug + fmt::Display + Send + Sync + 'static {}

// Implementation of externals.
impl<T, L, S, U> WasmiModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + Send + Sync + 'static,
{
    fn gas(&mut self, _gas_spent: u32) -> Result<Option<RuntimeValue>, Trap> {
        self.host_exports().check_timeout(self.start_time)?;
        Ok(None)
    }

    /// function abort(message?: string | null, fileName?: string | null, lineNumber?: u32, columnNumber?: u32): void
    /// Always returns a trap.
    fn abort(
        &mut self,
        message_ptr: AscPtr<AscString>,
        file_name_ptr: AscPtr<AscString>,
        line_number: u32,
        column_number: u32,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let message = match message_ptr.is_null() {
            false => Some(self.asc_get(message_ptr)),
            true => None,
        };
        let file_name = match file_name_ptr.is_null() {
            false => Some(self.asc_get(file_name_ptr)),
            true => None,
        };
        let line_number = match line_number {
            0 => None,
            _ => Some(line_number),
        };
        let column_number = match column_number {
            0 => None,
            _ => Some(column_number),
        };
        Err(self
            .host_exports()
            .abort(message, file_name, line_number, column_number)
            .unwrap_err()
            .into())
    }

    /// function store.set(entity: string, id: string, data: Entity): void
    fn store_set(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        if self.running_start {
            return Err(HostExportError("store.set may not be called in start function").into());
        }
        let entity = self.asc_get(entity_ptr);
        let id = self.asc_get(id_ptr);
        let data = self.asc_get(data_ptr);
        self.valid_module
            .host_exports
            .store_set(&mut self.ctx, entity, id, data)?;
        Ok(None)
    }

    /// function store.remove(entity: string, id: string): void
    fn store_remove(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        if self.running_start {
            return Err(HostExportError("store.remove may not be called in start function").into());
        }
        let entity = self.asc_get(entity_ptr);
        let id = self.asc_get(id_ptr);
        self.valid_module
            .host_exports
            .store_remove(&mut self.ctx, entity, id);
        Ok(None)
    }

    /// function store.get(entity: string, id: string): Entity | null
    fn store_get(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity_option = self.host_exports().store_get(
            &self.ctx,
            self.asc_get(entity_ptr),
            self.asc_get(id_ptr),
        )?;

        Ok(Some(match entity_option {
            Some(entity) => RuntimeValue::from(self.asc_new(&entity)),
            None => RuntimeValue::from(0),
        }))
    }

    /// function ethereum.call(call: SmartContractCall): Array<Token>
    fn ethereum_call(
        &mut self,
        call_ptr: AscPtr<AscUnresolvedContractCall>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let call = self.asc_get(call_ptr);
        let result = self
            .valid_module
            .host_exports
            .ethereum_call(&mut self.ctx, call)?;
        Ok(Some(RuntimeValue::from(self.asc_new(&*result))))
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn bytes_to_string(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let string = self
            .host_exports()
            .bytes_to_string(self.asc_get(bytes_ptr))?;
        Ok(Some(RuntimeValue::from(self.asc_new(&string))))
    }

    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    fn bytes_to_hex(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self.host_exports().bytes_to_hex(self.asc_get(bytes_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function typeConversion.bigIntToString(n: Uint8Array): string
    fn big_int_to_string(
        &mut self,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.asc_get(big_int_ptr);
        let n = BigInt::from_signed_bytes_le(&*bytes);
        let result = self.host_exports().big_int_to_string(n);
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    fn big_int_to_hex(
        &mut self,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let n: BigInt = self.asc_get(big_int_ptr);
        let result = self.host_exports().big_int_to_hex(n);
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function typeConversion.stringToH160(s: String): H160
    fn string_to_h160(&mut self, str_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let s: String = self.asc_get(str_ptr);
        let h160 = host_exports::string_to_h160(&s)?;
        let h160_obj: AscPtr<AscH160> = self.asc_new(&h160);
        Ok(Some(RuntimeValue::from(h160_obj)))
    }

    /// function typeConversion.i32ToBigInt(i: i32): Uint64Array
    fn i32_to_big_int(&mut self, i: i32) -> Result<Option<RuntimeValue>, Trap> {
        let bytes = BigInt::from(i).to_signed_bytes_le();
        Ok(Some(RuntimeValue::from(self.asc_new(&*bytes))))
    }

    /// function typeConversion.i32ToBigInt(i: i32): Uint64Array
    fn big_int_to_i32(&mut self, n_ptr: AscPtr<AscBigInt>) -> Result<Option<RuntimeValue>, Trap> {
        let n: BigInt = self.asc_get(n_ptr);
        let i = self.host_exports().big_int_to_i32(n)?;
        Ok(Some(RuntimeValue::from(i)))
    }

    /// function json.fromBytes(bytes: Bytes): JSONValue
    fn json_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .json_from_bytes(self.asc_get(bytes_ptr))?;
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function ipfs.cat(link: String): Bytes
    fn ipfs_cat(&mut self, link_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let link = self.asc_get(link_ptr);
        let ipfs_res = self.host_exports().ipfs_cat(link);
        match ipfs_res {
            Ok(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = self.asc_new(&*bytes);
                Ok(Some(RuntimeValue::from(bytes_obj)))
            }

            // Return null in case of error.
            Err(e) => {
                info!(self.logger, "Failed ipfs.cat, returning `null`";
                                    "link" => self.asc_get::<String, _>(link_ptr),
                                    "error" => e.to_string());
                Ok(Some(RuntimeValue::from(0)))
            }
        }
    }

    /// function ipfs.map(link: String, callback: String, flags: String[]): void
    fn ipfs_map(
        &mut self,
        link_ptr: AscPtr<AscString>,
        callback: AscPtr<AscString>,
        user_data: AscPtr<AscEnum<StoreValueKind>>,
        flags: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let link: String = self.asc_get(link_ptr);
        let callback: String = self.asc_get(callback);
        let user_data: store::Value = self.asc_get(user_data);

        let flags = self.asc_get(flags);
        let start_time = Instant::now();
        let result =
            match self
                .host_exports()
                .ipfs_map(&self, link.clone(), &*callback, user_data, flags)
            {
                Ok(ops) => {
                    debug!(
                        self.logger,
                        "Successfully processed file with ipfs.map";
                        "link" => &link,
                        "callback" => &*callback,
                        "entity_operations" => ops.len(),
                        "time" => format!("{}ms", start_time.elapsed().as_millis())
                    );
                    self.ctx.state.entity_operations.extend(ops);
                    Ok(None)
                }
                Err(e) => Err(e.into()),
            };

        // Advance this module's start time by the time it took to run the entire
        // ipfs_map. This has the effect of not charging this module for the time
        // spent running the callback on every JSON object in the IPFS file
        self.start_time += start_time.elapsed();
        result
    }

    /// Expects a decimal string.
    /// function json.toI64(json: String): i64
    fn json_to_i64(&mut self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports().json_to_i64(self.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// Expects a decimal string.
    /// function json.toU64(json: String): u64
    fn json_to_u64(&mut self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports().json_to_u64(self.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// Expects a decimal string.
    /// function json.toF64(json: String): f64
    fn json_to_f64(&mut self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports().json_to_f64(self.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(F64::from(number))))
    }

    /// Expects a decimal string.
    /// function json.toBigInt(json: String): BigInt
    fn json_to_big_int(
        &mut self,
        json_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let big_int = self
            .host_exports()
            .json_to_big_int(self.asc_get(json_ptr))?;
        let big_int_ptr: AscPtr<AscBigInt> = self.asc_new(&*big_int);
        Ok(Some(RuntimeValue::from(big_int_ptr)))
    }

    /// function crypto.keccak256(input: Bytes): Bytes
    fn crypto_keccak_256(
        &mut self,
        input_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let input = self
            .host_exports()
            .crypto_keccak_256(self.asc_get(input_ptr));
        let hash_ptr: AscPtr<Uint8Array> = self.asc_new(input.as_ref());
        Ok(Some(RuntimeValue::from(hash_ptr)))
    }

    /// function bigInt.plus(x: BigInt, y: BigInt): BigInt
    fn big_int_plus(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_int_plus(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigInt.minus(x: BigInt, y: BigInt): BigInt
    fn big_int_minus(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_int_minus(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigInt.times(x: BigInt, y: BigInt): BigInt
    fn big_int_times(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_int_times(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigInt.dividedBy(x: BigInt, y: BigInt): BigInt
    fn big_int_divided_by(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_int_divided_by(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigInt.dividedByDecimal(x: BigInt, y: BigDecimal): BigDecimal
    fn big_int_divided_by_decimal(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let x = self.asc_get::<BigInt, _>(x_ptr).to_big_decimal(0.into());
        let result = self
            .host_exports()
            .big_decimal_divided_by(x, self.asc_get(y_ptr))?;
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigInt.mod(x: BigInt, y: BigInt): BigInt
    fn big_int_mod(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_int_mod(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function typeConversion.bytesToBase58(bytes: Bytes): string
    fn bytes_to_base58(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self.host_exports().bytes_to_base58(self.asc_get(bytes_ptr));
        let result_ptr: AscPtr<AscString> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigDecimal.toString(x: BigDecimal): string
    fn big_decimal_to_string(
        &mut self,
        big_decimal_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_decimal_to_string(self.asc_get(big_decimal_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.fromString(x: string): BigDecimal
    fn big_decimal_from_string(
        &mut self,
        string_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_decimal_from_string(self.asc_get(string_ptr))?;
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.plus(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_plus(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_decimal_plus(self.asc_get(x_ptr), self.asc_get(y_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.minus(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_minus(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_decimal_minus(self.asc_get(x_ptr), self.asc_get(y_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.times(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_times(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_decimal_times(self.asc_get(x_ptr), self.asc_get(y_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.dividedBy(x: BigDecimal, y: BigDecimal): BigDecimal
    fn big_decimal_divided_by(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports()
            .big_decimal_divided_by(self.asc_get(x_ptr), self.asc_get(y_ptr))?;
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.equals(x: BigDecimal, y: BigDecimal): bool
    fn big_decimal_equals(
        &mut self,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let equals = self
            .host_exports()
            .big_decimal_equals(self.asc_get(x_ptr), self.asc_get(y_ptr));
        Ok(Some(RuntimeValue::I32(if equals { 1 } else { 0 })))
    }

    /// function dataSource.create(name: string, params: Array<string>): void
    fn data_source_create(
        &mut self,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let name: String = self.asc_get(name_ptr);
        let params: Vec<String> = self.asc_get(params_ptr);
        self.valid_module
            .host_exports
            .data_source_create(&mut self.ctx, name, params)?;
        Ok(None)
    }
}

impl<T, L, S, U> Externals for WasmiModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + Send + Sync + 'static,
{
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
            ABORT_FUNC_INDEX => self.abort(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
                args.nth_checked(3)?,
            ),
            STORE_SET_FUNC_INDEX => self.store_set(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
            STORE_GET_FUNC_INDEX => self.store_get(args.nth_checked(0)?, args.nth_checked(1)?),
            STORE_REMOVE_FUNC_INDEX => {
                self.store_remove(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            ETHEREUM_CALL_FUNC_INDEX => self.ethereum_call(args.nth_checked(0)?),
            TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX => {
                self.bytes_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX => self.bytes_to_hex(args.nth_checked(0)?),
            TYPE_CONVERSION_BIG_INT_TO_STRING_FUNC_INDEX => {
                self.big_int_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_BIG_INT_TO_HEX_FUNC_INDEX => self.big_int_to_hex(args.nth_checked(0)?),
            TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX => self.string_to_h160(args.nth_checked(0)?),
            TYPE_CONVERSION_I32_TO_BIG_INT_FUNC_INDEX => self.i32_to_big_int(args.nth_checked(0)?),
            TYPE_CONVERSION_BIG_INT_TO_I32_FUNC_INDEX => self.big_int_to_i32(args.nth_checked(0)?),
            JSON_FROM_BYTES_FUNC_INDEX => self.json_from_bytes(args.nth_checked(0)?),
            JSON_TO_I64_FUNC_INDEX => self.json_to_i64(args.nth_checked(0)?),
            JSON_TO_U64_FUNC_INDEX => self.json_to_u64(args.nth_checked(0)?),
            JSON_TO_F64_FUNC_INDEX => self.json_to_f64(args.nth_checked(0)?),
            JSON_TO_BIG_INT_FUNC_INDEX => self.json_to_big_int(args.nth_checked(0)?),
            IPFS_CAT_FUNC_INDEX => self.ipfs_cat(args.nth_checked(0)?),
            CRYPTO_KECCAK_256_INDEX => self.crypto_keccak_256(args.nth_checked(0)?),
            BIG_INT_PLUS => self.big_int_plus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_MINUS => self.big_int_minus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_TIMES => self.big_int_times(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_INT_DIVIDED_BY => {
                self.big_int_divided_by(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_INT_DIVIDED_BY_DECIMAL => {
                self.big_int_divided_by_decimal(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_INT_MOD => self.big_int_mod(args.nth_checked(0)?, args.nth_checked(1)?),
            GAS_FUNC_INDEX => self.gas(args.nth_checked(0)?),
            TYPE_CONVERSION_BYTES_TO_BASE_58_INDEX => self.bytes_to_base58(args.nth_checked(0)?),
            BIG_DECIMAL_PLUS => self.big_decimal_plus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_DECIMAL_MINUS => self.big_decimal_minus(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_DECIMAL_TIMES => self.big_decimal_times(args.nth_checked(0)?, args.nth_checked(1)?),
            BIG_DECIMAL_DIVIDED_BY => {
                self.big_decimal_divided_by(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_DECIMAL_EQUALS => {
                self.big_decimal_equals(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            BIG_DECIMAL_TO_STRING => self.big_decimal_to_string(args.nth_checked(0)?),
            BIG_DECIMAL_FROM_STRING => self.big_decimal_from_string(args.nth_checked(0)?),
            IPFS_MAP_FUNC_INDEX => self.ipfs_map(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
                args.nth_checked(3)?,
            ),
            DATA_SOURCE_CREATE_INDEX => {
                self.data_source_create(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

/// Env module resolver
pub struct EnvModuleResolver;

impl ModuleImportResolver for EnvModuleResolver {
    fn resolve_func(&self, field_name: &str, signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "gas" => FuncInstance::alloc_host(signature.clone(), GAS_FUNC_INDEX),
            "abort" => FuncInstance::alloc_host(signature.clone(), ABORT_FUNC_INDEX),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )));
            }
        })
    }
}

pub struct ModuleResolver;

impl ModuleImportResolver for ModuleResolver {
    fn resolve_func(&self, field_name: &str, signature: &Signature) -> Result<FuncRef, Error> {
        let signature = signature.clone();
        Ok(match field_name {
            // store
            "store.set" => FuncInstance::alloc_host(signature, STORE_SET_FUNC_INDEX),
            "store.remove" => FuncInstance::alloc_host(signature, STORE_REMOVE_FUNC_INDEX),
            "store.get" => FuncInstance::alloc_host(signature, STORE_GET_FUNC_INDEX),

            // ethereum
            "ethereum.call" => FuncInstance::alloc_host(signature, ETHEREUM_CALL_FUNC_INDEX),

            // typeConversion
            "typeConversion.bytesToString" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX)
            }
            "typeConversion.bytesToHex" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX)
            }
            "typeConversion.bigIntToString" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BIG_INT_TO_STRING_FUNC_INDEX)
            }
            "typeConversion.bigIntToHex" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BIG_INT_TO_HEX_FUNC_INDEX)
            }
            "typeConversion.stringToH160" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX)
            }
            "typeConversion.i32ToBigInt" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_I32_TO_BIG_INT_FUNC_INDEX)
            }
            "typeConversion.bigIntToI32" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BIG_INT_TO_I32_FUNC_INDEX)
            }
            "typeConversion.bytesToBase58" => {
                FuncInstance::alloc_host(signature, TYPE_CONVERSION_BYTES_TO_BASE_58_INDEX)
            }

            // json
            "json.fromBytes" => FuncInstance::alloc_host(signature, JSON_FROM_BYTES_FUNC_INDEX),
            "json.toI64" => FuncInstance::alloc_host(signature, JSON_TO_I64_FUNC_INDEX),
            "json.toU64" => FuncInstance::alloc_host(signature, JSON_TO_U64_FUNC_INDEX),
            "json.toF64" => FuncInstance::alloc_host(signature, JSON_TO_F64_FUNC_INDEX),
            "json.toBigInt" => FuncInstance::alloc_host(signature, JSON_TO_BIG_INT_FUNC_INDEX),

            // ipfs
            "ipfs.cat" => FuncInstance::alloc_host(signature, IPFS_CAT_FUNC_INDEX),
            "ipfs.map" => FuncInstance::alloc_host(signature, IPFS_MAP_FUNC_INDEX),

            // crypto
            "crypto.keccak256" => FuncInstance::alloc_host(signature, CRYPTO_KECCAK_256_INDEX),

            // bigInt
            "bigInt.plus" => FuncInstance::alloc_host(signature, BIG_INT_PLUS),
            "bigInt.minus" => FuncInstance::alloc_host(signature, BIG_INT_MINUS),
            "bigInt.times" => FuncInstance::alloc_host(signature, BIG_INT_TIMES),
            "bigInt.dividedBy" => FuncInstance::alloc_host(signature, BIG_INT_DIVIDED_BY),
            "bigInt.dividedByDecimal" => {
                FuncInstance::alloc_host(signature, BIG_INT_DIVIDED_BY_DECIMAL)
            }
            "bigInt.mod" => FuncInstance::alloc_host(signature, BIG_INT_MOD),

            // bigDecimal
            "bigDecimal.plus" => FuncInstance::alloc_host(signature, BIG_DECIMAL_PLUS),
            "bigDecimal.minus" => FuncInstance::alloc_host(signature, BIG_DECIMAL_MINUS),
            "bigDecimal.times" => FuncInstance::alloc_host(signature, BIG_DECIMAL_TIMES),
            "bigDecimal.dividedBy" => FuncInstance::alloc_host(signature, BIG_DECIMAL_DIVIDED_BY),
            "bigDecimal.equals" => FuncInstance::alloc_host(signature, BIG_DECIMAL_EQUALS),
            "bigDecimal.toString" => FuncInstance::alloc_host(signature, BIG_DECIMAL_TO_STRING),
            "bigDecimal.fromString" => FuncInstance::alloc_host(signature, BIG_DECIMAL_FROM_STRING),

            // dataSource
            "dataSource.create" => FuncInstance::alloc_host(signature, DATA_SOURCE_CREATE_INDEX),

            // Unknown export
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )));
            }
        })
    }
}
