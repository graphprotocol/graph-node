use std::collections::HashMap;
use std::convert::TryFrom;
use std::fmt;
use std::ops::Deref;
use std::time::Instant;

use semver::Version;
use wasmi::{
    nan_preserving_float::F64, Error, Externals, FuncInstance, FuncRef, HostError, ImportsBuilder,
    MemoryRef, ModuleImportResolver, ModuleInstance, ModuleRef, RuntimeArgs, RuntimeValue,
    Signature, Trap,
};

use crate::host_exports::{self, HostExportError};
use crate::mapping::MappingContext;
use ethabi::LogParam;
use graph::components::ethereum::*;
use graph::data::store;
use graph::prelude::{Error as FailureError, *};
use web3::types::{Log, Transaction, U256};

use crate::asc_abi::asc_ptr::*;
use crate::asc_abi::class::*;
use crate::asc_abi::*;
use crate::mapping::ValidModule;
use crate::UnresolvedContractCall;

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
const ENS_NAME_BY_HASH: usize = 36;
const LOG_LOG: usize = 37;
const BIG_INT_POW: usize = 38;
const DATA_SOURCE_ADDRESS: usize = 39;
const DATA_SOURCE_NETWORK: usize = 40;
const DATA_SOURCE_CREATE_WITH_CONTEXT: usize = 41;
const DATA_SOURCE_CONTEXT: usize = 42;
const JSON_TRY_FROM_BYTES_FUNC_INDEX: usize = 43;
const ARWEAVE_TRANSACTION_DATA: usize = 44;
const BOX_PROFILE: usize = 45;

/// Transform function index into the function name string
fn fn_index_to_metrics_string(index: usize) -> Option<&'static str> {
    match index {
        STORE_GET_FUNC_INDEX => Some("store_get"),
        ETHEREUM_CALL_FUNC_INDEX => Some("ethereum_call"),
        IPFS_MAP_FUNC_INDEX => Some("ipfs_map"),
        IPFS_CAT_FUNC_INDEX => Some("ipfs_cat"),
        _ => None,
    }
}

/// A common error is a trap in the host, so simplify the message in that case.
fn format_wasmi_error(e: Error) -> String {
    match e {
        Error::Trap(trap) => match trap.kind() {
            wasmi::TrapKind::Host(host_error) => host_error.to_string(),
            _ => trap.to_string(),
        },
        _ => e.to_string(),
    }
}

/// A WASM module based on wasmi that powers a subgraph runtime.
pub(crate) struct WasmiModule {
    pub module: ModuleRef,
    memory: MemoryRef,

    pub ctx: MappingContext,
    pub(crate) valid_module: Arc<ValidModule>,
    pub(crate) host_metrics: Arc<HostMetrics>,

    // Time when the current handler began processing.
    start_time: Instant,

    // True if `run_start` has not yet been called on the module.
    // This is used to prevent mutating store state in start.
    running_start: bool,

    // First free byte in the current arena.
    arena_start_ptr: u32,

    // Number of free bytes starting from `arena_start_ptr`.
    arena_free_size: u32,

    // How many times we've passed a timeout checkpoint during execution.
    timeout_checkpoint_count: u64,
}

impl WasmiModule {
    /// Creates a new wasmi module
    pub fn from_valid_module_with_ctx(
        valid_module: Arc<ValidModule>,
        ctx: MappingContext,
        host_metrics: Arc<HostMetrics>,
    ) -> Result<Self, FailureError> {
        // Build import resolver
        let mut imports = ImportsBuilder::new();

        for host_module_name in valid_module.host_module_names.iter() {
            if host_module_name.as_str() == "env" {
                imports.push_resolver(host_module_name.clone(), &EnvModuleResolver);
            } else {
                imports.push_resolver(host_module_name.clone(), &ModuleResolver);
            }
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
            module: not_started_module,
            memory,
            ctx,
            valid_module: valid_module.clone(),
            host_metrics,
            start_time: Instant::now(),
            running_start: true,

            // `arena_start_ptr` will be set on the first call to `raw_new`.
            arena_free_size: 0,
            arena_start_ptr: 0,
            timeout_checkpoint_count: 0,
        };

        this.module = module
            .run_start(&mut this)
            .map_err(|e| format_err!("Failed to start WASM module instance: {}", e))?;
        this.running_start = false;

        Ok(this)
    }

    pub(crate) fn handle_ethereum_log(
        mut self,
        handler_name: &str,
        transaction: Arc<Transaction>,
        log: Arc<Log>,
        params: Vec<LogParam>,
    ) -> Result<BlockState, FailureError> {
        self.start_time = Instant::now();

        let block = self.ctx.block.clone();

        // Prepare an EthereumEvent for the WASM runtime
        // Decide on the destination type using the mapping
        // api version provided in the subgraph manifest
        let event = if self.ctx.host_exports.api_version >= Version::new(0, 0, 2) {
            RuntimeValue::from(
                self.asc_new::<AscEthereumEvent<AscEthereumTransaction_0_0_2>, _>(
                    &EthereumEventData {
                        block: EthereumBlockData::from(block.as_ref()),
                        transaction: EthereumTransactionData::from(transaction.deref()),
                        address: log.address,
                        log_index: log.log_index.unwrap_or(U256::zero()),
                        transaction_log_index: log.log_index.unwrap_or(U256::zero()),
                        log_type: log.log_type.clone(),
                        params,
                    },
                ),
            )
        } else {
            RuntimeValue::from(self.asc_new::<AscEthereumEvent<AscEthereumTransaction>, _>(
                &EthereumEventData {
                    block: EthereumBlockData::from(block.as_ref()),
                    transaction: EthereumTransactionData::from(transaction.deref()),
                    address: log.address,
                    log_index: log.log_index.unwrap_or(U256::zero()),
                    transaction_log_index: log.log_index.unwrap_or(U256::zero()),
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
                format_wasmi_error(e)
            )
        })
    }

    pub(crate) fn handle_json_callback(
        mut self,
        handler_name: &str,
        value: &serde_json::Value,
        user_data: &store::Value,
    ) -> Result<BlockState, FailureError> {
        let value = RuntimeValue::from(self.asc_new(value));
        let user_data = RuntimeValue::from(self.asc_new(user_data));

        // Invoke the callback
        let result =
            self.module
                .clone()
                .invoke_export(handler_name, &[value, user_data], &mut self);

        // Return either the collected entity operations or an error
        result.map(|_| self.ctx.state).map_err(|e| {
            format_err!(
                "Failed to handle callback with handler \"{}\": {}",
                handler_name,
                format_wasmi_error(e),
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
    ) -> Result<BlockState, FailureError> {
        self.start_time = Instant::now();

        let call = EthereumCallData {
            to: call.to,
            from: call.from,
            block: EthereumBlockData::from(self.ctx.block.as_ref()),
            transaction: EthereumTransactionData::from(transaction.deref()),
            inputs,
            outputs,
        };
        let arg = if self.ctx.host_exports.api_version >= Version::new(0, 0, 3) {
            RuntimeValue::from(self.asc_new::<AscEthereumCall_0_0_3, _>(&call))
        } else {
            RuntimeValue::from(self.asc_new::<AscEthereumCall, _>(&call))
        };

        let result = self
            .module
            .clone()
            .invoke_export(handler_name, &[arg], &mut self);

        result.map(|_| self.ctx.state).map_err(|err| {
            format_err!(
                "Failed to handle Ethereum call with handler \"{}\": {}",
                handler_name,
                format_wasmi_error(err),
            )
        })
    }

    pub(crate) fn handle_ethereum_block(
        mut self,
        handler_name: &str,
    ) -> Result<BlockState, FailureError> {
        self.start_time = Instant::now();

        // Prepare an EthereumBlock for the WASM runtime
        let arg = EthereumBlockData::from(self.ctx.block.as_ref());

        let result = self.module.clone().invoke_export(
            handler_name,
            &[RuntimeValue::from(self.asc_new(&arg))],
            &mut self,
        );

        result.map(|_| self.ctx.state).map_err(|err| {
            format_err!(
                "Failed to handle Ethereum block with handler \"{}\": {}",
                handler_name,
                format_wasmi_error(err)
            )
        })
    }
}

impl AscHeap for WasmiModule {
    fn raw_new(&mut self, bytes: &[u8]) -> Result<u32, Error> {
        // We request large chunks from the AssemblyScript allocator to use as arenas that we
        // manage directly.

        static MIN_ARENA_SIZE: u32 = 10_000;

        let size = u32::try_from(bytes.len()).unwrap();
        if size > self.arena_free_size {
            // Allocate a new arena. Any free space left in the previous arena is left unused. This
            // causes at most half of memory to be wasted, which is acceptable.
            let arena_size = size.max(MIN_ARENA_SIZE);
            let allocated_ptr = self
                .module
                .clone()
                .invoke_export("memory.allocate", &[RuntimeValue::from(arena_size)], self)
                .expect("Failed to invoke memory allocation function")
                .expect("Function did not return a value")
                .try_into::<u32>()
                .expect("Function did not return u32");
            self.arena_start_ptr = allocated_ptr;
            self.arena_free_size = arena_size;
        };

        let ptr = self.arena_start_ptr;
        self.memory.set(ptr, bytes)?;
        self.arena_start_ptr += size;
        self.arena_free_size -= size;

        Ok(ptr)
    }

    fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>, Error> {
        self.memory.get(offset, size as usize)
    }
}

impl<E> HostError for HostExportError<E> where E: fmt::Debug + fmt::Display + Send + Sync + 'static {}

// Implementation of externals.
impl WasmiModule {
    fn gas(&mut self) -> Result<Option<RuntimeValue>, Trap> {
        // This function is called so often that the overhead of calling `Instant::now()` every
        // time would be significant, so we spread out the checks.
        if self.timeout_checkpoint_count % 100 == 0 {
            self.ctx.host_exports.check_timeout(self.start_time)?;
        }
        self.timeout_checkpoint_count += 1;
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
            .ctx
            .host_exports
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
        self.ctx
            .host_exports
            .store_set(&mut self.ctx.state, entity, id, data)?;
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
        self.ctx
            .host_exports
            .store_remove(&mut self.ctx.state, entity, id);
        Ok(None)
    }

    /// function store.get(entity: string, id: string): Entity | null
    fn store_get(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity_ptr = self.asc_get(entity_ptr);
        let id_ptr = self.asc_get(id_ptr);
        let entity_option =
            self.ctx
                .host_exports
                .store_get(&mut self.ctx.state, entity_ptr, id_ptr)?;

        Ok(Some(match entity_option {
            Some(entity) => {
                let _section = self
                    .host_metrics
                    .stopwatch
                    .start_section("store_get_asc_new");
                RuntimeValue::from(self.asc_new(&entity))
            }
            None => RuntimeValue::from(0),
        }))
    }

    /// function ethereum.call(call: SmartContractCall): Array<Token> | null
    fn ethereum_call(
        &mut self,
        call: UnresolvedContractCall,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result =
            self.ctx
                .host_exports
                .ethereum_call(&self.ctx.logger, &self.ctx.block, call)?;
        Ok(Some(match result {
            Some(tokens) => RuntimeValue::from(self.asc_new(tokens.as_slice())),
            None => RuntimeValue::from(0),
        }))
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn bytes_to_string(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let string = host_exports::bytes_to_string(&self.ctx.logger, self.asc_get(bytes_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&string))))
    }

    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    fn bytes_to_hex(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self.ctx.host_exports.bytes_to_hex(self.asc_get(bytes_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function typeConversion.bigIntToString(n: Uint8Array): string
    fn big_int_to_string(
        &mut self,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.asc_get(big_int_ptr);
        let n = BigInt::from_signed_bytes_le(&*bytes);
        let result = self.ctx.host_exports.big_int_to_string(n);
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    fn big_int_to_hex(
        &mut self,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let n: BigInt = self.asc_get(big_int_ptr);
        let result = self.ctx.host_exports.big_int_to_hex(n);
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
        let i = self.ctx.host_exports.big_int_to_i32(n)?;
        Ok(Some(RuntimeValue::from(i)))
    }

    /// function json.fromBytes(bytes: Bytes): JSONValue
    fn json_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr);
        let result = self.ctx.host_exports.json_from_bytes(&bytes).map_err(|e| {
            HostExportError(format!(
                "Failed to parse JSON from byte array. Bytes: `{bytes:?}`. Error: {error}",
                bytes = bytes,
                error = e,
            ))
        })?;
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function json.try_fromBytes(bytes: Bytes): Result<JSONValue, boolean>
    fn json_try_from_bytes(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.asc_get(bytes_ptr);
        let result = self.ctx.host_exports.json_from_bytes(&bytes).map_err(|e| {
            warn!(
                &self.ctx.logger,
                "Failed to parse JSON from byte array";
                "bytes" => format!("{:?}", bytes),
                "error" => format!("{}", e)
            );

            // Map JSON errors to boolean to match the `Result<JSONValue, boolean>`
            // result type expected by mappings
            true
        });
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function ipfs.cat(link: String): Bytes
    fn ipfs_cat(&mut self, link_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let link = self.asc_get(link_ptr);
        let ipfs_res = self.ctx.host_exports.ipfs_cat(&self.ctx.logger, link);
        match ipfs_res {
            Ok(bytes) => {
                let bytes_obj: AscPtr<Uint8Array> = self.asc_new(&*bytes);
                Ok(Some(RuntimeValue::from(bytes_obj)))
            }

            // Return null in case of error.
            Err(e) => {
                info!(&self.ctx.logger, "Failed ipfs.cat, returning `null`";
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
                .ctx
                .host_exports
                .ipfs_map(&self, link.clone(), &*callback, user_data, flags)
            {
                Ok(output_states) => {
                    debug!(
                        &self.ctx.logger,
                        "Successfully processed file with ipfs.map";
                        "link" => &link,
                        "callback" => &*callback,
                        "n_calls" => output_states.len(),
                        "time" => format!("{}ms", start_time.elapsed().as_millis())
                    );
                    for output_state in output_states {
                        self.ctx
                            .state
                            .entity_cache
                            .extend(output_state.entity_cache);
                        self.ctx
                            .state
                            .created_data_sources
                            .extend(output_state.created_data_sources);
                    }
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
        let number = self.ctx.host_exports.json_to_i64(self.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// Expects a decimal string.
    /// function json.toU64(json: String): u64
    fn json_to_u64(&mut self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.ctx.host_exports.json_to_u64(self.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// Expects a decimal string.
    /// function json.toF64(json: String): f64
    fn json_to_f64(&mut self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.ctx.host_exports.json_to_f64(self.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(F64::from(number))))
    }

    /// Expects a decimal string.
    /// function json.toBigInt(json: String): BigInt
    fn json_to_big_int(
        &mut self,
        json_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let big_int = self
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
            .big_int_divided_by(self.asc_get(x_ptr), self.asc_get(y_ptr))?;
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
            .big_int_mod(self.asc_get(x_ptr), self.asc_get(y_ptr));
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigInt.pow(x: BigInt, exp: u8): BigInt
    fn big_int_pow(
        &mut self,
        x_ptr: AscPtr<AscBigInt>,
        exp: u8,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self.ctx.host_exports.big_int_pow(self.asc_get(x_ptr), exp);
        let result_ptr: AscPtr<AscBigInt> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function typeConversion.bytesToBase58(bytes: Bytes): string
    fn bytes_to_base58(
        &mut self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .ctx
            .host_exports
            .bytes_to_base58(self.asc_get(bytes_ptr));
        let result_ptr: AscPtr<AscString> = self.asc_new(&result);
        Ok(Some(RuntimeValue::from(result_ptr)))
    }

    /// function bigDecimal.toString(x: BigDecimal): string
    fn big_decimal_to_string(
        &mut self,
        big_decimal_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .ctx
            .host_exports
            .big_decimal_to_string(self.asc_get(big_decimal_ptr));
        Ok(Some(RuntimeValue::from(self.asc_new(&result))))
    }

    /// function bigDecimal.fromString(x: string): BigDecimal
    fn big_decimal_from_string(
        &mut self,
        string_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
            .ctx
            .host_exports
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
        self.ctx.host_exports.data_source_create(
            &self.ctx.logger,
            &mut self.ctx.state,
            name,
            params,
            None,
        )?;
        Ok(None)
    }

    /// function createWithContext(name: string, params: Array<string>, context: DataSourceContext): void
    fn data_source_create_with_context(
        &mut self,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
        context_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let name: String = self.asc_get(name_ptr);
        let params: Vec<String> = self.asc_get(params_ptr);
        let context: HashMap<_, _> = self.asc_get(context_ptr);
        self.ctx.host_exports.data_source_create(
            &self.ctx.logger,
            &mut self.ctx.state,
            name,
            params,
            Some(context.into()),
        )?;
        Ok(None)
    }

    /// function dataSource.address(): Bytes
    fn data_source_address(&mut self) -> Result<Option<RuntimeValue>, Trap> {
        Ok(Some(RuntimeValue::from(
            self.asc_new(&self.ctx.host_exports.data_source_address()),
        )))
    }

    /// function dataSource.network(): String
    fn data_source_network(&mut self) -> Result<Option<RuntimeValue>, Trap> {
        Ok(Some(RuntimeValue::from(
            self.asc_new(&self.ctx.host_exports.data_source_network()),
        )))
    }

    /// function dataSource.context(): DataSourceContext
    fn data_source_context(&mut self) -> Result<Option<RuntimeValue>, Trap> {
        Ok(Some(RuntimeValue::from(
            self.asc_new(&self.ctx.host_exports.data_source_context()),
        )))
    }

    fn ens_name_by_hash(
        &mut self,
        hash_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let hash: String = self.asc_get(hash_ptr);
        let name = self.ctx.host_exports.ens_name_by_hash(&*hash)?;
        // map `None` to `null`, and `Some(s)` to a runtime string
        Ok(name
            .map(|name| RuntimeValue::from(self.asc_new(&*name)))
            .or(Some(RuntimeValue::from(0))))
    }

    fn log_log(
        &mut self,
        level: i32,
        msg: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let level = LogLevel::from(level).into();
        let msg: String = self.asc_get(msg);
        self.ctx.host_exports.log_log(&self.ctx.logger, level, msg);
        Ok(None)
    }

    /// function arweave.transactionData(txId: string): Bytes | null
    fn arweave_transaction_data(
        &mut self,
        tx_id: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let tx_id: String = self.asc_get(tx_id);
        let data = self.ctx.host_exports.arweave_transaction_data(&tx_id);
        Ok(data
            .map(|data| RuntimeValue::from(self.asc_new(&*data)))
            .or(Some(RuntimeValue::from(0))))
    }

    /// function box.profile(address: string): JSONValue | null
    fn box_profile(&mut self, address: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let address: String = self.asc_get(address);
        let profile = self.ctx.host_exports.box_profile(&address);
        Ok(profile
            .map(|profile| RuntimeValue::from(self.asc_new(&profile)))
            .or(Some(RuntimeValue::from(0))))
    }
}

impl Externals for WasmiModule {
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        // This function is hot, so avoid the cost of registering metrics.
        if index == GAS_FUNC_INDEX {
            return self.gas();
        }

        // Start a catch-all section for exports that don't have their own section.
        let stopwatch = self.host_metrics.stopwatch.clone();
        let _section = stopwatch.start_section("host_export_other");
        let start = Instant::now();
        let res = match index {
            ABORT_FUNC_INDEX => self.abort(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
                args.nth_checked(3)?,
            ),
            STORE_SET_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_store_set");
                self.store_set(
                    args.nth_checked(0)?,
                    args.nth_checked(1)?,
                    args.nth_checked(2)?,
                )
            }
            STORE_GET_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_store_get");
                self.store_get(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            STORE_REMOVE_FUNC_INDEX => {
                self.store_remove(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            ETHEREUM_CALL_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_ethereum_call");

                // For apiVersion >= 0.0.4 the call passed from the mapping includes the
                // function signature; subgraphs using an apiVersion < 0.0.4 don't pass
                // the the signature along with the call.
                let arg = if self.ctx.host_exports.api_version >= Version::new(0, 0, 4) {
                    self.asc_get::<_, AscUnresolvedContractCall_0_0_4>(args.nth_checked(0)?)
                } else {
                    self.asc_get::<_, AscUnresolvedContractCall>(args.nth_checked(0)?)
                };

                self.ethereum_call(arg)
            }
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
            IPFS_CAT_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_ipfs_cat");
                self.ipfs_cat(args.nth_checked(0)?)
            }
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
            BIG_INT_POW => self.big_int_pow(args.nth_checked(0)?, args.nth_checked(1)?),
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
            IPFS_MAP_FUNC_INDEX => {
                let _section = stopwatch.start_section("host_export_ipfs_map");
                self.ipfs_map(
                    args.nth_checked(0)?,
                    args.nth_checked(1)?,
                    args.nth_checked(2)?,
                    args.nth_checked(3)?,
                )
            }
            DATA_SOURCE_CREATE_INDEX => {
                self.data_source_create(args.nth_checked(0)?, args.nth_checked(1)?)
            }
            ENS_NAME_BY_HASH => self.ens_name_by_hash(args.nth_checked(0)?),
            LOG_LOG => self.log_log(args.nth_checked(0)?, args.nth_checked(1)?),
            DATA_SOURCE_ADDRESS => self.data_source_address(),
            DATA_SOURCE_NETWORK => self.data_source_network(),
            DATA_SOURCE_CREATE_WITH_CONTEXT => self.data_source_create_with_context(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
            DATA_SOURCE_CONTEXT => self.data_source_context(),
            JSON_TRY_FROM_BYTES_FUNC_INDEX => self.json_try_from_bytes(args.nth_checked(0)?),
            ARWEAVE_TRANSACTION_DATA => self.arweave_transaction_data(args.nth_checked(0)?),
            BOX_PROFILE => self.box_profile(args.nth_checked(0)?),
            _ => panic!("Unimplemented function at {}", index),
        };
        // Record execution time
        fn_index_to_metrics_string(index).map(|name| {
            self.host_metrics
                .observe_host_fn_execution_time(start.elapsed().as_secs_f64(), name);
        });
        res
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
            "json.try_fromBytes" => {
                FuncInstance::alloc_host(signature, JSON_TRY_FROM_BYTES_FUNC_INDEX)
            }
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
            "bigInt.pow" => FuncInstance::alloc_host(signature, BIG_INT_POW),

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
            "dataSource.address" => FuncInstance::alloc_host(signature, DATA_SOURCE_ADDRESS),
            "dataSource.network" => FuncInstance::alloc_host(signature, DATA_SOURCE_NETWORK),
            "dataSource.createWithContext" => {
                FuncInstance::alloc_host(signature, DATA_SOURCE_CREATE_WITH_CONTEXT)
            }
            "dataSource.context" => FuncInstance::alloc_host(signature, DATA_SOURCE_CONTEXT),

            // ens.nameByHash
            "ens.nameByHash" => FuncInstance::alloc_host(signature, ENS_NAME_BY_HASH),

            // log.log
            "log.log" => FuncInstance::alloc_host(signature, LOG_LOG),

            "arweave.transactionData" => {
                FuncInstance::alloc_host(signature, ARWEAVE_TRANSACTION_DATA)
            }
            "box.profile" => FuncInstance::alloc_host(signature, BOX_PROFILE),

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
