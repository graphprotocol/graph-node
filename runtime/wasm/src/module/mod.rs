use failure::Error as FailureError;
use nan_preserving_float::F64;
use std::fmt;
use std::ops::Deref;

use wasmi::{
    Error, Externals, FuncInstance, FuncRef, HostError, ImportsBuilder, MemoryRef, Module,
    ModuleImportResolver, ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue,
    Signature, Trap, ValueType,
};

use graph::components::ethereum::*;
use graph::data::subgraph::DataSource;
use graph::ethabi::LogParam;
use graph::prelude::*;
use graph::web3::types::{Log, H160, H256, U256};
use host_exports;
use EventHandlerContext;

use asc_abi::asc_ptr::*;
use asc_abi::class::*;
use asc_abi::*;

#[cfg(test)]
mod test;

/// AssemblyScript-compatible WASM memory heap.
#[derive(Clone)]
struct WasmiAscHeap {
    module: ModuleRef,
    memory: MemoryRef,
}

impl WasmiAscHeap {
    pub fn new(module: ModuleRef, memory: MemoryRef) -> Self {
        WasmiAscHeap { module, memory }
    }
}

impl AscHeap for WasmiAscHeap {
    fn raw_new(&self, bytes: &[u8]) -> Result<u32, Error> {
        let address = self
            .module
            .invoke_export(
                "allocate_memory",
                &[RuntimeValue::I32(bytes.len() as i32)],
                &mut NopExternals,
            ).expect("Failed to invoke memory allocation function")
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

// Indexes for exported host functions
const ABORT_FUNC_INDEX: usize = 0;
const STORE_SET_FUNC_INDEX: usize = 1;
const STORE_REMOVE_FUNC_INDEX: usize = 2;
const ETHEREUM_CALL_FUNC_INDEX: usize = 3;
const TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX: usize = 4;
const TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX: usize = 5;
const TYPE_CONVERSION_U64_ARRAY_TO_STRING_FUNC_INDEX: usize = 6;
const TYPE_CONVERSION_U64_ARRAY_TO_HEX_FUNC_INDEX: usize = 7;
const TYPE_CONVERSION_H256_TO_H160_FUNC_INDEX: usize = 8;
const TYPE_CONVERSION_H160_TO_H256_FUNC_INDEX: usize = 9;
const TYPE_CONVERSION_U256_TO_H160_FUNC_INDEX: usize = 10;
const TYPE_CONVERSION_U256_TO_H256_FUNC_INDEX: usize = 11;
const TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX: usize = 12;
const TYPE_CONVERSION_INT256_TO_BIG_INT_FUNC_INDEX: usize = 13;
const JSON_FROM_BYTES_FUNC_INDEX: usize = 14;
const JSON_TO_I64_FUNC_INDEX: usize = 15;
const JSON_TO_U64_FUNC_INDEX: usize = 16;
const JSON_TO_F64_FUNC_INDEX: usize = 17;
const JSON_TO_BIG_INT_FUNC_INDEX: usize = 18;
const IPFS_CAT_FUNC_INDEX: usize = 19;
const STORE_GET_FUNC_INDEX: usize = 20;
const TYPE_CONVERSION_BIG_INT_FUNC_TO_INT256_INDEX: usize = 21;
const CRYPTO_KECCAK_256_INDEX: usize = 22;
const TYPE_CONVERSION_U64_TO_U256_INDEX: usize = 23;
const TYPE_CONVERSION_I64_TO_U256_INDEX: usize = 24;
const TYPE_CONVERSION_U256_TO_U8_INDEX: usize = 25;
const TYPE_CONVERSION_U256_TO_U16_INDEX: usize = 26;
const TYPE_CONVERSION_U256_TO_U32_INDEX: usize = 27;
const TYPE_CONVERSION_U256_TO_U64_INDEX: usize = 28;
const TYPE_CONVERSION_U256_TO_I8_INDEX: usize = 29;
const TYPE_CONVERSION_U256_TO_I16_INDEX: usize = 30;
const TYPE_CONVERSION_U256_TO_I32_INDEX: usize = 31;
const TYPE_CONVERSION_U256_TO_I64_INDEX: usize = 32;

pub struct WasmiModuleConfig<T, L, S> {
    pub subgraph: SubgraphManifest,
    pub data_source: DataSource,
    pub ethereum_adapter: Arc<T>,
    pub link_resolver: Arc<L>,
    pub store: Arc<S>,
}

impl<T, L, S> Clone for WasmiModuleConfig<T, L, S> {
    fn clone(&self) -> Self {
        WasmiModuleConfig {
            subgraph: self.subgraph.clone(),
            data_source: self.data_source.clone(),
            ethereum_adapter: self.ethereum_adapter.clone(),
            link_resolver: self.link_resolver.clone(),
            store: self.store.clone(),
        }
    }
}

/// A WASM module based on wasmi that powers a subgraph runtime.
pub struct WasmiModule<T, L, S, U> {
    pub logger: Logger,
    pub module: ModuleRef,
    externals: HostExternals<T, L, S, U>,
    heap: WasmiAscHeap,
}

impl<T, L, S, U> WasmiModule<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
{
    /// Creates a new wasmi module
    pub fn new(logger: &Logger, config: WasmiModuleConfig<T, L, S>, task_sink: U) -> Self {
        let logger = logger.new(o!("component" => "WasmiModule"));

        let module = Module::from_parity_wasm_module(config.data_source.mapping.runtime.clone())
            .expect(
                format!(
                    "Wasmi could not interpret module of data source: {}",
                    config.data_source.name
                ).as_str(),
            );

        // Build import resolver
        let mut imports = ImportsBuilder::new();
        imports.push_resolver("env", &EnvModuleResolver);
        imports.push_resolver("store", &StoreModuleResolver);
        imports.push_resolver("ethereum", &EthereumModuleResolver);
        imports.push_resolver("typeConversion", &TypeConversionModuleResolver);
        imports.push_resolver("json", &JsonModuleResolver);
        imports.push_resolver("ipfs", &IpfsModuleResolver);
        imports.push_resolver("crypto", &CryptoModuleResolver);

        // Instantiate the runtime module using hosted functions and import resolver
        let module =
            ModuleInstance::new(&module, &imports).expect("Failed to instantiate WASM module");

        // Provide access to the WASM runtime linear memory
        let not_started_module = module.not_started_instance().clone();
        let memory = not_started_module
            .export_by_name("memory")
            .expect("Failed to find memory export in the WASM module")
            .as_memory()
            .expect("Export \"memory\" has an invalid type")
            .clone();

        // Create a AssemblyScript-compatible WASM memory heap
        let heap = WasmiAscHeap::new(not_started_module, memory);

        // Create new instance of externally hosted functions invoker
        let mut externals = HostExternals {
            heap: heap.clone(),
            host_exports: host_exports::HostExports::new(
                logger.clone(),
                config.subgraph,
                config.data_source,
                config.ethereum_adapter.clone(),
                config.link_resolver.clone(),
                config.store.clone(),
                task_sink,
                None,
            ),
        };

        let module = module
            .run_start(&mut externals)
            .expect("Failed to start WASM module instance");

        WasmiModule {
            logger,
            module,
            externals,
            heap,
        }
    }

    pub(crate) fn handle_ethereum_event(
        &mut self,
        ctx: EventHandlerContext,
        handler_name: &str,
        log: Arc<Log>,
        params: Vec<LogParam>,
    ) -> Result<Vec<EntityOperation>, FailureError> {
        self.externals.host_exports.ctx = Some(ctx);

        // Prepare an EthereumEvent for the WASM runtime
        let event = EthereumEventData {
            block: EthereumBlockData::from(
                &self
                    .externals
                    .host_exports
                    .ctx
                    .as_ref()
                    .unwrap()
                    .block
                    .block,
            ),
            transaction: EthereumTransactionData::from(
                self.externals
                    .host_exports
                    .ctx
                    .as_ref()
                    .unwrap()
                    .transaction
                    .deref(),
            ),
            address: log.address.clone(),
            params,
        };

        // Invoke the event handler
        let result = self.module.invoke_export(
            handler_name,
            &[RuntimeValue::from(self.heap.asc_new(&event))],
            &mut self.externals,
        );

        // Return either the collected entity operations or an error
        result
            .map(|_| {
                self.externals
                    .host_exports
                    .ctx
                    .take()
                    .expect("processing event without context")
                    .entity_operations
            }).map_err(|e| {
                format_err!(
                    "Failed to handle Ethereum event with handler \"{}\": {}",
                    handler_name,
                    e
                )
            })
    }
}

impl<E> HostError for host_exports::HostExportError<E> where
    E: fmt::Debug + fmt::Display + Send + Sync + 'static
{}

/// Hosted functions for external use by wasm module
pub struct HostExternals<T, L, S, U> {
    heap: WasmiAscHeap,
    host_exports: host_exports::HostExports<T, L, S, U>,
}

impl<T, L, S, U> HostExternals<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
{
    /// function store.set(entity: string, id: string, data: Entity): void
    fn store_set(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        self.host_exports.store_set(
            self.heap.asc_get(entity_ptr),
            self.heap.asc_get(id_ptr),
            self.heap.asc_get(data_ptr),
        )?;
        Ok(None)
    }

    /// function store.remove(entity: string, id: string): void
    fn store_remove(
        &mut self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        self.host_exports
            .store_remove(self.heap.asc_get(entity_ptr), self.heap.asc_get(id_ptr));
        Ok(None)
    }

    /// function store.get(entity: string, id: string): Entity | null
    fn store_get(
        &self,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let entity_option = self
            .host_exports
            .store_get(self.heap.asc_get(entity_ptr), self.heap.asc_get(id_ptr))?;

        Ok(Some(match entity_option {
            Some(entity) => RuntimeValue::from(self.heap.asc_new(&entity)),
            None => RuntimeValue::from(0),
        }))
    }

    /// function ethereum.call(call: SmartContractCall): Array<Token>
    fn ethereum_call(
        &self,
        call_ptr: AscPtr<AscUnresolvedContractCall>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports
            .ethereum_call(self.heap.asc_get(call_ptr))?;
        Ok(Some(RuntimeValue::from(self.heap.asc_new(&*result))))
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn convert_bytes_to_string(
        &self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let string = self
            .host_exports
            .convert_bytes_to_string(self.heap.asc_get(bytes_ptr))?;
        Ok(Some(RuntimeValue::from(self.heap.asc_new(&string))))
    }

    /// function typeConversion.u64ArrayToString(u64_array: U64Array): string
    fn u64_array_to_string(
        &self,
        u64_array_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let string = self
            .host_exports
            .u64_array_to_string(self.heap.asc_get(u64_array_ptr))?;
        Ok(Some(RuntimeValue::from(self.heap.asc_new(&string))))
    }

    /// function typeConversion.u64ArrayToHex(u64_array: U64Array): string
    fn u64_array_to_hex(
        &self,
        u64_array_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports
            .u64_array_to_hex(self.heap.asc_get(u64_array_ptr));
        Ok(Some(RuntimeValue::from(self.heap.asc_new(&*result))))
    }

    /// function typeConversion.h256ToH160(h256: H256): H160
    fn h256_to_h160(&self, h256_ptr: AscPtr<AscH256>) -> Result<Option<RuntimeValue>, Trap> {
        let h256: H256 = self.heap.asc_get(h256_ptr);
        let h160 = H160::from(h256);
        let h160_obj: AscPtr<AscH160> = self.heap.asc_new(&h160);
        Ok(Some(RuntimeValue::from(h160_obj)))
    }

    /// function typeConversion.h160ToH256(h160: H160): H256
    fn h160_to_h256(&self, h160_ptr: AscPtr<AscH160>) -> Result<Option<RuntimeValue>, Trap> {
        let h160: H160 = self.heap.asc_get(h160_ptr);
        let h256 = H256::from(h160);
        let h256_obj: AscPtr<AscH256> = self.heap.asc_new(&h256);
        Ok(Some(RuntimeValue::from(h256_obj)))
    }

    /// function typeConversion.u256ToH160(u256: U256): H160
    fn u256_to_h160(&self, u256_ptr: AscPtr<AscU256>) -> Result<Option<RuntimeValue>, Trap> {
        let u256: U256 = self.heap.asc_get(u256_ptr);
        let h256 = H256::from(u256);
        let h160 = H160::from(h256);
        let h160_obj: AscPtr<AscH160> = self.heap.asc_new(&h160);
        Ok(Some(RuntimeValue::from(h160_obj)))
    }

    /// function typeConversion.u256ToH256(u256: U256): H256
    fn u256_to_h256(&self, u256_ptr: AscPtr<AscU256>) -> Result<Option<RuntimeValue>, Trap> {
        let u256: U256 = self.heap.asc_get(u256_ptr);
        let h256 = H256::from(u256);
        let h256_obj: AscPtr<AscH256> = self.heap.asc_new(&h256);
        Ok(Some(RuntimeValue::from(h256_obj)))
    }

    /// function typeConversion.stringToH160(s: String): H160
    fn string_to_h160(&self, str_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let s: String = self.heap.asc_get(str_ptr);
        let h160 = self.host_exports.string_to_h160(&s)?;
        let h160_obj: AscPtr<AscH160> = self.heap.asc_new(&h160);
        Ok(Some(RuntimeValue::from(h160_obj)))
    }

    /// This works for both U256 and I256.
    /// function typeConversion.int256ToBigInt(int256: Uint64Array): BigInt
    fn int256_to_big_int(
        &self,
        int256_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let buffer = self
            .host_exports
            .int256_to_big_int(self.heap.asc_get(int256_ptr));
        let big_int_obj: AscPtr<BigInt> = self.heap.asc_new(&buffer[..]);
        Ok(Some(RuntimeValue::from(big_int_obj)))
    }

    /// function typeConversion.bigIntToInt256(i: BigInt): Uint64Array
    fn big_int_to_int256(&self, big_int_ptr: AscPtr<BigInt>) -> Result<Option<RuntimeValue>, Trap> {
        let int256 = self
            .host_exports
            .big_int_to_int256(self.heap.asc_get(big_int_ptr))?;
        let int256_ptr: AscPtr<Uint64Array> = self.heap.asc_new(&int256);
        Ok(Some(RuntimeValue::from(int256_ptr)))
    }

    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    fn bytes_to_hex(&self, bytes_ptr: AscPtr<Uint8Array>) -> Result<Option<RuntimeValue>, Trap> {
        let result = self.host_exports.bytes_to_hex(self.heap.asc_get(bytes_ptr));
        Ok(Some(RuntimeValue::from(self.heap.asc_new(&result))))
    }

    /// function json.fromBytes(bytes: Bytes): JSONValue
    fn json_from_bytes(&self, bytes_ptr: AscPtr<Uint8Array>) -> Result<Option<RuntimeValue>, Trap> {
        let result = self
            .host_exports
            .json_from_bytes(self.heap.asc_get(bytes_ptr))?;
        Ok(Some(RuntimeValue::from(self.heap.asc_new(&result))))
    }

    /// function ipfs.cat(link: String): Bytes
    fn ipfs_cat(&self, link_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let bytes = self.host_exports.ipfs_cat(self.heap.asc_get(link_ptr))?;
        let bytes_obj: AscPtr<Uint8Array> = self.heap.asc_new(&*bytes);
        Ok(Some(RuntimeValue::from(bytes_obj)))
    }

    /// Expects a decimal string.
    /// function json.toI64(json: String): i64
    fn json_to_i64(&self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.json_to_i64(self.heap.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// Expects a decimal string.
    /// function json.toU64(json: String): u64
    fn json_to_u64(&self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.json_to_u64(self.heap.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// Expects a decimal string.
    /// function json.toF64(json: String): f64
    fn json_to_f64(&self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.json_to_f64(self.heap.asc_get(json_ptr))?;
        Ok(Some(RuntimeValue::from(F64::from(number))))
    }

    /// Expects a decimal string.
    /// function json.toBigInt(json: String): BigInt
    fn json_to_big_int(&self, json_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let big_int = self
            .host_exports
            .json_to_big_int(self.heap.asc_get(json_ptr))?;
        let big_int_ptr: AscPtr<BigInt> = self.heap.asc_new(&*big_int);
        Ok(Some(RuntimeValue::from(big_int_ptr)))
    }

    /// function crypto.keccak256(input: Bytes): Bytes
    fn crypto_keccak_256(
        &self,
        input_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let input = self
            .host_exports
            .crypto_keccak_256(self.heap.asc_get(input_ptr));
        let hash_ptr: AscPtr<Uint8Array> = self.heap.asc_new(input.as_ref());
        Ok(Some(RuntimeValue::from(hash_ptr)))
    }

    /// function typeConversion.u64ToU256(x: u64): U64Array
    fn u64_to_u256(&self, x: u64) -> Result<Option<RuntimeValue>, Trap> {
        let u256 = U256::from(x);
        let u256_ptr: AscPtr<Uint64Array> = self.heap.asc_new(&u256);
        Ok(Some(RuntimeValue::from(u256_ptr)))
    }

    /// function typeConversion.i64ToU256(x: i64): U64Array
    fn i64_to_u256(&self, x: i64) -> Result<Option<RuntimeValue>, Trap> {
        let u256 = self.host_exports.i64_to_u256(x);
        let u256_ptr: AscPtr<Uint64Array> = self.heap.asc_new(&u256);
        Ok(Some(RuntimeValue::from(u256_ptr)))
    }

    /// function typeConversion.u256ToU8(x: U64Array): u8
    fn u256_to_u8(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_u8(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToU16(x: U64Array): u16
    fn u256_to_u16(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_u16(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToU32(x: U64Array): u32
    fn u256_to_u32(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_u32(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToU64(x: U64Array): u64
    fn u256_to_u64(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_u64(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToI8(x: U64Array): i8
    fn u256_to_i8(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_i8(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToI16(x: U64Array): i16
    fn u256_to_i16(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_i16(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToI32(x: U64Array): i32
    fn u256_to_i32(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_i32(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }

    /// function typeConversion.u256ToI64(x: U64Array): i64
    fn u256_to_i64(&self, x: AscPtr<Uint64Array>) -> Result<Option<RuntimeValue>, Trap> {
        let number = self.host_exports.u256_to_i64(self.heap.asc_get(x))?;
        Ok(Some(RuntimeValue::from(number)))
    }
}

impl<T, L, S, U> Externals for HostExternals<T, L, S, U>
where
    T: EthereumAdapter,
    L: LinkResolver,
    S: Store + Send + Sync + 'static,
    U: Sink<SinkItem = Box<Future<Item = (), Error = ()> + Send>> + Clone + 'static,
{
    fn invoke_index(
        &mut self,
        index: usize,
        args: RuntimeArgs,
    ) -> Result<Option<RuntimeValue>, Trap> {
        match index {
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
                self.convert_bytes_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX => self.bytes_to_hex(args.nth_checked(0)?),
            TYPE_CONVERSION_U64_ARRAY_TO_STRING_FUNC_INDEX => {
                self.u64_array_to_string(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_U64_ARRAY_TO_HEX_FUNC_INDEX => {
                self.u64_array_to_hex(args.nth_checked(0)?)
            }
            TYPE_CONVERSION_H256_TO_H160_FUNC_INDEX => self.h256_to_h160(args.nth_checked(0)?),
            TYPE_CONVERSION_H160_TO_H256_FUNC_INDEX => self.h160_to_h256(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_H160_FUNC_INDEX => self.u256_to_h160(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_H256_FUNC_INDEX => self.u256_to_h256(args.nth_checked(0)?),
            TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX => self.string_to_h160(args.nth_checked(0)?),
            TYPE_CONVERSION_INT256_TO_BIG_INT_FUNC_INDEX => {
                self.int256_to_big_int(args.nth_checked(0)?)
            }
            JSON_FROM_BYTES_FUNC_INDEX => self.json_from_bytes(args.nth_checked(0)?),
            JSON_TO_I64_FUNC_INDEX => self.json_to_i64(args.nth_checked(0)?),
            JSON_TO_U64_FUNC_INDEX => self.json_to_u64(args.nth_checked(0)?),
            JSON_TO_F64_FUNC_INDEX => self.json_to_f64(args.nth_checked(0)?),
            JSON_TO_BIG_INT_FUNC_INDEX => self.json_to_big_int(args.nth_checked(0)?),
            IPFS_CAT_FUNC_INDEX => self.ipfs_cat(args.nth_checked(0)?),
            TYPE_CONVERSION_BIG_INT_FUNC_TO_INT256_INDEX => {
                self.big_int_to_int256(args.nth_checked(0)?)
            }
            CRYPTO_KECCAK_256_INDEX => self.crypto_keccak_256(args.nth_checked(0)?),
            TYPE_CONVERSION_U64_TO_U256_INDEX => self.u64_to_u256(args.nth_checked(0)?),
            TYPE_CONVERSION_I64_TO_U256_INDEX => self.i64_to_u256(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_U8_INDEX => self.u256_to_u8(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_U16_INDEX => self.u256_to_u16(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_U32_INDEX => self.u256_to_u32(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_U64_INDEX => self.u256_to_u64(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_I8_INDEX => self.u256_to_i8(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_I16_INDEX => self.u256_to_i16(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_I32_INDEX => self.u256_to_i32(args.nth_checked(0)?),
            TYPE_CONVERSION_U256_TO_I64_INDEX => self.u256_to_i64(args.nth_checked(0)?),
            _ => panic!("Unimplemented function at {}", index),
        }
    }
}

/// Env module resolver
pub struct EnvModuleResolver;

impl ModuleImportResolver for EnvModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "abort" => FuncInstance::alloc_host(
                Signature::new(
                    &[
                        ValueType::I32,
                        ValueType::I32,
                        ValueType::I32,
                        ValueType::I32,
                    ][..],
                    None,
                ),
                ABORT_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )));
            }
        })
    }
}

/// Store module resolver
pub struct StoreModuleResolver;

impl ModuleImportResolver for StoreModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "set" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32][..], None),
                STORE_SET_FUNC_INDEX,
            ),
            "remove" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], None),
                STORE_REMOVE_FUNC_INDEX,
            ),
            "get" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32][..], Some(ValueType::I32)),
                STORE_GET_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

/// Ethereum module resolver
pub struct EthereumModuleResolver;

impl ModuleImportResolver for EthereumModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "call" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                ETHEREUM_CALL_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

/// Types conversion module resolver
pub struct TypeConversionModuleResolver;

impl ModuleImportResolver for TypeConversionModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "bytesToString" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_BYTES_TO_STRING_FUNC_INDEX,
            ),
            "bytesToHex" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_BYTES_TO_HEX_FUNC_INDEX,
            ),
            "u64ArrayToString" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U64_ARRAY_TO_STRING_FUNC_INDEX,
            ),
            "u64ArrayToHex" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U64_ARRAY_TO_HEX_FUNC_INDEX,
            ),
            "h256ToH160" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_H256_TO_H160_FUNC_INDEX,
            ),
            "h160ToH256" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_H160_TO_H256_FUNC_INDEX,
            ),
            "u256ToH160" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_H160_FUNC_INDEX,
            ),
            "u256ToH256" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_H256_FUNC_INDEX,
            ),
            "stringToH160" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_STRING_TO_H160_FUNC_INDEX,
            ),
            "int256ToBigInt" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_INT256_TO_BIG_INT_FUNC_INDEX,
            ),
            "bigIntToInt256" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_BIG_INT_FUNC_TO_INT256_INDEX,
            ),
            "u64ToU256" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I64][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U64_TO_U256_INDEX,
            ),
            "i64ToU256" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I64][..], Some(ValueType::I32)),
                TYPE_CONVERSION_I64_TO_U256_INDEX,
            ),
            "u256ToU8" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_U8_INDEX,
            ),
            "u256ToU16" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_U16_INDEX,
            ),
            "u256ToU32" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_U32_INDEX,
            ),
            "u256ToU64" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I64)),
                TYPE_CONVERSION_U256_TO_U64_INDEX,
            ),
            "u256ToI8" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_I8_INDEX,
            ),
            "u256ToI16" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_I16_INDEX,
            ),
            "u256ToI32" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_U256_TO_I32_INDEX,
            ),
            "u256ToI64" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I64)),
                TYPE_CONVERSION_U256_TO_I64_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

struct JsonModuleResolver;

impl ModuleImportResolver for JsonModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "fromBytes" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                JSON_FROM_BYTES_FUNC_INDEX,
            ),
            "toI64" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I64)),
                JSON_TO_I64_FUNC_INDEX,
            ),
            "toU64" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I64)),
                JSON_TO_U64_FUNC_INDEX,
            ),
            "toF64" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::F64)),
                JSON_TO_F64_FUNC_INDEX,
            ),
            "toBigInt" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                JSON_TO_BIG_INT_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

struct IpfsModuleResolver;

impl ModuleImportResolver for IpfsModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "cat" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                IPFS_CAT_FUNC_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}

struct CryptoModuleResolver;

impl ModuleImportResolver for CryptoModuleResolver {
    fn resolve_func(&self, field_name: &str, _signature: &Signature) -> Result<FuncRef, Error> {
        Ok(match field_name {
            "keccak256" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                CRYPTO_KECCAK_256_INDEX,
            ),
            _ => {
                return Err(Error::Instantiation(format!(
                    "Export '{}' not found",
                    field_name
                )))
            }
        })
    }
}
