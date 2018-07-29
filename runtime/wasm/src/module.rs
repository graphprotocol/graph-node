use ethereum_types::{H160, H256, U256};
use futures::prelude::*;
use futures::sync::mpsc::Sender;
use serde_json;
use slog::Logger;
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use tokio_core::reactor::Handle;
use wasmi::{
    Error, Externals, FuncInstance, FuncRef, HostError, ImportsBuilder, MemoryRef, Module,
    ModuleImportResolver, ModuleInstance, ModuleRef, NopExternals, RuntimeArgs, RuntimeValue,
    Signature, Trap, TrapKind, ValueType,
};
use web3::types::BlockId;

use graph::components::ethereum::*;
use graph::components::store::StoreKey;
use graph::components::subgraph::RuntimeHostEvent;
use graph::data::subgraph::DataSource;
use graph::prelude::*;

use super::UnresolvedContractCall;
use asc_abi::asc_ptr::*;
use asc_abi::class::*;
use asc_abi::*;
use hex;

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
        let address = self.module
            .invoke_export(
                "allocate_memory",
                &[RuntimeValue::I32(bytes.len() as i32)],
                &mut NopExternals,
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
const TYPE_CONVERSION_INT256_TO_BIG_INT_FUNC_INDEX: usize = 12;
const JSON_FROM_BYTES_FUNC_INDEX: usize = 13;
const IPFS_CAT_FUNC_INDEX: usize = 14;

pub struct WasmiModuleConfig<T, L> {
    pub subgraph: SubgraphManifest,
    pub data_source: DataSource,
    pub runtime: Handle,
    pub event_sink: Sender<RuntimeHostEvent>,
    pub ethereum_adapter: Arc<Mutex<T>>,
    pub link_resolver: Arc<L>,
}

/// A WASM module based on wasmi that powers a subgraph runtime.
pub struct WasmiModule<T, L> {
    pub logger: Logger,
    pub module: ModuleRef,
    externals: HostExternals<T, L>,
    heap: WasmiAscHeap,
}

impl<T, L> WasmiModule<T, L>
where
    T: EthereumAdapter,
    L: LinkResolver,
{
    /// Creates a new wasmi module
    pub fn new(logger: &Logger, config: WasmiModuleConfig<T, L>) -> Self {
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
            subgraph: config.subgraph,
            data_source: config.data_source,
            logger: logger.clone(),
            runtime: config.runtime.clone(),
            event_sink: config.event_sink.clone(),
            heap: heap.clone(),
            ethereum_adapter: config.ethereum_adapter.clone(),
            link_resolver: config.link_resolver.clone(),
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

    pub fn handle_ethereum_event(&mut self, handler_name: &str, event: EthereumEvent) {
        self.module
            .invoke_export(
                handler_name,
                &[RuntimeValue::from(self.heap.asc_new(&event))],
                &mut self.externals,
            )
            .unwrap_or_else(|e| {
                warn!(self.logger, "Failed to handle Ethereum event";
                      "handler" => &handler_name,
                      "error" => format!("{}", e));
                None
            });
    }
}

/// Error raised in host functions.
#[derive(Debug)]
struct HostExternalsError<E>(E);

impl<E> HostError for HostExternalsError<E>
where
    E: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
}

impl<E: fmt::Display> fmt::Display for HostExternalsError<E> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Hosted functions for external use by wasm module
pub struct HostExternals<T, L> {
    logger: Logger,
    runtime: Handle,
    subgraph: SubgraphManifest,
    data_source: DataSource,
    event_sink: Sender<RuntimeHostEvent>,
    heap: WasmiAscHeap,
    ethereum_adapter: Arc<Mutex<T>>,
    link_resolver: Arc<L>,
}

impl<T, L> HostExternals<T, L>
where
    T: EthereumAdapter,
    L: LinkResolver,
{
    /// function store.set(blockHash: H256, entity: string, id: string, data: Entity): void
    fn store_set(
        &self,
        block_hash_ptr: AscPtr<AscH256>,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let _block_hash: H256 = self.heap.asc_get(block_hash_ptr);
        let entity: String = self.heap.asc_get(entity_ptr);
        let id: String = self.heap.asc_get(id_ptr);
        let data: HashMap<String, Value> = self.heap.asc_get(data_ptr);
        let store_key = StoreKey {
            subgraph: self.subgraph.id.clone(),
            entity,
            id,
        };

        let entity_data = Entity::from(data);

        // Send an entity set event
        let logger = self.logger.clone();
        self.runtime.spawn(
            self.event_sink
                .clone()
                .send(RuntimeHostEvent::EntitySet(store_key, entity_data))
                .map_err(move |e| {
                    error!(logger, "Failed to forward runtime host event";
                           "error" => format!("{}", e));
                })
                .map(|_| ()),
        );

        Ok(None)
    }

    /// function store.remove(entity: string, id: string): void
    fn store_remove(
        &self,
        block_hash_ptr: AscPtr<AscH256>,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let _block_hash: H256 = self.heap.asc_get(block_hash_ptr);
        let entity: String = self.heap.asc_get(entity_ptr);
        let id: String = self.heap.asc_get(id_ptr);
        let store_key = StoreKey {
            subgraph: self.subgraph.id.clone(),
            entity,
            id,
        };

        // Send an entity removed event
        let logger = self.logger.clone();
        self.runtime.spawn(
            self.event_sink
                .clone()
                .send(RuntimeHostEvent::EntityRemoved(store_key))
                .map_err(move |e| {
                    error!(logger, "Failed to forward runtime host event";
                           "error" => format!("{}", e));
                })
                .and_then(|_| Ok(())),
        );

        Ok(None)
    }

    /// function ethereum.call(call: SmartContractCall): Array<Token>
    fn ethereum_call(
        &self,
        call_ptr: AscPtr<AscUnresolvedContractCall>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let unresolved_call: UnresolvedContractCall = self.heap.asc_get(call_ptr);

        info!(self.logger, "Call smart contract";
              "contract" => &unresolved_call.contract_name,
              "function" => &unresolved_call.function_name);

        // Obtain the path to the contract ABI
        let contract = self.data_source
            .mapping
            .abis
            .iter()
            .find(|abi| abi.name == unresolved_call.contract_name)
            .ok_or(Trap::new(TrapKind::Host(Box::new(HostExternalsError(
                format!(
                    "Unknown contract \"{}\" called from WASM runtime",
                    unresolved_call.contract_name
                ),
            )))))?
            .contract
            .clone();

        let function = contract
            .function(unresolved_call.function_name.as_str())
            .map_err(|e| {
                Trap::new(TrapKind::Host(Box::new(HostExternalsError(format!(
                    "Unknown function \"{}::{}\" called from WASM runtime: {}",
                    unresolved_call.contract_name, unresolved_call.function_name, e
                )))))
            })?;

        let call = EthereumContractCall {
            address: unresolved_call.contract_address.clone(),
            block_id: BlockId::Hash(unresolved_call.block_hash.clone()),
            function: function.clone(),
            args: unresolved_call.function_args.clone(),
        };

        self.ethereum_adapter
            .lock()
            .unwrap()
            .contract_call(call)
            .wait()
            .map(|result| Some(RuntimeValue::from(self.heap.asc_new(&*result))))
            .map_err(|e| {
                Trap::new(TrapKind::Host(Box::new(HostExternalsError(format!(
                    "Failed to call function \"{}\" of contract \"{}\": {}",
                    unresolved_call.function_name, unresolved_call.contract_name, e
                )))))
            })
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    fn convert_bytes_to_string(
        &self,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.heap.asc_get(bytes_ptr);
        let s = String::from_utf8_lossy(&*bytes);
        // The string may have been encoded in a fixed length
        // buffer and padded with null characters, so trim
        // trailing nulls.
        let trimmed_s = s.trim_right_matches('\u{0000}');
        Ok(Some(RuntimeValue::from(self.heap.asc_new(trimmed_s))))
    }

    /// function typeConversion.u64ArrayToString(u64_array: U64Array): string
    fn u64_array_to_string(
        &self,
        u64_array_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let u64_array: Vec<u64> = self.heap.asc_get(u64_array_ptr);
        let mut bytes: Vec<u8> = Vec::new();
        for x in u64_array {
            // This is just `x.to_bytes()` which is unstable.
            let x_bytes: [u8; 8] = unsafe { ::std::mem::transmute(x) };
            bytes.extend(x_bytes.iter());
        }

        let s = String::from_utf8_lossy(&*bytes);
        let trimmed_s = s.trim_right_matches('\u{0000}');
        Ok(Some(RuntimeValue::from(self.heap.asc_new(trimmed_s))))
    }

    /// function typeConversion.u64ArrayToHex(u64_array: U64Array): string
    fn u64_array_to_hex(
        &self,
        u64_array_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        let u64_array: Vec<u64> = self.heap.asc_get(u64_array_ptr);
        let mut bytes: Vec<u8> = Vec::new();
        for x in u64_array {
            // This is just `x.to_bytes()` which is unstable.
            let x_bytes: [u8; 8] = unsafe { ::std::mem::transmute(x) };
            bytes.extend(x_bytes.iter());
        }

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex_string = format!("0x{}", hex::encode(bytes));
        let hex_string_obj = self.heap.asc_new(hex_string.as_str());
        Ok(Some(RuntimeValue::from(hex_string_obj)))
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

    ///  This works for both U256 and I256.
    ///  function typeConversion.int256ToBigInt(int256: Uint64Array): BigInt
    fn int256_to_big_int(
        &self,
        int256_ptr: AscPtr<Uint64Array>,
    ) -> Result<Option<RuntimeValue>, Trap> {
        // Read as a U256 to use the convenient `to_little_endian` method.
        let int256: U256 = self.heap.asc_get(int256_ptr);
        let mut buffer = [0; 32];
        int256.to_little_endian(&mut buffer);
        let big_int_obj: AscPtr<BigInt> = self.heap.asc_new(&buffer[..]);
        Ok(Some(RuntimeValue::from(big_int_obj)))
    }

    /// Converts bytes to a hex string.
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    ///
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    fn bytes_to_hex(&self, bytes_ptr: AscPtr<Uint8Array>) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.heap.asc_get(bytes_ptr);

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex_string = format!("0x{}", hex::encode(bytes));
        let hex_string_obj = self.heap.asc_new(hex_string.as_str());

        Ok(Some(RuntimeValue::from(hex_string_obj)))
    }

    // function json.fromBytes(bytes: Bytes): JSONValue
    fn json_from_bytes(&self, bytes_ptr: AscPtr<Uint8Array>) -> Result<Option<RuntimeValue>, Trap> {
        let bytes: Vec<u8> = self.heap.asc_get(bytes_ptr);
        let json: serde_json::Value = serde_json::from_reader(&*bytes).map_err(HostExternalsError)?;
        let json_obj = self.heap.asc_new(&json);
        Ok(Some(RuntimeValue::from(json_obj)))
    }

    // function ipfs.cat(link: String): Bytes
    fn ipfs_cat(&self, link_ptr: AscPtr<AscString>) -> Result<Option<RuntimeValue>, Trap> {
        let link = self.heap.asc_get(link_ptr);
        let bytes = self.link_resolver
            .cat(&Link { link })
            .wait()
            .map_err(|e| HostExternalsError(e.to_string()))?;
        let bytes_obj: AscPtr<Uint8Array> = self.heap.asc_new(&*bytes);
        Ok(Some(RuntimeValue::from(bytes_obj)))
    }
}

impl<T, L> Externals for HostExternals<T, L>
where
    T: EthereumAdapter,
    L: LinkResolver,
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
                args.nth_checked(3)?,
            ),
            STORE_REMOVE_FUNC_INDEX => self.store_remove(
                args.nth_checked(0)?,
                args.nth_checked(1)?,
                args.nth_checked(2)?,
            ),
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
            TYPE_CONVERSION_INT256_TO_BIG_INT_FUNC_INDEX => {
                self.int256_to_big_int(args.nth_checked(0)?)
            }
            JSON_FROM_BYTES_FUNC_INDEX => self.json_from_bytes(args.nth_checked(0)?),
            IPFS_CAT_FUNC_INDEX => self.ipfs_cat(args.nth_checked(0)?),
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
                Signature::new(
                    &[
                        ValueType::I32,
                        ValueType::I32,
                        ValueType::I32,
                        ValueType::I32,
                    ][..],
                    None,
                ),
                STORE_SET_FUNC_INDEX,
            ),
            "remove" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32, ValueType::I32, ValueType::I32][..], None),
                STORE_REMOVE_FUNC_INDEX,
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
            "int256ToBigInt" => FuncInstance::alloc_host(
                Signature::new(&[ValueType::I32][..], Some(ValueType::I32)),
                TYPE_CONVERSION_INT256_TO_BIG_INT_FUNC_INDEX,
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

#[cfg(test)]
mod tests {
    extern crate graphql_parser;
    extern crate parity_wasm;

    use self::graphql_parser::schema::Document;
    use ethabi::{LogParam, Token};
    use ethereum_types::Address;
    use futures::prelude::*;
    use futures::sync::mpsc::channel;
    use slog;
    use std::collections::HashMap;
    use std::iter::FromIterator;
    use std::sync::{Arc, Mutex};
    use tokio_core;

    use graph::components::ethereum::*;
    use graph::components::store::*;
    use graph::components::subgraph::*;
    use graph::data::subgraph::*;
    use graph::prelude::*;
    use graph::util;

    use super::{WasmiModule, WasmiModuleConfig};

    #[derive(Default)]
    struct MockEthereumAdapter {}

    impl EthereumAdapter for MockEthereumAdapter {
        fn contract_call(
            &mut self,
            _call: EthereumContractCall,
        ) -> Box<Future<Item = Vec<Token>, Error = EthereumContractCallError>> {
            unimplemented!()
        }

        fn subscribe_to_event(
            &mut self,
            _subscription: EthereumEventSubscription,
        ) -> Box<Stream<Item = EthereumEvent, Error = EthereumSubscriptionError>> {
            unimplemented!()
        }

        fn unsubscribe_from_event(&mut self, _subscription_id: String) -> bool {
            false
        }
    }

    struct FakeLinkResolver;

    impl LinkResolver for FakeLinkResolver {
        fn cat(&self, _: &Link) -> Box<Future<Item = Vec<u8>, Error = Box<::std::error::Error>>> {
            unimplemented!()
        }
    }

    fn mock_subgraph() -> SubgraphManifest {
        SubgraphManifest {
            id: String::from("example subgraph"),
            location: String::from("/path/to/example-subgraph.yaml"),
            spec_version: String::from("0.1.0"),
            schema: Schema {
                id: String::from("exampled id"),
                document: Document {
                    definitions: vec![],
                },
            },
            data_sources: vec![],
        }
    }

    fn mock_data_source() -> DataSource {
        let runtime = parity_wasm::deserialize_file("test/example_event_handler.wasm")
            .expect("Failed to deserialize wasm");

        DataSource {
            kind: String::from("ethereum/contract"),
            name: String::from("example data source"),
            source: Source {
                address: String::from("0123123123"),
                abi: String::from("123123"),
            },
            mapping: Mapping {
                kind: String::from("ethereum/events"),
                api_version: String::from("0.1.0"),
                language: String::from("wasm/assemblyscript"),
                entities: vec![],
                abis: vec![],
                event_handlers: vec![],
                runtime,
            },
        }
    }

    #[test]
    fn call_invalid_event_handler_and_dont_crash() {
        // This test passing means the module doesn't crash when an invalid
        // event handler is called or when the event handler execution fails.

        // Load the module
        let logger = slog::Logger::root(slog::Discard, o!());
        let core = tokio_core::reactor::Core::new().unwrap();
        let (sender, _receiver) = channel(1);
        let mock_ethereum_adapter = Arc::new(Mutex::new(MockEthereumAdapter::default()));
        let mut module = WasmiModule::new(
            &logger,
            WasmiModuleConfig {
                subgraph: mock_subgraph(),
                data_source: mock_data_source(),
                runtime: core.handle(),
                event_sink: sender,
                ethereum_adapter: mock_ethereum_adapter,
                link_resolver: Arc::new(FakeLinkResolver),
            },
        );

        // Create a mock Ethereum event
        let ethereum_event = EthereumEvent {
            address: Address::from("22843e74c59580b3eaf6c233fa67d8b7c561a835"),
            event_signature: util::ethereum::string_to_h256("ExampleEvent(string)"),
            block_hash: util::ethereum::string_to_h256("example block hash"),
            params: vec![LogParam {
                name: String::from("exampleParam"),
                value: Token::String(String::from("some data")),
            }],
            removed: false,
        };

        // Call a non-existent event handler in the test module; if the test hasn't
        // crashed until now, it means it survives Ethereum event handler errors
        assert_eq!(
            module.handle_ethereum_event("handleNonExistentExampleEvent", ethereum_event),
            ()
        );
    }

    #[test]
    fn call_event_handler_and_receive_store_event() {
        // Load the example_event_handler.wasm test module. All this module does
        // is implement an `handleExampleEvent` function that calls `store.set()`
        // with sample data taken from the event parameters.
        //
        // This test verifies that the event is delivered and the example data
        // is returned to the RuntimeHostEvent stream.

        // Load the module
        let logger = slog::Logger::root(slog::Discard, o!());
        let mut core = tokio_core::reactor::Core::new().unwrap();
        let (sender, receiver) = channel(1);
        let mock_ethereum_adapter = Arc::new(Mutex::new(MockEthereumAdapter::default()));
        let mut module = WasmiModule::new(
            &logger,
            WasmiModuleConfig {
                subgraph: mock_subgraph(),
                data_source: mock_data_source(),
                runtime: core.handle(),
                event_sink: sender,
                ethereum_adapter: mock_ethereum_adapter,
                link_resolver: Arc::new(FakeLinkResolver),
            },
        );

        // Create a mock Ethereum event
        let ethereum_event = EthereumEvent {
            address: Address::from("22843e74c59580b3eaf6c233fa67d8b7c561a835"),
            event_signature: util::ethereum::string_to_h256("ExampleEvent(string)"),
            block_hash: util::ethereum::string_to_h256("example block hash"),
            params: vec![LogParam {
                name: String::from("exampleParam"),
                value: Token::String(String::from("some data")),
            }],
            removed: false,
        };

        // Call the event handler in the test module and pass the event to it
        module.handle_ethereum_event("handleExampleEvent", ethereum_event);

        // Expect a store set call to be made by the handler and a
        // RuntimeHostEvent::EntitySet event to be written to the event stream
        let work = receiver.take(1).into_future();
        let store_event = core.run(work)
            .expect("No store event received from runtime")
            .0
            .expect("Store event must not be None");

        // Verify that this event matches what the test module is sending
        assert_eq!(
            store_event,
            RuntimeHostEvent::EntitySet(
                StoreKey {
                    subgraph: String::from("example subgraph"),
                    entity: String::from("ExampleEntity"),
                    id: String::from("example id"),
                },
                Entity::from(HashMap::from_iter(
                    vec![(String::from("exampleAttribute"), Value::from("some data"))].into_iter()
                ))
            )
        );
    }
}
