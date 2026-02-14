use graph::data::value::Word;
use graph::runtime::gas;
use graph::util::lfu_cache::LfuCache;
use std::collections::HashMap;
use wasmtime::AsContext;
use wasmtime::AsContextMut;
use wasmtime::StoreContextMut;

use std::sync::Arc;
use std::time::Instant;

use anyhow::Error;
use graph::components::store::GetScope;
use never::Never;

use crate::asc_abi::class::*;
use crate::HostExports;
use graph::data::store;

use crate::asc_abi::class::AscEntity;
use crate::asc_abi::class::AscString;
use crate::mapping::MappingContext;
use crate::mapping::ValidModule;
use crate::ExperimentalFeatures;
use graph::prelude::*;
use graph::runtime::AscPtr;
use graph::runtime::{asc_new, gas::GasCounter, DeterministicHostError, HostExportError};

use super::asc_get;
use super::AscHeapCtx;

pub(crate) struct WasmInstanceContext<'a> {
    inner: StoreContextMut<'a, WasmInstanceData>,
}

impl WasmInstanceContext<'_> {
    pub fn new(ctx: &mut impl AsContextMut<Data = WasmInstanceData>) -> WasmInstanceContext<'_> {
        WasmInstanceContext {
            inner: ctx.as_context_mut(),
        }
    }

    pub fn as_ref(&self) -> &WasmInstanceData {
        self.inner.data()
    }

    pub fn as_mut(&mut self) -> &mut WasmInstanceData {
        self.inner.data_mut()
    }

    pub fn asc_heap(&self) -> &Arc<AscHeapCtx> {
        self.as_ref().asc_heap()
    }

    pub fn suspend_timeout(&mut self) {
        // See also: runtime-timeouts
        self.inner.set_epoch_deadline(u64::MAX / 2);
    }

    pub fn start_timeout(&mut self) {
        // Only re-arm the epoch deadline when this module actually has a
        // timeout configured; otherwise leave it at the suspended value so
        // the shared epoch counter does not interrupt no-timeout modules.
        // See also: runtime-timeouts
        if self.as_ref().valid_module.timeout.is_some() {
            self.inner.set_epoch_deadline(2);
        }
    }
}

impl AsContext for WasmInstanceContext<'_> {
    type Data = WasmInstanceData;

    fn as_context(&self) -> wasmtime::StoreContext<'_, Self::Data> {
        self.inner.as_context()
    }
}

impl AsContextMut for WasmInstanceContext<'_> {
    fn as_context_mut(&mut self) -> wasmtime::StoreContextMut<'_, Self::Data> {
        self.inner.as_context_mut()
    }
}

pub struct WasmInstanceData {
    pub ctx: MappingContext,
    pub valid_module: Arc<ValidModule>,
    pub host_metrics: Arc<HostMetrics>,

    // Per-trigger gas counter, shared via Arc so clones refer to the same counter.
    pub gas: GasCounter,

    // A trap ocurred due to a possible reorg detection.
    pub possible_reorg: bool,

    // A host export trap ocurred for a deterministic reason.
    pub deterministic_host_trap: bool,

    pub(crate) experimental_features: ExperimentalFeatures,

    // This option is needed to break the cyclic dependency between, instance, store, and context.
    // during execution it should always be populated.
    asc_heap: Option<Arc<AscHeapCtx>>,
}

impl WasmInstanceData {
    pub fn from_instance(
        ctx: MappingContext,
        valid_module: Arc<ValidModule>,
        host_metrics: Arc<HostMetrics>,
        gas: GasCounter,
        experimental_features: ExperimentalFeatures,
    ) -> Self {
        WasmInstanceData {
            asc_heap: None,
            ctx,
            valid_module,
            host_metrics,
            gas,
            possible_reorg: false,
            deterministic_host_trap: false,
            experimental_features,
        }
    }

    pub fn set_asc_heap(&mut self, asc_heap: Arc<AscHeapCtx>) {
        self.asc_heap = Some(asc_heap);
    }

    pub fn asc_heap(&self) -> &Arc<AscHeapCtx> {
        self.asc_heap.as_ref().expect("asc_heap not set")
    }

    pub fn take_state(mut self) -> BlockState {
        let state = &mut self.ctx.state;

        std::mem::replace(
            state,
            BlockState::new(state.entity_cache.store.cheap_clone(), LfuCache::default()),
        )
    }
}

impl WasmInstanceContext<'_> {
    async fn store_get_scoped(
        &mut self,
        gas: &GasCounter,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        scope: GetScope,
    ) -> Result<AscPtr<AscEntity>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let _timer = self
            .as_ref()
            .host_metrics
            .cheap_clone()
            .time_host_fn_execution_region("store_get");

        let entity_type: String = asc_get(self, entity_ptr, gas)?;
        let id: String = asc_get(self, id_ptr, gas)?;
        let entity_option = host_exports
            .store_get(
                &mut self.as_mut().ctx.state,
                entity_type.clone(),
                id.clone(),
                gas,
                scope,
            )
            .await?;

        if self.as_ref().ctx.instrument {
            debug!(self.as_ref().ctx.logger, "store_get";
                    "type" => &entity_type,
                    "id" => &id,
                    "found" => entity_option.is_some());
        }
        let host_metrics = self.as_ref().host_metrics.cheap_clone();
        let debug_fork = self.as_ref().ctx.debug_fork.cheap_clone();

        let ret = match entity_option {
            Some(entity) => {
                let _section = host_metrics.stopwatch.start_section("store_get_asc_new");
                asc_new(self, &entity.sorted_ref(), gas).await?
            }
            None => match &debug_fork {
                Some(fork) => {
                    let entity_option = fork.fetch(entity_type, id).await.map_err(|e| {
                        HostExportError::Unknown(anyhow!(
                            "store_get: failed to fetch entity from the debug fork: {}",
                            e
                        ))
                    })?;
                    match entity_option {
                        Some(entity) => {
                            let _section =
                                host_metrics.stopwatch.start_section("store_get_asc_new");
                            let entity = asc_new(self, &entity.sorted(), gas).await?;
                            self.store_set(gas, entity_ptr, id_ptr, entity).await?;
                            entity
                        }
                        None => AscPtr::null(),
                    }
                }
                None => AscPtr::null(),
            },
        };

        Ok(ret)
    }
}

// Implementation of externals.
impl WasmInstanceContext<'_> {
    /// function abort(message?: string | null, fileName?: string | null, lineNumber?: u32, columnNumber?: u32): void
    /// Always returns a trap.
    pub async fn abort(
        &mut self,
        gas: &GasCounter,
        message_ptr: AscPtr<AscString>,
        file_name_ptr: AscPtr<AscString>,
        line_number: u32,
        column_number: u32,
    ) -> Result<Never, DeterministicHostError> {
        let message = match message_ptr.is_null() {
            false => Some(asc_get(self, message_ptr, gas)?),
            true => None,
        };
        let file_name = match file_name_ptr.is_null() {
            false => Some(asc_get(self, file_name_ptr, gas)?),
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

        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        host_exports.abort(
            message,
            file_name,
            line_number,
            column_number,
            gas,
            &mut ctx.state,
        )
    }

    /// function store.set(entity: string, id: string, data: Entity): void
    pub async fn store_set(
        &mut self,
        gas: &GasCounter,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<AscEntity>,
    ) -> Result<(), HostExportError> {
        let stopwatch = self.as_ref().host_metrics.stopwatch.cheap_clone();
        let logger = self.as_ref().ctx.logger.cheap_clone();
        let block_number = self.as_ref().ctx.block_ptr.block_number();
        stopwatch.start_section("host_export_store_set__wasm_instance_context_store_set");

        let entity: String = asc_get(self, entity_ptr, gas)?;
        let id: String = asc_get(self, id_ptr, gas)?;
        let data = asc_get(self, data_ptr, gas)?;

        if self.as_ref().ctx.instrument {
            debug!(self.as_ref().ctx.logger, "store_set";
                    "type" => &entity,
                    "id" => &id);
        }

        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        host_exports
            .store_set(
                &logger,
                block_number,
                &mut ctx.state,
                &ctx.proof_of_indexing,
                ctx.timestamp,
                entity,
                id,
                data,
                &stopwatch,
                gas,
            )
            .await?;

        Ok(())
    }

    /// function store.remove(entity: string, id: string): void
    pub async fn store_remove(
        &mut self,
        gas: &GasCounter,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<(), HostExportError> {
        let logger = self.as_ref().ctx.logger.cheap_clone();

        let entity: String = asc_get(self, entity_ptr, gas)?;
        let id: String = asc_get(self, id_ptr, gas)?;
        if self.as_ref().ctx.instrument {
            debug!(self.as_ref().ctx.logger, "store_remove";
                    "type" => &entity,
                    "id" => &id);
        }
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        host_exports.store_remove(
            &logger,
            &mut ctx.state,
            &ctx.proof_of_indexing,
            entity,
            id,
            gas,
        )
    }

    /// function store.get(entity: string, id: string): Entity | null
    pub async fn store_get(
        &mut self,
        gas: &GasCounter,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscEntity>, HostExportError> {
        self.store_get_scoped(gas, entity_ptr, id_ptr, GetScope::Store)
            .await
    }

    /// function store.get_in_block(entity: string, id: string): Entity | null
    pub async fn store_get_in_block(
        &mut self,
        gas: &GasCounter,
        entity_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscEntity>, HostExportError> {
        self.store_get_scoped(gas, entity_ptr, id_ptr, GetScope::InBlock)
            .await
    }

    /// function store.loadRelated(entity_type: string, id: string, field: string): Array<Entity>
    pub async fn store_load_related(
        &mut self,

        gas: &GasCounter,
        entity_type_ptr: AscPtr<AscString>,
        id_ptr: AscPtr<AscString>,
        field_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<Array<AscPtr<AscEntity>>>, HostExportError> {
        let entity_type: String = asc_get(self, entity_type_ptr, gas)?;
        let id: String = asc_get(self, id_ptr, gas)?;
        let field: String = asc_get(self, field_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let entities = host_exports
            .store_load_related(
                &mut self.as_mut().ctx.state,
                entity_type.clone(),
                id.clone(),
                field.clone(),
                gas,
            )
            .await?;

        let entities: Vec<Vec<(Word, Value)>> =
            entities.into_iter().map(|entity| entity.sorted()).collect();
        let ret = asc_new(self, &entities, gas).await?;
        Ok(ret)
    }

    /// function typeConversion.bytesToString(bytes: Bytes): string
    pub async fn bytes_to_string(
        &mut self,
        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let bytes = asc_get(self, bytes_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let string = host_exports.bytes_to_string(&ctx.logger, bytes, gas, &mut ctx.state)?;
        asc_new(self, &string, gas).await
    }
    /// Converts bytes to a hex string.
    /// function typeConversion.bytesToHex(bytes: Bytes): string
    /// References:
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    /// https://github.com/ethereum/web3.js/blob/f98fe1462625a6c865125fecc9cb6b414f0a5e83/packages/web3-utils/src/utils.js#L283
    pub async fn bytes_to_hex(
        &mut self,
        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let bytes: Vec<u8> = asc_get(self, bytes_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        HostExports::track_gas_and_ops(
            gas,
            &mut ctx.state,
            gas::DEFAULT_GAS_OP.with_args(gas::complexity::Size, &bytes),
            "bytes_to_hex",
        )?;

        // Even an empty string must be prefixed with `0x`.
        // Encodes each byte as a two hex digits.
        let hex = format!("0x{}", hex::encode(bytes));
        asc_new(self, &hex, gas).await
    }

    /// function typeConversion.bigIntToString(n: Uint8Array): string
    pub async fn big_int_to_string(
        &mut self,
        gas: &GasCounter,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let n: BigInt = asc_get(self, big_int_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;
        HostExports::track_gas_and_ops(
            gas,
            &mut ctx.state,
            gas::DEFAULT_GAS_OP.with_args(gas::complexity::Mul, (&n, &n)),
            "big_int_to_string",
        )?;
        asc_new(self, &n.to_string(), gas).await
    }

    /// function bigInt.fromString(x: string): BigInt
    pub async fn big_int_from_string(
        &mut self,
        gas: &GasCounter,
        string_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let s = asc_get(self, string_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_int_from_string(s, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function typeConversion.bigIntToHex(n: Uint8Array): string
    pub async fn big_int_to_hex(
        &mut self,
        gas: &GasCounter,
        big_int_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let n: BigInt = asc_get(self, big_int_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let hex = host_exports.big_int_to_hex(n, gas, &mut ctx.state)?;
        asc_new(self, &hex, gas).await
    }

    /// function typeConversion.stringToH160(s: String): H160
    pub async fn string_to_h160(
        &mut self,
        gas: &GasCounter,
        str_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscH160>, HostExportError> {
        let s: String = asc_get(self, str_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let h160 = host_exports.string_to_address(&s, gas, &mut ctx.state)?;
        asc_new(self, &h160, gas).await
    }

    /// function json.fromBytes(bytes: Bytes): JSONValue
    pub async fn json_from_bytes(
        &mut self,
        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscEnum<JsonValueKind>>, HostExportError> {
        let bytes: Vec<u8> = asc_get(self, bytes_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports
            .json_from_bytes(&bytes, gas, &mut ctx.state)
            .with_context(|| {
                format!(
                    "Failed to parse JSON from byte array. Bytes (truncated to 1024 chars): `{:?}`",
                    &bytes[..bytes.len().min(1024)],
                )
            })
            .map_err(DeterministicHostError::from)?;
        asc_new(self, &result, gas).await
    }

    /// function json.try_fromBytes(bytes: Bytes): Result<JSONValue, boolean>
    pub async fn json_try_from_bytes(
        &mut self,
        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscResult<AscPtr<AscEnum<JsonValueKind>>, bool>>, HostExportError> {
        let bytes: Vec<u8> = asc_get(self, bytes_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports
            .json_from_bytes(&bytes, gas, &mut ctx.state)
            .map_err(|e| {
                warn!(
                    &self.as_ref().ctx.logger,
                    "Failed to parse JSON from byte array";
                    "bytes" => format!("{:?}", bytes),
                    "error" => format!("{}", e)
                );

                // Map JSON errors to boolean to match the `Result<JSONValue, boolean>`
                // result type expected by mappings
                true
            });
        asc_new(self, &result, gas).await
    }

    /// function ipfs.cat(link: String): Bytes
    pub async fn ipfs_cat(
        &mut self,
        gas: &GasCounter,
        link_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        // Note on gas: There is no gas costing for the ipfs call itself,
        // since it's not enabled on the network.

        if !self
            .as_ref()
            .experimental_features
            .allow_non_deterministic_ipfs
        {
            return Err(HostExportError::Deterministic(anyhow!(
                "`ipfs.cat` is deprecated. Improved support for IPFS will be added in the future"
            )));
        }

        let link = asc_get(self, link_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let logger = self.as_ref().ctx.logger.cheap_clone();
        let ipfs_res = host_exports.ipfs_cat(&logger, link).await;
        let logger = self.as_ref().ctx.logger.cheap_clone();
        match ipfs_res {
            Ok(bytes) => asc_new(self, &*bytes, gas).await,

            // Return null in case of error.
            Err(e) => {
                info!(&logger, "Failed ipfs.cat, returning `null`";
                                    "link" => asc_get::<String, _, _>( self, link_ptr, gas)?,
                                    "error" => e.to_string());
                Ok(AscPtr::null())
            }
        }
    }

    /// function ipfs.getBlock(link: String): Bytes
    pub async fn ipfs_get_block(
        &mut self,
        gas: &GasCounter,
        link_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        // Note on gas: There is no gas costing for the ipfs call itself,
        // since it's not enabled on the network.

        if !self
            .as_ref()
            .experimental_features
            .allow_non_deterministic_ipfs
        {
            return Err(HostExportError::Deterministic(anyhow!(
                "`ipfs.getBlock` is deprecated. Improved support for IPFS will be added in the future"
            )));
        }

        let link = asc_get(self, link_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ipfs_res = host_exports
            .ipfs_get_block(&self.as_ref().ctx.logger, link)
            .await;
        match ipfs_res {
            Ok(bytes) => asc_new(self, &*bytes, gas).await,

            // Return null in case of error.
            Err(e) => {
                info!(&self.as_ref().ctx.logger, "Failed ipfs.getBlock, returning `null`";
                                    "link" => asc_get::<String, _, _>( self, link_ptr, gas)?,
                                    "error" => e.to_string());
                Ok(AscPtr::null())
            }
        }
    }

    /// function ipfs.map(link: String, callback: String, flags: String[]): void
    pub async fn ipfs_map(
        &mut self,
        gas: &GasCounter,
        link_ptr: AscPtr<AscString>,
        callback: AscPtr<AscString>,
        user_data: AscPtr<AscEnum<StoreValueKind>>,
        flags: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<(), HostExportError> {
        // Note on gas:
        // Ideally we would consume gas the same as ipfs_cat and then share
        // gas across the spawned modules for callbacks.

        if !self
            .as_ref()
            .experimental_features
            .allow_non_deterministic_ipfs
        {
            return Err(HostExportError::Deterministic(anyhow!(
                "`ipfs.map` is deprecated. Improved support for IPFS will be added in the future"
            )));
        }

        let link: String = asc_get(self, link_ptr, gas)?;
        let callback: String = asc_get(self, callback, gas)?;
        let user_data: store::Value = asc_get(self, user_data, gas)?;

        let flags = asc_get(self, flags, gas)?;

        // Pause the timeout while running ipfs_map, and resume it when done.
        self.suspend_timeout();
        let start_time = Instant::now();
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let output_states = host_exports
            .ipfs_map(self.as_ref(), link.clone(), &callback, user_data, flags)
            .await?;
        self.start_timeout();

        debug!(
            &self.as_ref().ctx.logger,
            "Successfully processed file with ipfs.map";
            "link" => &link,
            "callback" => &*callback,
            "n_calls" => output_states.len(),
            "time" => format!("{}ms", start_time.elapsed().as_millis())
        );
        for output_state in output_states {
            self.as_mut().ctx.state.extend(output_state);
        }

        Ok(())
    }

    /// Expects a decimal string.
    /// function json.toI64(json: String): i64
    pub async fn json_to_i64(
        &mut self,
        gas: &GasCounter,
        json_ptr: AscPtr<AscString>,
    ) -> Result<i64, DeterministicHostError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let json = asc_get(self, json_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;
        host_exports.json_to_i64(json, gas, &mut ctx.state)
    }

    /// Expects a decimal string.
    /// function json.toU64(json: String): u64
    pub async fn json_to_u64(
        &mut self,

        gas: &GasCounter,
        json_ptr: AscPtr<AscString>,
    ) -> Result<u64, DeterministicHostError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let json: String = asc_get(self, json_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;
        host_exports.json_to_u64(json, gas, &mut ctx.state)
    }

    /// Expects a decimal string.
    /// function json.toF64(json: String): f64
    pub async fn json_to_f64(
        &mut self,
        gas: &GasCounter,
        json_ptr: AscPtr<AscString>,
    ) -> Result<f64, DeterministicHostError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let json = asc_get(self, json_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;
        host_exports.json_to_f64(json, gas, &mut ctx.state)
    }

    /// Expects a decimal string.
    /// function json.toBigInt(json: String): BigInt
    pub async fn json_to_big_int(
        &mut self,

        gas: &GasCounter,
        json_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let json = asc_get(self, json_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;
        let big_int = host_exports.json_to_big_int(json, gas, &mut ctx.state)?;
        asc_new(self, &*big_int, gas).await
    }

    /// function crypto.keccak256(input: Bytes): Bytes
    pub async fn crypto_keccak_256(
        &mut self,

        gas: &GasCounter,
        input_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let input = asc_get(self, input_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let input = host_exports.crypto_keccak_256(input, gas, &mut ctx.state)?;
        asc_new(self, input.as_ref(), gas).await
    }

    /// function bigInt.plus(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_plus(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_int_plus(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.minus(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_minus(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_int_minus(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.times(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_times(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_int_times(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.dividedBy(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_divided_by(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_int_divided_by(x, y, gas, &mut ctx.state)?;

        asc_new(self, &result, gas).await
    }

    /// function bigInt.dividedByDecimal(x: BigInt, y: BigDecimal): BigDecimal
    pub async fn big_int_divided_by_decimal(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, HostExportError> {
        let x = BigDecimal::new(asc_get(self, x_ptr, gas)?, 0);

        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_decimal_divided_by(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.mod(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_mod(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_int_mod(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.pow(x: BigInt, exp: u8): BigInt
    pub async fn big_int_pow(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        exp: u32,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let exp = u8::try_from(exp).map_err(|e| DeterministicHostError::from(Error::from(e)))?;
        let x = asc_get(self, x_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();

        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_int_pow(x, exp, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.bitOr(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_bit_or(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_int_bit_or(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.bitAnd(x: BigInt, y: BigInt): BigInt
    pub async fn big_int_bit_and(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        y_ptr: AscPtr<AscBigInt>,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_int_bit_and(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.leftShift(x: BigInt, bits: u8): BigInt
    pub async fn big_int_left_shift(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        bits: u32,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let bits = u8::try_from(bits).map_err(|e| DeterministicHostError::from(Error::from(e)))?;
        let x = asc_get(self, x_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_int_left_shift(x, bits, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigInt.rightShift(x: BigInt, bits: u8): BigInt
    pub async fn big_int_right_shift(
        &mut self,

        gas: &GasCounter,
        x_ptr: AscPtr<AscBigInt>,
        bits: u32,
    ) -> Result<AscPtr<AscBigInt>, HostExportError> {
        let bits = u8::try_from(bits).map_err(|e| DeterministicHostError::from(Error::from(e)))?;
        let x = asc_get(self, x_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();

        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_int_right_shift(x, bits, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function typeConversion.bytesToBase58(bytes: Bytes): string
    pub async fn bytes_to_base58(
        &mut self,

        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let bytes = asc_get(self, bytes_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.bytes_to_base58(bytes, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.toString(x: BigDecimal): string
    pub async fn big_decimal_to_string(
        &mut self,

        gas: &GasCounter,
        big_decimal_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let x = asc_get(self, big_decimal_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_decimal_to_string(x, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.fromString(x: string): BigDecimal
    pub async fn big_decimal_from_string(
        &mut self,

        gas: &GasCounter,
        string_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscBigDecimal>, HostExportError> {
        let s = asc_get(self, string_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.big_decimal_from_string(s, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.plus(x: BigDecimal, y: BigDecimal): BigDecimal
    pub async fn big_decimal_plus(
        &mut self,
        gas: &GasCounter,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_decimal_plus(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.minus(x: BigDecimal, y: BigDecimal): BigDecimal
    pub async fn big_decimal_minus(
        &mut self,
        gas: &GasCounter,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_decimal_minus(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.times(x: BigDecimal, y: BigDecimal): BigDecimal
    pub async fn big_decimal_times(
        &mut self,
        gas: &GasCounter,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_decimal_times(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.dividedBy(x: BigDecimal, y: BigDecimal): BigDecimal
    pub async fn big_decimal_divided_by(
        &mut self,
        gas: &GasCounter,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<AscPtr<AscBigDecimal>, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports.big_decimal_divided_by(x, y, gas, &mut ctx.state)?;
        asc_new(self, &result, gas).await
    }

    /// function bigDecimal.equals(x: BigDecimal, y: BigDecimal): bool
    pub async fn big_decimal_equals(
        &mut self,
        gas: &GasCounter,
        x_ptr: AscPtr<AscBigDecimal>,
        y_ptr: AscPtr<AscBigDecimal>,
    ) -> Result<bool, HostExportError> {
        let x = asc_get(self, x_ptr, gas)?;
        let y = asc_get(self, y_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        host_exports.big_decimal_equals(x, y, gas, &mut ctx.state)
    }

    /// function dataSource.create(name: string, params: Array<string>): void
    pub async fn data_source_create(
        &mut self,
        gas: &GasCounter,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
    ) -> Result<(), HostExportError> {
        let logger = self.as_ref().ctx.logger.cheap_clone();
        let block_number = self.as_ref().ctx.block_ptr.number;
        let name: String = asc_get(self, name_ptr, gas)?;
        let params: Vec<String> = asc_get(self, params_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        host_exports.data_source_create(
            &logger,
            &mut self.as_mut().ctx.state,
            name,
            params,
            None,
            block_number,
            gas,
        )
    }

    /// function createWithContext(name: string, params: Array<string>, context: DataSourceContext): void
    pub async fn data_source_create_with_context(
        &mut self,
        gas: &GasCounter,
        name_ptr: AscPtr<AscString>,
        params_ptr: AscPtr<Array<AscPtr<AscString>>>,
        context_ptr: AscPtr<AscEntity>,
    ) -> Result<(), HostExportError> {
        let logger = self.as_ref().ctx.logger.cheap_clone();
        let block_number = self.as_ref().ctx.block_ptr.number;
        let name: String = asc_get(self, name_ptr, gas)?;
        let params: Vec<String> = asc_get(self, params_ptr, gas)?;
        let context: HashMap<_, _> = asc_get(self, context_ptr, gas)?;
        let context = DataSourceContext::from(context);

        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        host_exports.data_source_create(
            &logger,
            &mut self.as_mut().ctx.state,
            name,
            params,
            Some(context),
            block_number,
            gas,
        )
    }

    /// function dataSource.address(): Bytes
    pub async fn data_source_address(
        &mut self,
        gas: &GasCounter,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let addr = host_exports.data_source_address(gas, &mut ctx.state)?;
        asc_new(self, addr.as_slice(), gas).await
    }

    /// function dataSource.network(): String
    pub async fn data_source_network(
        &mut self,
        gas: &GasCounter,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let data_source_network = host_exports.data_source_network(gas, &mut ctx.state)?;
        asc_new(self, &data_source_network, gas).await
    }

    /// function dataSource.context(): DataSourceContext
    pub async fn data_source_context(
        &mut self,
        gas: &GasCounter,
    ) -> Result<AscPtr<AscEntity>, HostExportError> {
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let ds_ctx = &host_exports
            .data_source_context(gas, &mut ctx.state)?
            .map(|e| e.sorted())
            .unwrap_or(vec![]);

        asc_new(self, &ds_ctx, gas).await
    }

    pub async fn ens_name_by_hash(
        &mut self,
        gas: &GasCounter,
        hash_ptr: AscPtr<AscString>,
    ) -> Result<AscPtr<AscString>, HostExportError> {
        let hash: String = asc_get(self, hash_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let name = host_exports
            .ens_name_by_hash(&hash, gas, &mut ctx.state)
            .await?;
        if name.is_none() && self.as_ref().ctx.host_exports.is_ens_data_empty().await? {
            return Err(anyhow!(
                "Missing ENS data: see https://github.com/graphprotocol/ens-rainbow"
            )
            .into());
        }

        // map `None` to `null`, and `Some(s)` to a runtime string
        match name {
            Some(name) => asc_new(self, &*name, gas).await,
            None => Ok(AscPtr::null()),
        }
    }

    pub async fn log_log(
        &mut self,
        gas: &GasCounter,
        level: u32,
        msg: AscPtr<AscString>,
    ) -> Result<(), DeterministicHostError> {
        let level = LogLevel::from(level).into();
        let msg: String = asc_get(self, msg, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        host_exports.log_log(&ctx.mapping_logger, level, msg, gas, &mut ctx.state)
    }

    /// function encode(token: ethereum.Value): Bytes | null
    pub async fn ethereum_encode(
        &mut self,
        gas: &GasCounter,
        token_ptr: AscPtr<AscEnum<EthereumValueKind>>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        let token = asc_get(self, token_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let data = host_exports.ethereum_encode(token, gas, &mut ctx.state);
        // return `null` if it fails
        match data {
            Ok(bytes) => asc_new(self, &*bytes, gas).await,
            Err(_) => Ok(AscPtr::null()),
        }
    }

    /// function decode(types: String, data: Bytes): ethereum.Value | null
    pub async fn ethereum_decode(
        &mut self,
        gas: &GasCounter,
        types_ptr: AscPtr<AscString>,
        data_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscEnum<EthereumValueKind>>, HostExportError> {
        let types = asc_get(self, types_ptr, gas)?;
        let data = asc_get(self, data_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;
        let result = host_exports.ethereum_decode(types, data, gas, &mut ctx.state);

        // return `null` if it fails
        match result {
            Ok(token) => asc_new(self, &token, gas).await,
            Err(_) => Ok(AscPtr::null()),
        }
    }

    /// function arweave.transactionData(txId: string): Bytes | null
    pub async fn arweave_transaction_data(
        &self,
        _gas: &GasCounter,
        _tx_id: AscPtr<AscString>,
    ) -> Result<AscPtr<Uint8Array>, HostExportError> {
        Err(HostExportError::Deterministic(anyhow!(
            "`arweave.transactionData` has been removed."
        )))
    }

    /// function box.profile(address: string): JSONValue | null
    pub async fn box_profile(
        &self,
        _gas: &GasCounter,
        _address: AscPtr<AscString>,
    ) -> Result<AscPtr<AscJson>, HostExportError> {
        Err(HostExportError::Deterministic(anyhow!(
            "`box.profile` has been removed."
        )))
    }

    /// function yaml.fromBytes(bytes: Bytes): YAMLValue
    pub async fn yaml_from_bytes(
        &mut self,
        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscEnum<YamlValueKind>>, HostExportError> {
        let bytes: Vec<u8> = asc_get(self, bytes_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let yaml_value = host_exports
            .yaml_from_bytes(&bytes, gas, &mut ctx.state)
            .inspect_err(|_| {
                debug!(
                    &self.as_ref().ctx.logger,
                    "Failed to parse YAML from byte array";
                    "bytes" => truncate_yaml_bytes_for_logging(&bytes),
                );
            })?;

        asc_new(self, &yaml_value, gas).await
    }

    /// function yaml.try_fromBytes(bytes: Bytes): Result<YAMLValue, bool>
    pub async fn yaml_try_from_bytes(
        &mut self,
        gas: &GasCounter,
        bytes_ptr: AscPtr<Uint8Array>,
    ) -> Result<AscPtr<AscResult<AscPtr<AscEnum<YamlValueKind>>, bool>>, HostExportError> {
        let bytes: Vec<u8> = asc_get(self, bytes_ptr, gas)?;
        let host_exports = self.as_ref().ctx.host_exports.cheap_clone();
        let ctx = &mut self.as_mut().ctx;

        let result = host_exports
            .yaml_from_bytes(&bytes, gas, &mut ctx.state)
            .map_err(|err| {
                warn!(
                    &self.as_ref().ctx.logger,
                    "Failed to parse YAML from byte array";
                    "bytes" => truncate_yaml_bytes_for_logging(&bytes),
                    "error" => format!("{:#}", err),
                );

                true
            });

        asc_new(self, &result, gas).await
    }
}

/// For debugging, it might be useful to know exactly which bytes could not be parsed as YAML, but
/// since we can parse large YAML documents, even one bad mapping could produce terabytes of logs.
/// To avoid this, we only log the first 1024 bytes of the failed YAML source.
fn truncate_yaml_bytes_for_logging(bytes: &[u8]) -> String {
    if bytes.len() > 1024 {
        return format!("(truncated) 0x{}", hex::encode(&bytes[..1024]));
    }

    format!("0x{}", hex::encode(bytes))
}
