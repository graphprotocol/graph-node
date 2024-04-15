use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::time::{Duration, Instant};

use graph::data::subgraph::API_VERSION_0_0_8;
use graph::data::value::Word;

use graph::futures03::stream::StreamExt;
use graph::schema::EntityType;
use never::Never;
use semver::Version;
use web3::types::H160;

use graph::blockchain::BlockTime;
use graph::blockchain::Blockchain;
use graph::components::store::{EnsLookup, GetScope, LoadRelatedRequest};
use graph::components::subgraph::{
    InstanceDSTemplate, PoICausalityRegion, ProofOfIndexingEvent, SharedProofOfIndexing,
};
use graph::data::store::{self};
use graph::data_source::{CausalityRegion, DataSource, EntityTypeAccess};
use graph::ensure;
use graph::prelude::ethabi::param_type::Reader;
use graph::prelude::ethabi::{decode, encode, Token};
use graph::prelude::serde_json;
use graph::prelude::{slog::b, slog::record_static, *};
use graph::runtime::gas::{self, complexity, Gas, GasCounter};
pub use graph::runtime::{DeterministicHostError, HostExportError};

use crate::module::WasmInstance;
use crate::{error::DeterminismLevel, module::IntoTrap};

use super::module::WasmInstanceData;

fn write_poi_event(
    proof_of_indexing: &SharedProofOfIndexing,
    poi_event: &ProofOfIndexingEvent,
    causality_region: &str,
    logger: &Logger,
) {
    if let Some(proof_of_indexing) = proof_of_indexing {
        let mut proof_of_indexing = proof_of_indexing.deref().borrow_mut();
        proof_of_indexing.write(logger, causality_region, poi_event);
    }
}

impl IntoTrap for HostExportError {
    fn determinism_level(&self) -> DeterminismLevel {
        match self {
            HostExportError::Deterministic(_) => DeterminismLevel::Deterministic,
            HostExportError::Unknown(_) => DeterminismLevel::Unimplemented,
            HostExportError::PossibleReorg(_) => DeterminismLevel::PossibleReorg,
        }
    }
}

pub struct HostExports {
    pub(crate) subgraph_id: DeploymentHash,
    subgraph_network: String,
    pub data_source: DataSourceDetails,

    /// Some data sources have indeterminism or different notions of time. These
    /// need to be each be stored separately to separate causality between them,
    /// and merge the results later. Right now, this is just the ethereum
    /// networks but will be expanded for ipfs and the availability chain.
    poi_causality_region: String,
    pub(crate) link_resolver: Arc<dyn LinkResolver>,
    ens_lookup: Arc<dyn EnsLookup>,
}

pub struct DataSourceDetails {
    pub api_version: Version,
    pub name: String,
    pub address: Vec<u8>,
    pub context: Arc<Option<DataSourceContext>>,
    pub entity_type_access: EntityTypeAccess,
    pub templates: Arc<Vec<InstanceDSTemplate>>,
    pub causality_region: CausalityRegion,
}

impl DataSourceDetails {
    pub fn from_data_source<C: Blockchain>(
        ds: &DataSource<C>,
        templates: Arc<Vec<InstanceDSTemplate>>,
    ) -> Self {
        Self {
            api_version: ds.api_version(),
            name: ds.name().to_string(),
            address: ds.address().unwrap_or_default(),
            context: ds.context(),
            entity_type_access: ds.entities(),
            templates,
            causality_region: ds.causality_region(),
        }
    }
}

impl HostExports {
    pub fn new(
        subgraph_id: DeploymentHash,
        subgraph_network: String,
        data_source_details: DataSourceDetails,
        link_resolver: Arc<dyn LinkResolver>,
        ens_lookup: Arc<dyn EnsLookup>,
    ) -> Self {
        Self {
            subgraph_id,
            data_source: data_source_details,
            poi_causality_region: PoICausalityRegion::from_network(&subgraph_network),
            subgraph_network,
            link_resolver,
            ens_lookup,
        }
    }

    pub fn track_gas_and_ops(
        gas: &GasCounter,
        state: &mut BlockState,
        gas_used: Gas,
        method: &str,
    ) -> Result<(), DeterministicHostError> {
        gas.consume_host_fn_with_metrics(gas_used, method)?;

        state.metrics.track_gas_and_ops(gas_used, method);

        Ok(())
    }

    /// Enfore the entity type access restrictions. See also: entity-type-access
    fn check_entity_type_access(&self, entity_type: &EntityType) -> Result<(), HostExportError> {
        match self.data_source.entity_type_access.allows(entity_type) {
            true => Ok(()),
            false => Err(HostExportError::Deterministic(anyhow!(
                "entity type `{}` is not on the 'entities' list for data source `{}`. \
                 Hint: Add `{}` to the 'entities' list, which currently is: `{}`.",
                entity_type,
                self.data_source.name,
                entity_type,
                self.data_source.entity_type_access
            ))),
        }
    }

    pub(crate) fn abort(
        &self,
        message: Option<String>,
        file_name: Option<String>,
        line_number: Option<u32>,
        column_number: Option<u32>,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Never, DeterministicHostError> {
        Self::track_gas_and_ops(gas, state, Gas::new(gas::DEFAULT_BASE_COST), "abort")?;

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
        Err(DeterministicHostError::from(anyhow::anyhow!(
            "Mapping aborted at {}, with {}",
            location,
            message
        )))
    }

    fn check_invalid_fields(
        &self,
        api_version: Version,
        data: &HashMap<Word, Value>,
        state: &BlockState,
        entity_type: &EntityType,
    ) -> Result<(), HostExportError> {
        if api_version >= API_VERSION_0_0_8 {
            let has_invalid_fields = data.iter().any(|(field_name, _)| {
                !state
                    .entity_cache
                    .schema
                    .has_field_with_name(entity_type, &field_name)
            });

            if has_invalid_fields {
                let mut invalid_fields: Vec<Word> = data
                    .iter()
                    .filter_map(|(field_name, _)| {
                        if !state
                            .entity_cache
                            .schema
                            .has_field_with_name(entity_type, &field_name)
                        {
                            Some(field_name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                invalid_fields.sort();

                return Err(HostExportError::Deterministic(anyhow!(
                    "Attempted to set undefined fields [{}] for the entity type `{}`. Make sure those fields are defined in the schema.",
                    invalid_fields
                        .iter()
                        .map(|f| f.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    entity_type
                )));
            }
        }

        Ok(())
    }

    /// Ensure that `entity_type` is of the right kind
    fn expect_object_type(entity_type: &EntityType, op: &str) -> Result<(), HostExportError> {
        if entity_type.is_object_type() {
            return Ok(());
        }
        Err(HostExportError::Deterministic(anyhow!(
            "Cannot {op} entity of type `{}`. The type must be an @entity type",
            entity_type.as_str()
        )))
    }

    pub(crate) fn store_set(
        &self,
        logger: &Logger,
        block: BlockNumber,
        state: &mut BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        block_time: BlockTime,
        entity_type: String,
        entity_id: String,
        mut data: HashMap<Word, Value>,
        stopwatch: &StopwatchMetrics,
        gas: &GasCounter,
    ) -> Result<(), HostExportError> {
        let entity_type = state.entity_cache.schema.entity_type(&entity_type)?;

        Self::expect_object_type(&entity_type, "set")?;

        let entity_id = if entity_id == "auto"
            || entity_type
                .object_type()
                .map(|ot| ot.timeseries)
                .unwrap_or(false)
        {
            if self.data_source.causality_region != CausalityRegion::ONCHAIN {
                return Err(anyhow!(
                    "Autogenerated IDs are only supported for onchain data sources"
                )
                .into());
            }
            let id_type = entity_type.id_type()?;
            let id = state.entity_cache.generate_id(id_type, block)?;
            data.insert(store::ID.clone(), id.clone().into());
            id.to_string()
        } else {
            entity_id
        };

        let key = entity_type.parse_key_in(entity_id, self.data_source.causality_region)?;
        self.check_entity_type_access(&key.entity_type)?;

        Self::track_gas_and_ops(
            gas,
            state,
            gas::STORE_SET.with_args(complexity::Linear, (&key, &data)),
            "store_set",
        )?;

        if entity_type.object_type()?.timeseries {
            data.insert(Word::from("timestamp"), block_time.into());
        }

        // Set the id if there isn't one yet, and make sure that a
        // previously set id agrees with the one in the `key`
        match data.get(&store::ID) {
            Some(v) => {
                if v != &key.entity_id {
                    if v.type_name() != key.entity_id.id_type().as_str() {
                        return Err(anyhow!(
                            "Attribute `{}.id` has wrong type: expected {} but got {}",
                            key.entity_type,
                            key.entity_id.id_type().as_str(),
                            v.type_name(),
                        )
                        .into());
                    }
                    return Err(anyhow!(
                        "Value of {} attribute 'id' conflicts with ID passed to `store.set()`: \
                    {:?} != {:?}",
                        key.entity_type,
                        v,
                        key.entity_id,
                    )
                    .into());
                }
            }
            None => {
                let value = Value::from(key.entity_id.clone());
                data.insert(store::ID.clone(), value);
            }
        }

        self.check_invalid_fields(
            self.data_source.api_version.clone(),
            &data,
            state,
            &key.entity_type,
        )?;

        // Filter out fields that are not in the schema
        let filtered_entity_data = data.into_iter().filter(|(field_name, _)| {
            state
                .entity_cache
                .schema
                .has_field_with_name(&key.entity_type, field_name)
        });

        let entity = state
            .entity_cache
            .make_entity(filtered_entity_data)
            .map_err(|e| HostExportError::Deterministic(anyhow!(e)))?;

        let poi_section = stopwatch.start_section("host_export_store_set__proof_of_indexing");
        write_poi_event(
            proof_of_indexing,
            &ProofOfIndexingEvent::SetEntity {
                entity_type: &key.entity_type.typename(),
                id: &key.entity_id.to_string(),
                data: &entity,
            },
            &self.poi_causality_region,
            logger,
        );
        poi_section.end();

        state.metrics.track_entity_write(&entity_type, &entity);

        state.entity_cache.set(key, entity)?;

        Ok(())
    }

    pub(crate) fn store_remove(
        &self,
        logger: &Logger,
        state: &mut BlockState,
        proof_of_indexing: &SharedProofOfIndexing,
        entity_type: String,
        entity_id: String,
        gas: &GasCounter,
    ) -> Result<(), HostExportError> {
        write_poi_event(
            proof_of_indexing,
            &ProofOfIndexingEvent::RemoveEntity {
                entity_type: &entity_type,
                id: &entity_id,
            },
            &self.poi_causality_region,
            logger,
        );
        let entity_type = state.entity_cache.schema.entity_type(&entity_type)?;
        Self::expect_object_type(&entity_type, "remove")?;

        let key = entity_type.parse_key_in(entity_id, self.data_source.causality_region)?;
        self.check_entity_type_access(&key.entity_type)?;

        Self::track_gas_and_ops(
            gas,
            state,
            gas::STORE_REMOVE.with_args(complexity::Size, &key),
            "store_remove",
        )?;

        state.entity_cache.remove(key);

        Ok(())
    }

    pub(crate) fn store_get<'a>(
        &self,
        state: &'a mut BlockState,
        entity_type: String,
        entity_id: String,
        gas: &GasCounter,
        scope: GetScope,
    ) -> Result<Option<Arc<Entity>>, anyhow::Error> {
        let entity_type = state.entity_cache.schema.entity_type(&entity_type)?;
        Self::expect_object_type(&entity_type, "get")?;

        let store_key = entity_type.parse_key_in(entity_id, self.data_source.causality_region)?;
        self.check_entity_type_access(&store_key.entity_type)?;

        let result = state.entity_cache.get(&store_key, scope)?;

        Self::track_gas_and_ops(
            gas,
            state,
            gas::STORE_GET.with_args(
                complexity::Linear,
                (&store_key, result.as_ref().map(|e| e.as_ref())),
            ),
            "store_get",
        )?;

        if let Some(ref entity) = result {
            state.metrics.track_entity_read(&entity_type, &entity)
        }

        Ok(result)
    }

    pub(crate) fn store_load_related(
        &self,
        state: &mut BlockState,
        entity_type: String,
        entity_id: String,
        entity_field: String,
        gas: &GasCounter,
    ) -> Result<Vec<Entity>, anyhow::Error> {
        let entity_type = state.entity_cache.schema.entity_type(&entity_type)?;
        let key = entity_type.parse_key_in(entity_id, self.data_source.causality_region)?;
        let store_key = LoadRelatedRequest {
            entity_type: key.entity_type,
            entity_id: key.entity_id,
            entity_field: entity_field.into(),
            causality_region: self.data_source.causality_region,
        };
        self.check_entity_type_access(&store_key.entity_type)?;

        let result = state.entity_cache.load_related(&store_key)?;

        Self::track_gas_and_ops(
            gas,
            state,
            gas::STORE_GET.with_args(complexity::Linear, (&store_key, &result)),
            "store_load_related",
        )?;

        state.metrics.track_entity_read_batch(&entity_type, &result);

        Ok(result)
    }

    /// Prints the module of `n` in hex.
    /// Integers are encoded using the least amount of digits (no leading zero digits).
    /// Their encoding may be of uneven length. The number zero encodes as "0x0".
    ///
    /// https://godoc.org/github.com/ethereum/go-ethereum/common/hexutil#hdr-Encoding_Rules
    pub(crate) fn big_int_to_hex(
        &self,
        n: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<String, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &n),
            "big_int_to_hex",
        )?;

        if n == 0.into() {
            return Ok("0x0".to_string());
        }

        let bytes = n.to_bytes_be().1;
        Ok(format!(
            "0x{}",
            ::hex::encode(bytes).trim_start_matches('0')
        ))
    }

    pub(crate) fn ipfs_cat(&self, logger: &Logger, link: String) -> Result<Vec<u8>, anyhow::Error> {
        // Does not consume gas because this is not a part of the deterministic feature set.
        // Ideally this would first consume gas for fetching the file stats, and then again
        // for the bytes of the file.
        graph::block_on(self.link_resolver.cat(logger, &Link { link }))
    }

    pub(crate) fn ipfs_get_block(
        &self,
        logger: &Logger,
        link: String,
    ) -> Result<Vec<u8>, anyhow::Error> {
        // Does not consume gas because this is not a part of the deterministic feature set.
        // Ideally this would first consume gas for fetching the file stats, and then again
        // for the bytes of the file.
        graph::block_on(self.link_resolver.get_block(logger, &Link { link }))
    }

    // Read the IPFS file `link`, split it into JSON objects, and invoke the
    // exported function `callback` on each JSON object. The successful return
    // value contains the block state produced by each callback invocation. Each
    // invocation of `callback` happens in its own instance of a WASM module,
    // which is identical to `module` when it was first started. The signature
    // of the callback must be `callback(JSONValue, Value)`, and the `userData`
    // parameter is passed to the callback without any changes
    pub(crate) fn ipfs_map(
        link_resolver: &Arc<dyn LinkResolver>,
        wasm_ctx: &WasmInstanceData,
        link: String,
        callback: &str,
        user_data: store::Value,
        flags: Vec<String>,
    ) -> Result<Vec<BlockState>, anyhow::Error> {
        // Does not consume gas because this is not a part of deterministic APIs.
        // Ideally we would consume gas the same as ipfs_cat and then share
        // gas across the spawned modules for callbacks.

        const JSON_FLAG: &str = "json";
        ensure!(
            flags.contains(&JSON_FLAG.to_string()),
            "Flags must contain 'json'"
        );

        let host_metrics = wasm_ctx.host_metrics.clone();
        let valid_module = wasm_ctx.valid_module.clone();
        let ctx = wasm_ctx.ctx.derive_with_empty_block_state();
        let callback = callback.to_owned();
        // Create a base error message to avoid borrowing headaches
        let errmsg = format!(
            "ipfs_map: callback '{}' failed when processing file '{}'",
            &*callback, &link
        );

        let start = Instant::now();
        let mut last_log = start;
        let logger = ctx.logger.new(o!("ipfs_map" => link.clone()));

        let result = {
            let mut stream: JsonValueStream =
                graph::block_on(link_resolver.json_stream(&logger, &Link { link }))?;
            let mut v = Vec::new();
            while let Some(sv) = graph::block_on(stream.next()) {
                let sv = sv?;
                let module = WasmInstance::from_valid_module_with_ctx(
                    valid_module.clone(),
                    ctx.derive_with_empty_block_state(),
                    host_metrics.clone(),
                    wasm_ctx.experimental_features,
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
        };
        result.map_err(move |e: Error| anyhow::anyhow!("{}: {}", errmsg, e.to_string()))
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_i64(
        &self,
        json: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<i64, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json),
            "json_to_i64",
        )?;
        i64::from_str(&json)
            .with_context(|| format!("JSON `{}` cannot be parsed as i64", json))
            .map_err(DeterministicHostError::from)
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_u64(
        &self,
        json: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<u64, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json),
            "json_to_u64",
        )?;

        u64::from_str(&json)
            .with_context(|| format!("JSON `{}` cannot be parsed as u64", json))
            .map_err(DeterministicHostError::from)
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_f64(
        &self,
        json: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<f64, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json),
            "json_to_f64",
        )?;

        f64::from_str(&json)
            .with_context(|| format!("JSON `{}` cannot be parsed as f64", json))
            .map_err(DeterministicHostError::from)
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_big_int(
        &self,
        json: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Vec<u8>, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json),
            "json_to_big_int",
        )?;

        let big_int = BigInt::from_str(&json)
            .with_context(|| format!("JSON `{}` is not a decimal string", json))
            .map_err(DeterministicHostError::from)?;
        Ok(big_int.to_signed_bytes_le())
    }

    pub(crate) fn crypto_keccak_256(
        &self,
        input: Vec<u8>,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<[u8; 32], DeterministicHostError> {
        let data = &input[..];
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, data),
            "crypto_keccak_256",
        )?;
        Ok(tiny_keccak::keccak256(data))
    }

    pub(crate) fn big_int_plus(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Max, (&x, &y)),
            "big_int_plus",
        )?;
        Ok(x + y)
    }

    pub(crate) fn big_int_minus(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Max, (&x, &y)),
            "big_int_minus",
        )?;
        Ok(x - y)
    }

    pub(crate) fn big_int_times(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)),
            "big_int_times",
        )?;
        Ok(x * y)
    }

    pub(crate) fn big_int_divided_by(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)),
            "big_int_divided_by",
        )?;
        if y == 0.into() {
            return Err(DeterministicHostError::from(anyhow!(
                "attempted to divide BigInt `{}` by zero",
                x
            )));
        }
        Ok(x / y)
    }

    pub(crate) fn big_int_mod(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)),
            "big_int_mod",
        )?;
        if y == 0.into() {
            return Err(DeterministicHostError::from(anyhow!(
                "attempted to calculate the remainder of `{}` with a divisor of zero",
                x
            )));
        }
        Ok(x % y)
    }

    /// Limited to a small exponent to avoid creating huge BigInts.
    pub(crate) fn big_int_pow(
        &self,
        x: BigInt,
        exp: u8,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP
                .with_args(complexity::Exponential, (&x, (exp as f32).log2() as u8)),
            "big_int_pow",
        )?;
        Ok(x.pow(exp)?)
    }

    pub(crate) fn big_int_from_string(
        &self,
        s: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &s),
            "big_int_from_string",
        )?;
        BigInt::from_str(&s)
            .with_context(|| format!("string is not a BigInt: `{}`", s))
            .map_err(DeterministicHostError::from)
    }

    pub(crate) fn big_int_bit_or(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Max, (&x, &y)),
            "big_int_bit_or",
        )?;
        Ok(x | y)
    }

    pub(crate) fn big_int_bit_and(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Min, (&x, &y)),
            "big_int_bit_and",
        )?;
        Ok(x & y)
    }

    pub(crate) fn big_int_left_shift(
        &self,
        x: BigInt,
        bits: u8,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &bits)),
            "big_int_left_shift",
        )?;
        Ok(x << bits)
    }

    pub(crate) fn big_int_right_shift(
        &self,
        x: BigInt,
        bits: u8,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigInt, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &bits)),
            "big_int_right_shift",
        )?;
        Ok(x >> bits)
    }

    /// Useful for IPFS hashes stored as bytes
    pub(crate) fn bytes_to_base58(
        &self,
        bytes: Vec<u8>,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<String, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &bytes),
            "bytes_to_base58",
        )?;
        Ok(::bs58::encode(&bytes).into_string())
    }

    pub(crate) fn big_decimal_plus(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigDecimal, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &y)),
            "big_decimal_plus",
        )?;
        Ok(x + y)
    }

    pub(crate) fn big_decimal_minus(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigDecimal, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &y)),
            "big_decimal_minus",
        )?;
        Ok(x - y)
    }

    pub(crate) fn big_decimal_times(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigDecimal, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)),
            "big_decimal_times",
        )?;
        Ok(x * y)
    }

    /// Maximum precision of 100 decimal digits.
    pub(crate) fn big_decimal_divided_by(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigDecimal, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)),
            "big_decimal_divided_by",
        )?;
        if y == 0.into() {
            return Err(DeterministicHostError::from(anyhow!(
                "attempted to divide BigDecimal `{}` by zero",
                x
            )));
        }
        Ok(x / y)
    }

    pub(crate) fn big_decimal_equals(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<bool, HostExportError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::BIG_MATH_GAS_OP.with_args(complexity::Min, (&x, &y)),
            "big_decimal_equals",
        )?;
        Ok(x == y)
    }

    pub(crate) fn big_decimal_to_string(
        &self,
        x: BigDecimal,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<String, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Mul, (&x, &x)),
            "big_decimal_to_string",
        )?;
        Ok(x.to_string())
    }

    pub(crate) fn big_decimal_from_string(
        &self,
        s: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<BigDecimal, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &s),
            "big_decimal_from_string",
        )?;
        BigDecimal::from_str(&s)
            .with_context(|| format!("string  is not a BigDecimal: '{}'", s))
            .map_err(DeterministicHostError::from)
    }

    pub(crate) fn data_source_create(
        &self,
        logger: &Logger,
        state: &mut BlockState,
        name: String,
        params: Vec<String>,
        context: Option<DataSourceContext>,
        creation_block: BlockNumber,
        gas: &GasCounter,
    ) -> Result<(), HostExportError> {
        Self::track_gas_and_ops(gas, state, gas::CREATE_DATA_SOURCE, "data_source_create")?;
        info!(
            logger,
            "Create data source";
            "name" => &name,
            "params" => format!("{}", params.join(","))
        );

        // Resolve the name into the right template
        let template = self
            .data_source
            .templates
            .iter()
            .find(|template| template.name().eq(&name))
            .with_context(|| {
                format!(
                    "Failed to create data source from name `{}`: \
                     No template with this name in parent data source `{}`. \
                     Available names: {}.",
                    name,
                    self.data_source.name,
                    self.data_source
                        .templates
                        .iter()
                        .map(|t| t.name())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .map_err(DeterministicHostError::from)?
            .clone();

        // Remember that we need to create this data source
        state.push_created_data_source(InstanceDSTemplateInfo {
            template,
            params,
            context,
            creation_block,
        });

        Ok(())
    }

    pub(crate) fn ens_name_by_hash(
        &self,
        hash: &str,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Option<String>, anyhow::Error> {
        Self::track_gas_and_ops(gas, state, gas::ENS_NAME_BY_HASH, "ens_name_by_hash")?;
        Ok(self.ens_lookup.find_name(hash)?)
    }

    pub(crate) fn is_ens_data_empty(&self) -> Result<bool, anyhow::Error> {
        Ok(self.ens_lookup.is_table_empty()?)
    }

    pub(crate) fn log_log(
        &self,
        logger: &Logger,
        level: slog::Level,
        msg: String,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<(), DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::LOG_OP.with_args(complexity::Size, &msg),
            "log_log",
        )?;

        let rs = record_static!(level, self.data_source.name.as_str());

        logger.log(&slog::Record::new(
            &rs,
            &format_args!("{}", msg),
            b!("data_source" => &self.data_source.name),
        ));

        if level == slog::Level::Critical {
            return Err(DeterministicHostError::from(anyhow!(
                "Critical error logged in mapping"
            )));
        }
        Ok(())
    }

    pub(crate) fn data_source_address(
        &self,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Vec<u8>, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            Gas::new(gas::DEFAULT_BASE_COST),
            "data_source_address",
        )?;
        Ok(self.data_source.address.clone())
    }

    pub(crate) fn data_source_network(
        &self,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<String, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            Gas::new(gas::DEFAULT_BASE_COST),
            "data_source_network",
        )?;
        Ok(self.subgraph_network.clone())
    }

    pub(crate) fn data_source_context(
        &self,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Option<DataSourceContext>, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            Gas::new(gas::DEFAULT_BASE_COST),
            "data_source_context",
        )?;
        Ok(self.data_source.context.as_ref().clone())
    }

    pub(crate) fn json_from_bytes(
        &self,
        bytes: &Vec<u8>,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<serde_json::Value, DeterministicHostError> {
        // Max JSON size is 10MB.
        const MAX_JSON_SIZE: usize = 10_000_000;

        Self::track_gas_and_ops(
            gas,
            state,
            gas::JSON_FROM_BYTES.with_args(gas::complexity::Size, &bytes),
            "json_from_bytes",
        )?;

        if bytes.len() > MAX_JSON_SIZE {
            return Err(DeterministicHostError::Other(
                anyhow!("JSON size exceeds max size of {}", MAX_JSON_SIZE).into(),
            ));
        }

        serde_json::from_slice(bytes.as_slice())
            .map_err(|e| DeterministicHostError::from(Error::from(e)))
    }

    pub(crate) fn string_to_h160(
        &self,
        string: &str,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<H160, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &string),
            "string_to_h160",
        )?;
        string_to_h160(string)
    }

    pub(crate) fn bytes_to_string(
        &self,
        logger: &Logger,
        bytes: Vec<u8>,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<String, DeterministicHostError> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &bytes),
            "bytes_to_string",
        )?;

        Ok(bytes_to_string(logger, bytes))
    }

    pub(crate) fn ethereum_encode(
        &self,
        token: Token,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Vec<u8>, DeterministicHostError> {
        let encoded = encode(&[token]);

        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &encoded),
            "ethereum_encode",
        )?;

        Ok(encoded)
    }

    pub(crate) fn ethereum_decode(
        &self,
        types: String,
        data: Vec<u8>,
        gas: &GasCounter,
        state: &mut BlockState,
    ) -> Result<Token, anyhow::Error> {
        Self::track_gas_and_ops(
            gas,
            state,
            gas::DEFAULT_GAS_OP.with_args(complexity::Size, &data),
            "ethereum_decode",
        )?;

        let param_types =
            Reader::read(&types).map_err(|e| anyhow::anyhow!("Failed to read types: {}", e))?;

        decode(&[param_types], &data)
            // The `.pop().unwrap()` here is ok because we're always only passing one
            // `param_types` to `decode`, so the returned `Vec` has always size of one.
            // We can't do `tokens[0]` because the value can't be moved out of the `Vec`.
            .map(|mut tokens| tokens.pop().unwrap())
            .context("Failed to decode")
    }
}

fn string_to_h160(string: &str) -> Result<H160, DeterministicHostError> {
    // `H160::from_str` takes a hex string with no leading `0x`.
    let s = string.trim_start_matches("0x");
    H160::from_str(s)
        .with_context(|| format!("Failed to convert string to Address/H160: '{}'", s))
        .map_err(DeterministicHostError::from)
}

fn bytes_to_string(logger: &Logger, bytes: Vec<u8>) -> String {
    let s = String::from_utf8_lossy(&bytes);

    // If the string was re-allocated, that means it was not UTF8.
    if matches!(s, std::borrow::Cow::Owned(_)) {
        warn!(
            logger,
            "Bytes contain invalid UTF8. This may be caused by attempting \
            to convert a value such as an address that cannot be parsed to a unicode string. \
            You may want to use 'toHexString()' instead. String (truncated to 1024 chars): '{}'",
            &s.chars().take(1024).collect::<String>(),
        )
    }

    // The string may have been encoded in a fixed length buffer and padded with null
    // characters, so trim trailing nulls.
    s.trim_end_matches('\u{0000}').to_string()
}

/// Expose some host functions for testing only
#[cfg(debug_assertions)]
pub mod test_support {
    use std::{collections::HashMap, sync::Arc};

    use graph::{
        blockchain::BlockTime,
        components::{
            store::{BlockNumber, GetScope},
            subgraph::SharedProofOfIndexing,
        },
        data::value::Word,
        prelude::{BlockState, Entity, StopwatchMetrics, Value},
        runtime::{gas::GasCounter, HostExportError},
        slog::Logger,
    };

    use crate::MappingContext;

    pub struct HostExports {
        host_exports: Arc<super::HostExports>,
        block_time: BlockTime,
    }

    impl HostExports {
        pub fn new(ctx: &MappingContext) -> Self {
            HostExports {
                host_exports: ctx.host_exports.clone(),
                block_time: ctx.timestamp,
            }
        }

        pub fn store_set(
            &self,
            logger: &Logger,
            block: BlockNumber,
            state: &mut BlockState,
            proof_of_indexing: &SharedProofOfIndexing,
            entity_type: String,
            entity_id: String,
            data: HashMap<Word, Value>,
            stopwatch: &StopwatchMetrics,
            gas: &GasCounter,
        ) -> Result<(), HostExportError> {
            self.host_exports.store_set(
                logger,
                block,
                state,
                proof_of_indexing,
                self.block_time,
                entity_type,
                entity_id,
                data,
                stopwatch,
                gas,
            )
        }

        pub fn store_get(
            &self,
            state: &mut BlockState,
            entity_type: String,
            entity_id: String,
            gas: &GasCounter,
        ) -> Result<Option<Arc<Entity>>, anyhow::Error> {
            self.host_exports
                .store_get(state, entity_type, entity_id, gas, GetScope::Store)
        }
    }
}
#[test]
fn test_string_to_h160_with_0x() {
    assert_eq!(
        H160::from_str("A16081F360e3847006dB660bae1c6d1b2e17eC2A").unwrap(),
        string_to_h160("0xA16081F360e3847006dB660bae1c6d1b2e17eC2A").unwrap()
    )
}

#[test]
fn bytes_to_string_is_lossy() {
    assert_eq!(
        "Downcoin WETH-USDT",
        bytes_to_string(
            &graph::log::logger(true),
            vec![68, 111, 119, 110, 99, 111, 105, 110, 32, 87, 69, 84, 72, 45, 85, 83, 68, 84],
        )
    );

    assert_eq!(
        "Downcoin WETH-USDT�",
        bytes_to_string(
            &graph::log::logger(true),
            vec![
                68, 111, 119, 110, 99, 111, 105, 110, 32, 87, 69, 84, 72, 45, 85, 83, 68, 84, 160,
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
            ],
        )
    )
}
