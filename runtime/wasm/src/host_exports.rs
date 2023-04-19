use std::collections::HashMap;
use std::ops::Deref;
use std::str::FromStr;
use std::time::{Duration, Instant};

use graph::data::value::Word;
use never::Never;
use semver::Version;
use wasmtime::Trap;
use web3::types::H160;

use graph::blockchain::Blockchain;
use graph::components::store::{EnsLookup, GetScope, LoadRelatedRequest};
use graph::components::store::{EntityKey, EntityType};
use graph::components::subgraph::{
    PoICausalityRegion, ProofOfIndexingEvent, SharedProofOfIndexing,
};
use graph::data::store;
use graph::data_source::{CausalityRegion, DataSource, DataSourceTemplate, EntityTypeAccess};
use graph::ensure;
use graph::prelude::ethabi::param_type::Reader;
use graph::prelude::ethabi::{decode, encode, Token};
use graph::prelude::serde_json;
use graph::prelude::{slog::b, slog::record_static, *};
use graph::runtime::gas::{self, complexity, Gas, GasCounter};
pub use graph::runtime::{DeterministicHostError, HostExportError};

use crate::module::{WasmInstance, WasmInstanceContext};
use crate::{error::DeterminismLevel, module::IntoTrap};

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
    fn into_trap(self) -> Trap {
        match self {
            HostExportError::Unknown(e)
            | HostExportError::PossibleReorg(e)
            | HostExportError::Deterministic(e) => Trap::from(e),
        }
    }
}

pub struct HostExports<C: Blockchain> {
    pub(crate) subgraph_id: DeploymentHash,
    pub api_version: Version,
    data_source_name: String,
    data_source_address: Vec<u8>,
    subgraph_network: String,
    data_source_context: Arc<Option<DataSourceContext>>,
    entity_type_access: EntityTypeAccess,
    data_source_causality_region: CausalityRegion,

    /// Some data sources have indeterminism or different notions of time. These
    /// need to be each be stored separately to separate causality between them,
    /// and merge the results later. Right now, this is just the ethereum
    /// networks but will be expanded for ipfs and the availability chain.
    poi_causality_region: String,
    templates: Arc<Vec<DataSourceTemplate<C>>>,
    pub(crate) link_resolver: Arc<dyn LinkResolver>,
    ens_lookup: Arc<dyn EnsLookup>,
}

impl<C: Blockchain> HostExports<C> {
    pub fn new(
        subgraph_id: DeploymentHash,
        data_source: &DataSource<C>,
        subgraph_network: String,
        templates: Arc<Vec<DataSourceTemplate<C>>>,
        link_resolver: Arc<dyn LinkResolver>,
        ens_lookup: Arc<dyn EnsLookup>,
    ) -> Self {
        Self {
            subgraph_id,
            api_version: data_source.api_version(),
            data_source_name: data_source.name().to_owned(),
            data_source_address: data_source.address().unwrap_or_default(),
            data_source_context: data_source.context().cheap_clone(),
            entity_type_access: data_source.entities(),
            data_source_causality_region: data_source.causality_region(),
            poi_causality_region: PoICausalityRegion::from_network(&subgraph_network),
            subgraph_network,
            templates,
            link_resolver,
            ens_lookup,
        }
    }

    /// Enfore the entity type access restrictions. See also: entity-type-access
    fn check_entity_type_access(&self, entity_type: &EntityType) -> Result<(), HostExportError> {
        match self.entity_type_access.allows(entity_type) {
            true => Ok(()),
            false => Err(HostExportError::Deterministic(anyhow!(
                "entity type `{}` is not on the 'entities' list for data source `{}`. \
                 Hint: Add `{}` to the 'entities' list, which currently is: `{}`.",
                entity_type,
                self.data_source_name,
                entity_type,
                self.entity_type_access
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
    ) -> Result<Never, DeterministicHostError> {
        gas.consume_host_fn(Gas::new(gas::DEFAULT_BASE_COST))?;

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

    pub(crate) fn store_set(
        &self,
        logger: &Logger,
        state: &mut BlockState<C>,
        proof_of_indexing: &SharedProofOfIndexing,
        entity_type: String,
        entity_id: String,
        mut data: HashMap<Word, Value>,
        stopwatch: &StopwatchMetrics,
        gas: &GasCounter,
    ) -> Result<(), HostExportError> {
        let poi_section = stopwatch.start_section("host_export_store_set__proof_of_indexing");
        write_poi_event(
            proof_of_indexing,
            &ProofOfIndexingEvent::SetEntity {
                entity_type: &entity_type,
                id: &entity_id,
                data: &data,
            },
            &self.poi_causality_region,
            logger,
        );
        poi_section.end();

        let key = EntityKey {
            entity_type: EntityType::new(entity_type),
            entity_id: entity_id.into(),
            causality_region: self.data_source_causality_region,
        };
        self.check_entity_type_access(&key.entity_type)?;

        gas.consume_host_fn(gas::STORE_SET.with_args(complexity::Linear, (&key, &data)))?;

        fn check_id(key: &EntityKey, prev_id: &str) -> Result<(), anyhow::Error> {
            if prev_id != key.entity_id.as_str() {
                Err(anyhow!(
                    "Value of {} attribute 'id' conflicts with ID passed to `store.set()`: \
                {} != {}",
                    key.entity_type,
                    prev_id,
                    key.entity_id,
                ))
            } else {
                Ok(())
            }
        }

        // Set the id if there isn't one yet, and make sure that a
        // previously set id agrees with the one in the `key`
        match data.get(&store::ID) {
            Some(Value::String(s)) => check_id(&key, s)?,
            Some(Value::Bytes(b)) => check_id(&key, &b.to_string())?,
            Some(_) => {
                // The validation will catch the type mismatch
            }
            None => {
                let value = state.entity_cache.schema.id_value(&key)?;
                data.insert(store::ID.clone(), value);
            }
        }

        let entity = state
            .entity_cache
            .make_entity(data.into_iter().map(|(key, value)| (key, value)))?;

        state.entity_cache.set(key, entity)?;

        Ok(())
    }

    pub(crate) fn store_remove(
        &self,
        logger: &Logger,
        state: &mut BlockState<C>,
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
        let key = EntityKey {
            entity_type: EntityType::new(entity_type),
            entity_id: entity_id.into(),
            causality_region: self.data_source_causality_region,
        };
        self.check_entity_type_access(&key.entity_type)?;

        gas.consume_host_fn(gas::STORE_REMOVE.with_args(complexity::Size, &key))?;

        state.entity_cache.remove(key);

        Ok(())
    }

    pub(crate) fn store_get(
        &self,
        state: &mut BlockState<C>,
        entity_type: String,
        entity_id: String,
        gas: &GasCounter,
        scope: GetScope,
    ) -> Result<Option<Entity>, anyhow::Error> {
        let store_key = EntityKey {
            entity_type: EntityType::new(entity_type),
            entity_id: entity_id.into(),
            causality_region: self.data_source_causality_region,
        };
        self.check_entity_type_access(&store_key.entity_type)?;

        let result = state.entity_cache.get(&store_key, scope)?;
        gas.consume_host_fn(gas::STORE_GET.with_args(complexity::Linear, (&store_key, &result)))?;

        Ok(result)
    }

    pub(crate) fn store_load_related(
        &self,
        state: &mut BlockState<C>,
        entity_type: String,
        entity_id: String,
        entity_field: String,
        gas: &GasCounter,
    ) -> Result<Vec<Entity>, anyhow::Error> {
        let store_key = LoadRelatedRequest {
            entity_type: EntityType::new(entity_type),
            entity_id: entity_id.into(),
            entity_field: entity_field.into(),
            causality_region: self.data_source_causality_region,
        };
        self.check_entity_type_access(&store_key.entity_type)?;

        let result = state.entity_cache.load_related(&store_key)?;
        gas.consume_host_fn(gas::STORE_GET.with_args(complexity::Linear, (&store_key, &result)))?;

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
    ) -> Result<String, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &n))?;

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
        module: &mut WasmInstanceContext<C>,
        link: String,
        callback: &str,
        user_data: store::Value,
        flags: Vec<String>,
    ) -> Result<Vec<BlockState<C>>, anyhow::Error> {
        // Does not consume gas because this is not a part of deterministic APIs.
        // Ideally we would consume gas the same as ipfs_cat and then share
        // gas across the spawned modules for callbacks.

        const JSON_FLAG: &str = "json";
        ensure!(
            flags.contains(&JSON_FLAG.to_string()),
            "Flags must contain 'json'"
        );

        let host_metrics = module.host_metrics.clone();
        let valid_module = module.valid_module.clone();
        let ctx = module.ctx.derive_with_empty_block_state();
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
                    module.timeout,
                    module.experimental_features,
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
    ) -> Result<i64, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json))?;
        i64::from_str(&json)
            .with_context(|| format!("JSON `{}` cannot be parsed as i64", json))
            .map_err(DeterministicHostError::from)
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_u64(
        &self,
        json: String,
        gas: &GasCounter,
    ) -> Result<u64, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json))?;

        u64::from_str(&json)
            .with_context(|| format!("JSON `{}` cannot be parsed as u64", json))
            .map_err(DeterministicHostError::from)
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_f64(
        &self,
        json: String,
        gas: &GasCounter,
    ) -> Result<f64, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json))?;

        f64::from_str(&json)
            .with_context(|| format!("JSON `{}` cannot be parsed as f64", json))
            .map_err(DeterministicHostError::from)
    }

    /// Expects a decimal string.
    pub(crate) fn json_to_big_int(
        &self,
        json: String,
        gas: &GasCounter,
    ) -> Result<Vec<u8>, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &json))?;

        let big_int = BigInt::from_str(&json)
            .with_context(|| format!("JSON `{}` is not a decimal string", json))
            .map_err(DeterministicHostError::from)?;
        Ok(big_int.to_signed_bytes_le())
    }

    pub(crate) fn crypto_keccak_256(
        &self,
        input: Vec<u8>,
        gas: &GasCounter,
    ) -> Result<[u8; 32], DeterministicHostError> {
        let data = &input[..];
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, data))?;
        Ok(tiny_keccak::keccak256(data))
    }

    pub(crate) fn big_int_plus(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Max, (&x, &y)))?;
        Ok(x + y)
    }

    pub(crate) fn big_int_minus(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Max, (&x, &y)))?;
        Ok(x - y)
    }

    pub(crate) fn big_int_times(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)))?;
        Ok(x * y)
    }

    pub(crate) fn big_int_divided_by(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)))?;
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
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)))?;
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
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(
            gas::BIG_MATH_GAS_OP
                .with_args(complexity::Exponential, (&x, (exp as f32).log2() as u8)),
        )?;
        Ok(x.pow(exp)?)
    }

    pub(crate) fn big_int_from_string(
        &self,
        s: String,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &s))?;
        BigInt::from_str(&s)
            .with_context(|| format!("string is not a BigInt: `{}`", s))
            .map_err(DeterministicHostError::from)
    }

    pub(crate) fn big_int_bit_or(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Max, (&x, &y)))?;
        Ok(x | y)
    }

    pub(crate) fn big_int_bit_and(
        &self,
        x: BigInt,
        y: BigInt,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Min, (&x, &y)))?;
        Ok(x & y)
    }

    pub(crate) fn big_int_left_shift(
        &self,
        x: BigInt,
        bits: u8,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &bits)))?;
        Ok(x << bits)
    }

    pub(crate) fn big_int_right_shift(
        &self,
        x: BigInt,
        bits: u8,
        gas: &GasCounter,
    ) -> Result<BigInt, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &bits)))?;
        Ok(x >> bits)
    }

    /// Useful for IPFS hashes stored as bytes
    pub(crate) fn bytes_to_base58(
        &self,
        bytes: Vec<u8>,
        gas: &GasCounter,
    ) -> Result<String, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &bytes))?;
        Ok(::bs58::encode(&bytes).into_string())
    }

    pub(crate) fn big_decimal_plus(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
    ) -> Result<BigDecimal, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &y)))?;
        Ok(x + y)
    }

    pub(crate) fn big_decimal_minus(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
    ) -> Result<BigDecimal, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Linear, (&x, &y)))?;
        Ok(x - y)
    }

    pub(crate) fn big_decimal_times(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
    ) -> Result<BigDecimal, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)))?;
        Ok(x * y)
    }

    /// Maximum precision of 100 decimal digits.
    pub(crate) fn big_decimal_divided_by(
        &self,
        x: BigDecimal,
        y: BigDecimal,
        gas: &GasCounter,
    ) -> Result<BigDecimal, DeterministicHostError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Mul, (&x, &y)))?;
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
    ) -> Result<bool, HostExportError> {
        gas.consume_host_fn(gas::BIG_MATH_GAS_OP.with_args(complexity::Min, (&x, &y)))?;
        Ok(x == y)
    }

    pub(crate) fn big_decimal_to_string(
        &self,
        x: BigDecimal,
        gas: &GasCounter,
    ) -> Result<String, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Mul, (&x, &x)))?;
        Ok(x.to_string())
    }

    pub(crate) fn big_decimal_from_string(
        &self,
        s: String,
        gas: &GasCounter,
    ) -> Result<BigDecimal, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &s))?;
        BigDecimal::from_str(&s)
            .with_context(|| format!("string  is not a BigDecimal: '{}'", s))
            .map_err(DeterministicHostError::from)
    }

    pub(crate) fn data_source_create(
        &self,
        logger: &Logger,
        state: &mut BlockState<C>,
        name: String,
        params: Vec<String>,
        context: Option<DataSourceContext>,
        creation_block: BlockNumber,
        gas: &GasCounter,
    ) -> Result<(), HostExportError> {
        gas.consume_host_fn(gas::CREATE_DATA_SOURCE)?;
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
            .find(|template| template.name() == name)
            .with_context(|| {
                format!(
                    "Failed to create data source from name `{}`: \
                     No template with this name in parent data source `{}`. \
                     Available names: {}.",
                    name,
                    self.data_source_name,
                    self.templates
                        .iter()
                        .map(|template| template.name())
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .map_err(DeterministicHostError::from)?
            .clone();

        // Remember that we need to create this data source
        state.push_created_data_source(DataSourceTemplateInfo {
            template,
            params,
            context,
            creation_block,
        });

        Ok(())
    }

    pub(crate) fn ens_name_by_hash(&self, hash: &str) -> Result<Option<String>, anyhow::Error> {
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
    ) -> Result<(), DeterministicHostError> {
        gas.consume_host_fn(gas::LOG_OP.with_args(complexity::Size, &msg))?;

        let rs = record_static!(level, self.data_source_name.as_str());

        logger.log(&slog::Record::new(
            &rs,
            &format_args!("{}", msg),
            b!("data_source" => &self.data_source_name),
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
    ) -> Result<Vec<u8>, DeterministicHostError> {
        gas.consume_host_fn(Gas::new(gas::DEFAULT_BASE_COST))?;
        Ok(self.data_source_address.clone())
    }

    pub(crate) fn data_source_network(
        &self,
        gas: &GasCounter,
    ) -> Result<String, DeterministicHostError> {
        gas.consume_host_fn(Gas::new(gas::DEFAULT_BASE_COST))?;
        Ok(self.subgraph_network.clone())
    }

    pub(crate) fn data_source_context(
        &self,
        gas: &GasCounter,
    ) -> Result<Option<DataSourceContext>, DeterministicHostError> {
        gas.consume_host_fn(Gas::new(gas::DEFAULT_BASE_COST))?;
        Ok(self.data_source_context.as_ref().clone())
    }

    pub(crate) fn json_from_bytes(
        &self,
        bytes: &Vec<u8>,
        gas: &GasCounter,
    ) -> Result<serde_json::Value, DeterministicHostError> {
        // Max JSON size is 10MB.
        const MAX_JSON_SIZE: usize = 10_000_000;

        gas.consume_host_fn(gas::JSON_FROM_BYTES.with_args(gas::complexity::Size, &bytes))?;

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
    ) -> Result<H160, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &string))?;
        string_to_h160(string)
    }

    pub(crate) fn bytes_to_string(
        &self,
        logger: &Logger,
        bytes: Vec<u8>,
        gas: &GasCounter,
    ) -> Result<String, DeterministicHostError> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &bytes))?;

        Ok(bytes_to_string(logger, bytes))
    }

    pub(crate) fn ethereum_encode(
        &self,
        token: Token,
        gas: &GasCounter,
    ) -> Result<Vec<u8>, DeterministicHostError> {
        let encoded = encode(&[token]);

        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &encoded))?;

        Ok(encoded)
    }

    pub(crate) fn ethereum_decode(
        &self,
        types: String,
        data: Vec<u8>,
        gas: &GasCounter,
    ) -> Result<Token, anyhow::Error> {
        gas.consume_host_fn(gas::DEFAULT_GAS_OP.with_args(complexity::Size, &data))?;

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
        blockchain::Blockchain,
        components::{store::GetScope, subgraph::SharedProofOfIndexing},
        data::value::Word,
        prelude::{BlockState, Entity, StopwatchMetrics, Value},
        runtime::{gas::GasCounter, HostExportError},
        slog::Logger,
    };

    use crate::MappingContext;

    pub struct HostExports<C: Blockchain>(Arc<super::HostExports<C>>);

    impl<C: Blockchain> HostExports<C> {
        pub fn new(ctx: &MappingContext<C>) -> Self {
            HostExports(ctx.host_exports.clone())
        }

        pub fn store_set(
            &self,
            logger: &Logger,
            state: &mut BlockState<C>,
            proof_of_indexing: &SharedProofOfIndexing,
            entity_type: String,
            entity_id: String,
            data: HashMap<Word, Value>,
            stopwatch: &StopwatchMetrics,
            gas: &GasCounter,
        ) -> Result<(), HostExportError> {
            self.0.store_set(
                logger,
                state,
                proof_of_indexing,
                entity_type,
                entity_id,
                data,
                stopwatch,
                gas,
            )
        }

        pub fn store_get(
            &self,
            state: &mut BlockState<C>,
            entity_type: String,
            entity_id: String,
            gas: &GasCounter,
        ) -> Result<Option<Entity>, anyhow::Error> {
            self.0
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
