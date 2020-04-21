use bytes::Bytes;
use ethabi::{Address, Token};
use graph::components::arweave::ArweaveAdapter;
use graph::components::ethereum::*;
use graph::components::store::EntityKey;
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
        Self {
            subgraph_id,
            api_version,
            data_source_name,
            data_source_address,
            data_source_network,
            data_source_context,
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

    // // Read the IPFS file `link`, split it into JSON objects, and invoke the
    // // exported function `callback` on each JSON object. The successful return
    // // value contains the block state produced by each callback invocation. Each
    // // invocation of `callback` happens in its own instance of a WASM module,
    // // which is identical to `module` when it was first started. The signature
    // // of the callback must be `callback(JSONValue, Value)`, and the `userData`
    // // parameter is passed to the callback without any changes
    // pub(crate) fn ipfs_map(
    //     &self,
    //     module: &WasmiModule,
    //     link: String,
    //     callback: &str,
    //     user_data: store::Value,
    //     flags: Vec<String>,
    // ) -> Result<Vec<BlockState>, HostExportError<impl ExportError>> {
    //     const JSON_FLAG: &str = "json";
    //     if !flags.contains(&JSON_FLAG.to_string()) {
    //         return Err(HostExportError(format!("Flags must contain 'json'")));
    //     }

    //     let host_metrics = module.host_metrics.clone();
    //     let valid_module = module.valid_module.clone();
    //     let ctx = module.ctx.clone_with_empty_block_state();
    //     let callback = callback.to_owned();
    //     // Create a base error message to avoid borrowing headaches
    //     let errmsg = format!(
    //         "ipfs_map: callback '{}' failed when processing file '{}'",
    //         &*callback, &link
    //     );

    //     let start = Instant::now();
    //     let mut last_log = start;
    //     let logger = ctx.logger.new(o!("ipfs_map" => link.clone()));

    //     let result = block_on03(async move {
    //         let mut stream: JsonValueStream = self
    //             .link_resolver
    //             .json_stream(&logger, &Link { link })
    //             .await?;
    //         let mut v = Vec::new();
    //         while let Some(sv) = stream.next().await {
    //             let sv = sv?;
    //             let module = WasmiModule::from_valid_module_with_ctx(
    //                 valid_module.clone(),
    //                 ctx.clone_with_empty_block_state(),
    //                 host_metrics.clone(),
    //             )?;
    //             let result = module.handle_json_callback(&callback, &sv.value, &user_data)?;
    //             // Log progress every 15s
    //             if last_log.elapsed() > Duration::from_secs(15) {
    //                 debug!(
    //                     logger,
    //                     "Processed {} lines in {}s so far",
    //                     sv.line,
    //                     start.elapsed().as_secs()
    //                 );
    //                 last_log = Instant::now();
    //             }
    //             v.push(result)
    //         }
    //         Ok(v)
    //     });
    //     result.map_err(move |e: Error| HostExportError(format!("{}: {}", errmsg, e.to_string())))
    // }

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
