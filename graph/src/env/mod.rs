mod graphql;
mod mappings;
mod store;

use envconfig::Envconfig;
use lazy_static::lazy_static;
use semver::Version;
use std::{collections::HashSet, env::VarError, fmt, str::FromStr, time::Duration};

use self::graphql::*;
use self::mappings::*;
use self::store::*;
use crate::{
    components::{store::BlockNumber, subgraph::SubgraphVersionSwitchingMode},
    runtime::gas::CONST_MAX_GAS_PER_HANDLER,
};

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

/// Panics if:
/// - The value is not UTF8.
/// - The value cannot be parsed as T..
pub fn env_var<E: std::error::Error + Send + Sync, T: FromStr<Err = E> + Eq>(
    name: &'static str,
    default_value: T,
) -> T {
    let var = match std::env::var(name) {
        Ok(var) => var,
        Err(VarError::NotPresent) => return default_value,
        Err(VarError::NotUnicode(_)) => panic!("environment variable {} is not UTF8", name),
    };

    var.parse::<T>()
        .unwrap_or_else(|e| panic!("failed to parse environment variable {}: {}", name, e))
}

#[derive(Clone)]
#[non_exhaustive]
pub struct EnvVars {
    pub graphql: EnvVarsGraphQl,
    pub mappings: EnvVarsMapping,
    pub store: EnvVarsStore,

    /// Enables query throttling when getting database connections goes over this value.
    /// Load management can be disabled by setting this to 0.
    ///
    /// Set by the environment variable `GRAPH_LOAD_THRESHOLD` (expressed in
    /// milliseconds). The default value is 0.
    pub load_threshold: Duration,
    /// When the system is overloaded, any query that causes more than this
    /// fraction of the effort will be rejected for as long as the process is
    /// running (i.e. even after the overload situation is resolved).
    ///
    /// Set by the environment variable `GRAPH_LOAD_THRESHOLD`
    /// (expressed as a number). No default value is provided. When *not* set,
    /// no queries will ever be jailed, even though they will still be subject
    /// to normal load management when the system is overloaded.
    pub load_jail_threshold: Option<f64>,
    /// When this is active, the system will trigger all the steps that the load
    /// manager would given the other load management configuration settings,
    /// but never actually decline to run a query; instead, log about load
    /// management decisions.
    ///
    /// Set by the flag `GRAPH_LOAD_SIMULATE`.
    pub load_simulate: bool,
    /// Set by the flag `GRAPH_ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH`, but
    /// enabled anyway (overridden) if [debug
    /// assertions](https://doc.rust-lang.org/reference/conditional-compilation.html#debug_assertions)
    /// are enabled.
    pub allow_non_deterministic_fulltext_search: bool,
    /// Set by the environment variable `GRAPH_MAX_SPEC_VERSION`.
    pub max_spec_version: Version,
    /// Set by the environment variable `GRAPH_LOAD_WINDOW_SIZE` (expressed in
    /// seconds). The default value is 300 seconds.
    pub load_window_size: Duration,
    /// Set by the environment variable `GRAPH_LOAD_BIN_SIZE` (expressed in
    /// seconds). The default value is 1 second.
    pub load_bin_size: Duration,
    /// Set by the environment variable
    /// `GRAPH_ELASTIC_SEARCH_FLUSH_INTERVAL_SECS` (expressed in seconds). The
    /// default value is 5 seconds.
    pub elastic_search_flush_interval: Duration,
    /// Set by the environment variable
    /// `GRAPH_ELASTIC_SEARCH_MAX_RETRIES`. The default value is 5.
    pub elastic_search_max_retries: usize,
    /// The name of the index in ElasticSearch to which we should log. Set
    /// by `GRAPH_ELASTIC_SEARCH_INDEX`. The default is `subgraph`.
    pub elastic_search_index: String,
    /// If an instrumented lock is contended for longer than the specified
    /// duration, a warning will be logged.
    ///
    /// Set by the environment variable `GRAPH_LOCK_CONTENTION_LOG_THRESHOLD_MS`
    /// (expressed in milliseconds). The default value is 100ms.
    pub lock_contention_log_threshold: Duration,
    /// This is configurable only for debugging purposes. This value is set by
    /// the protocol, so indexers running in the network should never set this
    /// config.
    ///
    /// Set by the environment variable `GRAPH_MAX_GAS_PER_HANDLER`.
    pub max_gas_per_handler: u64,
    /// Set by the environment variable `GRAPH_LOG_QUERY_TIMING`.
    pub log_query_timing: HashSet<String>,
    /// A
    /// [`chrono`](https://docs.rs/chrono/latest/chrono/#formatting-and-parsing)
    /// -like format string for logs.
    ///
    /// Set by the environment variable `GRAPH_LOG_TIME_FORMAT`. The default
    /// value is `%b %d %H:%M:%S%.3f`.
    pub log_time_format: String,
    /// Set by the flag `GRAPH_LOG_POI_EVENTS`.
    pub log_poi_events: bool,
    /// Set by the environment variable `GRAPH_LOG`.
    pub log_levels: Option<String>,
    /// Set by the flag `EXPERIMENTAL_STATIC_FILTERS`. Off by default.
    pub experimental_static_filters: bool,
    /// Set by the environment variable
    /// `EXPERIMENTAL_SUBGRAPH_VERSION_SWITCHING_MODE`. The default value is
    /// `"instant"`.
    pub subgraph_version_switching_mode: SubgraphVersionSwitchingMode,
    /// Set by the flag `GRAPH_KILL_IF_UNRESPONSIVE`. Off by default.
    pub kill_if_unresponsive: bool,
    /// Max timeout in seconds before killing the node.
    /// Set by the environment variable `GRAPH_KILL_IF_UNRESPONSIVE_TIMEOUT_SECS`
    /// (expressed in seconds). The default value is 10s.
    pub kill_if_unresponsive_timeout: Duration,
    /// Guards public access to POIs in the `index-node`.
    ///
    /// Set by the environment variable `GRAPH_POI_ACCESS_TOKEN`. No default
    /// value is provided.
    pub poi_access_token: Option<String>,
    /// Set by the environment variable `GRAPH_SUBGRAPH_MAX_DATA_SOURCES`. Defaults to 1 billion.
    pub subgraph_max_data_sources: usize,
    /// Keep deterministic errors non-fatal even if the subgraph is pending.
    /// Used for testing Graph Node itself.
    ///
    /// Set by the flag `GRAPH_DISABLE_FAIL_FAST`. Off by default.
    pub disable_fail_fast: bool,
    /// Ceiling for the backoff retry of non-deterministic errors.
    ///
    /// Set by the environment variable `GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS`
    /// (expressed in seconds). The default value is 3600s (60 minutes).
    pub subgraph_error_retry_ceil: Duration,
    /// Jitter factor for the backoff retry of non-deterministic errors.
    ///
    /// Set by the environment variable `GRAPH_SUBGRAPH_ERROR_RETRY_JITTER`
    /// (clamped between 0.0 and 1.0). The default value is 0.2.
    pub subgraph_error_retry_jitter: f64,
    /// Experimental feature.
    ///
    /// Set by the flag `GRAPH_ENABLE_SELECT_BY_SPECIFIC_ATTRIBUTES`. On by
    /// default.
    pub enable_select_by_specific_attributes: bool,
    /// Experimental feature.
    ///
    /// Set the flag `GRAPH_POSTPONE_ATTRIBUTE_INDEX_CREATION`. Off by default.
    pub postpone_attribute_index_creation: bool,
    /// Verbose logging of mapping inputs.
    ///
    /// Set by the flag `GRAPH_LOG_TRIGGER_DATA`. Off by
    /// default.
    pub log_trigger_data: bool,
    /// Set by the environment variable `GRAPH_EXPLORER_TTL`
    /// (expressed in seconds). The default value is 10s.
    pub explorer_ttl: Duration,
    /// Set by the environment variable `GRAPH_EXPLORER_LOCK_THRESHOLD`
    /// (expressed in milliseconds). The default value is 100ms.
    pub explorer_lock_threshold: Duration,
    /// Set by the environment variable `GRAPH_EXPLORER_QUERY_THRESHOLD`
    /// (expressed in milliseconds). The default value is 500ms.
    pub explorer_query_threshold: Duration,
    /// Set by the environment variable `EXTERNAL_HTTP_BASE_URL`. No default
    /// value is provided.
    pub external_http_base_url: Option<String>,
    /// Set by the environment variable `EXTERNAL_WS_BASE_URL`. No default
    /// value is provided.
    pub external_ws_base_url: Option<String>,
    /// Maximum number of Dynamic Data Sources after which a Subgraph will
    /// switch to using static filter.
    pub static_filters_threshold: usize,
    /// Set by the environment variable `ETHEREUM_REORG_THRESHOLD`. The default
    /// value is 250 blocks.
    pub reorg_threshold: BlockNumber,
    /// The time to wait between polls when using polling block ingestor.
    /// The value is set by `ETHERUM_POLLING_INTERVAL` in millis and the
    /// default is 1000.
    pub ingestor_polling_interval: Duration,
    /// Set by the env var `GRAPH_EXPERIMENTAL_SUBGRAPH_SETTINGS` which should point
    /// to a file with subgraph-specific settings
    pub subgraph_settings: Option<String>,
    /// Whether to prefer substreams blocks streams over firehose when available.
    pub prefer_substreams_block_streams: bool,
    /// Set by the flag `GRAPH_ENABLE_DIPS_METRICS`. Whether to enable
    /// gas metrics. Off by default.
    pub enable_dips_metrics: bool,
    /// Set by the env var `GRAPH_HISTORY_BLOCKS_OVERRIDE`. Defaults to None
    /// Sets an override for the amount history to keep regardless of the
    /// historyBlocks set in the manifest
    pub history_blocks_override: Option<BlockNumber>,
    /// Set by the env var `GRAPH_MIN_HISTORY_BLOCKS`
    /// The amount of history to keep when using 'min' historyBlocks
    /// in the manifest
    pub min_history_blocks: BlockNumber,
    /// Set by the env var `dips_metrics_object_store_url`
    /// The name of the object store bucket to store DIPS metrics
    pub dips_metrics_object_store_url: Option<String>,
    /// Write a list of how sections are nested to the file `section_map`
    /// which must be an absolute path. This only has an effect in debug
    /// builds. Set with `GRAPH_SECTION_MAP`. Defaults to `None`.
    pub section_map: Option<String>,
    /// Set the maximum grpc decode size(in MB) for firehose BlockIngestor connections.
    /// Defaults to 25MB
    pub firehose_grpc_max_decode_size_mb: usize,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;
        let graphql = InnerGraphQl::init_from_env()?.into();
        let mapping_handlers = InnerMappingHandlers::init_from_env()?.into();
        let store = InnerStore::init_from_env()?.into();

        // The default reorganization (reorg) threshold is set to 250.
        // For testing purposes, we need to set this threshold to 0 because:
        // 1. Many tests involve reverting blocks.
        // 2. Blocks cannot be reverted below the reorg threshold.
        // Therefore, during tests, we want to set the reorg threshold to 0.
        let reorg_threshold =
            inner
                .reorg_threshold
                .unwrap_or_else(|| if cfg!(debug_assertions) { 0 } else { 250 });

        Ok(Self {
            graphql,
            mappings: mapping_handlers,
            store,

            load_threshold: Duration::from_millis(inner.load_threshold_in_ms),
            load_jail_threshold: inner.load_jail_threshold,
            load_simulate: inner.load_simulate.0,
            allow_non_deterministic_fulltext_search: inner
                .allow_non_deterministic_fulltext_search
                .0
                || cfg!(debug_assertions),
            max_spec_version: inner.max_spec_version,
            load_window_size: Duration::from_secs(inner.load_window_size_in_secs),
            load_bin_size: Duration::from_secs(inner.load_bin_size_in_secs),
            elastic_search_flush_interval: Duration::from_secs(
                inner.elastic_search_flush_interval_in_secs,
            ),
            elastic_search_max_retries: inner.elastic_search_max_retries,
            elastic_search_index: inner.elastic_search_index,
            lock_contention_log_threshold: Duration::from_millis(
                inner.lock_contention_log_threshold_in_ms,
            ),
            max_gas_per_handler: inner.max_gas_per_handler.0 .0,
            log_query_timing: inner
                .log_query_timing
                .split(',')
                .map(str::to_string)
                .collect(),
            log_time_format: inner.log_time_format,
            log_poi_events: inner.log_poi_events.0,
            log_levels: inner.log_levels,
            experimental_static_filters: inner.experimental_static_filters.0,
            subgraph_version_switching_mode: inner.subgraph_version_switching_mode,
            kill_if_unresponsive: inner.kill_if_unresponsive.0,
            kill_if_unresponsive_timeout: Duration::from_secs(
                inner.kill_if_unresponsive_timeout_secs,
            ),
            poi_access_token: inner.poi_access_token,
            subgraph_max_data_sources: inner.subgraph_max_data_sources.0,
            disable_fail_fast: inner.disable_fail_fast.0,
            subgraph_error_retry_ceil: Duration::from_secs(inner.subgraph_error_retry_ceil_in_secs),
            subgraph_error_retry_jitter: inner.subgraph_error_retry_jitter,
            enable_select_by_specific_attributes: inner.enable_select_by_specific_attributes.0,
            postpone_attribute_index_creation: inner.postpone_attribute_index_creation.0
                || cfg!(debug_assertions),
            log_trigger_data: inner.log_trigger_data.0,
            explorer_ttl: Duration::from_secs(inner.explorer_ttl_in_secs),
            explorer_lock_threshold: Duration::from_millis(inner.explorer_lock_threshold_in_msec),
            explorer_query_threshold: Duration::from_millis(inner.explorer_query_threshold_in_msec),
            external_http_base_url: inner.external_http_base_url,
            external_ws_base_url: inner.external_ws_base_url,
            static_filters_threshold: inner.static_filters_threshold,
            reorg_threshold,
            ingestor_polling_interval: Duration::from_millis(inner.ingestor_polling_interval),
            subgraph_settings: inner.subgraph_settings,
            prefer_substreams_block_streams: inner.prefer_substreams_block_streams,
            enable_dips_metrics: inner.enable_dips_metrics.0,
            history_blocks_override: inner.history_blocks_override,
            min_history_blocks: inner.min_history_blocks.unwrap_or(2 * reorg_threshold),
            dips_metrics_object_store_url: inner.dips_metrics_object_store_url,
            section_map: inner.section_map,
            firehose_grpc_max_decode_size_mb: inner.firehose_grpc_max_decode_size_mb,
        })
    }

    /// Equivalent to checking if [`EnvVar::load_threshold`] is set to
    /// [`Duration::ZERO`].
    pub fn load_management_is_disabled(&self) -> bool {
        self.load_threshold.is_zero()
    }

    fn log_query_timing_contains(&self, kind: &str) -> bool {
        self.log_query_timing.iter().any(|s| s == kind)
    }

    pub fn log_sql_timing(&self) -> bool {
        self.log_query_timing_contains("sql")
    }

    pub fn log_gql_timing(&self) -> bool {
        self.log_query_timing_contains("gql")
    }

    pub fn log_gql_cache_timing(&self) -> bool {
        self.log_query_timing_contains("cache") && self.log_gql_timing()
    }
}

impl Default for EnvVars {
    fn default() -> Self {
        ENV_VARS.clone()
    }
}

// This does not print any values avoid accidentally leaking any sensitive env vars
impl fmt::Debug for EnvVars {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "env vars")
    }
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "GRAPH_LOAD_THRESHOLD", default = "0")]
    load_threshold_in_ms: u64,
    #[envconfig(from = "GRAPH_LOAD_JAIL_THRESHOLD")]
    load_jail_threshold: Option<f64>,
    #[envconfig(from = "GRAPH_LOAD_SIMULATE", default = "false")]
    load_simulate: EnvVarBoolean,
    #[envconfig(
        from = "GRAPH_ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH",
        default = "false"
    )]
    allow_non_deterministic_fulltext_search: EnvVarBoolean,
    #[envconfig(from = "GRAPH_MAX_SPEC_VERSION", default = "1.2.0")]
    max_spec_version: Version,
    #[envconfig(from = "GRAPH_LOAD_WINDOW_SIZE", default = "300")]
    load_window_size_in_secs: u64,
    #[envconfig(from = "GRAPH_LOAD_BIN_SIZE", default = "1")]
    load_bin_size_in_secs: u64,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_FLUSH_INTERVAL_SECS", default = "5")]
    elastic_search_flush_interval_in_secs: u64,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_MAX_RETRIES", default = "5")]
    elastic_search_max_retries: usize,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_INDEX", default = "subgraph")]
    elastic_search_index: String,
    #[envconfig(from = "GRAPH_LOCK_CONTENTION_LOG_THRESHOLD_MS", default = "100")]
    lock_contention_log_threshold_in_ms: u64,

    // For now this is set absurdly high by default because we've seen many cases of gas being
    // overestimated and failing otherwise legit subgraphs. Once gas costs have been better
    // benchmarked and adjusted, and out of gas has been made a deterministic error, this default
    // should be removed and this should somehow be gated on `UNSAFE_CONFIG`.
    #[envconfig(from = "GRAPH_MAX_GAS_PER_HANDLER", default = "1_000_000_000_000_000")]
    max_gas_per_handler:
        WithDefaultUsize<NoUnderscores<u64>, { CONST_MAX_GAS_PER_HANDLER as usize }>,
    #[envconfig(from = "GRAPH_LOG_QUERY_TIMING", default = "")]
    log_query_timing: String,
    #[envconfig(from = "GRAPH_LOG_TIME_FORMAT", default = "%b %d %H:%M:%S%.3f")]
    log_time_format: String,
    #[envconfig(from = "GRAPH_LOG_POI_EVENTS", default = "false")]
    log_poi_events: EnvVarBoolean,
    #[envconfig(from = "GRAPH_LOG")]
    log_levels: Option<String>,
    #[envconfig(from = "EXPERIMENTAL_STATIC_FILTERS", default = "false")]
    experimental_static_filters: EnvVarBoolean,
    #[envconfig(
        from = "EXPERIMENTAL_SUBGRAPH_VERSION_SWITCHING_MODE",
        default = "instant"
    )]
    subgraph_version_switching_mode: SubgraphVersionSwitchingMode,
    #[envconfig(from = "GRAPH_KILL_IF_UNRESPONSIVE", default = "false")]
    kill_if_unresponsive: EnvVarBoolean,
    #[envconfig(from = "GRAPH_KILL_IF_UNRESPONSIVE_TIMEOUT_SECS", default = "10")]
    kill_if_unresponsive_timeout_secs: u64,
    #[envconfig(from = "GRAPH_POI_ACCESS_TOKEN")]
    poi_access_token: Option<String>,
    #[envconfig(from = "GRAPH_SUBGRAPH_MAX_DATA_SOURCES", default = "1_000_000_000")]
    subgraph_max_data_sources: NoUnderscores<usize>,
    #[envconfig(from = "GRAPH_DISABLE_FAIL_FAST", default = "false")]
    disable_fail_fast: EnvVarBoolean,
    #[envconfig(from = "GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS", default = "3600")]
    subgraph_error_retry_ceil_in_secs: u64,
    #[envconfig(from = "GRAPH_SUBGRAPH_ERROR_RETRY_JITTER", default = "0.2")]
    subgraph_error_retry_jitter: f64,
    #[envconfig(from = "GRAPH_ENABLE_SELECT_BY_SPECIFIC_ATTRIBUTES", default = "true")]
    enable_select_by_specific_attributes: EnvVarBoolean,
    #[envconfig(from = "GRAPH_POSTPONE_ATTRIBUTE_INDEX_CREATION", default = "false")]
    postpone_attribute_index_creation: EnvVarBoolean,
    #[envconfig(from = "GRAPH_LOG_TRIGGER_DATA", default = "false")]
    log_trigger_data: EnvVarBoolean,
    #[envconfig(from = "GRAPH_EXPLORER_TTL", default = "10")]
    explorer_ttl_in_secs: u64,
    #[envconfig(from = "GRAPH_EXPLORER_LOCK_THRESHOLD", default = "100")]
    explorer_lock_threshold_in_msec: u64,
    #[envconfig(from = "GRAPH_EXPLORER_QUERY_THRESHOLD", default = "500")]
    explorer_query_threshold_in_msec: u64,
    #[envconfig(from = "EXTERNAL_HTTP_BASE_URL")]
    external_http_base_url: Option<String>,
    #[envconfig(from = "EXTERNAL_WS_BASE_URL")]
    external_ws_base_url: Option<String>,
    #[envconfig(from = "GRAPH_STATIC_FILTERS_THRESHOLD", default = "10000")]
    static_filters_threshold: usize,
    // JSON-RPC specific.
    #[envconfig(from = "ETHEREUM_REORG_THRESHOLD")]
    reorg_threshold: Option<BlockNumber>,
    #[envconfig(from = "ETHEREUM_POLLING_INTERVAL", default = "1000")]
    ingestor_polling_interval: u64,
    #[envconfig(from = "GRAPH_EXPERIMENTAL_SUBGRAPH_SETTINGS")]
    subgraph_settings: Option<String>,
    #[envconfig(
        from = "GRAPH_EXPERIMENTAL_PREFER_SUBSTREAMS_BLOCK_STREAMS",
        default = "false"
    )]
    prefer_substreams_block_streams: bool,
    #[envconfig(from = "GRAPH_ENABLE_DIPS_METRICS", default = "false")]
    enable_dips_metrics: EnvVarBoolean,
    #[envconfig(from = "GRAPH_HISTORY_BLOCKS_OVERRIDE")]
    history_blocks_override: Option<BlockNumber>,
    #[envconfig(from = "GRAPH_MIN_HISTORY_BLOCKS")]
    min_history_blocks: Option<BlockNumber>,
    #[envconfig(from = "GRAPH_DIPS_METRICS_OBJECT_STORE_URL")]
    dips_metrics_object_store_url: Option<String>,
    #[envconfig(from = "GRAPH_SECTION_MAP")]
    section_map: Option<String>,
    #[envconfig(from = "GRAPH_NODE_FIREHOSE_MAX_DECODE_SIZE", default = "25")]
    firehose_grpc_max_decode_size_mb: usize,
}

#[derive(Clone, Debug)]
pub enum CachedSubgraphIds {
    All,
    Only(Vec<String>),
}

/// When reading [`bool`] values from environment variables, we must be able to
/// parse many different ways to specify booleans:
///
///  - Empty strings, i.e. as a flag.
///  - `true` or `false`.
///  - `1` or `0`.
#[derive(Copy, Clone, Debug)]
pub struct EnvVarBoolean(pub bool);

impl FromStr for EnvVarBoolean {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "" | "true" | "1" => Ok(Self(true)),
            "false" | "0" => Ok(Self(false)),
            _ => Err("Invalid env. var. flag, expected true / false / 1 / 0".to_string()),
        }
    }
}

/// Allows us to parse stuff ignoring underscores, notably big numbers.
#[derive(Copy, Clone, Debug)]
pub struct NoUnderscores<T>(T);

impl<T> FromStr for NoUnderscores<T>
where
    T: FromStr,
    T::Err: ToString,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match T::from_str(s.replace('_', "").as_str()) {
            Ok(x) => Ok(Self(x)),
            Err(e) => Err(e.to_string()),
        }
    }
}

/// Provide a numeric ([`usize`]) default value if the environment flag is
/// empty.
#[derive(Copy, Clone, Debug)]
pub struct WithDefaultUsize<T, const N: usize>(T);

impl<T, const N: usize> FromStr for WithDefaultUsize<T, N>
where
    T: FromStr,
    T::Err: ToString,
{
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let x = if s.is_empty() {
            T::from_str(N.to_string().as_str())
        } else {
            T::from_str(s)
        };
        match x {
            Ok(x) => Ok(Self(x)),
            Err(e) => Err(e.to_string()),
        }
    }
}
