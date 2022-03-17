use envconfig::Envconfig;
use lazy_static::lazy_static;
use parking_lot::{RwLock, RwLockReadGuard};
use semver::Version;
use std::{
    collections::HashSet,
    env::VarError,
    ops::Deref,
    str::FromStr,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use crate::{
    components::{store::BlockNumber, subgraph::SubgraphVersionSwitchingMode},
    runtime::gas::CONST_MAX_GAS_PER_HANDLER,
};

pub static UNSAFE_CONFIG: AtomicBool = AtomicBool::new(false);

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

// This is currently unused but is kept as a potentially useful mechanism.
/// Panics if:
/// - The value is not UTF8.
/// - The value cannot be parsed as T.
/// - The value differs from the default, and `--unsafe-config` flag is not set.
pub fn unsafe_env_var<E: std::error::Error + Send + Sync, T: FromStr<Err = E> + Eq>(
    name: &'static str,
    default_value: T,
) -> T {
    let var = match std::env::var(name) {
        Ok(var) => var,
        Err(VarError::NotPresent) => return default_value,
        Err(VarError::NotUnicode(_)) => panic!("environment variable {} is not UTF8", name),
    };

    let value = var
        .parse::<T>()
        .unwrap_or_else(|e| panic!("failed to parse environment variable {}: {}", name, e));

    if !UNSAFE_CONFIG.load(Ordering::SeqCst) && value != default_value {
        panic!(
            "unsafe environment variable {} is set. The recommended action is to unset it. \
             If this is not an indexer on the network, \
             you may provide the `--unsafe-config` to allow setting this variable.",
            name
        )
    }

    value
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

struct Inner {
    misc: EnvVarsMisc,
    graphql: EnvVarsGraphQl,
    mapping_handlers: EnvVarsMappingHandlers,
    ethereum: EnvVarsEthereum,
    store: EnvVarsStore,

    log_query_timing: Vec<String>,
    account_tables: HashSet<String>,
    geth_eth_call_errors: Vec<String>,
    cached_subgraph_ids: Option<Vec<String>>,
}

impl Inner {
    fn from_env() -> Result<Self, envconfig::Error> {
        let inner = EnvVarsMisc::init_from_env()?;
        let graphql = EnvVarsGraphQl::init_from_env()?;
        let mapping_handlers = EnvVarsMappingHandlers::init_from_env()?;
        let ethereum = EnvVarsEthereum::init_from_env()?;
        let store = EnvVarsStore::init_from_env()?;

        let log_query_timing = inner
            .log_query_timing
            .split(',')
            .map(str::to_string)
            .collect();
        let account_tables = store
            .account_tables
            .split(',')
            .map(|s| format!("\"{}\"", s.replace(".", "\".\"")))
            .collect();
        let geth_eth_call_errors = ethereum
            .geth_eth_call_errors
            .split(';')
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        let cached_subgraph_ids = if inner.cached_subgraph_ids == "*" {
            None
        } else {
            Some(
                inner
                    .cached_subgraph_ids
                    .split(',')
                    .map(str::to_string)
                    .collect(),
            )
        };

        Ok(Self {
            misc: inner,
            graphql,
            mapping_handlers,
            ethereum,
            store,
            log_query_timing,
            account_tables,
            geth_eth_call_errors,
            cached_subgraph_ids,
        })
    }
}

pub struct EnvVars {
    inner: RwLock<Inner>,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        Ok(Self {
            inner: RwLock::new(Inner::from_env()?),
        })
    }

    /// Refreshes all internally stored environment variables to reflect the
    /// current environment variable values.
    ///
    /// Only available in debug builds.
    #[cfg(debug_assertions)]
    pub fn refresh(&self) -> Result<(), envconfig::Error> {
        *self.inner.write() = Inner::from_env()?;
        Ok(())
    }

    fn inner(&self) -> RwLockReadGuard<Inner> {
        self.inner.read()
    }
}

/// Miscellaneous.
impl EnvVars {
    /// Enables query throttling when getting database connections goes over this value.
    /// Load management can be disabled by setting this to 0.
    ///
    /// Set by the environment variable `GRAPH_LOAD_THRESHOLD` (expressed in
    /// milliseconds). The default value is 0.
    pub fn load_threshold(&self) -> Duration {
        Duration::from_millis(self.inner().misc.load_threshold_in_ms)
    }

    /// Equivalent to checking if [`EnvVar::load_threshold`] is set to
    /// [`Duration::ZERO`].
    pub fn load_management_is_disabled(&self) -> bool {
        self.load_threshold().is_zero()
    }

    /// When the system is overloaded, any query that causes more than this
    /// fraction of the effort will be rejected for as long as the process is
    /// running (i.e. even after the overload situation is resolved).
    ///
    /// Set by the environment variable `GRAPH_LOAD_THRESHOLD`
    /// (expressed as a number). No default value is provided. When *not* set,
    /// no queries will ever be jailed, even though they will still be subject
    /// to normal load management when the system is overloaded.
    pub fn load_jail_threshold(&self) -> Option<f64> {
        self.inner().misc.load_jail_threshold
    }

    /// When this is active, the system will trigger all the steps that the load
    /// manager would given the other load management configuration settings,
    /// but never actually decline to run a query; instead, log about load
    /// management decisions.
    ///
    /// Set by the flag `GRAPH_LOAD_SIMULATE`.
    pub fn load_simulate(&self) -> bool {
        self.inner().misc.load_simulate.0
    }

    /// Set by the flag `GRAPH_ALLOW_NON_DETERMINISTIC_FULLTEXT_SEARCH`, but
    /// enabled anyway (overridden) if [debug
    /// assertions](https://doc.rust-lang.org/reference/conditional-compilation.html#debug_assertions)
    /// are enabled.
    pub fn allow_non_deterministic_fulltext_search(&self) -> bool {
        self.inner().misc.allow_non_deterministic_fulltext_search.0 || cfg!(debug_assertions)
    }

    /// Set by the environment variable `GRAPH_MAX_SPEC_VERSION`. The default
    /// value is `0.0.4`.
    pub fn max_spec_version(&self) -> Version {
        self.inner().misc.max_spec_version.clone()
    }

    /// Set by the flag `GRAPH_DISABLE_GRAFTS`.
    pub fn disable_grafts(&self) -> bool {
        self.inner().misc.disable_grafts.0
    }

    /// Set by the environment variable `GRAPH_LOAD_WINDOW_SIZE` (expressed in
    /// seconds). The default value is 300 seconds.
    pub fn load_window_size(&self) -> Duration {
        Duration::from_secs(self.inner().misc.load_window_size_in_secs)
    }

    /// Set by the environment variable `GRAPH_LOAD_BIN_SIZE` (expressed in
    /// seconds). The default value is 1 second.
    pub fn load_bin_size(&self) -> Duration {
        Duration::from_secs(self.inner().misc.load_bin_size_in_secs)
    }

    /// Set by the environment variable
    /// `GRAPH_ELASTIC_SEARCH_FLUSH_INTERVAL_SECS` (expressed in seconds). The
    /// default value is 5 seconds.
    pub fn elastic_search_flush_interval(&self) -> Duration {
        Duration::from_secs(self.inner().misc.elastic_search_flush_interval_in_secs)
    }

    /// Set by the environment variable
    /// `GRAPH_ELASTIC_SEARCH_MAX_RETRIES`. The default value is 5.
    pub fn elastic_search_max_retries(&self) -> usize {
        self.inner().misc.elastic_search_max_retries
    }

    /// If an instrumented lock is contended for longer than the specified
    /// duration, a warning will be logged.
    ///
    /// Set by the environment variable `GRAPH_LOCK_CONTENTION_LOG_THRESHOLD_MS`
    /// (expressed in milliseconds). The default value is 100ms.
    pub fn lock_contention_log_threshold(&self) -> Duration {
        Duration::from_millis(self.inner().misc.lock_contention_log_threshold_in_ms)
    }

    /// This is configurable only for debugging purposes. This value is set by
    /// the protocol, so indexers running in the network should never set this
    /// config.
    ///
    /// Set by the environment variable `GRAPH_MAX_GAS_PER_HANDLER`.
    pub fn max_gas_per_handler(&self) -> u64 {
        self.inner().misc.max_gas_per_handler.0 .0
    }

    /// Set by the environment variable `GRAPH_LOG_QUERY_TIMING`.
    pub fn log_query_timing(&self) -> impl Deref<Target = [String]> + '_ {
        RwLockReadGuard::map(self.inner(), |x| &x.log_query_timing[..])
    }

    fn log_query_timing_contains(&self, kind: &str) -> bool {
        self.log_query_timing().iter().any(|s| s == kind)
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

    /// A
    /// [`chrono`](https://docs.rs/chrono/latest/chrono/#formatting-and-parsing)
    /// -like format string for logs.
    ///
    /// Set by the environment variable `GRAPH_LOG_TIME_FORMAT`. The default
    /// value is `%b %d %H:%M:%S%.3f`.
    pub fn log_time_format(&self) -> String {
        self.inner().misc.log_time_format.clone()
    }

    /// Set by the flag `GRAPH_LOG_POI_EVENTS`.
    pub fn log_poi_events(&self) -> bool {
        self.inner().misc.log_poi_events.0
    }

    /// Set by the environment variable `GRAPH_LOG`.
    pub fn log_levels(&self) -> Option<String> {
        self.inner().misc.log_levels.clone()
    }

    /// Set by the flag `EXPERIMENTAL_STATIC_FILTERS`. Off by default.
    pub fn experimental_static_filters(&self) -> bool {
        self.inner().misc.experimental_static_filters.0
    }

    /// Set by the environment variable
    /// `EXPERIMENTAL_SUBGRAPH_VERSION_SWITCHING_MODE`. The default value is
    /// `"instant"`.
    pub fn subgraph_version_switching_mode(&self) -> SubgraphVersionSwitchingMode {
        self.inner().misc.subgraph_version_switching_mode
    }

    /// Set by the flag `GRAPH_KILL_IF_UNRESPONSIVE`. Off by default.
    pub fn kill_if_unresponsive(&self) -> bool {
        self.inner().misc.kill_if_unresponsive.0
    }

    /// Set by the environment variable `GRAPH_SUBGRAPH_MAX_DATA_SOURCES`. No
    /// default value is provided.
    pub fn subgraph_max_data_sources(&self) -> Option<usize> {
        self.inner().misc.subgraph_max_data_sources
    }

    /// Keep deterministic errors non-fatal even if the subgraph is pending.
    /// Used for testing Graph Node itself.
    ///
    /// Set by the flag `GRAPH_DISABLE_FAIL_FAST`. Off by default.
    pub fn disable_fail_fast(&self) -> bool {
        self.inner().misc.disable_fail_fast.0
    }

    /// Ceiling for the backoff retry of non-deterministic errors.
    ///
    /// Set by the environment variable `GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS`
    /// (expressed in seconds). The default value is 1800s (30 minutes).
    pub fn subgraph_error_retry_ceil(&self) -> Duration {
        Duration::from_secs(self.inner().misc.subgraph_error_retry_ceil_in_secs)
    }

    /// Set by the environment variable `GRAPH_CACHED_SUBGRAPH_IDS` (comma
    /// separated). When the value of the variable is `*`, queries are cached
    /// for all subgraphs and this method returns [`None`], which is the default
    /// behavior.
    pub fn cached_subgraph_ids(&self) -> Option<impl Deref<Target = [String]> + '_> {
        if self.inner().cached_subgraph_ids.is_none() {
            None
        } else {
            Some(RwLockReadGuard::map(self.inner(), |x| {
                x.cached_subgraph_ids.as_deref().unwrap_or(&[])
            }))
        }
    }

    /// In how many shards (mutexes) the query block cache is split.
    /// Ideally this should divide 256 so that the distribution of queries to
    /// shards is even.
    ///
    /// Set by the environment variable `GRAPH_QUERY_BLOCK_CACHE_SHARDS`. The
    /// default value is 128.
    pub fn query_block_cache_shards(&self) -> u8 {
        self.inner().misc.query_block_cache_shards
    }

    /// Set by the environment variable `GRAPH_QUERY_LFU_CACHE_SHARDS`. The
    /// default value is set to whatever `GRAPH_QUERY_BLOCK_CACHE_SHARDS` is set
    /// to.
    pub fn query_lfu_cache_shards(&self) -> u8 {
        let default = self.query_block_cache_shards();
        self.inner().misc.query_lfu_cache_shards.unwrap_or(default)
    }

    /// Set by the environment variable `GRAPH_EXPLORER_TTL`
    /// (expressed in seconds). The default value is 10s.
    pub fn explorer_ttl(&self) -> Duration {
        Duration::from_secs(self.inner().misc.explorer_ttl_in_secs)
    }

    /// Set by the environment variable `GRAPH_EXPLORER_LOCK_THRESHOLD`
    /// (expressed in milliseconds). The default value is 100ms.
    pub fn explorer_lock_threshold(&self) -> Duration {
        Duration::from_millis(self.inner().misc.explorer_lock_threshold_in_msec)
    }

    /// Set by the environment variable `GRAPH_EXPLORER_QUERY_THRESHOLD`
    /// (expressed in milliseconds). The default value is 500ms.
    pub fn explorer_query_threshold(&self) -> Duration {
        Duration::from_millis(self.inner().misc.explorer_query_threshold_in_msec)
    }

    /// Set by the environment variable `EXTERNAL_HTTP_BASE_URL`. No default
    /// value is provided.
    pub fn external_http_base_url(&self) -> Option<String> {
        self.inner().misc.external_http_base_url.clone()
    }

    /// Set by the environment variable `EXTERNAL_WS_BASE_URL`. No default
    /// value is provided.
    pub fn external_ws_base_url(&self) -> Option<String> {
        self.inner().misc.external_ws_base_url.clone()
    }

    /// Experimental feature.
    ///
    /// Set by the flag `GRAPH_ENABLE_SELECT_BY_SPECIFIC_ATTRIBUTES`. Off by
    /// default.
    pub fn enable_select_by_specific_attributes(&self) -> bool {
        self.inner().misc.enable_select_by_specific_attributes.0
    }

    /// Verbose logging of mapping inputs.
    ///
    /// Set by the flag `GRAPH_LOG_TRIGGER_DATA`. Off by
    /// default.
    pub fn log_trigger_data(&self) -> bool {
        self.inner().misc.log_trigger_data.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct EnvVarsMisc {
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
    #[envconfig(from = "GRAPH_MAX_SPEC_VERSION", default = "0.0.4")]
    max_spec_version: Version,
    #[envconfig(from = "GRAPH_DISABLE_GRAFTS", default = "false")]
    disable_grafts: EnvVarBoolean,
    #[envconfig(from = "GRAPH_LOAD_WINDOW_SIZE", default = "300")]
    load_window_size_in_secs: u64,
    #[envconfig(from = "GRAPH_LOAD_BIN_SIZE", default = "1")]
    load_bin_size_in_secs: u64,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_FLUSH_INTERVAL_SECS", default = "5")]
    elastic_search_flush_interval_in_secs: u64,
    #[envconfig(from = "GRAPH_ELASTIC_SEARCH_MAX_RETRIES", default = "5")]
    elastic_search_max_retries: usize,
    #[envconfig(from = "GRAPH_LOCK_CONTENTION_LOG_THRESHOLD_MS", default = "100")]
    lock_contention_log_threshold_in_ms: u64,
    #[envconfig(from = "GRAPH_MAX_GAS_PER_HANDLER", default = "")]
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
    #[envconfig(from = "GRAPH_SUBGRAPH_MAX_DATA_SOURCES")]
    subgraph_max_data_sources: Option<usize>,
    #[envconfig(from = "GRAPH_DISABLE_FAIL_FAST", default = "false")]
    disable_fail_fast: EnvVarBoolean,
    #[envconfig(from = "GRAPH_SUBGRAPH_ERROR_RETRY_CEIL_SECS", default = "1800")]
    subgraph_error_retry_ceil_in_secs: u64,
    #[envconfig(from = "GRAPH_CACHED_SUBGRAPH_IDS", default = "*")]
    cached_subgraph_ids: String,
    #[envconfig(from = "GRAPH_QUERY_BLOCK_CACHE_SHARDS", default = "128")]
    query_block_cache_shards: u8,
    #[envconfig(from = "GRAPH_QUERY_LFU_CACHE_SHARDS")]
    query_lfu_cache_shards: Option<u8>,
    #[envconfig(from = "GRAPH_ENABLE_SELECT_BY_SPECIFIC_ATTRIBUTES", default = "false")]
    enable_select_by_specific_attributes: EnvVarBoolean,
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
}

/// GraphQL.
impl EnvVars {
    /// Set by the flag `ENABLE_GRAPHQL_VALIDATIONS`. Off by default.
    pub fn enable_graphql_validations(&self) -> bool {
        self.inner().graphql.enable_graphql_validations.0
    }

    pub fn subscription_throttle_interval(&self) -> Duration {
        Duration::from_millis(self.inner().graphql.subscription_throttle_interval_in_ms)
    }

    /// This is the timeout duration for SQL queries.
    ///
    /// If it is not set, no statement timeout will be enforced. The statement
    /// timeout is local, i.e., can only be used within a transaction and
    /// will be cleared at the end of the transaction.
    ///
    /// Set by the environment variable `GRAPH_SQL_STATEMENT_TIMEOUT` (expressed
    /// in seconds). No default value is provided.
    pub fn sql_statement_timeout(&self) -> Option<Duration> {
        self.inner()
            .graphql
            .sql_statement_timeout_in_secs
            .map(Duration::from_secs)
    }

    /// Set by the environment variable `GRAPH_GRAPHQL_QUERY_TIMEOUT` (expressed in
    /// seconds). No default value is provided.
    pub fn graphql_query_timeout(&self) -> Option<Duration> {
        self.inner()
            .graphql
            .graphql_query_timeout_in_secs
            .map(Duration::from_secs)
    }

    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_COMPLEXITY`. No
    /// default value is provided.
    pub fn graphql_max_complexity(&self) -> Option<u64> {
        self.inner().graphql.graphql_max_complexity.map(|x| x.0)
    }

    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_DEPTH`. The default
    /// value is 255.
    pub fn graphql_max_depth(&self) -> u8 {
        self.inner().graphql.graphql_max_depth.0
    }

    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_FIRST`. The default
    /// value is 1000.
    pub fn graphql_max_first(&self) -> u32 {
        self.inner().graphql.graphql_max_first
    }

    /// Set by the environment variable `4294967295`. The default
    /// value is 4294967295 ([`u32::MAX`]).
    pub fn graphql_max_skip(&self) -> u32 {
        self.inner().graphql.graphql_max_skip.0
    }

    /// Allow skipping the check whether a deployment has changed while
    /// we were running a query. Once we are sure that the check mechanism
    /// is reliable, this variable should be removed.
    ///
    /// Set by the flag `GRAPHQL_ALLOW_DEPLOYMENT_CHANGE`. Off by default.
    pub fn graphql_allow_deployment_change(&self) -> bool {
        self.inner().graphql.graphql_allow_deployment_change.0
    }

    /// Set by the flag `GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION`. No
    /// default is provided.
    pub fn graphql_max_operations_per_connection(&self) -> Option<usize> {
        self.inner().graphql.graphql_max_operations_per_connection
    }

    /// Set by the environment variable `GRAPH_GRAPHQL_WARN_RESULT_SIZE`. The
    /// default value is [`usize::MAX`].
    pub fn graphql_warn_result_size(&self) -> usize {
        self.inner().graphql.graphql_warn_result_size.0 .0
    }

    /// Set by the environment variable `GRAPH_GRAPHQL_ERROR_RESULT_SIZE`. The
    /// default value is [`usize::MAX`].
    pub fn graphql_error_result_size(&self) -> usize {
        self.inner().graphql.graphql_error_result_size.0 .0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct EnvVarsGraphQl {
    #[envconfig(from = "ENABLE_GRAPHQL_VALIDATIONS", default = "false")]
    enable_graphql_validations: EnvVarBoolean,
    #[envconfig(from = "SUBSCRIPTION_THROTTLE_INTERVAL", default = "1000")]
    subscription_throttle_interval_in_ms: u64,
    #[envconfig(from = "GRAPH_SQL_STATEMENT_TIMEOUT")]
    sql_statement_timeout_in_secs: Option<u64>,

    #[envconfig(from = "GRAPH_GRAPHQL_QUERY_TIMEOUT")]
    graphql_query_timeout_in_secs: Option<u64>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_COMPLEXITY")]
    graphql_max_complexity: Option<NoUnderscores<u64>>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_DEPTH", default = "")]
    graphql_max_depth: WithDefaultUsize<u8, { u8::MAX as usize }>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_FIRST", default = "1000")]
    graphql_max_first: u32,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_SKIP", default = "")]
    graphql_max_skip: WithDefaultUsize<u32, { u32::MAX as usize }>,
    #[envconfig(from = "GRAPHQL_ALLOW_DEPLOYMENT_CHANGE", default = "false")]
    graphql_allow_deployment_change: EnvVarBoolean,
    #[envconfig(from = "GRAPH_GRAPHQL_WARN_RESULT_SIZE", default = "")]
    graphql_warn_result_size: WithDefaultUsize<NoUnderscores<usize>, { usize::MAX }>,
    #[envconfig(from = "GRAPH_GRAPHQL_ERROR_RESULT_SIZE", default = "")]
    graphql_error_result_size: WithDefaultUsize<NoUnderscores<usize>, { usize::MAX }>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION")]
    graphql_max_operations_per_connection: Option<usize>,
}

/// Mapping handlers.
impl EnvVars {
    /// Size limit of the entity LFU cache.
    ///
    /// Set by the environment variable `GRAPH_ENTITY_CACHE_SIZE` (expressed in
    /// kilobytes). The default value is 10 megabytes.
    pub fn entity_cache_size(&self) -> usize {
        self.inner().mapping_handlers.entity_cache_size_in_kb * 1000
    }

    /// Set by the environment variable `GRAPH_MAX_API_VERSION`. The default
    /// value is `0.0.6`.
    pub fn max_api_version(&self) -> Version {
        self.inner().mapping_handlers.max_api_version.clone()
    }

    /// How many blocks per network should be kept in the query cache. When the
    /// limit is reached, older blocks are evicted. This should be kept small
    /// since a lookup to the cache is O(n) on this value, and the cache memory
    /// usage also increases with larger number. Set to 0 to disable
    /// the cache.
    ///
    /// Set by the environment variable `GRAPH_QUERY_CACHE_BLOCKS`. The default
    /// value is 2.
    pub fn query_cache_blocks(&self) -> usize {
        self.inner().mapping_handlers.query_cache_blocks
    }

    /// Set by the environment variable `GRAPH_MAPPING_HANDLER_TIMEOUT`
    /// (expressed in seconds). No default is provided.
    pub fn mapping_handler_timeout(&self) -> Option<Duration> {
        self.inner()
            .mapping_handlers
            .mapping_handler_timeout_in_secs
            .map(Duration::from_secs)
    }

    /// Maximum stack size for the WASM runtime.
    ///
    /// Set by the environment variable `GRAPH_RUNTIME_MAX_STACK_SIZE`
    /// (expressed in bytes). The default value is 512KiB.
    pub fn runtime_max_stack_size(&self) -> usize {
        self.inner().mapping_handlers.runtime_max_stack_size.0 .0
    }

    /// Maximum total memory to be used by the cache. Each block has a max size of
    /// `QUERY_CACHE_MAX_MEM` / (`QUERY_CACHE_BLOCKS` *
    /// `GRAPH_QUERY_BLOCK_CACHE_SHARDS`).
    ///
    /// Set by the environment variable `GRAPH_QUERY_CACHE_MAX_MEM` (expressed
    /// in MB). The default value is 1GB.
    pub fn query_cache_max_mem(&self) -> usize {
        self.inner().mapping_handlers.query_cache_max_mem_in_mb.0 * 1_000_000
    }

    /// Set by the environment variable `GRAPH_QUERY_CACHE_STALE_PERIOD`. The
    /// default value is 100.
    pub fn query_cache_stale_period(&self) -> u64 {
        self.inner().mapping_handlers.query_cache_stale_period
    }

    // IPFS
    // ----

    /// Set by the environment variable `GRAPH_MAX_IPFS_CACHE_FILE_SIZE`
    /// (expressed in bytes). The default value is 1MiB.
    pub fn max_ipfs_cache_file_size(&self) -> usize {
        self.inner().mapping_handlers.max_ipfs_cache_file_size.0
    }

    /// Set by the environment variable `GRAPH_MAX_IPFS_CACHE_SIZE`. The default
    /// value is 50 items.
    pub fn max_ipfs_cache_size(&self) -> u64 {
        self.inner().mapping_handlers.max_ipfs_cache_size
    }

    /// The timeout for all IPFS requests.
    ///
    /// Set by the environment variable `GRAPH_IPFS_TIMEOUT` (expressed in
    /// seconds). The default value is 30s.
    pub fn ipfs_timeout(&self) -> Duration {
        Duration::from_secs(self.inner().mapping_handlers.ipfs_timeout_in_secs)
    }

    /// Sets the `ipfs.map` file size limit.
    ///
    /// Set by the environment variable `GRAPH_MAX_IPFS_MAP_FILE_SIZE_LIMIT`
    /// (expressed in bytes). The default value is 256MiB.
    pub fn max_ipfs_map_file_size(&self) -> usize {
        self.inner().mapping_handlers.max_ipfs_map_file_size.0
    }

    /// Sets the `ipfs.cat` file size limit.
    ///
    /// Set by the environment variable `GRAPH_MAX_IPFS_FILE_BYTES` (expressed in
    /// bytes). No default value is provided.
    ///
    /// FIXME: Having an env variable here is a problem for consensus.
    /// Index Nodes should not disagree on whether the file should be read.
    pub fn max_ipfs_file_bytes(&self) -> Option<usize> {
        self.inner().mapping_handlers.max_ipfs_file_bytes
    }

    /// Set by the flag `GRAPH_ALLOW_NON_DETERMINISTIC_IPFS`. Off by
    /// default.
    pub fn allow_non_deterministic_ipfs(&self) -> bool {
        self.inner().mapping_handlers.allow_non_deterministic_ipfs.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct EnvVarsMappingHandlers {
    #[envconfig(from = "GRAPH_ENTITY_CACHE_SIZE", default = "10000")]
    entity_cache_size_in_kb: usize,
    #[envconfig(from = "GRAPH_MAX_API_VERSION", default = "0.0.6")]
    max_api_version: Version,
    #[envconfig(from = "GRAPH_MAPPING_HANDLER_TIMEOUT")]
    mapping_handler_timeout_in_secs: Option<u64>,
    #[envconfig(from = "GRAPH_RUNTIME_MAX_STACK_SIZE", default = "")]
    runtime_max_stack_size: WithDefaultUsize<NoUnderscores<usize>, { 512 * 1024 }>,
    #[envconfig(from = "GRAPH_QUERY_CACHE_BLOCKS", default = "2")]
    query_cache_blocks: usize,
    #[envconfig(from = "GRAPH_QUERY_CACHE_MAX_MEM", default = "1000")]
    query_cache_max_mem_in_mb: NoUnderscores<usize>,
    #[envconfig(from = "GRAPH_QUERY_CACHE_STALE_PERIOD", default = "100")]
    query_cache_stale_period: u64,

    // IPFS.
    #[envconfig(from = "GRAPH_MAX_IPFS_CACHE_FILE_SIZE", default = "")]
    max_ipfs_cache_file_size: WithDefaultUsize<usize, { 1024 * 1024 }>,
    #[envconfig(from = "GRAPH_MAX_IPFS_CACHE_SIZE", default = "50")]
    max_ipfs_cache_size: u64,
    #[envconfig(from = "GRAPH_IPFS_TIMEOUT", default = "30")]
    ipfs_timeout_in_secs: u64,
    #[envconfig(from = "GRAPH_MAX_IPFS_MAP_FILE_SIZE", default = "")]
    max_ipfs_map_file_size: WithDefaultUsize<usize, { 256 * 1024 * 1024 }>,
    #[envconfig(from = "GRAPH_MAX_IPFS_FILE_BYTES")]
    max_ipfs_file_bytes: Option<usize>,
    #[envconfig(from = "GRAPH_ALLOW_NON_DETERMINISTIC_IPFS", default = "false")]
    allow_non_deterministic_ipfs: EnvVarBoolean,
}

/// Store.
impl EnvVars {
    /// Set by the environment variable `GRAPH_CHAIN_HEAD_WATCHER_TIMEOUT`
    /// (expressed in seconds). The default value is 30 seconds.
    pub fn chain_head_watcher_timeout(&self) -> Duration {
        Duration::from_secs(self.inner().store.chain_head_watcher_timeout_in_secs)
    }

    /// This is how long statistics that influence query execution are cached in
    /// memory before they are reloaded from the database.
    ///
    /// Set by the environment variable `GRAPH_QUERY_STATS_REFRESH_INTERVAL`
    /// (expressed in seconds). The default value is 300 seconds.
    pub fn query_stats_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.inner().store.query_stats_refresh_interval_in_secs)
    }

    /// This can be used to effectively disable the query semaphore by setting
    /// it to a high number, but there's typically no need to configure this.
    ///
    /// Set by the environment variable `GRAPH_EXTRA_QUERY_PERMITS`. The default
    /// value is 0.
    pub fn extra_query_permits(&self) -> usize {
        self.inner().store.extra_query_permits
    }

    /// Set by the environment variable `LARGE_NOTIFICATION_CLEANUP_INTERVAL`
    /// (expressed in seconds). The default value is 300 seconds.
    pub fn large_notification_cleanup_interval(&self) -> Duration {
        Duration::from_secs(
            self.inner()
                .store
                .large_notification_cleanup_interval_in_secs,
        )
    }

    /// Set by the environment variable `GRAPH_NOTIFICATION_BROADCAST_TIMEOUT`
    /// (expressed in seconds). The default value is 60 seconds.
    pub fn notification_broadcast_timeout(&self) -> Duration {
        Duration::from_secs(self.inner().store.notification_broadcast_timeout_in_secs)
    }

    /// This variable is only here temporarily until we can settle on the right
    /// batch size through experimentation, and should then just become an
    /// ordinary constant.
    ///
    /// Set by the environment variable `TYPEA_BATCH_SIZE`.
    pub fn typea_batch_size(&self) -> usize {
        self.inner().store.typea_batch_size
    }

    /// Allows for some optimizations when running relational queries. Set this
    /// to 0 to turn off this optimization.
    ///
    /// Set by the environment variable `TYPED_CHILDREN_SET_SIZE`.
    pub fn typed_children_set_size(&self) -> usize {
        self.inner().store.typed_children_set_size
    }

    /// When enabled, turns `ORDER BY id` into `ORDER BY id, block_range` in
    /// some relational queries.
    ///
    /// Set by the flag `ORDER_BY_BLOCK_RANGE`.
    pub fn order_by_block_range(&self) -> bool {
        self.inner().store.order_by_block_range.0
    }

    /// When the flag is present, `ORDER BY` clauses are changed so that `asc`
    /// and `desc` ordering produces reverse orders. Setting the flag turns the
    /// new, correct behavior off.
    ///
    /// Set by the flag `REVERSIBLE_ORDER_BY_OFF`.
    pub fn reversible_order_by_off(&self) -> bool {
        self.inner().store.reversible_order_by_off.0
    }

    /// A list of fully qualified table names that contain entities that are
    /// like accounts in that they have a relatively small number of entities,
    /// with a large number of change for each entity. It is useful to treat
    /// such tables special in queries by changing the clause that selects
    /// for a specific block range in a way that makes the BRIN index on
    /// block_range usable.
    ///
    /// The use of this environment variable is deprecated; use `graphman stats
    /// account-like` instead.
    ///
    /// Set by the environment variable `GRAPH_ACCOUNT_TABLES` (comma
    /// separated). Empty by default. E.g.
    /// `GRAPH_ACCOUNT_TABLES=sgd21902.pair,sgd1708.things`.
    pub fn account_tables(&self) -> impl Deref<Target = HashSet<String>> + '_ {
        RwLockReadGuard::map(self.inner(), |x| &x.account_tables)
    }

    /// Whether to disable the notifications that feed GraphQL
    /// subscriptions. When the flag is set, no updates
    /// about entity changes will be sent to query nodes.
    ///
    /// Set by the flag `GRAPH_DISABLE_SUBSCRIPTION_NOTIFICATIONS`. Not set
    /// by default.
    pub fn disable_subscription_notifications(&self) -> bool {
        self.inner().store.disable_subscription_notifications.0
    }

    /// Set by the environment variable `GRAPH_STORE_CONNECTION_TIMEOUT` (expressed
    /// in milliseconds). The default value is 5000ms.
    pub fn store_connection_timeout(&self) -> Duration {
        Duration::from_millis(self.inner().store.store_connection_timeout_in_millis)
    }

    /// Set by the environment variable `GRAPH_STORE_CONNECTION_MIN_IDLE`. No
    /// default value is provided.
    pub fn store_connection_min_idle(&self) -> Option<u32> {
        self.inner().store.store_connection_min_idle
    }

    /// Set by the environment variable `GRAPH_STORE_CONNECTION_IDLE_TIMEOUT`
    /// (expressed in seconds). The default value is 600s.
    pub fn store_connection_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.inner().store.store_connection_idle_timeout_in_secs)
    }

    /// A fallback in case the logic to remember database availability goes
    /// wrong; when this is set, we always try to get a connection and never
    /// use the availability state we remembered.
    ///
    /// Set by the flag `GRAPH_STORE_CONNECTION_TRY_ALWAYS`. Disabled by
    /// default.
    pub fn store_connection_try_always(&self) -> bool {
        self.inner().store.store_connection_try_always.0
    }

    /// Set by the environment variable `GRAPH_REMOVE_UNUSED_INTERVAL`
    /// (expressed in minutes). The default value is 360 minutes.
    pub fn remove_unused_interval(&self) -> chrono::Duration {
        chrono::Duration::minutes(self.inner().store.remove_unused_interval_in_minutes as i64)
    }
}

#[derive(Clone, Debug, Envconfig)]
struct EnvVarsStore {
    #[envconfig(from = "GRAPH_CHAIN_HEAD_WATCHER_TIMEOUT", default = "30")]
    chain_head_watcher_timeout_in_secs: u64,
    #[envconfig(from = "GRAPH_QUERY_STATS_REFRESH_INTERVAL", default = "300")]
    query_stats_refresh_interval_in_secs: u64,
    #[envconfig(from = "GRAPH_EXTRA_QUERY_PERMITS", default = "0")]
    extra_query_permits: usize,
    #[envconfig(from = "LARGE_NOTIFICATION_CLEANUP_INTERVAL", default = "300")]
    large_notification_cleanup_interval_in_secs: u64,
    #[envconfig(from = "GRAPH_NOTIFICATION_BROADCAST_TIMEOUT", default = "60")]
    notification_broadcast_timeout_in_secs: u64,
    #[envconfig(from = "TYPEA_BATCH_SIZE", default = "150")]
    typea_batch_size: usize,
    #[envconfig(from = "TYPED_CHILDREN_SET_SIZE", default = "150")]
    typed_children_set_size: usize,
    #[envconfig(from = "ORDER_BY_BLOCK_RANGE", default = "false")]
    order_by_block_range: EnvVarBoolean,
    #[envconfig(from = "REVERSIBLE_ORDER_BY_OFF", default = "false")]
    reversible_order_by_off: EnvVarBoolean,
    #[envconfig(from = "GRAPH_ACCOUNT_TABLES", default = "")]
    account_tables: String,
    #[envconfig(from = "GRAPH_DISABLE_SUBSCRIPTION_NOTIFICATIONS", default = "false")]
    disable_subscription_notifications: EnvVarBoolean,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_TRY_ALWAYS", default = "false")]
    store_connection_try_always: EnvVarBoolean,
    #[envconfig(from = "GRAPH_REMOVE_UNUSED_INTERVAL", default = "360")]
    remove_unused_interval_in_minutes: u64,

    // These should really be set through the configuration file, especially for
    // `GRAPH_STORE_CONNECTION_MIN_IDLE` and
    // `GRAPH_STORE_CONNECTION_IDLE_TIMEOUT`. It's likely that they should be
    // configured differently for each pool.
    #[envconfig(from = "GRAPH_STORE_CONNECTION_TIMEOUT", default = "5000")]
    store_connection_timeout_in_millis: u64,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_MIN_IDLE")]
    store_connection_min_idle: Option<u32>,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_IDLE_TIMEOUT", default = "600")]
    store_connection_idle_timeout_in_secs: u64,
}

/// Ethereum.
impl EnvVars {
    /// Set by the environment variable `ETHEREUM_REORG_THRESHOLD`. The default
    /// value is 250 blocks.
    pub fn ethereum_reorg_threshold(&self) -> BlockNumber {
        self.inner().ethereum.ethereum_reorg_threshold
    }

    /// Controls if firehose should be preferred over RPC if Firehose endpoints
    /// are present, if not set, the default behavior is is kept which is to
    /// automatically favor Firehose.
    ///
    /// Set by the flag `GRAPH_ETHEREUM_IS_FIREHOSE_PREFERRED`. On by default.
    pub fn ethereum_is_firehose_preferred(&self) -> bool {
        self.inner().ethereum.ethereum_is_firehose_preferred.0
    }

    /// Ideal number of triggers in a range. The range size will adapt to try to
    /// meet this.
    ///
    /// Set by the environment variable
    /// `GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE`. The default value is
    /// 100.
    pub fn ethereum_target_triggers_per_block_range(&self) -> u64 {
        self.inner()
            .ethereum
            .ethereum_target_triggers_per_block_range
    }

    /// Maximum number of blocks to request in each chunk.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE`.
    /// The default value is 2000 blocks.
    pub fn ethereum_max_block_range_size(&self) -> BlockNumber {
        self.inner().ethereum.ethereum_max_block_range_size
    }

    /// Set by the environment variable `ETHEREUM_TRACE_STREAM_STEP_SIZE`. The
    /// default value is 50 blocks.
    pub fn ethereum_trace_stream_step_size(&self) -> BlockNumber {
        self.inner().ethereum.ethereum_trace_stream_step_size
    }

    /// Maximum range size for `eth.getLogs` requests that don't filter on
    /// contract address, only event signature, and are therefore expensive.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE`. The
    /// default value is 500 blocks, which is reasonable according to Ethereum
    /// node operators.
    pub fn ethereum_max_event_only_range(&self) -> BlockNumber {
        self.inner().ethereum.ethereum_max_event_only_range
    }

    /// Set by the environment variable `ETHEREUM_BLOCK_BATCH_SIZE`. The
    /// default value is 10 blocks.
    pub fn ethereum_block_batch_size(&self) -> usize {
        self.inner().ethereum.ethereum_block_batch_size
    }

    /// This should not be too large that it causes requests to timeout without
    /// us catching it, nor too small that it causes us to timeout requests that
    /// would've succeeded. We've seen successful `eth_getLogs` requests take
    /// over 120 seconds.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_JSON_RPC_TIMEOUT`
    /// (expressed in seconds). The default value is 180s.
    pub fn ethereum_json_rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.inner().ethereum.ethereum_json_rpc_timeout_in_secs)
    }

    /// This is used for requests that will not fail the subgraph if the limit
    /// is reached, but will simply restart the syncing step, so it can be low.
    /// This limit guards against scenarios such as requesting a block hash that
    /// has been reorged.
    ///
    /// Set by the environment variable `GRAPH_ETHEREUM_REQUEST_RETRIES`. The
    /// default value is 10.
    pub fn ethereum_request_retries(&self) -> usize {
        self.inner().ethereum.ethereum_request_retries
    }

    /// Additional deterministic errors that have not yet been hardcoded.
    ///
    /// Set by the environment variable `GRAPH_GETH_ETH_CALL_ERRORS`, separated
    /// by `;`.
    pub fn geth_eth_call_errors(&self) -> impl Deref<Target = [String]> + '_ {
        RwLockReadGuard::map(self.inner(), |x| &x.geth_eth_call_errors[..])
    }

    /// Set by the environment variable `GRAPH_ETH_GET_LOGS_MAX_CONTRACTS`. The
    /// default value is 2000.
    pub fn ethereum_get_logs_max_contracts(&self) -> usize {
        self.inner().ethereum.ethereum_get_logs_max_contracts
    }

    /// Set by the environment variable
    /// `GRAPH_ETHEREUM_BLOCK_INGESTOR_MAX_CONCURRENT_JSON_RPC_CALLS_FOR_TXN_RECEIPTS`.
    /// The default value is 1000.
    pub fn ethereum_block_ingestor_max_concurrent_json_rpc_calls(&self) -> usize {
        self.inner()
            .ethereum
            .ethereum_block_ingestor_max_concurrent_json_rpc_calls
    }

    /// Set by the flag `GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES`. Enabled
    /// by default on macOS (to avoid DNS issues) and disabled by default on all
    /// other systems.
    pub fn ethereum_fetch_receipts_in_batches(&self) -> bool {
        let default = cfg!(target_os = "macos");

        self.inner()
            .ethereum
            .ethereum_fetch_receipts_in_batches
            .map(|x| x.0)
            .unwrap_or(default)
    }

    /// `graph_node::config` disallows setting this in a store with multiple
    /// shards. See 8b6ad0c64e244023ac20ced7897fe666 for the reason.
    ///
    /// Set by the flag `GRAPH_ETHEREUM_CLEANUP_BLOCKS`. Off by default.
    pub fn ethereum_cleanup_blocks(&self) -> bool {
        self.inner().ethereum.ethereum_cleanup_blocks.0
    }
}

#[derive(Clone, Debug, Envconfig)]
struct EnvVarsEthereum {
    #[envconfig(from = "GRAPH_ETHEREUM_IS_FIREHOSE_PREFERRED", default = "true")]
    ethereum_is_firehose_preferred: EnvVarBoolean,
    #[envconfig(from = "GRAPH_GETH_ETH_CALL_ERRORS", default = "")]
    geth_eth_call_errors: String,
    #[envconfig(from = "GRAPH_ETH_GET_LOGS_MAX_CONTRACTS", default = "2000")]
    ethereum_get_logs_max_contracts: usize,

    // JSON-RPC specific.
    #[envconfig(from = "ETHEREUM_REORG_THRESHOLD", default = "250")]
    ethereum_reorg_threshold: BlockNumber,
    #[envconfig(from = "ETHEREUM_TRACE_STREAM_STEP_SIZE", default = "50")]
    ethereum_trace_stream_step_size: BlockNumber,
    #[envconfig(from = "GRAPH_ETHEREUM_MAX_EVENT_ONLY_RANGE", default = "500")]
    ethereum_max_event_only_range: BlockNumber,
    #[envconfig(from = "ETHEREUM_BLOCK_BATCH_SIZE", default = "10")]
    ethereum_block_batch_size: usize,
    #[envconfig(from = "GRAPH_ETHEREUM_MAX_BLOCK_RANGE_SIZE", default = "2000")]
    ethereum_max_block_range_size: BlockNumber,
    #[envconfig(from = "GRAPH_ETHEREUM_JSON_RPC_TIMEOUT", default = "180")]
    ethereum_json_rpc_timeout_in_secs: u64,
    #[envconfig(from = "GRAPH_ETHEREUM_REQUEST_RETRIES", default = "10")]
    ethereum_request_retries: usize,
    #[envconfig(
        from = "GRAPH_ETHEREUM_BLOCK_INGESTOR_MAX_CONCURRENT_JSON_RPC_CALLS_FOR_TXN_RECEIPTS",
        default = "1000"
    )]
    ethereum_block_ingestor_max_concurrent_json_rpc_calls: usize,
    #[envconfig(from = "GRAPH_ETHEREUM_FETCH_TXN_RECEIPTS_IN_BATCHES")]
    ethereum_fetch_receipts_in_batches: Option<EnvVarBoolean>,
    #[envconfig(from = "GRAPH_ETHEREUM_CLEANUP_BLOCKS", default = "false")]
    ethereum_cleanup_blocks: EnvVarBoolean,
    #[envconfig(
        from = "GRAPH_ETHEREUM_TARGET_TRIGGERS_PER_BLOCK_RANGE",
        default = "100"
    )]
    ethereum_target_triggers_per_block_range: u64,
}

/// When reading [`bool`] values from environment variables, we must be able to
/// parse many different ways to specify booleans:
///
///  - Empty strings, i.e. as a flag.
///  - `true` or `false`.
///  - `1` or `0`.
#[derive(Copy, Clone, Debug)]
struct EnvVarBoolean(pub bool);

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
struct NoUnderscores<T>(T);

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
struct WithDefaultUsize<T, const N: usize>(T);

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
