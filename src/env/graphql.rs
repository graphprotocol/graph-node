use std::fmt;

use super::*;

#[derive(Clone)]
pub struct EnvVarsGraphQl {
    /// Set by the flag `ENABLE_GRAPHQL_VALIDATIONS`. Off by default.
    pub enable_validations: bool,
    pub subscription_throttle_interval: Duration,
    /// This is the timeout duration for SQL queries.
    ///
    /// If it is not set, no statement timeout will be enforced. The statement
    /// timeout is local, i.e., can only be used within a transaction and
    /// will be cleared at the end of the transaction.
    ///
    /// Set by the environment variable `GRAPH_SQL_STATEMENT_TIMEOUT` (expressed
    /// in seconds). No default value is provided.
    pub sql_statement_timeout: Option<Duration>,

    /// Set by the environment variable `GRAPH_GRAPHQL_QUERY_TIMEOUT` (expressed in
    /// seconds). No default value is provided.
    pub query_timeout: Option<Duration>,
    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_COMPLEXITY`. No
    /// default value is provided.
    pub max_complexity: Option<u64>,
    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_DEPTH`. The default
    /// value is 255.
    pub max_depth: u8,
    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_FIRST`. The default
    /// value is 1000.
    pub max_first: u32,
    /// Set by the environment variable `GRAPH_GRAPHQL_MAX_SKIP`. The default
    /// value is 4294967295 ([`u32::MAX`]).
    pub max_skip: u32,
    /// Allow skipping the check whether a deployment has changed while
    /// we were running a query. Once we are sure that the check mechanism
    /// is reliable, this variable should be removed.
    ///
    /// Set by the flag `GRAPHQL_ALLOW_DEPLOYMENT_CHANGE`. Off by default.
    pub allow_deployment_change: bool,
    /// Set by the environment variable `GRAPH_GRAPHQL_WARN_RESULT_SIZE`. The
    /// default value is [`usize::MAX`].
    pub warn_result_size: usize,
    /// Set by the environment variable `GRAPH_GRAPHQL_ERROR_RESULT_SIZE`. The
    /// default value is [`usize::MAX`].
    pub error_result_size: usize,
    /// Set by the flag `GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION`. No
    /// default is provided.
    pub max_operations_per_connection: Option<usize>,
}

// This does not print any values avoid accidentally leaking any sensitive env vars
impl fmt::Debug for EnvVarsGraphQl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "env vars")
    }
}

impl From<InnerGraphQl> for EnvVarsGraphQl {
    fn from(x: InnerGraphQl) -> Self {
        Self {
            enable_validations: x.enable_validations.0,
            subscription_throttle_interval: Duration::from_millis(
                x.subscription_throttle_interval_in_ms,
            ),
            sql_statement_timeout: x.sql_statement_timeout_in_secs.map(Duration::from_secs),
            query_timeout: x.query_timeout_in_secs.map(Duration::from_secs),
            max_complexity: x.max_complexity.map(|x| x.0),
            max_depth: x.max_depth.0,
            max_first: x.max_first,
            max_skip: x.max_skip.0,
            allow_deployment_change: x.allow_deployment_change.0,
            warn_result_size: x.warn_result_size.0 .0,
            error_result_size: x.error_result_size.0 .0,
            max_operations_per_connection: x.max_operations_per_connection,
        }
    }
}

#[derive(Clone, Debug, Envconfig)]
pub struct InnerGraphQl {
    #[envconfig(from = "ENABLE_GRAPHQL_VALIDATIONS", default = "false")]
    enable_validations: EnvVarBoolean,
    #[envconfig(from = "SUBSCRIPTION_THROTTLE_INTERVAL", default = "1000")]
    subscription_throttle_interval_in_ms: u64,
    #[envconfig(from = "GRAPH_SQL_STATEMENT_TIMEOUT")]
    sql_statement_timeout_in_secs: Option<u64>,

    #[envconfig(from = "GRAPH_GRAPHQL_QUERY_TIMEOUT")]
    query_timeout_in_secs: Option<u64>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_COMPLEXITY")]
    max_complexity: Option<NoUnderscores<u64>>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_DEPTH", default = "")]
    max_depth: WithDefaultUsize<u8, { u8::MAX as usize }>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_FIRST", default = "1000")]
    max_first: u32,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_SKIP", default = "")]
    max_skip: WithDefaultUsize<u32, { u32::MAX as usize }>,
    #[envconfig(from = "GRAPHQL_ALLOW_DEPLOYMENT_CHANGE", default = "false")]
    allow_deployment_change: EnvVarBoolean,
    #[envconfig(from = "GRAPH_GRAPHQL_WARN_RESULT_SIZE", default = "")]
    warn_result_size: WithDefaultUsize<NoUnderscores<usize>, { usize::MAX }>,
    #[envconfig(from = "GRAPH_GRAPHQL_ERROR_RESULT_SIZE", default = "")]
    error_result_size: WithDefaultUsize<NoUnderscores<usize>, { usize::MAX }>,
    #[envconfig(from = "GRAPH_GRAPHQL_MAX_OPERATIONS_PER_CONNECTION")]
    max_operations_per_connection: Option<usize>,
}
