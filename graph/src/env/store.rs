use std::fmt;

use super::*;

#[derive(Clone)]
pub struct EnvVarsStore {
    /// Set by the environment variable `GRAPH_CHAIN_HEAD_WATCHER_TIMEOUT`
    /// (expressed in seconds). The default value is 30 seconds.
    pub chain_head_watcher_timeout: Duration,
    /// This is how long statistics that influence query execution are cached in
    /// memory before they are reloaded from the database.
    ///
    /// Set by the environment variable `GRAPH_QUERY_STATS_REFRESH_INTERVAL`
    /// (expressed in seconds). The default value is 300 seconds.
    pub query_stats_refresh_interval: Duration,
    /// This can be used to effectively disable the query semaphore by setting
    /// it to a high number, but there's typically no need to configure this.
    ///
    /// Set by the environment variable `GRAPH_EXTRA_QUERY_PERMITS`. The default
    /// value is 0.
    pub extra_query_permits: usize,
    /// Set by the environment variable `LARGE_NOTIFICATION_CLEANUP_INTERVAL`
    /// (expressed in seconds). The default value is 300 seconds.
    pub large_notification_cleanup_interval: Duration,
    /// Set by the environment variable `GRAPH_NOTIFICATION_BROADCAST_TIMEOUT`
    /// (expressed in seconds). The default value is 60 seconds.
    pub notification_broadcast_timeout: Duration,
    /// This variable is only here temporarily until we can settle on the right
    /// batch size through experimentation, and should then just become an
    /// ordinary constant.
    ///
    /// Set by the environment variable `TYPEA_BATCH_SIZE`.
    pub typea_batch_size: usize,
    /// Allows for some optimizations when running relational queries. Set this
    /// to 0 to turn off this optimization.
    ///
    /// Set by the environment variable `TYPED_CHILDREN_SET_SIZE`.
    pub typed_children_set_size: usize,
    /// When enabled, turns `ORDER BY id` into `ORDER BY id, block_range` in
    /// some relational queries.
    ///
    /// Set by the flag `ORDER_BY_BLOCK_RANGE`.
    pub order_by_block_range: bool,
    /// When the flag is present, `ORDER BY` clauses are changed so that `asc`
    /// and `desc` ordering produces reverse orders. Setting the flag turns the
    /// new, correct behavior off.
    ///
    /// Set by the flag `REVERSIBLE_ORDER_BY_OFF`.
    pub reversible_order_by_off: bool,
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
    pub account_tables: HashSet<String>,
    /// Whether to disable the notifications that feed GraphQL
    /// subscriptions. When the flag is set, no updates
    /// about entity changes will be sent to query nodes.
    ///
    /// Set by the flag `GRAPH_DISABLE_SUBSCRIPTION_NOTIFICATIONS`. Not set
    /// by default.
    pub disable_subscription_notifications: bool,
    /// A fallback in case the logic to remember database availability goes
    /// wrong; when this is set, we always try to get a connection and never
    /// use the availability state we remembered.
    ///
    /// Set by the flag `GRAPH_STORE_CONNECTION_TRY_ALWAYS`. Disabled by
    /// default.
    pub connection_try_always: bool,
    /// Set by the environment variable `GRAPH_REMOVE_UNUSED_INTERVAL`
    /// (expressed in minutes). The default value is 360 minutes.
    pub remove_unused_interval: chrono::Duration,

    // These should really be set through the configuration file, especially for
    // `GRAPH_STORE_CONNECTION_MIN_IDLE` and
    // `GRAPH_STORE_CONNECTION_IDLE_TIMEOUT`. It's likely that they should be
    // configured differently for each pool.
    /// Set by the environment variable `GRAPH_STORE_CONNECTION_TIMEOUT` (expressed
    /// in milliseconds). The default value is 5000ms.
    pub connection_timeout: Duration,
    /// Set by the environment variable `GRAPH_STORE_CONNECTION_MIN_IDLE`. No
    /// default value is provided.
    pub connection_min_idle: Option<u32>,
    /// Set by the environment variable `GRAPH_STORE_CONNECTION_IDLE_TIMEOUT`
    /// (expressed in seconds). The default value is 600s.
    pub connection_idle_timeout: Duration,

    /// The size of the write queue; this many blocks can be buffered for
    /// writing before calls to transact block operations will block.
    /// Setting this to `0` disables pipelined writes, and writes will be
    /// done synchronously.
    pub write_queue_size: usize,

    /// This is just in case new behavior causes issues. This can be removed
    /// once the new behavior has run in the hosted service for a few days
    /// without issues.
    pub disable_error_for_toplevel_parents: bool,
}

// This does not print any values avoid accidentally leaking any sensitive env vars
impl fmt::Debug for EnvVarsStore {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "env vars")
    }
}

impl From<InnerStore> for EnvVarsStore {
    fn from(x: InnerStore) -> Self {
        Self {
            chain_head_watcher_timeout: Duration::from_secs(x.chain_head_watcher_timeout_in_secs),
            query_stats_refresh_interval: Duration::from_secs(
                x.query_stats_refresh_interval_in_secs,
            ),
            extra_query_permits: x.extra_query_permits,
            large_notification_cleanup_interval: Duration::from_secs(
                x.large_notification_cleanup_interval_in_secs,
            ),
            notification_broadcast_timeout: Duration::from_secs(
                x.notification_broadcast_timeout_in_secs,
            ),
            typea_batch_size: x.typea_batch_size,
            typed_children_set_size: x.typed_children_set_size,
            order_by_block_range: x.order_by_block_range.0,
            reversible_order_by_off: x.reversible_order_by_off.0,
            account_tables: x
                .account_tables
                .split(',')
                .map(|s| format!("\"{}\"", s.replace(".", "\".\"")))
                .collect(),
            disable_subscription_notifications: x.disable_subscription_notifications.0,
            connection_try_always: x.connection_try_always.0,
            remove_unused_interval: chrono::Duration::minutes(
                x.remove_unused_interval_in_minutes as i64,
            ),
            connection_timeout: Duration::from_millis(x.connection_timeout_in_millis),
            connection_min_idle: x.connection_min_idle,
            connection_idle_timeout: Duration::from_secs(x.connection_idle_timeout_in_secs),
            write_queue_size: x.write_queue_size,
            disable_error_for_toplevel_parents: x.disable_error_for_toplevel_parents.0,
        }
    }
}

#[derive(Clone, Debug, Envconfig)]
pub struct InnerStore {
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
    connection_try_always: EnvVarBoolean,
    #[envconfig(from = "GRAPH_REMOVE_UNUSED_INTERVAL", default = "360")]
    remove_unused_interval_in_minutes: u64,

    // These should really be set through the configuration file, especially for
    // `GRAPH_STORE_CONNECTION_MIN_IDLE` and
    // `GRAPH_STORE_CONNECTION_IDLE_TIMEOUT`. It's likely that they should be
    // configured differently for each pool.
    #[envconfig(from = "GRAPH_STORE_CONNECTION_TIMEOUT", default = "5000")]
    connection_timeout_in_millis: u64,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_MIN_IDLE")]
    connection_min_idle: Option<u32>,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_IDLE_TIMEOUT", default = "600")]
    connection_idle_timeout_in_secs: u64,
    #[envconfig(from = "GRAPH_STORE_WRITE_QUEUE", default = "5")]
    write_queue_size: usize,
    #[envconfig(from = "GRAPH_DISABLE_ERROR_FOR_TOPLEVEL_PARENTS", default = "false")]
    disable_error_for_toplevel_parents: EnvVarBoolean,
}
