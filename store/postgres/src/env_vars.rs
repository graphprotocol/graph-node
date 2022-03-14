use envconfig::Envconfig;
use graph::prelude::chrono;
use lazy_static::lazy_static;
use std::{collections::HashSet, str::FromStr, time::Duration};

lazy_static! {
    pub static ref ENV_VARS: EnvVars = EnvVars::from_env().unwrap();
}

#[derive(Clone, Debug, Envconfig)]
struct Inner {
    #[envconfig(from = "GRAPH_CHAIN_HEAD_WATCHER_TIMEOUT", default = "30")]
    chain_head_watcher_timeout_in_sec: u64,
    #[envconfig(from = "GRAPH_EXTRA_QUERY_PERMITS", default = "0")]
    extra_query_permits: u64,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_TIMEOUT", default = "5000")]
    connection_timeout_in_msec: u64,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_MIN_IDLE")]
    connection_min_idle: Option<u32>,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_MIN_IDLE_TIMEOUT", default = "600")]
    connection_min_idle_timeout_in_sec: u64,
    #[envconfig(from = "GRAPH_STORE_CONNECTION_TRY_ALWAYS", default = "false")]
    connection_try_always: EnvVarBoolean,
    #[envconfig(from = "GRAPH_QUERY_STATS_REFRESH_INTERVAL", default = "300")]
    query_stats_refresh_interval_in_sec: u64,
    #[envconfig(from = "LARGE_NOTIFICATION_CLEANUP_INTERVAL", default = "300")]
    large_notification_cleanup_interval_in_sec: u64,
    #[envconfig(from = "GRAPH_NOTIFICATION_BROADCAST_TIMEOUT", default = "60")]
    notification_broadcast_timeout_in_sec: u64,
    #[envconfig(from = "TYPEA_BATCH_SIZE", default = "150")]
    typea_batch_size: usize,
    #[envconfig(from = "TYPED_CHILDREN_SET_SIZE", default = "150")]
    typed_children_set_size: usize,
    #[envconfig(from = "ORDER_BY_BLOCK_RANGE", default = "false")]
    order_by_block_range: EnvVarBoolean,
    #[envconfig(from = "REVERSIBLE_ORDER_BY_OFF", default = "false")]
    reservible_order_by_off: EnvVarBoolean,
    #[envconfig(from = "GRAPH_ACCOUNT_TABLES", default = "")]
    account_tables: String,
    #[envconfig(from = "GRAPH_SQL_STATEMENT_TIMEOUT")]
    sql_statement_timeout_in_sec: Option<u64>,
    #[envconfig(from = "GRAPH_DISABLE_SUBSCRIPTION_NOTIFICATION", default = "false")]
    disable_subscription_notification: EnvVarBoolean,
    #[envconfig(from = "GRAPH_REMOVE_UNUSED_INTERVAL", default = "360")]
    remove_unused_interval_in_minutes: u32,
}

/// Some of these environment variables should really be set through the
/// configuration file:
///  - [`EnvVars::connection_timeout`].
///  - [`EnvVars::connection_min_idle`].
///  - [`EnvVars::connection_idle_timeout`].
///  - [`EnvVars::connection_try_always`].
//
/// Especially [`EnvVars::connection_min_idle`] and [`EnvVars::connection_idle_timeout`].
/// It's likely that they should be configured differently for each pool.
#[derive(Clone, Debug)]
pub struct EnvVars {
    inner: Inner,
    account_tables: HashSet<String>,
    sql_statement_timeout: Option<String>,
}

impl EnvVars {
    pub fn from_env() -> Result<Self, envconfig::Error> {
        let inner = Inner::init_from_env()?;
        let account_tables = inner
            .account_tables
            .split(',')
            .map(|s| format!("\"{}\"", s.replace(".", "\".\"")))
            .collect();
        let sql_statement_timeout = inner
            .sql_statement_timeout_in_sec
            .map(|x| format!("set local statement_timeout = {}", x * 1000));
        Ok(Self {
            inner,
            account_tables,
            sql_statement_timeout,
        })
    }

    pub fn chain_head_watcher_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.chain_head_watcher_timeout_in_sec)
    }

    /// There is typically no need to configure this. But this can be used to effectivey disable the
    /// query semaphore by setting it to a high number.
    pub fn extra_query_permits(&self) -> u64 {
        self.inner.extra_query_permits
    }

    pub fn connection_timeout(&self) -> Duration {
        Duration::from_millis(self.inner.connection_timeout_in_msec)
    }

    pub fn connection_min_idle(&self) -> Option<u32> {
        self.inner.connection_min_idle
    }

    pub fn connection_idle_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.connection_min_idle_timeout_in_sec)
    }

    /// A fallback in case the logic to remember database availability goes
    /// wrong; when this is set, we always try to get a connection and never
    /// use the availability state we remembered.
    pub fn connection_try_always(&self) -> bool {
        self.inner.connection_try_always.0
    }

    /// `GRAPH_QUERY_STATS_REFRESH_INTERVAL` is how long statistics that
    /// influence query execution are cached in memory (in seconds) before
    /// they are reloaded from the database. Defaults to 300s (5 minutes).
    pub fn query_stats_refresh_interval(&self) -> Duration {
        Duration::from_secs(self.inner.query_stats_refresh_interval_in_sec)
    }

    pub fn large_notification_cleanup_interval(&self) -> Duration {
        Duration::from_secs(self.inner.large_notification_cleanup_interval_in_sec)
    }

    pub fn notification_broadcast_timeout(&self) -> Duration {
        Duration::from_secs(self.inner.notification_broadcast_timeout_in_sec)
    }

    /// Use a variant of the query for child_type_a when we are looking up
    /// fewer than this many entities. This variable is only here temporarily
    /// until we can settle on the right batch size through experimentation
    /// and should then just become an ordinary constant
    pub fn typea_batch_size(&self) -> usize {
        self.inner.typea_batch_size
    }

    /// Include a constraint on the child ids as a set in child_type_d
    /// queries if the size of the set is below this threshold. Set this to
    /// 0 to turn off this optimization
    pub fn typed_children_set_size(&self) -> usize {
        self.inner.typed_children_set_size
    }

    /// When we add `order by id` to a query should we add instead
    /// `order by id, block_range`
    pub fn order_by_block_range(&self) -> bool {
        self.inner.order_by_block_range.0
    }

    /// Reversible order by. Change our `order by` clauses so that `asc`
    /// and `desc` ordering produce reverse orders. Setting this
    /// turns the new, correct behavior off
    pub fn reservible_order_by_off(&self) -> bool {
        self.inner.reservible_order_by_off.0
    }

    /// Deprecated; use 'graphman stats account-like' instead. A list of
    /// fully qualified table names that contain entities that are like
    /// accounts in that they have a relatively small number of entities,
    /// with a large number of change for each entity. It is useful to treat
    /// such tables special in queries by changing the clause that selects
    /// for a specific block range in a way that makes the BRIN index on
    /// block_range usable
    ///
    /// Example: GRAPH_ACCOUNT_TABLES=sgd21902.pair,sgd1708.things
    pub fn account_tables(&self) -> &HashSet<String> {
        &self.account_tables
    }

    /// `GRAPH_SQL_STATEMENT_TIMEOUT` is the timeout for queries in seconds.
    /// If it is not set, no statement timeout will be enforced. The statement
    /// timeout is local, i.e., can only be used within a transaction and
    /// will be cleared at the end of the transaction
    pub fn sql_statement_timeout(&self) -> Option<&str> {
        self.sql_statement_timeout.as_deref()
    }

    /// Whether to disable the notifications that feed GraphQL
    /// subscriptions; when the environment variable is set, no updates
    /// about entity changes will be sent to query nodes
    pub fn send_subscription_notifications(&self) -> bool {
        let disabled = self.inner.disable_subscription_notification.0;
        !disabled
    }

    pub fn unused_interval(&self) -> chrono::Duration {
        chrono::Duration::minutes(self.inner.remove_unused_interval_in_minutes.into())
    }
}

#[derive(Copy, Clone, Debug)]
struct EnvVarBoolean(pub bool);

impl FromStr for EnvVarBoolean {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "true" | "1" => Ok(Self(true)),
            "false" | "0" => Ok(Self(false)),
            _ => Err("Invalid env. var. flag, expected true / false / 1 / 0".to_string()),
        }
    }
}
