use clap::{Parser, Subcommand};
use config::PoolSize;
use git_testament::{git_testament, render_testament};
use graph::bail;
use graph::blockchain::BlockHash;
use graph::cheap_clone::CheapClone;
use graph::components::network_provider::ChainName;
use graph::endpoint::EndpointMetrics;
use graph::env::ENV_VARS;
use graph::log::logger_with_levels;
use graph::prelude::{BlockNumber, MetricsRegistry, BLOCK_NUMBER_MAX};
use graph::{data::graphql::load_manager::LoadManager, prelude::chrono, prometheus::Registry};
use graph::{
    prelude::{
        anyhow::{self, anyhow, Context as AnyhowContextTrait},
        info, tokio, Logger, NodeId,
    },
    url::Url,
};
use graph_chain_ethereum::EthereumAdapter;
use graph_graphql::prelude::GraphQlRunner;
use graph_node::config::{self, Config as Cfg};
use graph_node::manager::color::Terminal;
use graph_node::manager::commands;
use graph_node::network_setup::Networks;
use graph_node::{
    manager::deployment::DeploymentSearch, store_builder::StoreBuilder, MetricsContext,
};
use graph_store_postgres::{
    BlockStore, ChainStore, ConnectionPool, NotificationSender, PoolCoordinator, Shard, Store,
    SubgraphStore, SubscriptionManager, PRIMARY_SHARD,
};
use itertools::Itertools;
use lazy_static::lazy_static;
use std::env;
use std::str::FromStr;
use std::{collections::HashMap, num::ParseIntError, sync::Arc, time::Duration};
const VERSION_LABEL_KEY: &str = "version";

git_testament!(TESTAMENT);

lazy_static! {
    static ref RENDERED_TESTAMENT: String = render_testament!(TESTAMENT);
}

#[derive(Parser, Clone, Debug)]
#[clap(
    name = "graphman",
    about = "Management tool for a graph-node infrastructure",
    author = "Graph Protocol, Inc.",
    version = RENDERED_TESTAMENT.as_str()
)]
pub struct Opt {
    #[clap(
        long,
        default_value = "off",
        env = "GRAPHMAN_LOG",
        help = "level for log output in slog format"
    )]
    pub log_level: String,
    #[clap(
        long,
        default_value = "auto",
        help = "whether to colorize the output. Set to 'auto' to colorize only on\nterminals (the default), 'always' to always colorize, or 'never'\nto not colorize at all"
    )]
    pub color: String,
    #[clap(
        long,
        short,
        env = "GRAPH_NODE_CONFIG",
        help = "the name of the configuration file\n"
    )]
    pub config: String,
    #[clap(
        long,
        default_value = "default",
        value_name = "NODE_ID",
        env = "GRAPH_NODE_ID",
        help = "a unique identifier for this node. Should have the same value\nbetween consecutive node restarts\n"
    )]
    pub node_id: String,
    #[clap(
        long,
        value_name = "{HOST:PORT|URL}",
        default_value = "https://api.thegraph.com/ipfs/",
        env = "IPFS",
        help = "HTTP addresses of IPFS nodes\n"
    )]
    pub ipfs: Vec<String>,
    #[clap(
        long,
        value_name = "{HOST:PORT|URL}",
        default_value = "https://arweave.net",
        env = "GRAPH_NODE_ARWEAVE_URL",
        help = "HTTP base URL for arweave gateway"
    )]
    pub arweave: String,
    #[clap(
        long,
        default_value = "3",
        help = "the size for connection pools. Set to 0 to use pool size from\nconfiguration file corresponding to NODE_ID\n"
    )]
    pub pool_size: u32,
    #[clap(long, value_name = "URL", help = "Base URL for forking subgraphs")]
    pub fork_base: Option<String>,
    #[clap(long, help = "version label, used for prometheus metrics")]
    pub version_label: Option<String>,
    #[clap(subcommand)]
    pub cmd: Command,
}

#[derive(Clone, Debug, Subcommand)]
pub enum Command {
    /// Calculate the transaction speed
    TxnSpeed {
        #[clap(long, short, default_value = "60")]
        delay: u64,
    },
    /// Print details about a deployment
    ///
    /// The deployment can be specified as either a subgraph name, an IPFS
    /// hash `Qm..`, or the database namespace `sgdNNN`. Since the same IPFS
    /// hash can be deployed in multiple shards, it is possible to specify
    /// the shard by adding `:shard` to the IPFS hash.
    Info {
        /// The deployment (see above)
        deployment: Option<DeploymentSearch>,
        /// List all the deployments in the graph-node
        #[clap(long, short)]
        all: bool,
        /// List only current version
        #[clap(long, short)]
        current: bool,
        /// List only pending versions
        #[clap(long, short)]
        pending: bool,
        /// Include status information
        #[clap(long, short)]
        status: bool,
        /// List only used (current and pending) versions
        #[clap(long, short)]
        used: bool,
        /// List names only for the active deployment
        #[clap(long, short)]
        brief: bool,
        /// Do not print subgraph names
        #[clap(long, short = 'N')]
        no_name: bool,
    },
    /// Manage unused deployments
    ///
    /// Record which deployments are unused with `record`, then remove them
    /// with `remove`
    #[clap(subcommand)]
    Unused(UnusedCommand),
    /// Remove a named subgraph
    Remove {
        /// The name of the subgraph to remove
        name: String,
    },
    /// Create a subgraph name
    Create {
        /// The name of the subgraph to create
        name: String,
    },
    /// Assign or reassign a deployment
    Reassign {
        /// The deployment (see `help info`)
        deployment: DeploymentSearch,
        /// The name of the node that should index the deployment
        node: String,
    },
    /// Unassign a deployment
    Unassign {
        /// The deployment (see `help info`)
        deployment: DeploymentSearch,
    },
    /// Pause a deployment
    Pause {
        /// The deployment (see `help info`)
        deployment: DeploymentSearch,
    },
    /// Resume a deployment
    Resume {
        /// The deployment (see `help info`)
        deployment: DeploymentSearch,
    },
    /// Pause and resume one or multiple deployments
    Restart {
        /// The deployment(s) (see `help info`)
        deployments: Vec<DeploymentSearch>,
        /// Sleep for this many seconds after pausing subgraphs
        #[clap(
            long,
            short,
            default_value = "20",
            value_parser = parse_duration_in_secs
        )]
        sleep: Duration,
    },
    /// Rewind a subgraph to a specific block
    Rewind {
        /// Force rewinding even if the block hash is not found in the local
        /// database
        #[clap(long, short)]
        force: bool,
        /// Rewind to the start block of the subgraph
        #[clap(long)]
        start_block: bool,
        /// Sleep for this many seconds after pausing subgraphs
        #[clap(
            long,
            short,
            default_value = "20",
            value_parser = parse_duration_in_secs
        )]
        sleep: Duration,
        /// The block hash of the target block
        #[clap(
            required_unless_present = "start_block",
            conflicts_with = "start_block",
            long,
            short = 'H'
        )]
        block_hash: Option<String>,
        /// The block number of the target block
        #[clap(
            required_unless_present = "start_block",
            conflicts_with = "start_block",
            long,
            short = 'n'
        )]
        block_number: Option<i32>,
        /// The deployments to rewind (see `help info`)
        #[clap(required = true)]
        deployments: Vec<DeploymentSearch>,
    },
    /// Deploy and run an arbitrary subgraph up to a certain block
    ///
    /// The run can surpass it by a few blocks, it's not exact (use for dev
    /// and testing purposes) -- WARNING: WILL RUN MIGRATIONS ON THE DB, DO
    /// NOT USE IN PRODUCTION
    ///
    /// Also worth noting that the deployed subgraph will be removed at the
    /// end.
    Run {
        /// Network name (must fit one of the chain)
        network_name: String,

        /// Subgraph in the form `<IPFS Hash>` or `<name>:<IPFS Hash>`
        subgraph: String,

        /// Highest block number to process before stopping (inclusive)
        stop_block: i32,

        /// Prometheus push gateway endpoint.
        prometheus_host: Option<String>,
    },
    /// Check and interrogate the configuration
    ///
    /// Print information about a configuration file without
    /// actually connecting to databases or network clients
    #[clap(subcommand)]
    Config(ConfigCommand),
    /// Listen for store events and print them
    #[clap(subcommand)]
    Listen(ListenCommand),
    /// Manage deployment copies and grafts
    #[clap(subcommand)]
    Copy(CopyCommand),
    /// Run a GraphQL query
    Query {
        /// Save the JSON query result in this file
        #[clap(long, short)]
        output: Option<String>,
        /// Save the query trace in this file
        #[clap(long, short)]
        trace: Option<String>,

        /// The subgraph to query
        ///
        /// Either a deployment id `Qm..` or a subgraph name
        target: String,
        /// The GraphQL query
        query: String,
        /// The variables in the form `key=value`
        vars: Vec<String>,
    },
    /// Get information about chains and manipulate them
    #[clap(subcommand)]
    Chain(ChainCommand),
    /// Manipulate internal subgraph statistics
    #[clap(subcommand)]
    Stats(StatsCommand),

    /// Manage database indexes
    #[clap(subcommand)]
    Index(IndexCommand),

    /// Prune subgraphs by removing old entity versions
    ///
    /// Keep only entity versions that are needed to respond to queries at
    /// block heights that are within `history` blocks of the subgraph head;
    /// all other entity versions are removed.
    #[clap(subcommand)]
    Prune(PruneCommand),

    /// General database management
    #[clap(subcommand)]
    Database(DatabaseCommand),

    /// Deploy a subgraph
    Deploy {
        name: DeploymentSearch,
        deployment: DeploymentSearch,

        /// The url of the graph-node
        #[clap(long, short, default_value = "http://localhost:8020")]
        url: String,
    },
}

impl Command {
    /// Return `true` if the command should not override connection pool
    /// sizes, in general only when we will not actually connect to any
    /// databases
    fn use_configured_pool_size(&self) -> bool {
        matches!(self, Command::Config(_))
    }
}

#[derive(Clone, Debug, Subcommand)]
pub enum UnusedCommand {
    /// List unused deployments
    List {
        /// Only list unused deployments that still exist
        #[clap(short, long, conflicts_with = "deployment")]
        existing: bool,

        /// Deployment
        #[clap(short, long)]
        deployment: Option<DeploymentSearch>,
    },
    /// Update and record currently unused deployments
    Record,
    /// Remove deployments that were marked as unused with `record`.
    ///
    /// Deployments are removed in descending order of number of entities,
    /// i.e., smaller deployments are removed before larger ones
    Remove {
        /// How many unused deployments to remove (default: all)
        #[clap(short, long)]
        count: Option<usize>,
        /// Remove a specific deployment
        #[clap(short, long, conflicts_with = "count")]
        deployment: Option<String>,
        /// Remove unused deployments that were recorded at least this many minutes ago
        #[clap(short, long)]
        older: Option<u32>,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ConfigCommand {
    /// Check and validate the configuration file
    Check {
        /// Print the configuration as JSON
        #[clap(long)]
        print: bool,
    },
    /// Print how a specific subgraph would be placed
    Place {
        /// The name of the subgraph
        name: String,
        /// The network the subgraph indexes
        network: String,
    },
    /// Information about the size of database pools
    Pools {
        /// The names of the nodes that are going to run
        nodes: Vec<String>,
        /// Print connections by shard rather than by node
        #[clap(short, long)]
        shard: bool,
    },
    /// Show eligible providers
    ///
    /// Prints the providers that can be used for a deployment on a given
    /// network with the given features. Set the name of the node for which
    /// to simulate placement with the toplevel `--node-id` option
    Provider {
        #[clap(short, long, default_value = "")]
        features: String,
        network: String,
    },

    /// Run all available provider checks against all providers.
    CheckProviders {
        /// Maximum duration of all provider checks for a provider.
        ///
        /// Defaults to 60 seconds.
        timeout_seconds: Option<u64>,
    },

    /// Show subgraph-specific settings
    ///
    /// GRAPH_EXPERIMENTAL_SUBGRAPH_SETTINGS can add a file that contains
    /// subgraph-specific settings. This command determines which settings
    /// would apply when a subgraph <name> is deployed and prints the result
    Setting {
        /// The subgraph name for which to print settings
        name: String,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ListenCommand {
    /// Listen only to assignment events
    Assignments,
}

#[derive(Clone, Debug, Subcommand)]
pub enum CopyCommand {
    /// Create a copy of an existing subgraph
    ///
    /// The copy will be treated as its own deployment. The deployment with
    /// IPFS hash `src` will be copied to a new deployment in the database
    /// shard `shard` and will be assigned to `node` for indexing. The new
    /// subgraph will start as a copy of all blocks of `src` that are
    /// `offset` behind the current subgraph head of `src`. The offset
    /// should be chosen such that only final blocks are copied
    Create {
        /// How far behind `src` subgraph head to copy
        #[clap(long, short, default_value = "200")]
        offset: u32,
        /// Activate this copy once it has synced
        #[clap(long, short, conflicts_with = "replace")]
        activate: bool,
        /// Replace the source with this copy once it has synced
        #[clap(long, short, conflicts_with = "activate")]
        replace: bool,
        /// The source deployment (see `help info`)
        src: DeploymentSearch,
        /// The name of the database shard into which to copy
        shard: String,
        /// The name of the node that should index the copy
        node: String,
    },
    /// Activate the copy of a deployment.
    ///
    /// This will route queries to that specific copy (with some delay); the
    /// previously active copy will become inactive. Only copies that have
    /// progressed at least as far as the original should be activated.
    Activate {
        /// The IPFS hash of the deployment to activate
        deployment: String,
        /// The name of the database shard that holds the copy
        shard: String,
    },
    /// List all currently running copy and graft operations
    List,
    /// Print the progress of a copy operation
    Status {
        /// The destination deployment of the copy operation (see `help info`)
        dst: DeploymentSearch,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum ChainCommand {
    /// List all chains that are in the database
    List,
    /// Show information about a chain
    Info {
        #[clap(
            long,
            short,
            default_value = "50",
            env = "ETHEREUM_REORG_THRESHOLD",
            help = "the reorg threshold to check\n"
        )]
        reorg_threshold: i32,
        #[clap(long, help = "display block hashes\n")]
        hashes: bool,
        name: String,
    },
    /// Remove a chain and all its data
    ///
    /// There must be no deployments using that chain. If there are, the
    /// subgraphs and/or deployments using the chain must first be removed
    Remove { name: String },

    /// Compares cached blocks with fresh ones and clears the block cache when they differ.
    CheckBlocks {
        #[clap(subcommand)] // Note that we mark a field as a subcommand
        method: CheckBlockMethod,
        /// Chain name (must be an existing chain, see 'chain list')
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        chain_name: String,
    },
    /// Truncates the whole block cache for the given chain.
    Truncate {
        /// Chain name (must be an existing chain, see 'chain list')
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        chain_name: String,
        /// Skips confirmation prompt
        #[clap(long, short)]
        force: bool,
    },

    /// Update the genesis block hash for a chain
    UpdateGenesis {
        #[clap(long, short)]
        force: bool,
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        block_hash: String,
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        chain_name: String,
    },

    /// Change the block cache shard for a chain
    ChangeShard {
        /// Chain name (must be an existing chain, see 'chain list')
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        chain_name: String,
        /// Shard name
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        shard: String,
    },

    /// Execute operations on call cache.
    CallCache {
        #[clap(subcommand)]
        method: CallCacheCommand,
        /// Chain name (must be an existing chain, see 'chain list')
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        chain_name: String,
    },

    /// Ingest a block into the block cache.
    ///
    /// This will overwrite any blocks we may already have in the block
    /// cache, and can therefore be used to get rid of duplicate blocks in
    /// the block cache as well as making sure that a certain block is in
    /// the cache
    Ingest {
        /// The name of the chain
        name: String,
        /// The block number to ingest
        number: BlockNumber,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum CallCacheCommand {
    /// Remove the call cache of the specified chain.
    ///
    /// Either remove entries in the range `--from` and `--to`, or remove
    /// the entire cache with `--remove-entire-cache`. Removing the entire
    /// cache can reduce indexing performance significantly and should
    /// generally be avoided.
    Remove {
        /// Remove the entire cache
        #[clap(long, conflicts_with_all = &["from", "to"])]
        remove_entire_cache: bool,
        /// Starting block number
        #[clap(long, short, conflicts_with = "remove-entire-cache", requires = "to")]
        from: Option<i32>,
        /// Ending block number
        #[clap(long, short, conflicts_with = "remove-entire-cache", requires = "from")]
        to: Option<i32>,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum StatsCommand {
    /// Toggle whether a table is account-like
    ///
    /// Setting a table to 'account-like' enables a query optimization that
    /// is very effective for tables with a high ratio of entity versions
    /// to distinct entities. It can take up to 5 minutes for this to take
    /// effect.
    AccountLike {
        #[clap(long, short, help = "do not set but clear the account-like flag\n")]
        clear: bool,
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The name of the database table
        table: String,
    },
    /// Show statistics for the tables of a deployment
    ///
    /// Show how many distinct entities and how many versions the tables of
    /// each subgraph have. The data is based on the statistics that
    /// Postgres keeps, and only refreshed when a table is analyzed.
    Show {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
    },
    /// Perform a SQL ANALYZE in a Entity table
    Analyze {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The name of the Entity to ANALYZE, in camel case. Analyze all
        /// tables if omitted
        entity: Option<String>,
    },
    /// Show statistics targets for the statistics collector
    ///
    /// For all tables in the given deployment, show the target for each
    /// column. A value of `-1` means that the global default is used
    Target {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
    },
    /// Set the statistics targets for the statistics collector
    ///
    /// Set (or reset) the target for a deployment. The statistics target
    /// determines how much of a table Postgres will sample when it analyzes
    /// a table. This can be particularly beneficial when Postgres chooses
    /// suboptimal query plans for some queries. Increasing the target will
    /// make analyzing tables take longer and will require more space in
    /// Postgres' internal statistics storage.
    ///
    /// If no `columns` are provided, change the statistics target for the
    /// `id` and `block_range` columns which will usually be enough to
    /// improve query performance, but it might be necessary to increase the
    /// target for other columns, too.
    SetTarget {
        /// The value of the statistics target
        #[clap(short, long, default_value = "200", conflicts_with = "reset")]
        target: u32,
        /// Reset the target so the default is used
        #[clap(long, conflicts_with = "target")]
        reset: bool,
        /// Do not analyze changed tables
        #[clap(long)]
        no_analyze: bool,
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The table for which to set the target, all if omitted
        entity: Option<String>,
        /// The columns to which to apply the target. Defaults to `id, block_range`
        columns: Vec<String>,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum PruneCommand {
    /// Prune a deployment in the foreground
    ///
    /// Unless `--once` is given, this setting is permanent and the subgraph
    /// will periodically be pruned to remove history as the subgraph head
    /// moves forward.
    Run {
        /// The deployment to prune (see `help info`)
        deployment: DeploymentSearch,
        /// Prune by rebuilding tables when removing more than this fraction
        /// of history. Defaults to GRAPH_STORE_HISTORY_REBUILD_THRESHOLD
        #[clap(long, short)]
        rebuild_threshold: Option<f64>,
        /// Prune by deleting when removing more than this fraction of
        /// history but less than rebuild_threshold. Defaults to
        /// GRAPH_STORE_HISTORY_DELETE_THRESHOLD
        #[clap(long, short)]
        delete_threshold: Option<f64>,
        /// How much history to keep in blocks. Defaults to
        /// GRAPH_MIN_HISTORY_BLOCKS
        #[clap(long, short = 'y')]
        history: Option<usize>,
        /// Prune only this once
        #[clap(long, short)]
        once: bool,
    },
    /// Prune a deployment in the background
    ///
    /// Set the amount of history the subgraph should retain. The actual
    /// data removal happens in the background and can be monitored with
    /// `prune status`. It can take several minutes of the first pruning to
    /// start, during which time `prune status` will not return any
    /// information
    Set {
        /// The deployment to prune (see `help info`)
        deployment: DeploymentSearch,
        /// Prune by rebuilding tables when removing more than this fraction
        /// of history. Defaults to GRAPH_STORE_HISTORY_REBUILD_THRESHOLD
        #[clap(long, short)]
        rebuild_threshold: Option<f64>,
        /// Prune by deleting when removing more than this fraction of
        /// history but less than rebuild_threshold. Defaults to
        /// GRAPH_STORE_HISTORY_DELETE_THRESHOLD
        #[clap(long, short)]
        delete_threshold: Option<f64>,
        /// How much history to keep in blocks. Defaults to
        /// GRAPH_MIN_HISTORY_BLOCKS
        #[clap(long, short = 'y')]
        history: Option<usize>,
    },
    /// Show the status of a pruning operation
    Status {
        /// The number of the pruning run
        #[clap(long, short)]
        run: Option<usize>,
        /// The deployment to check (see `help info`)
        deployment: DeploymentSearch,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum IndexCommand {
    /// Creates a new database index.
    ///
    /// The new index will be created concurrenly for the provided entity and its fields. whose
    /// names must be declared the in camel case, following GraphQL conventions.
    ///
    /// The index will have its validity checked after the operation and will be dropped if it is
    /// invalid.
    ///
    /// This command may be time-consuming.
    Create {
        /// The deployment (see `help info`).
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        deployment: DeploymentSearch,
        /// The Entity name.
        ///
        /// Can be expressed either in upper camel case (as its GraphQL definition) or in snake case
        /// (as its SQL table name).
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        entity: String,
        /// The Field names.
        ///
        /// Each field can be expressed either in camel case (as its GraphQL definition) or in snake
        /// case (as its SQL colmun name).
        #[clap(required = true)]
        fields: Vec<String>,
        /// The index method. Defaults to `btree` in general, and to `gist` when the index includes the `block_range` column
        #[clap(
            short, long, default_value = "btree",
            value_parser = clap::builder::PossibleValuesParser::new(&["btree", "hash", "gist", "spgist", "gin", "brin"])
        )]
        method: Option<String>,

        #[clap(long)]
        /// Specifies a starting block number for creating a partial index.
        after: Option<i32>,
    },
    /// Lists existing indexes for a given Entity
    List {
        /// Do not list attribute indexes
        #[clap(short = 'A', long)]
        no_attribute_indexes: bool,
        /// Do not list any of the indexes that are generated by default,
        /// including attribute indexes
        #[clap(short = 'D', long)]
        no_default_indexes: bool,
        /// Print SQL statements instead of a more human readable overview
        #[clap(long)]
        sql: bool,
        /// When `--sql` is used, make statements run concurrently
        #[clap(long, requires = "sql")]
        concurrent: bool,
        /// When `--sql` is used, add `if not exists` clause
        #[clap(long, requires = "sql")]
        if_not_exists: bool,
        ///  The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The Entity name.
        ///
        /// Can be expressed either in upper camel case (as its GraphQL definition) or in snake case
        /// (as its SQL table name).
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        entity: String,
    },

    /// Drops an index for a given deployment, concurrently
    Drop {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The name of the index to be dropped
        #[clap(value_parser = clap::builder::NonEmptyStringValueParser::new())]
        index_name: String,
    },
}

#[derive(Clone, Debug, Subcommand)]
pub enum DatabaseCommand {
    /// Apply any pending migrations to the database schema in all shards
    Migrate,
    /// Refresh the mapping of tables into different shards
    ///
    /// This command rebuilds the mappings of tables from one shard into all
    /// other shards. It makes it possible to fix these mappings when a
    /// database migration was interrupted before it could rebuild the
    /// mappings
    ///
    /// Each shard imports certain tables from all other shards. To recreate
    /// the mappings in a given shard, use `--dest SHARD`, to recreate the
    /// mappings in other shards that depend on a shard, use `--source
    /// SHARD`. Without `--dest` and `--source` options, recreate all
    /// possible mappings. Recreating mappings needlessly is harmless, but
    /// might take quite a bit of time with a lot of shards.
    Remap {
        /// Only refresh mappings from SOURCE
        #[clap(long, short)]
        source: Option<String>,
        /// Only refresh mappings inside DEST
        #[clap(long, short)]
        dest: Option<String>,
        /// Continue remapping even when one operation fails
        #[clap(long, short)]
        force: bool,
    },
}
#[derive(Clone, Debug, Subcommand)]
pub enum CheckBlockMethod {
    /// The hash of the target block
    ByHash {
        /// The block hash to verify
        hash: String,
    },

    /// The number of the target block
    ByNumber {
        /// The block number to verify
        number: i32,
        /// Delete duplicated blocks (by number) if found
        #[clap(long, short, action)]
        delete_duplicates: bool,
    },

    /// A block number range, inclusive on both ends.
    ByRange {
        /// The first block number to verify
        #[clap(long, short)]
        from: Option<i32>,
        /// The last block number to verify
        #[clap(long, short)]
        to: Option<i32>,
        /// Delete duplicated blocks (by number) if found
        #[clap(long, short, action)]
        delete_duplicates: bool,
    },
}

impl From<Opt> for config::Opt {
    fn from(opt: Opt) -> Self {
        let mut config_opt = config::Opt::default();
        config_opt.config = Some(opt.config);
        config_opt.store_connection_pool_size = 5;
        config_opt.node_id = opt.node_id;
        config_opt
    }
}

/// Utilities to interact mostly with the store and build the parts of the
/// store we need for specific commands
struct Context {
    logger: Logger,
    node_id: NodeId,
    config: Cfg,
    ipfs_url: Vec<String>,
    arweave_url: String,
    fork_base: Option<Url>,
    registry: Arc<MetricsRegistry>,
    pub prometheus_registry: Arc<Registry>,
}

impl Context {
    fn new(
        logger: Logger,
        node_id: NodeId,
        config: Cfg,
        ipfs_url: Vec<String>,
        arweave_url: String,
        fork_base: Option<Url>,
        version_label: Option<String>,
    ) -> Self {
        let prometheus_registry = Arc::new(
            Registry::new_custom(
                None,
                version_label.map(|label| {
                    let mut m = HashMap::<String, String>::new();
                    m.insert(VERSION_LABEL_KEY.into(), label);
                    m
                }),
            )
            .expect("unable to build prometheus registry"),
        );
        let registry = Arc::new(MetricsRegistry::new(
            logger.clone(),
            prometheus_registry.clone(),
        ));

        Self {
            logger,
            node_id,
            config,
            ipfs_url,
            fork_base,
            registry,
            prometheus_registry,
            arweave_url,
        }
    }

    fn metrics_registry(&self) -> Arc<MetricsRegistry> {
        self.registry.clone()
    }

    fn config(&self) -> Cfg {
        self.config.clone()
    }

    fn node_id(&self) -> NodeId {
        self.node_id.clone()
    }

    fn notification_sender(&self) -> Arc<NotificationSender> {
        Arc::new(NotificationSender::new(self.registry.clone()))
    }

    fn primary_pool(self) -> ConnectionPool {
        let primary = self.config.primary_store();
        let coord = Arc::new(PoolCoordinator::new(&self.logger, Arc::new(vec![])));
        let pool = StoreBuilder::main_pool(
            &self.logger,
            &self.node_id,
            PRIMARY_SHARD.as_str(),
            primary,
            self.metrics_registry(),
            coord,
        );
        pool.skip_setup();
        pool
    }

    fn subgraph_store(self) -> Arc<SubgraphStore> {
        self.store_and_pools().0.subgraph_store()
    }

    fn subscription_manager(&self) -> Arc<SubscriptionManager> {
        let primary = self.config.primary_store();

        Arc::new(SubscriptionManager::new(
            self.logger.clone(),
            primary.connection.clone(),
            self.registry.clone(),
        ))
    }

    fn store(&self) -> Arc<Store> {
        let (store, _) = self.store_and_pools();
        store
    }

    fn pools(self) -> HashMap<Shard, ConnectionPool> {
        let (_, pools) = self.store_and_pools();
        pools
    }

    async fn store_builder(&self) -> StoreBuilder {
        StoreBuilder::new(
            &self.logger,
            &self.node_id,
            &self.config,
            self.fork_base.clone(),
            self.registry.clone(),
        )
        .await
    }

    fn store_and_pools(&self) -> (Arc<Store>, HashMap<Shard, ConnectionPool>) {
        let (subgraph_store, pools, _) = StoreBuilder::make_subgraph_store_and_pools(
            &self.logger,
            &self.node_id,
            &self.config,
            self.fork_base.clone(),
            self.registry.clone(),
        );

        for pool in pools.values() {
            pool.skip_setup();
        }

        let store = StoreBuilder::make_store(
            &self.logger,
            pools.clone(),
            subgraph_store,
            HashMap::default(),
            Vec::new(),
            self.registry.cheap_clone(),
        );

        (store, pools)
    }

    fn store_and_primary(self) -> (Arc<Store>, ConnectionPool) {
        let (store, pools) = self.store_and_pools();
        let primary = pools.get(&*PRIMARY_SHARD).expect("there is a primary pool");
        (store, primary.clone())
    }

    fn block_store_and_primary_pool(self) -> (Arc<BlockStore>, ConnectionPool) {
        let (store, pools) = self.store_and_pools();

        let primary = pools.get(&*PRIMARY_SHARD).unwrap();
        (store.block_store(), primary.clone())
    }

    fn graphql_runner(self) -> Arc<GraphQlRunner<Store>> {
        let logger = self.logger.clone();
        let registry = self.registry.clone();

        let store = self.store();

        let load_manager = Arc::new(LoadManager::new(&logger, vec![], vec![], registry.clone()));

        Arc::new(GraphQlRunner::new(&logger, store, load_manager, registry))
    }

    async fn networks(&self) -> anyhow::Result<Networks> {
        let logger = self.logger.clone();
        let registry = self.metrics_registry();
        let metrics = Arc::new(EndpointMetrics::mock());

        Networks::from_config(logger, &self.config, registry, metrics, &[]).await
    }

    fn chain_store(self, chain_name: &str) -> anyhow::Result<Arc<ChainStore>> {
        use graph::components::store::BlockStore;
        self.store()
            .block_store()
            .chain_store(chain_name)
            .ok_or_else(|| anyhow::anyhow!("Could not find a network named '{}'", chain_name))
    }

    async fn chain_store_and_adapter(
        self,
        chain_name: &str,
    ) -> anyhow::Result<(Arc<ChainStore>, Arc<EthereumAdapter>)> {
        let logger = self.logger.clone();
        let registry = self.metrics_registry();
        let metrics = Arc::new(EndpointMetrics::mock());
        let networks = Networks::from_config_for_chain(
            logger,
            &self.config,
            registry,
            metrics,
            &[],
            chain_name,
        )
        .await?;

        let chain_store = self.chain_store(chain_name)?;
        let ethereum_adapter = networks
            .ethereum_rpcs(chain_name.into())
            .cheapest()
            .await
            .ok_or(anyhow::anyhow!(
                "Failed to obtain an Ethereum adapter for chain '{}'",
                chain_name
            ))?;
        Ok((chain_store, ethereum_adapter))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Disable load management for graphman commands
    env::set_var("GRAPH_LOAD_THRESHOLD", "0");

    let opt = Opt::parse();

    Terminal::set_color_preference(&opt.color);

    let version_label = opt.version_label.clone();
    // Set up logger
    let logger = logger_with_levels(false, Some(&opt.log_level));

    // Log version information
    info!(
        logger,
        "Graph Node version: {}",
        render_testament!(TESTAMENT)
    );

    let mut config = Cfg::load(&logger, &opt.clone().into()).context("Configuration error")?;
    config.stores.iter_mut().for_each(|(_, shard)| {
        shard.pool_size = PoolSize::Fixed(5);
        shard.fdw_pool_size = PoolSize::Fixed(5);
    });

    if opt.pool_size > 0 && !opt.cmd.use_configured_pool_size() {
        // Override pool size from configuration
        for shard in config.stores.values_mut() {
            shard.pool_size = PoolSize::Fixed(opt.pool_size);
            for replica in shard.replicas.values_mut() {
                replica.pool_size = PoolSize::Fixed(opt.pool_size);
            }
        }
    }

    let node = match NodeId::new(&opt.node_id) {
        Err(()) => {
            eprintln!("invalid node id: {}", opt.node_id);
            std::process::exit(1);
        }
        Ok(node) => node,
    };

    let fork_base = match &opt.fork_base {
        Some(url) => {
            // Make sure the endpoint ends with a terminating slash.
            let url = if !url.ends_with('/') {
                let mut url = url.clone();
                url.push('/');
                Url::parse(&url)
            } else {
                Url::parse(url)
            };

            match url {
                Err(e) => {
                    eprintln!("invalid fork base URL: {}", e);
                    std::process::exit(1);
                }
                Ok(url) => Some(url),
            }
        }
        None => None,
    };

    let ctx = Context::new(
        logger.clone(),
        node,
        config,
        opt.ipfs,
        opt.arweave,
        fork_base,
        version_label.clone(),
    );

    use Command::*;
    match opt.cmd {
        TxnSpeed { delay } => commands::txn_speed::run(ctx.primary_pool(), delay),
        Info {
            deployment,
            current,
            pending,
            status,
            used,
            all,
            brief,
            no_name,
        } => {
            let (store, primary_pool) = ctx.store_and_primary();

            let ctx = commands::deployment::info::Context {
                primary_pool,
                store,
            };

            let args = commands::deployment::info::Args {
                deployment: deployment.map(make_deployment_selector),
                current,
                pending,
                status,
                used,
                all,
                brief,
                no_name,
            };

            commands::deployment::info::run(ctx, args)
        }
        Unused(cmd) => {
            let store = ctx.subgraph_store();
            use UnusedCommand::*;

            match cmd {
                List {
                    existing,
                    deployment,
                } => commands::unused_deployments::list(store, existing, deployment),
                Record => commands::unused_deployments::record(store),
                Remove {
                    count,
                    deployment,
                    older,
                } => {
                    let count = count.unwrap_or(1_000_000);
                    let older = older.map(|older| chrono::Duration::minutes(older as i64));
                    commands::unused_deployments::remove(store, count, deployment.as_deref(), older)
                }
            }
        }
        Config(cmd) => {
            use ConfigCommand::*;

            match cmd {
                CheckProviders { timeout_seconds } => {
                    let logger = ctx.logger.clone();
                    let networks = ctx.networks().await?;
                    let store = ctx.store().block_store();
                    let timeout = Duration::from_secs(timeout_seconds.unwrap_or(60));

                    commands::provider_checks::execute(&logger, &networks, store, timeout).await;

                    Ok(())
                }
                Place { name, network } => {
                    commands::config::place(&ctx.config.deployment, &name, &network)
                }
                Check { print } => commands::config::check(&ctx.config, print),
                Pools { nodes, shard } => commands::config::pools(&ctx.config, nodes, shard),
                Provider { features, network } => {
                    let logger = ctx.logger.clone();
                    let registry = ctx.registry.clone();
                    commands::config::provider(logger, &ctx.config, registry, features, network)
                        .await
                }
                Setting { name } => commands::config::setting(&name),
            }
        }
        Remove { name } => commands::remove::run(ctx.subgraph_store(), &name),
        Create { name } => commands::create::run(ctx.subgraph_store(), name),
        Unassign { deployment } => {
            let notifications_sender = ctx.notification_sender();
            let primary_pool = ctx.primary_pool();
            let deployment = make_deployment_selector(deployment);
            commands::deployment::unassign::run(primary_pool, notifications_sender, deployment)
        }
        Reassign { deployment, node } => {
            let notifications_sender = ctx.notification_sender();
            let primary_pool = ctx.primary_pool();
            let deployment = make_deployment_selector(deployment);
            let node = NodeId::new(node).map_err(|node| anyhow!("invalid node id {:?}", node))?;
            commands::deployment::reassign::run(
                primary_pool,
                notifications_sender,
                deployment,
                &node,
            )
        }
        Pause { deployment } => {
            let notifications_sender = ctx.notification_sender();
            let primary_pool = ctx.primary_pool();
            let deployment = make_deployment_selector(deployment);

            commands::deployment::pause::run(primary_pool, notifications_sender, deployment)
        }
        Resume { deployment } => {
            let notifications_sender = ctx.notification_sender();
            let primary_pool = ctx.primary_pool();
            let deployment = make_deployment_selector(deployment);

            commands::deployment::resume::run(primary_pool, notifications_sender, deployment)
        }
        Restart { deployments, sleep } => {
            let notifications_sender = ctx.notification_sender();
            let primary_pool = ctx.primary_pool();

            for deployment in deployments.into_iter().unique() {
                let deployment = make_deployment_selector(deployment);

                commands::deployment::restart::run(
                    primary_pool.clone(),
                    notifications_sender.clone(),
                    deployment,
                    sleep,
                )?;
            }

            Ok(())
        }
        Rewind {
            force,
            sleep,
            block_hash,
            block_number,
            deployments,
            start_block,
        } => {
            let notification_sender = ctx.notification_sender();
            let (store, primary) = ctx.store_and_primary();

            commands::rewind::run(
                primary,
                store,
                deployments,
                block_hash,
                block_number,
                &notification_sender,
                force,
                sleep,
                start_block,
            )
            .await
        }
        Run {
            network_name,
            subgraph,
            stop_block,
            prometheus_host,
        } => {
            let logger = ctx.logger.clone();
            let config = ctx.config();
            let registry = ctx.metrics_registry().clone();
            let node_id = ctx.node_id().clone();
            let store_builder = ctx.store_builder().await;
            let job_name = version_label.clone();
            let ipfs_url = ctx.ipfs_url.clone();
            let arweave_url = ctx.arweave_url.clone();
            let metrics_ctx = MetricsContext {
                prometheus: ctx.prometheus_registry.clone(),
                registry: registry.clone(),
                prometheus_host,
                job_name,
            };

            commands::run::run(
                logger,
                store_builder,
                network_name,
                ipfs_url,
                arweave_url,
                config,
                metrics_ctx,
                node_id,
                subgraph,
                stop_block,
            )
            .await
        }
        Listen(cmd) => {
            use ListenCommand::*;
            match cmd {
                Assignments => commands::listen::assignments(ctx.subscription_manager()).await,
            }
        }
        Copy(cmd) => {
            use CopyCommand::*;
            match cmd {
                Create {
                    src,
                    shard,
                    node,
                    offset,
                    activate,
                    replace,
                } => {
                    let shards: Vec<_> = ctx.config.stores.keys().cloned().collect();
                    let (store, primary) = ctx.store_and_primary();
                    commands::copy::create(
                        store, primary, src, shard, shards, node, offset, activate, replace,
                    )
                    .await
                }
                Activate { deployment, shard } => {
                    commands::copy::activate(ctx.subgraph_store(), deployment, shard)
                }
                List => commands::copy::list(ctx.pools()),
                Status { dst } => commands::copy::status(ctx.pools(), &dst),
            }
        }
        Query {
            output,
            trace,
            target,
            query,
            vars,
        } => commands::query::run(ctx.graphql_runner(), target, query, vars, output, trace).await,
        Chain(cmd) => {
            use ChainCommand::*;
            match cmd {
                List => {
                    let (block_store, primary) = ctx.block_store_and_primary_pool();
                    commands::chain::list(primary, block_store).await
                }
                Info {
                    name,
                    reorg_threshold,
                    hashes,
                } => {
                    let (block_store, primary) = ctx.block_store_and_primary_pool();
                    commands::chain::info(primary, block_store, name, reorg_threshold, hashes).await
                }
                Remove { name } => {
                    let (block_store, primary) = ctx.block_store_and_primary_pool();
                    commands::chain::remove(primary, block_store, name)
                }
                ChangeShard { chain_name, shard } => {
                    let (block_store, primary) = ctx.block_store_and_primary_pool();
                    commands::chain::change_block_cache_shard(
                        primary,
                        block_store,
                        chain_name,
                        shard,
                    )
                }

                UpdateGenesis {
                    force,
                    block_hash,
                    chain_name,
                } => {
                    let store_builder = ctx.store_builder().await;
                    let store = ctx.store().block_store();
                    let networks = ctx.networks().await?;
                    let chain_id = ChainName::from(chain_name);
                    let block_hash = BlockHash::from_str(&block_hash)?;
                    commands::chain::update_chain_genesis(
                        &networks,
                        store_builder.coord.cheap_clone(),
                        store,
                        &logger,
                        chain_id,
                        block_hash,
                        force,
                    )
                    .await
                }

                CheckBlocks { method, chain_name } => {
                    use commands::check_blocks::{by_hash, by_number, by_range};
                    use CheckBlockMethod::*;
                    let logger = ctx.logger.clone();
                    let (chain_store, ethereum_adapter) =
                        ctx.chain_store_and_adapter(&chain_name).await?;
                    match method {
                        ByHash { hash } => {
                            by_hash(&hash, chain_store, &ethereum_adapter, &logger).await
                        }
                        ByNumber {
                            number,
                            delete_duplicates,
                        } => {
                            by_number(
                                number,
                                chain_store,
                                &ethereum_adapter,
                                &logger,
                                delete_duplicates,
                            )
                            .await
                        }
                        ByRange {
                            from,
                            to,
                            delete_duplicates,
                        } => {
                            by_range(
                                chain_store,
                                &ethereum_adapter,
                                from,
                                to,
                                &logger,
                                delete_duplicates,
                            )
                            .await
                        }
                    }
                }
                Truncate { chain_name, force } => {
                    use commands::check_blocks::truncate;
                    let chain_store = ctx.chain_store(&chain_name)?;
                    truncate(chain_store, force)
                }
                CallCache { method, chain_name } => {
                    match method {
                        CallCacheCommand::Remove {
                            from,
                            to,
                            remove_entire_cache,
                        } => {
                            let chain_store = ctx.chain_store(&chain_name)?;
                            if !remove_entire_cache && from.is_none() && to.is_none() {
                                bail!("you must specify either --from and --to or --remove-entire-cache");
                            }
                            let (from, to) = if remove_entire_cache {
                                (0, BLOCK_NUMBER_MAX)
                            } else {
                                // Clap makes sure that this does not panic
                                (from.unwrap(), to.unwrap())
                            };
                            commands::chain::clear_call_cache(chain_store, from, to).await
                        }
                    }
                }
                Ingest { name, number } => {
                    let logger = ctx.logger.cheap_clone();
                    let (chain_store, ethereum_adapter) =
                        ctx.chain_store_and_adapter(&name).await?;
                    commands::chain::ingest(&logger, chain_store, ethereum_adapter, number).await
                }
            }
        }
        Stats(cmd) => {
            use StatsCommand::*;
            match cmd {
                AccountLike {
                    clear,
                    deployment,
                    table,
                } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    let subgraph_store = store.subgraph_store();
                    commands::stats::account_like(
                        subgraph_store,
                        primary_pool,
                        clear,
                        &deployment,
                        table,
                    )
                    .await
                }
                Show { deployment } => commands::stats::show(ctx.pools(), &deployment),
                Analyze { deployment, entity } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    let subgraph_store = store.subgraph_store();
                    commands::stats::analyze(
                        subgraph_store,
                        primary_pool,
                        deployment,
                        entity.as_deref(),
                    )
                }
                Target { deployment } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    let subgraph_store = store.subgraph_store();
                    commands::stats::target(subgraph_store, primary_pool, &deployment)
                }
                SetTarget {
                    target,
                    reset,
                    no_analyze,
                    deployment,
                    entity,
                    columns,
                } => {
                    let (store, primary) = ctx.store_and_primary();
                    let store = store.subgraph_store();
                    let target = if reset { -1 } else { target as i32 };
                    commands::stats::set_target(
                        store,
                        primary,
                        &deployment,
                        entity.as_deref(),
                        columns,
                        target,
                        no_analyze,
                    )
                }
            }
        }
        Index(cmd) => {
            use IndexCommand::*;
            let (store, primary_pool) = ctx.store_and_primary();
            let subgraph_store = store.subgraph_store();
            match cmd {
                Create {
                    deployment,
                    entity,
                    fields,
                    method,
                    after,
                } => {
                    commands::index::create(
                        subgraph_store,
                        primary_pool,
                        deployment,
                        &entity,
                        fields,
                        method,
                        after,
                    )
                    .await
                }
                List {
                    deployment,
                    entity,
                    no_attribute_indexes,
                    no_default_indexes,
                    sql,
                    concurrent,
                    if_not_exists,
                } => {
                    commands::index::list(
                        subgraph_store,
                        primary_pool,
                        deployment,
                        &entity,
                        no_attribute_indexes,
                        no_default_indexes,
                        sql,
                        concurrent,
                        if_not_exists,
                    )
                    .await
                }
                Drop {
                    deployment,
                    index_name,
                } => {
                    commands::index::drop(subgraph_store, primary_pool, deployment, &index_name)
                        .await
                }
            }
        }
        Database(cmd) => {
            match cmd {
                DatabaseCommand::Migrate => {
                    /* creating the store builder runs migrations */
                    let _store_builder = ctx.store_builder().await;
                    println!("All database migrations have been applied");
                    Ok(())
                }
                DatabaseCommand::Remap {
                    source,
                    dest,
                    force,
                } => {
                    let store_builder = ctx.store_builder().await;
                    commands::database::remap(&store_builder.coord, source, dest, force).await
                }
            }
        }
        Prune(cmd) => {
            use PruneCommand::*;
            match cmd {
                Run {
                    deployment,
                    history,
                    rebuild_threshold,
                    delete_threshold,
                    once,
                } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    let history = history.unwrap_or(ENV_VARS.min_history_blocks.try_into()?);
                    commands::prune::run(
                        store,
                        primary_pool,
                        deployment,
                        history,
                        rebuild_threshold,
                        delete_threshold,
                        once,
                    )
                    .await
                }
                Set {
                    deployment,
                    rebuild_threshold,
                    delete_threshold,
                    history,
                } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    let history = history.unwrap_or(ENV_VARS.min_history_blocks.try_into()?);
                    commands::prune::set(
                        store,
                        primary_pool,
                        deployment,
                        history,
                        rebuild_threshold,
                        delete_threshold,
                    )
                    .await
                }
                Status { run, deployment } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    commands::prune::status(store, primary_pool, deployment, run).await
                }
            }
        }

        Deploy {
            deployment,
            name,
            url,
        } => {
            let store = ctx.store();
            let subgraph_store = store.subgraph_store();

            commands::deploy::run(subgraph_store, deployment, name, url).await
        }
    }
}

fn parse_duration_in_secs(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(s.parse()?))
}

fn make_deployment_selector(
    deployment: DeploymentSearch,
) -> graphman::deployment::DeploymentSelector {
    use graphman::deployment::DeploymentSelector::*;

    match deployment {
        DeploymentSearch::Name { name } => Name(name),
        DeploymentSearch::Hash { hash, shard } => Subgraph { hash, shard },
        DeploymentSearch::All => All,
        DeploymentSearch::Deployment { namespace } => Schema(namespace),
    }
}
