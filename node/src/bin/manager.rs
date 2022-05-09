use config::PoolSize;
use git_testament::{git_testament, render_testament};
use graph::{data::graphql::effort::LoadManager, prelude::chrono, prometheus::Registry};
use graph::{
    log::logger,
    prelude::{
        anyhow::{self, Context as AnyhowContextTrait},
        info, o, slog, tokio, Logger, NodeId, ENV_VARS,
    },
    url::Url,
};
use graph_chain_ethereum::{EthereumAdapter, EthereumNetworks};
use graph_core::MetricsRegistry;
use graph_graphql::prelude::GraphQlRunner;
use graph_node::config::{self, Config as Cfg};
use graph_node::manager::commands;
use graph_node::{
    chain::create_ethereum_networks,
    manager::{deployment::DeploymentSearch, PanicSubscriptionManager},
    store_builder::StoreBuilder,
    MetricsContext,
};
use graph_store_postgres::ChainStore;
use graph_store_postgres::{
    connection_pool::ConnectionPool, BlockStore, NotificationSender, Shard, Store, SubgraphStore,
    SubscriptionManager, PRIMARY_SHARD,
};
use lazy_static::lazy_static;
use std::{collections::HashMap, env, num::ParseIntError, sync::Arc, time::Duration};
use structopt::StructOpt;

const VERSION_LABEL_KEY: &str = "version";

git_testament!(TESTAMENT);

lazy_static! {
    static ref RENDERED_TESTAMENT: String = render_testament!(TESTAMENT);
}

#[derive(Clone, Debug, StructOpt)]
#[structopt(
    name = "graphman",
    about = "Management tool for a graph-node infrastructure",
    author = "Graph Protocol, Inc.",
    version = RENDERED_TESTAMENT.as_str()
)]
pub struct Opt {
    #[structopt(
        long,
        short,
        env = "GRAPH_NODE_CONFIG",
        help = "the name of the configuration file\n"
    )]
    pub config: String,
    #[structopt(
        long,
        default_value = "default",
        value_name = "NODE_ID",
        env = "GRAPH_NODE_ID",
        help = "a unique identifier for this node.\nShould have the same value between consecutive node restarts\n"
    )]
    pub node_id: String,
    #[structopt(
        long,
        value_name = "{HOST:PORT|URL}",
        default_value = "https://api.thegraph.com/ipfs/",
        env = "IPFS",
        help = "HTTP addresses of IPFS nodes"
    )]
    pub ipfs: Vec<String>,
    #[structopt(
        long,
        default_value = "3",
        help = "the size for connection pools. Set to 0\n to use pool size from configuration file\n corresponding to NODE_ID"
    )]
    pub pool_size: u32,
    #[structopt(long, value_name = "URL", help = "Base URL for forking subgraphs")]
    pub fork_base: Option<String>,
    #[structopt(long, help = "version label, used for prometheus metrics")]
    pub version_label: Option<String>,
    #[structopt(subcommand)]
    pub cmd: Command,
}

#[derive(Clone, Debug, StructOpt)]
pub enum Command {
    /// Calculate the transaction speed
    TxnSpeed {
        #[structopt(long, short, default_value = "60")]
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
        deployment: DeploymentSearch,
        /// List only current version
        #[structopt(long, short)]
        current: bool,
        /// List only pending versions
        #[structopt(long, short)]
        pending: bool,
        /// Include status information
        #[structopt(long, short)]
        status: bool,
        /// List only used (current and pending) versions
        #[structopt(long, short)]
        used: bool,
    },
    /// Manage unused deployments
    ///
    /// Record which deployments are unused with `record`, then remove them
    /// with `remove`
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
    /// Rewind a subgraph to a specific block
    Rewind {
        /// Force rewinding even if the block hash is not found in the local
        /// database
        #[structopt(long, short)]
        force: bool,
        /// Sleep for this many seconds after pausing subgraphs
        #[structopt(
            long,
            short,
            default_value = "10",
            parse(try_from_str = parse_duration_in_secs)
        )]
        sleep: Duration,
        /// The block hash of the target block
        block_hash: String,
        /// The block number of the target block
        block_number: i32,
        /// The deployments to rewind (see `help info`)
        deployments: Vec<DeploymentSearch>,
    },
    /// Deploy and run an arbitrary subgraph up to a certain block, although it can surpass it by a few blocks, it's not exact (use for dev and testing purposes) -- WARNING: WILL RUN MIGRATIONS ON THE DB, DO NOT USE IN PRODUCTION
    ///
    /// Also worth noting that the deployed subgraph will be removed at the end.
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
    Config(ConfigCommand),
    /// Listen for store events and print them
    Listen(ListenCommand),
    /// Manage deployment copies and grafts
    Copy(CopyCommand),
    /// Run a GraphQL query
    Query {
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
    Chain(ChainCommand),
    /// Manipulate internal subgraph statistics
    Stats(StatsCommand),

    /// Manage database indexes
    Index(IndexCommand),
}

impl Command {
    /// Return `true` if the command should not override connection pool
    /// sizes, in general only when we will not actually connect to any
    /// databases
    fn use_configured_pool_size(&self) -> bool {
        matches!(self, Command::Config(_))
    }
}

#[derive(Clone, Debug, StructOpt)]
pub enum UnusedCommand {
    /// List unused deployments
    List {
        /// Only list unused deployments that still exist
        #[structopt(short, long)]
        existing: bool,
    },
    /// Update and record currently unused deployments
    Record,
    /// Remove deployments that were marked as unused with `record`.
    ///
    /// Deployments are removed in descending order of number of entities,
    /// i.e., smaller deployments are removed before larger ones
    Remove {
        /// How many unused deployments to remove (default: all)
        #[structopt(short, long)]
        count: Option<usize>,
        /// Remove a specific deployment
        #[structopt(short, long, conflicts_with = "count")]
        deployment: Option<String>,
        /// Remove unused deployments that were recorded at least this many minutes ago
        #[structopt(short, long)]
        older: Option<u32>,
    },
}

#[derive(Clone, Debug, StructOpt)]
pub enum ConfigCommand {
    /// Check and validate the configuration file
    Check {
        /// Print the configuration as JSON
        #[structopt(long)]
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
        #[structopt(short, long)]
        shard: bool,
    },
}

#[derive(Clone, Debug, StructOpt)]
pub enum ListenCommand {
    /// Listen only to assignment events
    Assignments,
    /// Listen to events for entities in a specific deployment
    Entities {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The entity types for which to print change notifications
        entity_types: Vec<String>,
    },
}
#[derive(Clone, Debug, StructOpt)]
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
        #[structopt(long, short, default_value = "200")]
        offset: u32,
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

#[derive(Clone, Debug, StructOpt)]
pub enum ChainCommand {
    /// List all chains that are in the database
    List,
    /// Show information about a chain
    Info {
        #[structopt(
            long,
            short,
            default_value = "50",
            env = "ETHEREUM_REORG_THRESHOLD",
            help = "the reorg threshold to check\n"
        )]
        reorg_threshold: i32,
        #[structopt(long, help = "display block hashes\n")]
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
        #[structopt(subcommand)] // Note that we mark a field as a subcommand
        method: CheckBlockMethod,

        /// Chain name (must be an existing chain, see 'chain list')
        #[structopt(empty_values = false)]
        chain_name: String,
    },
    /// Truncates the whole block cache for the given chain.
    Truncate {
        /// Chain name (must be an existing chain, see 'chain list')
        #[structopt(empty_values = false)]
        chain_name: String,
        /// Skips confirmation prompt
        #[structopt(long, short)]
        force: bool,
    },
}

#[derive(Clone, Debug, StructOpt)]
pub enum StatsCommand {
    /// Toggle whether a table is account-like
    ///
    /// Setting a table to 'account-like' enables a query optimization that
    /// is very effective for tables with a high ratio of entity versions
    /// to distinct entities. It can take up to 5 minutes for this to take
    /// effect.
    AccountLike {
        #[structopt(long, short, help = "do not set but clear the account-like flag\n")]
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
    /// Postgres keeps, and only refreshed when a table is analyzed. If a
    /// table name is passed, perform a full count of entities and versions
    /// in that table, which can be very slow, but is needed since the
    /// statistics based data can be off by an order of magnitude.
    Show {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The name of a table to fully count
        table: Option<String>,
    },
    /// Perform a SQL ANALYZE in a Entity table
    Analyze {
        /// The deployment (see `help info`).
        deployment: DeploymentSearch,
        /// The name of the Entity to ANALYZE, in camel case
        entity: String,
    },
}

#[derive(Clone, Debug, StructOpt)]
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
        #[structopt(empty_values = false)]
        deployment: DeploymentSearch,
        /// The Entity name.
        ///
        /// Can be expressed either in upper camel case (as its GraphQL definition) or in snake case
        /// (as its SQL table name).
        #[structopt(empty_values = false)]
        entity: String,
        /// The Field names.
        ///
        /// Each field can be expressed either in camel case (as its GraphQL definition) or in snake
        /// case (as its SQL colmun name).
        #[structopt(min_values = 1, required = true)]
        fields: Vec<String>,
        /// The index method. Defaults to `btree`.
        #[structopt(
            short, long, default_value = "btree",
            possible_values = &["btree", "hash", "gist", "spgist", "gin", "brin"]
        )]
        method: String,
    },
    /// Lists existing indexes for a given Entity
    List {
        /// The deployment (see `help info`).
        #[structopt(empty_values = false)]
        deployment: DeploymentSearch,
        /// The Entity name.
        ///
        /// Can be expressed either in upper camel case (as its GraphQL definition) or in snake case
        /// (as its SQL table name).
        #[structopt(empty_values = false)]
        entity: String,
    },

    /// Drops an index for a given deployment, concurrently
    Drop {
        /// The deployment (see `help info`).
        #[structopt(empty_values = false)]
        deployment: DeploymentSearch,
        /// The name of the index to be dropped
        #[structopt(empty_values = false)]
        index_name: String,
    },
}

#[derive(Clone, Debug, StructOpt)]
pub enum CheckBlockMethod {
    /// The number of the target block
    ByHash { hash: String },

    /// The hash of the target block
    ByNumber { number: i32 },

    /// A block number range, inclusive on both ends.
    ByRange {
        #[structopt(long, short)]
        from: Option<i32>,
        #[structopt(long, short)]
        to: Option<i32>,
    },
}

impl From<Opt> for config::Opt {
    fn from(opt: Opt) -> Self {
        let mut config_opt = config::Opt::default();
        config_opt.config = Some(opt.config);
        config_opt.store_connection_pool_size = 5;
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
        let pool = StoreBuilder::main_pool(
            &self.logger,
            &self.node_id,
            PRIMARY_SHARD.as_str(),
            primary,
            self.metrics_registry(),
            Arc::new(vec![]),
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
            primary.connection.to_owned(),
            self.registry.clone(),
        ))
    }

    fn primary_and_subscription_manager(self) -> (ConnectionPool, Arc<SubscriptionManager>) {
        let mgr = self.subscription_manager();
        let primary_pool = self.primary_pool();

        (primary_pool, mgr)
    }

    fn store(self) -> Arc<Store> {
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

    fn store_and_pools(self) -> (Arc<Store>, HashMap<Shard, ConnectionPool>) {
        let (subgraph_store, pools) = StoreBuilder::make_subgraph_store_and_pools(
            &self.logger,
            &self.node_id,
            &self.config,
            self.fork_base,
            self.registry,
        );

        for pool in pools.values() {
            pool.skip_setup();
        }

        let store = StoreBuilder::make_store(
            &self.logger,
            pools.clone(),
            subgraph_store,
            HashMap::default(),
            vec![],
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

    fn graphql_runner(self) -> Arc<GraphQlRunner<Store, PanicSubscriptionManager>> {
        let logger = self.logger.clone();
        let registry = self.registry.clone();

        let store = self.store();

        let subscription_manager = Arc::new(PanicSubscriptionManager);
        let load_manager = Arc::new(LoadManager::new(&logger, vec![], registry.clone()));

        Arc::new(GraphQlRunner::new(
            &logger,
            store,
            subscription_manager,
            load_manager,
            registry,
        ))
    }

    async fn ethereum_networks(&self) -> anyhow::Result<EthereumNetworks> {
        let logger = self.logger.clone();
        let registry = self.metrics_registry();
        create_ethereum_networks(logger, registry, &self.config).await
    }

    fn chain_store(self, chain_name: &str) -> anyhow::Result<Arc<ChainStore>> {
        use graph::components::store::BlockStore;
        self.store()
            .block_store()
            .chain_store(&chain_name)
            .ok_or_else(|| anyhow::anyhow!("Could not find a network named '{}'", chain_name))
    }

    async fn chain_store_and_adapter(
        self,
        chain_name: &str,
    ) -> anyhow::Result<(Arc<ChainStore>, Arc<EthereumAdapter>)> {
        let ethereum_networks = self.ethereum_networks().await?;
        let chain_store = self.chain_store(chain_name)?;
        let ethereum_adapter = ethereum_networks
            .networks
            .get(chain_name)
            .map(|adapters| adapters.cheapest())
            .flatten()
            .ok_or(anyhow::anyhow!(
                "Failed to obtain an Ethereum adapter for chain '{}'",
                chain_name
            ))?;
        Ok((chain_store, ethereum_adapter))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opt = Opt::from_args();

    let version_label = opt.version_label.clone();
    // Set up logger
    let logger = match ENV_VARS.log_levels {
        Some(_) => logger(false),
        None => Logger::root(slog::Discard, o!()),
    };

    // Log version information
    info!(
        logger,
        "Graph Node version: {}",
        render_testament!(TESTAMENT)
    );

    let mut config = Cfg::load(&logger, &opt.clone().into()).context("Configuration error")?;

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
            let url = if !url.ends_with("/") {
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
        } => {
            let (primary, store) = if status {
                let (store, primary) = ctx.store_and_primary();
                (primary.clone(), Some(store))
            } else {
                (ctx.primary_pool(), None)
            };
            commands::info::run(primary, store, deployment, current, pending, used)
        }
        Unused(cmd) => {
            let store = ctx.subgraph_store();
            use UnusedCommand::*;

            match cmd {
                List { existing } => commands::unused_deployments::list(store, existing),
                Record => commands::unused_deployments::record(store),
                Remove {
                    count,
                    deployment,
                    older,
                } => {
                    let count = count.unwrap_or(1_000_000);
                    let older = older.map(|older| chrono::Duration::minutes(older as i64));
                    commands::unused_deployments::remove(store, count, deployment, older)
                }
            }
        }
        Config(cmd) => {
            use ConfigCommand::*;

            match cmd {
                Place { name, network } => {
                    commands::config::place(&ctx.config.deployment, &name, &network)
                }
                Check { print } => commands::config::check(&ctx.config, print),
                Pools { nodes, shard } => commands::config::pools(&ctx.config, nodes, shard),
            }
        }
        Remove { name } => commands::remove::run(ctx.subgraph_store(), name),
        Create { name } => commands::create::run(ctx.subgraph_store(), name),
        Unassign { deployment } => {
            let sender = ctx.notification_sender();
            commands::assign::unassign(ctx.primary_pool(), &sender, &deployment).await
        }
        Reassign { deployment, node } => {
            let sender = ctx.notification_sender();
            commands::assign::reassign(ctx.primary_pool(), &sender, &deployment, node)
        }
        Rewind {
            force,
            sleep,
            block_hash,
            block_number,
            deployments,
        } => {
            let (store, primary) = ctx.store_and_primary();
            commands::rewind::run(
                primary,
                store,
                deployments,
                block_hash,
                block_number,
                force,
                sleep,
            )
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
                Entities {
                    deployment,
                    entity_types,
                } => {
                    let (primary, mgr) = ctx.primary_and_subscription_manager();
                    commands::listen::entities(primary, mgr, &deployment, entity_types).await
                }
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
                } => {
                    let shards: Vec<_> = ctx.config.stores.keys().cloned().collect();
                    let (store, primary) = ctx.store_and_primary();
                    commands::copy::create(store, primary, src, shard, shards, node, offset).await
                }
                Activate { deployment, shard } => {
                    commands::copy::activate(ctx.subgraph_store(), deployment, shard)
                }
                List => commands::copy::list(ctx.pools()),
                Status { dst } => commands::copy::status(ctx.pools(), &dst),
            }
        }
        Query {
            target,
            query,
            vars,
        } => commands::query::run(ctx.graphql_runner(), target, query, vars).await,
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
                        ByNumber { number } => {
                            by_number(number, chain_store, &ethereum_adapter, &logger).await
                        }
                        ByRange { from, to } => {
                            by_range(chain_store, &ethereum_adapter, from, to, &logger).await
                        }
                    }
                }
                Truncate { chain_name, force } => {
                    use commands::check_blocks::truncate;
                    let chain_store = ctx.chain_store(&chain_name)?;
                    truncate(chain_store, force)
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
                } => commands::stats::account_like(ctx.pools(), clear, &deployment, table),
                Show { deployment, table } => {
                    commands::stats::show(ctx.pools(), &deployment, table)
                }
                Analyze { deployment, entity } => {
                    let (store, primary_pool) = ctx.store_and_primary();
                    let subgraph_store = store.subgraph_store();
                    commands::stats::analyze(subgraph_store, primary_pool, deployment, &entity)
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
                } => {
                    commands::index::create(
                        subgraph_store,
                        primary_pool,
                        deployment,
                        &entity,
                        fields,
                        method,
                    )
                    .await
                }
                List { deployment, entity } => {
                    commands::index::list(subgraph_store, primary_pool, deployment, &entity).await
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
    }
}

fn parse_duration_in_secs(s: &str) -> Result<Duration, ParseIntError> {
    Ok(Duration::from_secs(s.parse()?))
}
