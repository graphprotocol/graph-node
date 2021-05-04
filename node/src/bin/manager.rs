use std::{collections::HashMap, env, sync::Arc};

use config::PoolSize;
use git_testament::{git_testament, render_testament};
use graph::{data::graphql::effort::LoadManager, prometheus::Registry};
use graph_core::MetricsRegistry;
use graph_graphql::prelude::GraphQlRunner;
use lazy_static::lazy_static;
use structopt::StructOpt;

use graph::{
    log::logger,
    prelude::{info, o, slog, tokio, Logger, NodeId},
};
use graph_node::{manager::PanicSubscriptionManager, store_builder::StoreBuilder};
use graph_store_postgres::{
    connection_pool::ConnectionPool, Shard, Store, SubgraphStore, SubscriptionManager,
    PRIMARY_SHARD,
};

use graph_node::config::{self, Config as Cfg};
use graph_node::manager::commands;

git_testament!(TESTAMENT);

macro_rules! die {
    ($fmt:expr, $($arg:tt)*) => {{
        use std::io::Write;
        writeln!(&mut ::std::io::stderr(), $fmt, $($arg)*).unwrap();
        ::std::process::exit(1)
    }}
}

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
        help = "a unique identifier for this node\n"
    )]
    pub node_id: String,
    #[structopt(
        long,
        default_value = "3",
        help = "the size for connection pools. Set to 0\n to use pool size from configuration file\n corresponding to NODE_ID"
    )]
    pub pool_size: u32,
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
    Info {
        /// The deployment, an id, schema name or subgraph name
        name: String,
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
        /// The id of the deployment to reassign
        id: String,
        /// The name of the node that should index the deployment
        node: String,
        /// The shard of the deployment if `id` itself is ambiguous
        shard: Option<String>,
    },
    /// Unassign a deployment
    Unassign {
        /// The id of the deployment to unassign
        id: String,
        /// The shard of the deployment if `id` itself is ambiguous
        shard: Option<String>,
    },
    /// Rewind a subgraph to a specific block
    Rewind {
        /// The hash of the deployment to rewind
        id: String,
        /// The block hash of the target block
        block_hash: String,
        /// The block number of the target block
        block_number: i32,
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
        /// The deployment hash
        deployment: String,
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
        /// The IPFS hash of the source deployment
        src: String,
        /// The name of the database shard into which to copy
        shard: String,
        /// The name of the node that should index the copy
        node: String,
        /// The shard of the `src` subgraph in case that is ambiguous
        src_shard: Option<String>,
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
        /// The internal id of the destination of the copy operation (number)
        dst: i32,
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
    registry: Arc<MetricsRegistry>,
}

impl Context {
    fn new(logger: Logger, node_id: NodeId, config: Cfg) -> Self {
        let prometheus_registry = Arc::new(Registry::new());
        let registry = Arc::new(MetricsRegistry::new(
            logger.clone(),
            prometheus_registry.clone(),
        ));

        Self {
            logger,
            node_id,
            config,
            registry,
        }
    }

    fn primary_pool(self) -> ConnectionPool {
        let primary = self.config.primary_store();
        StoreBuilder::main_pool(
            &self.logger,
            &self.node_id,
            PRIMARY_SHARD.as_str(),
            primary,
            self.registry,
        )
    }

    fn subgraph_store(self) -> Arc<SubgraphStore> {
        StoreBuilder::make_subgraph_store(&self.logger, &self.node_id, &self.config, self.registry)
    }

    fn subscription_manager(self) -> Arc<SubscriptionManager> {
        let primary = self.config.primary_store();
        Arc::new(SubscriptionManager::new(
            self.logger.clone(),
            primary.connection.to_owned(),
        ))
    }

    fn store(self) -> Arc<Store> {
        let (store, _) = self.store_and_pools();
        store
    }

    fn pools(self) -> HashMap<Shard, ConnectionPool> {
        let (_, pools) = self.store_and_pools();
        pools
    }

    fn store_and_pools(self) -> (Arc<Store>, HashMap<Shard, ConnectionPool>) {
        let (subgraph_store, pools) = StoreBuilder::make_subgraph_store_and_pools(
            &self.logger,
            &self.node_id,
            &self.config,
            self.registry,
        );

        let store = StoreBuilder::make_store(
            &self.logger,
            pools.clone(),
            subgraph_store,
            HashMap::default(),
            vec![],
        );

        (store, pools)
    }

    fn graphql_runner(self) -> Arc<GraphQlRunner<Store, PanicSubscriptionManager>> {
        let logger = self.logger.clone();
        let registry = self.registry.clone();

        let store = self.store();

        let subscription_manager = Arc::new(PanicSubscriptionManager);
        let load_manager = Arc::new(LoadManager::new(&logger, vec![], registry, 128));

        Arc::new(GraphQlRunner::new(
            &logger,
            store,
            subscription_manager,
            load_manager,
        ))
    }
}

#[tokio::main]
async fn main() {
    let opt = Opt::from_args();

    // Set up logger
    let logger = match env::var_os("GRAPH_LOG") {
        Some(_) => logger(false),
        None => Logger::root(slog::Discard, o!()),
    };

    // Log version information
    info!(
        logger,
        "Graph Node version: {}",
        render_testament!(TESTAMENT)
    );

    let mut config = match Cfg::load(&logger, &opt.clone().into()) {
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
        Ok(config) => config,
    };
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
    let ctx = Context::new(logger, node, config);

    use Command::*;
    let result = match opt.cmd {
        TxnSpeed { delay } => commands::txn_speed::run(ctx.primary_pool(), delay),
        Info {
            name,
            current,
            pending,
            status,
            used,
        } => {
            let (pool, store) = if status {
                let (store, pools) = ctx.store_and_pools();
                let primary = pools.get(&*PRIMARY_SHARD).expect("there is a primary pool");
                (primary.clone(), Some(store))
            } else {
                (ctx.primary_pool(), None)
            };
            commands::info::run(pool, store, name, current, pending, used)
        }
        Unused(cmd) => {
            let store = ctx.subgraph_store();
            use UnusedCommand::*;

            match cmd {
                List { existing } => commands::unused_deployments::list(store, existing),
                Record => commands::unused_deployments::record(store),
                Remove { count, deployment } => {
                    let count = count.unwrap_or(1_000_000);
                    commands::unused_deployments::remove(store, count, deployment)
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
        Unassign { id, shard } => commands::assign::unassign(ctx.subgraph_store(), id, shard),
        Reassign { id, node, shard } => {
            commands::assign::reassign(ctx.subgraph_store(), id, node, shard)
        }
        Rewind {
            id,
            block_hash,
            block_number,
        } => commands::rewind::run(ctx.subgraph_store(), id, block_hash, block_number),
        Listen(cmd) => {
            use ListenCommand::*;
            match cmd {
                Assignments => commands::listen::assignments(ctx.subscription_manager()).await,
                Entities {
                    deployment,
                    entity_types,
                } => {
                    commands::listen::entities(ctx.subscription_manager(), deployment, entity_types)
                        .await
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
                    src_shard,
                } => commands::copy::create(ctx.store(), src, src_shard, shard, node, offset).await,
                Activate { deployment, shard } => {
                    commands::copy::activate(ctx.subgraph_store(), deployment, shard)
                }
                List => commands::copy::list(ctx.pools()),
                Status { dst } => commands::copy::status(ctx.pools(), dst),
            }
        }
        Query {
            target,
            query,
            vars,
        } => commands::query::run(ctx.graphql_runner(), target, query, vars).await,
    };
    if let Err(e) = result {
        die!("error: {}", e)
    }
}
