use std::{env, sync::Arc};

use git_testament::{git_testament, render_testament};
use graph::prometheus::Registry;
use graph_core::MetricsRegistry;
use lazy_static::lazy_static;
use structopt::StructOpt;

use graph::{
    log::logger,
    prelude::{info, o, slog, tokio, Logger, NodeId},
};
use graph_node::config;
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::{
    connection_pool::ConnectionPool, SubgraphStore, SubscriptionManager, PRIMARY_SHARD,
};

use crate::config::Config as Cfg;
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
        help = "the name of the configuration file"
    )]
    pub config: String,
    #[structopt(
        long,
        default_value = "default",
        value_name = "NODE_ID",
        env = "GRAPH_NODE_ID",
        help = "a unique identifier for this node"
    )]
    pub node_id: String,
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
    /// Assign or reassign a deployment
    Reassign {
        /// The id of the deployment to reassign
        id: String,
        /// The name of the node that should index the deployment
        node: String,
    },
    /// Unassign a deployment
    Unassign {
        /// The id of the deployment to unassign
        id: String,
    },
    /// Check and interrogate the configuration
    ///
    /// Print information about a configuration file without
    /// actually connecting to databases or network clients
    Config(ConfigCommand),
    /// Listen for store events and print them
    Listen(ListenCommand),
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
    Assignments,
    Entities {
        deployment: String,
        entity_types: Vec<String>,
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

fn make_registry(logger: &Logger) -> Arc<MetricsRegistry> {
    let prometheus_registry = Arc::new(Registry::new());
    Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ))
}

fn make_main_pool(logger: &Logger, node_id: &NodeId, config: &Cfg) -> ConnectionPool {
    let primary = config.primary_store();
    StoreBuilder::main_pool(
        &logger,
        node_id,
        PRIMARY_SHARD.as_str(),
        primary,
        make_registry(logger),
    )
}

fn make_store(logger: &Logger, node_id: &NodeId, config: &Cfg) -> Arc<SubgraphStore> {
    StoreBuilder::make_sharded_store(logger, node_id, config, make_registry(logger))
}

fn make_subscription_manager(logger: &Logger, config: &Cfg) -> Arc<SubscriptionManager> {
    let primary = config.primary_store();
    Arc::new(SubscriptionManager::new(
        logger.clone(),
        primary.connection.to_owned(),
    ))
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

    let config = match Cfg::load(&logger, &opt.clone().into()) {
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
        Ok(config) => config,
    };
    let node = match NodeId::new(&opt.node_id) {
        Err(()) => {
            eprintln!("invalid node id: {}", opt.node_id);
            std::process::exit(1);
        }
        Ok(node) => node,
    };
    let make_main_pool = || make_main_pool(&logger, &node, &config);
    let make_store = || make_store(&logger, &node, &config);

    use Command::*;
    let result = match opt.cmd {
        TxnSpeed { delay } => {
            let pool = make_main_pool();
            commands::txn_speed::run(pool, delay)
        }
        Info {
            name,
            current,
            pending,
            used,
        } => {
            let pool = make_main_pool();
            commands::info::run(pool, name, current, pending, used)
        }
        Unused(cmd) => {
            let store = make_store();
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
                    commands::config::place(&config.deployment, &name, &network)
                }
                Check { print } => commands::config::check(&config, print),
                Pools { nodes, shard } => commands::config::pools(&config, nodes, shard),
            }
        }
        Remove { name } => {
            let store = make_store();
            commands::remove::run(store, name)
        }
        Unassign { id } => {
            let store = make_store();
            commands::assign::unassign(store, id)
        }
        Reassign { id, node } => {
            let store = make_store();
            commands::assign::reassign(store, id, node)
        }
        Listen(cmd) => {
            use ListenCommand::*;
            match cmd {
                Assignments => {
                    let subscription_manager = make_subscription_manager(&logger, &config);
                    commands::listen::assignments(subscription_manager).await
                }
                Entities {
                    deployment,
                    entity_types,
                } => {
                    let subscription_manager = make_subscription_manager(&logger, &config);
                    commands::listen::entities(subscription_manager, deployment, entity_types).await
                }
            }
        }
    };
    if let Err(e) = result {
        die!("error: {}", e)
    }
}
