use std::{env, sync::Arc};

use git_testament::{git_testament, render_testament};
use graph_core::MetricsRegistry;
use lazy_static::lazy_static;
use prometheus::Registry;
use structopt::StructOpt;

use graph::{
    log::logger,
    prelude::{anyhow, info, o, slog, tokio, Logger},
};
use graph_node::config;
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::{connection_pool::ConnectionPool, SubgraphStore, PRIMARY_SHARD};

use crate::config::Config;
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
        env = "GRAPH_NODE_CONFIG",
        help = "the name of the configuration file"
    )]
    pub config: String,
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
    /// Print how a specific subgraph would be placed
    Place { name: String, network: String },
    /// Manage unused deployments
    ///
    /// Record which deployments are unused with `record`, then remove them
    /// with `remove`
    Unused(UnusedCommand),
    /// Check the configuration file
    Check,
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

fn make_main_pool(logger: &Logger, config: &Config) -> ConnectionPool {
    let primary = config.primary_store();
    StoreBuilder::main_pool(
        &logger,
        PRIMARY_SHARD.as_str(),
        primary,
        make_registry(logger),
    )
}

fn make_store(logger: &Logger, config: &Config) -> Arc<SubgraphStore> {
    StoreBuilder::make_sharded_store(logger, config, make_registry(logger))
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

    let config = match Config::load(&logger, &opt.clone().into()) {
        Err(e) => {
            eprintln!("configuration error: {}", e);
            std::process::exit(1);
        }
        Ok(config) => config,
    };

    use Command::*;
    let result = match opt.cmd {
        TxnSpeed { delay } => {
            let pool = make_main_pool(&logger, &config);
            commands::txn_speed::run(pool, delay)
        }
        Info {
            name,
            current,
            pending,
            used,
        } => {
            let pool = make_main_pool(&logger, &config);
            commands::info::run(pool, name, current, pending, used)
        }
        Place { name, network } => commands::place::run(&config.deployment, &name, &network),
        Unused(cmd) => {
            let store = make_store(&logger, &config);
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
        Check => match config.to_json() {
            Ok(txt) => {
                println!("{}", txt);
                eprintln!("Successfully validated configuration");
                Ok(())
            }
            Err(e) => Err(anyhow!("error serializing config: {}", e)),
        },
    };
    if let Err(e) = result {
        die!("error: {}", e)
    }
}
