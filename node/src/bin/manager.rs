use std::{env, sync::Arc};

use git_testament::{git_testament, render_testament};
use graph_core::MetricsRegistry;
use lazy_static::lazy_static;
use prometheus::Registry;
use structopt::StructOpt;

use graph::{
    log::logger,
    prelude::{info, o, slog, tokio, Logger},
};
use graph_node::config;
use graph_node::store_builder::StoreBuilder;
use graph_store_postgres::connection_pool::ConnectionPool;

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

fn make_main_pool(logger: &Logger, config: &Config) -> ConnectionPool {
    let prometheus_registry = Arc::new(Registry::new());
    let metrics_registry = Arc::new(MetricsRegistry::new(
        logger.clone(),
        prometheus_registry.clone(),
    ));

    StoreBuilder::main_pool(&logger, &config, metrics_registry)
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

    let pool = make_main_pool(&logger, &config);

    use Command::*;
    let result = match opt.cmd {
        TxnSpeed { delay } => commands::txn_speed::run(pool, delay),
        Info { name } => commands::info::run(pool, name),
    };
    if let Err(e) = result {
        die!("error: {}", e)
    }
}
