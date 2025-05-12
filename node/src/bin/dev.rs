use std::{collections::HashMap, path::Path, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use git_testament::{git_testament, render_testament};
use graph::{
    components::link_resolver::FileLinkResolver,
    env::EnvVars,
    log::logger,
    slog::{error, info},
    tokio::{self, sync::mpsc},
};
use graph_node::{
    dev::{
        helpers::DevModeContext,
        watcher::{parse_manifest_args, watch_subgraphs},
    },
    launcher,
    opt::Opt,
};
use lazy_static::lazy_static;
use pgtemp::PgTempDBBuilder;

git_testament!(TESTAMENT);
lazy_static! {
    static ref RENDERED_TESTAMENT: String = render_testament!(TESTAMENT);
}

#[derive(Clone, Debug, Parser)]
#[clap(
    name = "gnd",
    about = "Graph Node Dev", 
    author = "Graph Protocol, Inc.",
    version = RENDERED_TESTAMENT.as_str()
)]
pub struct DevOpt {
    #[clap(
        long,
        help = "Start a graph-node in dev mode watching a build directory for changes"
    )]
    pub watch: bool,

    #[clap(
        long,
        value_name = "MANIFEST:[BUILD_DIR]",
        help = "The location of the subgraph manifest file. If no build directory is provided, the default is 'build'. The file can be an alias, in the format '[BUILD_DIR:]manifest' where 'manifest' is the path to the manifest file, and 'BUILD_DIR' is the path to the build directory relative to the manifest file.",
        default_value = "./build/subgraph.yaml",
        value_delimiter = ','
    )]
    pub manifests: Vec<String>,

    #[clap(
        long,
        help = "The location of the database directory.",
        default_value = "./build"
    )]
    pub database_dir: String,

    #[clap(
        long,
        allow_negative_numbers = false,
        value_name = "NETWORK_NAME:[CAPABILITIES]:URL",
        env = "ETHEREUM_RPC",
        help = "Ethereum network name (e.g. 'mainnet'), optional comma-seperated capabilities (eg 'full,archive'), and an Ethereum RPC URL, separated by a ':'"
    )]
    pub ethereum_rpc: Vec<String>,

    #[clap(
        long,
        value_name = "HOST:PORT",
        env = "IPFS",
        help = "HTTP addresses of IPFS servers (RPC, Gateway)",
        default_value = "https://api.thegraph.com/ipfs"
    )]
    pub ipfs: Vec<String>,
}

/// Builds the Graph Node options from DevOpt
fn build_args(dev_opt: &DevOpt, db_url: &str) -> Result<Opt> {
    let mut args = vec!["gnd".to_string()];

    if !dev_opt.ipfs.is_empty() {
        args.push("--ipfs".to_string());
        args.push(dev_opt.ipfs.join(","));
    }

    if !dev_opt.ethereum_rpc.is_empty() {
        args.push("--ethereum-rpc".to_string());
        args.push(dev_opt.ethereum_rpc.join(","));
    }

    args.push("--postgres-url".to_string());
    args.push(db_url.to_string());

    let opt = Opt::parse_from(args);

    Ok(opt)
}

async fn run_graph_node(opt: Opt, ctx: Option<DevModeContext>) -> Result<()> {
    let env_vars = Arc::new(EnvVars::from_env().context("Failed to load environment variables")?);

    launcher::run(opt, env_vars, ctx).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let dev_opt = DevOpt::parse();

    let database_dir = Path::new(&dev_opt.database_dir);

    let logger = logger(true);

    info!(logger, "Starting Graph Node Dev");
    info!(logger, "Database directory: {}", database_dir.display());

    let db = PgTempDBBuilder::new()
        .with_data_dir_prefix(database_dir)
        .with_initdb_param("-E", "UTF8")
        .with_initdb_param("--locale", "C")
        .start_async()
        .await;

    let (tx, rx) = mpsc::channel(1);
    let opt = build_args(&dev_opt, &db.connection_uri())?;

    let manifests_paths = parse_manifest_args(dev_opt.manifests, &logger)?;
    let file_link_resolver = Arc::new(FileLinkResolver::new(None, HashMap::new()));

    let ctx = DevModeContext {
        watch: dev_opt.watch,
        file_link_resolver,
        updates_rx: rx,
    };

    // Run graph node
    graph::spawn(async move {
        let _ = run_graph_node(opt, Some(ctx)).await;
    });

    if dev_opt.watch {
        graph::spawn_blocking(async move {
            let result =
                watch_subgraphs(&logger, manifests_paths, vec!["pgtemp-*".to_string()], tx).await;
            if let Err(e) = result {
                error!(logger, "Error watching subgraphs"; "error" => e.to_string());
                std::process::exit(1);
            }
        });
    }

    graph::futures03::future::pending::<()>().await;
    Ok(())
}
