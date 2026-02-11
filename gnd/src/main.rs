use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use git_testament::{git_testament, render_testament};
use graph::{
    components::link_resolver::FileLinkResolver,
    env::EnvVars,
    log::logger,
    prelude::{CheapClone, DeploymentHash, LinkResolver, SubgraphName},
    slog::{error, info, Logger},
};
use graph_core::polling_monitor::ipfs_service;
use graph_node::{launcher, opt::Opt};
use lazy_static::lazy_static;
use tokio::{self, sync::mpsc};
use tokio_util::sync::CancellationToken;

use gnd::watcher::{deploy_all_subgraphs, parse_manifest_args, watch_subgraphs};

#[cfg(unix)]
use pgtemp::{PgTempDB, PgTempDBBuilder};

// Add an alias for the temporary Postgres DB handle. On non unix
// targets we don't have pgtemp, but we still need the type to satisfy the
// function signatures.
#[cfg(unix)]
type TempPgDB = PgTempDB;
#[cfg(not(unix))]
type TempPgDB = ();

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
        default_value = "./subgraph.yaml",
        value_delimiter = ','
    )]
    pub manifests: Vec<String>,

    #[clap(
        long,
        value_name = "ALIAS:MANIFEST:[BUILD_DIR]",
        value_delimiter = ',',
        help = "The location of the source subgraph manifest files. This is used to resolve aliases in the manifest files for subgraph data sources. The format is ALIAS:MANIFEST:[BUILD_DIR], where ALIAS is the alias name, BUILD_DIR is the build directory relative to the manifest file, and MANIFEST is the manifest file location."
    )]
    pub sources: Vec<String>,

    #[clap(
        long,
        help = "The location of the database directory.",
        default_value = "./build"
    )]
    pub database_dir: String,

    #[clap(
        long,
        value_name = "URL",
        env = "POSTGRES_URL",
        help = "Location of the Postgres database used for storing entities"
    )]
    pub postgres_url: Option<String>,

    #[clap(
        long,
        allow_negative_numbers = false,
        value_name = "NETWORK_NAME:[CAPABILITIES]:URL",
        env = "ETHEREUM_RPC",
        help = "Ethereum network name (e.g. 'mainnet'), optional comma-separated capabilities (eg 'full,archive'), and an Ethereum RPC URL, separated by a ':'"
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
    #[clap(
        long,
        default_value = "8000",
        value_name = "PORT",
        help = "Port for the GraphQL HTTP server",
        env = "GRAPH_GRAPHQL_HTTP_PORT"
    )]
    pub http_port: u16,
    #[clap(
        long,
        default_value = "8030",
        value_name = "PORT",
        help = "Port for the index node server"
    )]
    pub index_node_port: u16,
    #[clap(
        long,
        default_value = "8020",
        value_name = "PORT",
        help = "Port for the JSON-RPC admin server"
    )]
    pub admin_port: u16,
    #[clap(
        long,
        default_value = "8040",
        value_name = "PORT",
        help = "Port for the Prometheus metrics server"
    )]
    pub metrics_port: u16,
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

    let mut opt = Opt::parse_from(args);

    opt.http_port = dev_opt.http_port;
    opt.admin_port = dev_opt.admin_port;
    opt.metrics_port = dev_opt.metrics_port;
    opt.index_node_port = dev_opt.index_node_port;

    Ok(opt)
}

async fn run_graph_node(
    logger: &Logger,
    opt: Opt,
    link_resolver: Arc<dyn LinkResolver>,
    subgraph_updates_channel: mpsc::Receiver<(DeploymentHash, SubgraphName)>,
    cancel_token: CancellationToken,
) -> Result<()> {
    let env_vars = Arc::new(EnvVars::from_env().context("Failed to load environment variables")?);

    let (prometheus_registry, metrics_registry) = launcher::setup_metrics(logger);

    let ipfs_client = graph::ipfs::new_ipfs_client(&opt.ipfs, &metrics_registry, logger)
        .await
        .unwrap_or_else(|err| panic!("Failed to create IPFS client: {err:#}"));

    let ipfs_service = ipfs_service(
        ipfs_client.cheap_clone(),
        env_vars.mappings.max_ipfs_file_bytes,
        env_vars.mappings.ipfs_timeout,
        env_vars.mappings.ipfs_request_limit,
    );

    launcher::run(
        logger.clone(),
        opt,
        env_vars,
        ipfs_service,
        link_resolver,
        Some(subgraph_updates_channel),
        prometheus_registry,
        metrics_registry,
        cancel_token,
    )
    .await;
    Ok(())
}

/// Get the database URL, either from the provided option or by creating a temporary database
fn get_database_url(
    postgres_url: Option<&String>,
    database_dir: &Path,
) -> Result<(String, Option<TempPgDB>)> {
    if let Some(url) = postgres_url {
        Ok((url.clone(), None))
    } else {
        #[cfg(unix)]
        {
            // Check the database directory exists
            if !database_dir.exists() {
                anyhow::bail!(
                    "Database directory does not exist: {}",
                    database_dir.display()
                );
            }

            let db = PgTempDBBuilder::new()
                .with_data_dir_prefix(database_dir)
                .persist_data(false)
                .with_initdb_arg("-E", "UTF8")
                .with_initdb_arg("--locale", "C")
                .start();
            let url = db.connection_uri().to_string();
            // Return the handle so it lives for the lifetime of the program; dropping it will
            // shut down Postgres and remove the temporary directory automatically.
            Ok((url, Some(db)))
        }

        #[cfg(not(unix))]
        {
            anyhow::bail!(
                "Please provide a postgres_url manually using the --postgres-url option."
            );
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("ETHEREUM_REORG_THRESHOLD", "10");
    std::env::set_var("GRAPH_NODE_DISABLE_DEPLOYMENT_HASH_VALIDATION", "true");
    env_logger::init();
    let dev_opt = DevOpt::parse();

    let database_dir = Path::new(&dev_opt.database_dir);

    let cancel_token = shutdown_token();
    let logger = logger(true);

    info!(logger, "Starting Graph Node Dev 1");
    info!(logger, "Database directory: {}", database_dir.display());

    // Get the database URL and keep the temporary database handle alive for the life of the
    // program so that it is dropped (and cleaned up) on graceful shutdown.
    let (db_url, mut temp_db_opt) = get_database_url(dev_opt.postgres_url.as_ref(), database_dir)?;

    let opt = build_args(&dev_opt, &db_url)?;

    let (manifests_paths, source_subgraph_aliases) =
        parse_manifest_args(dev_opt.manifests, dev_opt.sources, &logger)?;
    let file_link_resolver = Arc::new(FileLinkResolver::new(None, source_subgraph_aliases.clone()));

    let (tx, rx) = mpsc::channel(1);

    let logger_clone = logger.clone();
    graph::spawn(async move {
        let _ = run_graph_node(&logger_clone, opt, file_link_resolver, rx, cancel_token).await;
    });

    if let Err(e) =
        deploy_all_subgraphs(&logger, &manifests_paths, &source_subgraph_aliases, &tx).await
    {
        error!(logger, "Error deploying subgraphs"; "error" => e.to_string());
        std::process::exit(1);
    }

    if dev_opt.watch {
        let logger_clone_watch = logger.clone();
        graph::spawn_blocking(async move {
            if let Err(e) = watch_subgraphs(
                &logger_clone_watch,
                manifests_paths,
                source_subgraph_aliases,
                vec!["pgtemp-*".to_string()],
                tx,
            )
            .await
            {
                error!(logger_clone_watch, "Error watching subgraphs"; "error" => e.to_string());
                std::process::exit(1);
            }
        });
    }

    // Wait for Ctrl+C so we can shut down cleanly and drop the temporary database, which removes
    // the data directory.
    tokio::signal::ctrl_c()
        .await
        .expect("Failed to listen for Ctrl+C signal");
    info!(logger, "Received Ctrl+C, shutting down.");

    // Explicitly shut down and clean up the temporary database directory if we started one.
    #[cfg(unix)]
    if let Some(db) = temp_db_opt.take() {
        db.shutdown();
    }

    std::process::exit(0);

    #[allow(unreachable_code)]
    Ok(())
}

fn shutdown_token() -> CancellationToken {
    use tokio::signal;

    let cancel_token = CancellationToken::new();
    let cancel_token_clone = cancel_token.clone();

    async fn shutdown_signal_handler() {
        let ctrl_c = async {
            signal::ctrl_c().await.unwrap();
        };

        #[cfg(unix)]
        let terminate = async {
            signal::unix::signal(signal::unix::SignalKind::terminate())
                .unwrap()
                .recv()
                .await;
        };

        #[cfg(not(unix))]
        let terminate = std::future::pending::<()>();

        tokio::select! {
            _ = ctrl_c => {},
            _ = terminate => {},
        };
    }

    tokio::spawn(async move {
        shutdown_signal_handler().await;
        cancel_token_clone.cancel();
    });

    cancel_token
}
