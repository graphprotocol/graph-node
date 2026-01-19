use anyhow::Result;
use clap::{Parser, Subcommand};
use git_testament::{git_testament, render_testament};
use graph::{log::logger, slog::info};
use lazy_static::lazy_static;
use tokio_util::sync::CancellationToken;

use gnd::commands::{run_dev, DevOpt};

git_testament!(TESTAMENT);
lazy_static! {
    static ref RENDERED_TESTAMENT: String = render_testament!(TESTAMENT);
    static ref VERSION_STRING: String = format!(
        "{} (graph-cli compatible: {})",
        RENDERED_TESTAMENT.as_str(),
        GRAPH_CLI_COMPAT_VERSION
    );
    static ref LONG_VERSION_STRING: String = format!(
        "{}\ngraph-cli compatibility version: {}",
        RENDERED_TESTAMENT.as_str(),
        GRAPH_CLI_COMPAT_VERSION
    );
}

/// The version of graph-cli that gnd emulates
const GRAPH_CLI_COMPAT_VERSION: &str = "0.98.1";

#[derive(Parser)]
#[clap(
    name = "gnd",
    about = "Graph Node Dev - A drop-in replacement for graph-cli",
    author = "Graph Protocol, Inc.",
    version = VERSION_STRING.as_str(),
    long_version = LONG_VERSION_STRING.as_str(),
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run graph-node in dev mode
    Dev(DevOpt),

    /// Generate AssemblyScript types from subgraph manifest
    Codegen,

    /// Compile subgraph to WASM
    Build,

    /// Deploy subgraph to a Graph Node
    Deploy,

    /// Scaffold a new subgraph project
    Init,

    /// Add a datasource to an existing subgraph
    Add,

    /// Register a subgraph name with a Graph Node
    Create,

    /// Unregister a subgraph name from a Graph Node
    Remove,

    /// Set the deploy key for a Graph Node
    Auth,

    /// Publish subgraph to The Graph's decentralized network
    Publish,

    /// Run Matchstick tests
    Test,

    /// Remove build artifacts and generated files
    Clean,
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

#[tokio::main]
async fn main() -> Result<()> {
    std::env::set_var("ETHEREUM_REORG_THRESHOLD", "10");
    std::env::set_var("GRAPH_NODE_DISABLE_DEPLOYMENT_HASH_VALIDATION", "true");
    env_logger::init();

    let cli = Cli::parse();
    let logger = logger(true);
    let cancel_token = shutdown_token();

    match cli.command {
        Commands::Dev(dev_opt) => {
            run_dev(dev_opt, logger, cancel_token).await?;
        }
        Commands::Codegen => {
            info!(logger, "codegen command not yet implemented");
            std::process::exit(1);
        }
        Commands::Build => {
            info!(logger, "build command not yet implemented");
            std::process::exit(1);
        }
        Commands::Deploy => {
            info!(logger, "deploy command not yet implemented");
            std::process::exit(1);
        }
        Commands::Init => {
            info!(logger, "init command not yet implemented");
            std::process::exit(1);
        }
        Commands::Add => {
            info!(logger, "add command not yet implemented");
            std::process::exit(1);
        }
        Commands::Create => {
            info!(logger, "create command not yet implemented");
            std::process::exit(1);
        }
        Commands::Remove => {
            info!(logger, "remove command not yet implemented");
            std::process::exit(1);
        }
        Commands::Auth => {
            info!(logger, "auth command not yet implemented");
            std::process::exit(1);
        }
        Commands::Publish => {
            info!(logger, "publish command not yet implemented");
            std::process::exit(1);
        }
        Commands::Test => {
            info!(logger, "test command not yet implemented");
            std::process::exit(1);
        }
        Commands::Clean => {
            info!(logger, "clean command not yet implemented");
            std::process::exit(1);
        }
    }

    Ok(())
}
