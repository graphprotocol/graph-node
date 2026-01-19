use anyhow::Result;
use clap::{Parser, Subcommand};
use git_testament::{git_testament, render_testament};
use graph::{log::logger, slog::info};
use lazy_static::lazy_static;
use tokio_util::sync::CancellationToken;

use gnd::commands::{
    run_auth, run_clean, run_codegen, run_create, run_dev, run_remove, AuthOpt, CleanOpt,
    CodegenOpt, CreateOpt, DevOpt, RemoveOpt,
};

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
    Codegen(CodegenOpt),

    /// Compile subgraph to WASM
    Build,

    /// Deploy subgraph to a Graph Node
    Deploy,

    /// Scaffold a new subgraph project
    Init,

    /// Add a datasource to an existing subgraph
    Add,

    /// Register a subgraph name with a Graph Node
    Create(CreateOpt),

    /// Unregister a subgraph name from a Graph Node
    Remove(RemoveOpt),

    /// Set the deploy key for a Graph Node
    Auth(AuthOpt),

    /// Publish subgraph to The Graph's decentralized network
    Publish,

    /// Run Matchstick tests
    Test,

    /// Remove build artifacts and generated files
    Clean(CleanOpt),
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

    let res = match cli.command {
        Commands::Dev(dev_opt) => {
            run_dev(dev_opt, logger, cancel_token).await?;
            Ok(())
        }
        Commands::Codegen(codegen_opt) => run_codegen(codegen_opt),
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
        Commands::Create(create_opt) => run_create(create_opt).await,
        Commands::Remove(remove_opt) => run_remove(remove_opt).await,
        Commands::Auth(auth_opt) => run_auth(auth_opt),
        Commands::Publish => {
            info!(logger, "publish command not yet implemented");
            std::process::exit(1);
        }
        Commands::Test => {
            info!(logger, "test command not yet implemented");
            std::process::exit(1);
        }
        Commands::Clean(clean_opt) => run_clean(clean_opt),
    };

    if let Err(e) = res {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
