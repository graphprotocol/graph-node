use std::io;

use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand, ValueEnum};
use clap_complete::{generate, Shell};
use git_testament::{git_testament, render_testament};
use graph::log::logger;
use lazy_static::lazy_static;
use tokio_util::sync::CancellationToken;

use gnd::commands::{
    run_add, run_auth, run_build, run_clean, run_codegen, run_create, run_deploy, run_dev,
    run_init, run_publish, run_remove, run_test, AddOpt, AuthOpt, BuildOpt, CleanOpt, CodegenOpt,
    CreateOpt, DeployOpt, DevOpt, InitOpt, PublishOpt, RemoveOpt, TestOpt,
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
    Build(BuildOpt),

    /// Deploy subgraph to a Graph Node
    Deploy(DeployOpt),

    /// Scaffold a new subgraph project
    Init(InitOpt),

    /// Add a datasource to an existing subgraph
    Add(AddOpt),

    /// Register a subgraph name with a Graph Node
    Create(CreateOpt),

    /// Unregister a subgraph name from a Graph Node
    Remove(RemoveOpt),

    /// Set the deploy key for a Graph Node
    Auth(AuthOpt),

    /// Publish subgraph to The Graph's decentralized network
    Publish(PublishOpt),

    /// Run Matchstick tests
    Test(TestOpt),

    /// Remove build artifacts and generated files
    Clean(CleanOpt),

    /// Generate shell completions
    Completions(CompletionsOpt),
}

/// Options for shell completion generation.
#[derive(Parser, Debug)]
pub struct CompletionsOpt {
    /// Shell to generate completions for
    #[clap(value_enum)]
    pub shell: CompletionShell,
}

/// Supported shells for completion generation.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum CompletionShell {
    Bash,
    Elvish,
    Fish,
    PowerShell,
    Zsh,
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

fn generate_completions(completions_opt: CompletionsOpt) -> Result<()> {
    let shell = match completions_opt.shell {
        CompletionShell::Bash => Shell::Bash,
        CompletionShell::Elvish => Shell::Elvish,
        CompletionShell::Fish => Shell::Fish,
        CompletionShell::PowerShell => Shell::PowerShell,
        CompletionShell::Zsh => Shell::Zsh,
    };
    generate(shell, &mut Cli::command(), "gnd", &mut io::stdout());
    Ok(())
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
        Commands::Codegen(codegen_opt) => run_codegen(codegen_opt).await,
        Commands::Build(build_opt) => run_build(build_opt).await.map(|_| ()),
        Commands::Deploy(deploy_opt) => run_deploy(deploy_opt).await,
        Commands::Init(init_opt) => run_init(init_opt).await,
        Commands::Add(add_opt) => run_add(add_opt).await,
        Commands::Create(create_opt) => run_create(create_opt).await,
        Commands::Remove(remove_opt) => run_remove(remove_opt).await,
        Commands::Auth(auth_opt) => run_auth(auth_opt),
        Commands::Publish(publish_opt) => run_publish(publish_opt).await,
        Commands::Test(test_opt) => run_test(test_opt).await,
        Commands::Clean(clean_opt) => run_clean(clean_opt),
        Commands::Completions(completions_opt) => generate_completions(completions_opt),
    };

    if let Err(e) = res {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }

    Ok(())
}
