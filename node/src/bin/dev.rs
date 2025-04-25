use std::{path::Path, sync::Arc};

use anyhow::{Context, Result};
use clap::Parser;
use git_testament::{git_testament, render_testament};
use graph::{components::link_resolver::FileLinkResolver, env::EnvVars, tokio};
use graph_node::{launcher, opt::Opt};
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
        help = "The location of the subgraph manifest file.",
        default_value = "./build/subgraph.yaml"
    )]
    pub manifest: String,

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
fn build_args(dev_opt: &DevOpt, db_url: &str, manifest_path: &str) -> Result<Opt> {
    let mut args = vec!["gnd".to_string()];

    if !dev_opt.ipfs.is_empty() {
        args.push("--ipfs".to_string());
        args.push(dev_opt.ipfs.join(","));
    }

    if !dev_opt.ethereum_rpc.is_empty() {
        args.push("--ethereum-rpc".to_string());
        args.push(dev_opt.ethereum_rpc.join(","));
    }

    let path = Path::new(manifest_path);
    let file_name = path
        .file_name()
        .context("Invalid manifest path: no file name component")?
        .to_str()
        .context("Invalid file name")?;

    args.push("--subgraph".to_string());
    args.push(file_name.to_string());

    args.push("--postgres-url".to_string());
    args.push(db_url.to_string());

    let opt = Opt::parse_from(args);

    Ok(opt)
}

/// Validates the manifest file exists and returns the build directory
fn get_build_dir(manifest_path_str: &str) -> Result<std::path::PathBuf> {
    let manifest_path = Path::new(manifest_path_str);

    if !manifest_path.exists() {
        anyhow::bail!("Subgraph manifest file not found at {}", manifest_path_str);
    }

    let dir = manifest_path
        .parent()
        .context("Failed to get parent directory of manifest")?;

    dir.canonicalize()
        .context("Failed to canonicalize build directory path")
}

async fn run_graph_node(opt: Opt, file_link_resolver: Arc<FileLinkResolver>) -> Result<()> {
    let env_vars = Arc::new(EnvVars::from_env().context("Failed to load environment variables")?);

    launcher::run(opt, env_vars, Some(file_link_resolver)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let dev_opt = DevOpt::parse();

    let build_dir = get_build_dir(&dev_opt.manifest)?;

    let db = PgTempDBBuilder::new()
        .with_data_dir_prefix(build_dir.clone())
        .with_initdb_param("-E", "UTF8")
        .with_initdb_param("--locale", "C")
        .start_async()
        .await;

    let opt = build_args(&dev_opt, &db.connection_uri(), &dev_opt.manifest)?;
    let file_link_resolver = Arc::new(FileLinkResolver::with_base_dir(&build_dir));

    // Run graph node
    run_graph_node(opt, file_link_resolver).await?;

    Ok(())
}
