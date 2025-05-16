use clap::Parser as _;
use git_testament::git_testament;

use graph::prelude::*;
use graph::{env::EnvVars, log::logger};

use graph_core::polling_monitor::ipfs_service;
use graph_node::{launcher, opt};

git_testament!(TESTAMENT);

fn main() {
    let max_blocking: usize = std::env::var("GRAPH_MAX_BLOCKING_THREADS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(512);

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(max_blocking)
        .build()
        .unwrap()
        .block_on(async { main_inner().await })
}

async fn main_inner() {
    env_logger::init();
    let env_vars = Arc::new(EnvVars::from_env().unwrap());
    let opt = opt::Opt::parse();

    // Set up logger
    let logger = logger(opt.debug);

    let ipfs_client = graph::ipfs::new_ipfs_client(&opt.ipfs, &logger)
        .await
        .unwrap_or_else(|err| panic!("Failed to create IPFS client: {err:#}"));

    let ipfs_service = ipfs_service(
        ipfs_client.cheap_clone(),
        env_vars.mappings.max_ipfs_file_bytes,
        env_vars.mappings.ipfs_timeout,
        env_vars.mappings.ipfs_request_limit,
    );

    let link_resolver = Arc::new(IpfsResolver::new(ipfs_client, env_vars.cheap_clone()));

    launcher::run(logger, opt, env_vars, ipfs_service, link_resolver, None).await;
}
