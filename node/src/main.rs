use clap::Parser as _;
use git_testament::git_testament;

use graph::env::EnvVars;
use graph::prelude::*;

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

    launcher::run(opt, env_vars, None).await;
}
