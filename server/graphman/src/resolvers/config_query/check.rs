use async_graphql::Result;
use clap::Parser;
use graph::{log::logger, prelude::error};
use graphman::{commands::config::check::check, config::Config, opt::Opt};

use crate::entities::ConfigCheckResponse;

pub fn run() -> Result<ConfigCheckResponse> {
    let config = fetch_config()?;
    let res = check(&config, true)?;

    Ok(ConfigCheckResponse::from(
        res.validated,
        res.validated_subgraph_settings,
        res.config_json.unwrap_or_default(),
    ))
}

pub fn fetch_config() -> Result<Config> {
    let args: Vec<String> = std::env::args().collect();
    let accepted_flags = vec![
        "--config",
        "--check-config",
        "--subgraph",
        "--start-block",
        "--postgres-url",
        "--postgres-secondary-hosts",
        "--postgres-host-weights",
        "--ethereum-rpc",
        "--ethereum-ws",
        "--ethereum-ipc",
        "--ipfs",
        "--arweave",
        "--http-port",
        "--index-node-port",
        "--ws-port",
        "--admin-port",
        "--metrics-port",
        "--node-id",
        "--expensive-queries-filename",
        "--debug",
        "--elasticsearch-url",
        "--elasticsearch-user",
        "--elasticsearch-password",
        "--disable-block-ingestor",
        "--store-connection-pool-size",
        "--unsafe-config",
        "--debug-fork",
        "--fork-base",
        "--graphman-port",
    ];
    let mut filtered_args: Vec<String> = vec![args[0].clone()];
    for (i, arg) in args.iter().enumerate() {
        if accepted_flags.contains(&arg.as_str()) {
            filtered_args.push(arg.clone());
            filtered_args.push(args[i + 1].clone());
        }
    }
    let opt = Opt::try_parse_from(filtered_args).expect("Failed to parse args");
    let logger = logger(opt.debug);
    match Config::load(&logger, &opt.clone().into()) {
        Ok(config) => Ok(config),
        Err(e) => {
            error!(logger, "Failed to load config due to: {}", e.to_string());
            return Err(e.into());
        }
    }
}
