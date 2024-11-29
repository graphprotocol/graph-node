use std::sync::Arc;
use std::time::Duration;

use graph::components::network_provider::ChainIdentifierStore;
use graph::components::network_provider::ChainName;
use graph::components::network_provider::ExtendedBlocksCheck;
use graph::components::network_provider::GenesisHashCheck;
use graph::components::network_provider::NetworkDetails;
use graph::components::network_provider::ProviderCheck;
use graph::components::network_provider::ProviderCheckStatus;
use graph::prelude::tokio;
use graph::prelude::Logger;
use graph_store_postgres::BlockStore;
use itertools::Itertools;

use crate::network_setup::Networks;

pub async fn execute(
    logger: &Logger,
    networks: &Networks,
    store: Arc<BlockStore>,
    timeout: Duration,
) {
    let chain_name_iter = networks
        .adapters
        .iter()
        .map(|a| a.chain_id())
        .sorted()
        .dedup();

    for chain_name in chain_name_iter {
        let mut errors = Vec::new();

        for adapter in networks
            .rpc_provider_manager
            .providers_unchecked(chain_name)
            .unique_by(|x| x.provider_name())
        {
            match tokio::time::timeout(
                timeout,
                run_checks(logger, chain_name, adapter, store.clone()),
            )
            .await
            {
                Ok(result) => {
                    errors.extend(result);
                }
                Err(_) => {
                    errors.push("Timeout".to_owned());
                }
            }
        }

        for adapter in networks
            .firehose_provider_manager
            .providers_unchecked(chain_name)
            .unique_by(|x| x.provider_name())
        {
            match tokio::time::timeout(
                timeout,
                run_checks(logger, chain_name, adapter, store.clone()),
            )
            .await
            {
                Ok(result) => {
                    errors.extend(result);
                }
                Err(_) => {
                    errors.push("Timeout".to_owned());
                }
            }
        }

        for adapter in networks
            .substreams_provider_manager
            .providers_unchecked(chain_name)
            .unique_by(|x| x.provider_name())
        {
            match tokio::time::timeout(
                timeout,
                run_checks(logger, chain_name, adapter, store.clone()),
            )
            .await
            {
                Ok(result) => {
                    errors.extend(result);
                }
                Err(_) => {
                    errors.push("Timeout".to_owned());
                }
            }
        }

        if errors.is_empty() {
            println!("Chain: {chain_name}; Status: OK");
            continue;
        }

        println!("Chain: {chain_name}; Status: ERROR");
        for error in errors.into_iter().unique() {
            println!("ERROR: {error}");
        }
    }
}

async fn run_checks(
    logger: &Logger,
    chain_name: &ChainName,
    adapter: &dyn NetworkDetails,
    store: Arc<dyn ChainIdentifierStore>,
) -> Vec<String> {
    let provider_name = adapter.provider_name();

    let mut errors = Vec::new();

    let genesis_check = GenesisHashCheck::new(store);

    let status = genesis_check
        .check(logger, chain_name, &provider_name, adapter)
        .await;

    errors_from_status(status, &mut errors);

    let blocks_check = ExtendedBlocksCheck::new([]);

    let status = blocks_check
        .check(logger, chain_name, &provider_name, adapter)
        .await;

    errors_from_status(status, &mut errors);

    errors
}

fn errors_from_status(status: ProviderCheckStatus, out: &mut Vec<String>) {
    match status {
        ProviderCheckStatus::NotChecked => {}
        ProviderCheckStatus::TemporaryFailure { message, .. } => {
            out.push(message);
        }
        ProviderCheckStatus::Valid => {}
        ProviderCheckStatus::Failed { message, .. } => {
            out.push(message);
        }
    }
}
