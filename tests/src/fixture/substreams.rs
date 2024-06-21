use std::sync::Arc;

use graph::{blockchain::client::ChainClient, components::adapter::ChainId};

use super::{CommonChainConfig, Stores, TestChainSubstreams};

pub async fn chain(test_name: &str, stores: &Stores) -> TestChainSubstreams {
    let CommonChainConfig {
        logger_factory,
        mock_registry,
        chain_store,
        firehose_endpoints,
        ..
    } = CommonChainConfig::new(test_name, stores).await;

    let block_stream_builder = Arc::new(graph_chain_substreams::BlockStreamBuilder::new());
    let client = Arc::new(ChainClient::<graph_chain_substreams::Chain>::new_firehose(
        firehose_endpoints,
    ));

    let chain = Arc::new(graph_chain_substreams::Chain::new(
        logger_factory,
        client,
        mock_registry,
        chain_store,
        block_stream_builder.clone(),
        ChainId::from("test-chain"),
    ));

    TestChainSubstreams {
        chain,
        block_stream_builder,
    }
}
