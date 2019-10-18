use futures::sync::mpsc::{channel, Receiver, Sender};

use graph::prelude::*;

pub struct MockBlockStream {
    chain_head_update_sink: Sender<ChainHeadUpdate>,
    _chain_head_update_stream: Receiver<ChainHeadUpdate>,
}

impl MockBlockStream {
    fn new() -> Self {
        let (chain_head_update_sink, chain_head_update_stream) = channel(100);

        Self {
            chain_head_update_sink,
            _chain_head_update_stream: chain_head_update_stream,
        }
    }
}

impl Stream for MockBlockStream {
    type Item = EthereumBlockWithTriggers;
    type Error = Error;

    fn poll(&mut self) -> Result<Async<Option<EthereumBlockWithTriggers>>, Error> {
        Ok(Async::Ready(None))
    }
}

impl EventConsumer<ChainHeadUpdate> for MockBlockStream {
    fn event_sink(&self) -> Box<dyn Sink<SinkItem = ChainHeadUpdate, SinkError = ()> + Send> {
        Box::new(self.chain_head_update_sink.clone().sink_map_err(|_| ()))
    }
}

impl BlockStream for MockBlockStream {
    fn triggers_in_block(
        &self,
        _: EthereumLogFilter,
        _: EthereumCallFilter,
        _: EthereumBlockFilter,
        _descendant_block: BlockFinality,
    ) -> Box<dyn Future<Item = EthereumBlockWithTriggers, Error = Error> + Send> {
        unimplemented!()
    }
}

#[derive(Clone)]
pub struct MockBlockStreamBuilder;

impl MockBlockStreamBuilder {
    pub fn new() -> Self {
        Self {}
    }
}

impl BlockStreamBuilder for MockBlockStreamBuilder {
    type Stream = MockBlockStream;

    fn build(
        &self,
        _logger: Logger,
        _deployment_id: SubgraphDeploymentId,
        _network_name: String,
        _start_blocks: Vec<u64>,
        _: EthereumLogFilter,
        _: EthereumCallFilter,
        _: EthereumBlockFilter,
        _include_calls_in_blocks: bool,
        ethrpc_metrics: Arc<SubgraphEthRpcMetrics>,
    ) -> Self::Stream {
        MockBlockStream::new()
    }
}
