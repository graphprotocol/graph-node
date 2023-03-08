use crate::{
    blockchain::block_stream::FirehoseCursor,
    blockchain::Block as BlockchainBlock,
    blockchain::BlockPtr,
    cheap_clone::CheapClone,
    components::store::BlockNumber,
    endpoint::{EndpointMetrics, Host},
    firehose::decode_firehose_block,
    prelude::{anyhow, debug, info},
    substreams,
};

use crate::firehose::fetch_client::FetchClient;
use crate::firehose::interceptors::AuthInterceptor;
use anyhow::bail;
use futures03::StreamExt;
use http::uri::{Scheme, Uri};
use slog::Logger;
use std::{collections::BTreeMap, fmt::Display, sync::Arc, time::Duration};
use tonic::codegen::InterceptedService;
use tonic::{
    codegen::CompressionEncoding,
    metadata::{Ascii, MetadataValue},
    transport::{Channel, ClientTlsConfig},
};

use super::{codec as firehose, interceptors::MetricsInterceptor, stream_client::StreamClient};

/// This is constant because we found this magic number of connections after
/// which the grpc connections start to hang.
/// For more details see: https://github.com/graphprotocol/graph-node/issues/3879
pub const SUBGRAPHS_PER_CONN: usize = 100;

#[derive(Debug)]
pub struct FirehoseEndpoint {
    pub provider: String,
    pub host: Host,
    pub auth: AuthInterceptor,
    pub filters_enabled: bool,
    pub compression_enabled: bool,
    pub subgraph_limit: SubgraphLimit,
    endpoint_metrics: Arc<EndpointMetrics>,
    channel: Channel,
}

// TODO: Find a new home for this type.
#[derive(Clone, Debug, PartialEq, Ord, Eq, PartialOrd)]
pub enum SubgraphLimit {
    Disabled,
    Limit(usize),
    Unlimited,
}

impl SubgraphLimit {
    pub fn has_capacity(&self, current: usize) -> bool {
        match self {
            SubgraphLimit::Unlimited => true,
            SubgraphLimit::Limit(limit) => limit > &current,
            SubgraphLimit::Disabled => false,
        }
    }
}

impl Display for FirehoseEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.provider.as_str(), f)
    }
}

impl FirehoseEndpoint {
    pub fn new<S: AsRef<str>>(
        provider: S,
        url: S,
        token: Option<String>,
        filters_enabled: bool,
        compression_enabled: bool,
        subgraph_limit: SubgraphLimit,
        endpoint_metrics: Arc<EndpointMetrics>,
    ) -> Self {
        let uri = url
            .as_ref()
            .parse::<Uri>()
            .expect("the url should have been validated by now, so it is a valid Uri");
        let host = Host::from(uri.to_string());

        let endpoint_builder = match uri.scheme().unwrap_or(&Scheme::HTTP).as_str() {
            "http" => Channel::builder(uri),
            "https" => Channel::builder(uri)
                .tls_config(ClientTlsConfig::new())
                .expect("TLS config on this host is invalid"),
            _ => panic!("invalid uri scheme for firehose endpoint"),
        };

        // These tokens come from the config so they have to be ascii.
        let token: Option<MetadataValue<Ascii>> = token
            .map_or(Ok(None), |token| {
                token.parse::<MetadataValue<Ascii>>().map(Some)
            })
            .expect("Firehose token is invalid");

        // Note on the connection window size: We run multiple block streams on a same connection,
        // and a problematic subgraph with a stalled block stream might consume the entire window
        // capacity for its http2 stream and never release it. If there are enough stalled block
        // streams to consume all the capacity on the http2 connection, then _all_ subgraphs using
        // this same http2 connection will stall. At a default stream window size of 2^16, setting
        // the connection window size to the maximum of 2^31 allows for 2^15 streams without any
        // contention, which is effectively unlimited for normal graph node operation.
        //
        // Note: Do not set `http2_keep_alive_interval` or `http2_adaptive_window`, as these will
        // send ping frames, and many cloud load balancers will drop connections that frequently
        // send pings.
        let endpoint = endpoint_builder
            .initial_connection_window_size(Some((1 << 31) - 1))
            .connect_timeout(Duration::from_secs(10))
            .tcp_keepalive(Some(Duration::from_secs(15)))
            // Timeout on each request, so the timeout to estabilish each 'Blocks' stream.
            .timeout(Duration::from_secs(120));

        let subgraph_limit = match subgraph_limit {
            // See the comment on the constant
            SubgraphLimit::Unlimited => SubgraphLimit::Limit(SUBGRAPHS_PER_CONN),
            // This is checked when parsing from config but doesn't hurt to be defensive.
            SubgraphLimit::Limit(limit) => SubgraphLimit::Limit(limit.min(SUBGRAPHS_PER_CONN)),
            l => l,
        };

        FirehoseEndpoint {
            provider: provider.as_ref().to_string(),
            channel: endpoint.connect_lazy(),
            auth: AuthInterceptor { token },
            filters_enabled,
            compression_enabled,
            subgraph_limit,
            endpoint_metrics,
            host,
        }
    }

    // we need to -1 because there will always be a reference
    // inside FirehoseEndpoints that is not used (is always cloned).
    pub fn has_subgraph_capacity(self: &Arc<Self>) -> bool {
        self.subgraph_limit
            .has_capacity(Arc::strong_count(self).saturating_sub(1))
    }

    fn new_client(
        &self,
    ) -> FetchClient<
        InterceptedService<MetricsInterceptor<Channel>, impl tonic::service::Interceptor>,
    > {
        let metrics = MetricsInterceptor {
            metrics: self.endpoint_metrics.cheap_clone(),
            service: self.channel.cheap_clone(),
            host: self.host.clone(),
        };

        let mut client: FetchClient<
            InterceptedService<MetricsInterceptor<Channel>, AuthInterceptor>,
        > = FetchClient::with_interceptor(metrics, self.auth.clone())
            .accept_compressed(CompressionEncoding::Gzip);

        if self.compression_enabled {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }

        client
    }

    fn new_stream_client(
        &self,
    ) -> StreamClient<
        InterceptedService<MetricsInterceptor<Channel>, impl tonic::service::Interceptor>,
    > {
        let metrics = MetricsInterceptor {
            metrics: self.endpoint_metrics.cheap_clone(),
            service: self.channel.cheap_clone(),
            host: self.host.clone(),
        };

        let mut client = StreamClient::with_interceptor(metrics, self.auth.clone())
            .accept_compressed(CompressionEncoding::Gzip);

        if self.compression_enabled {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }

        client
    }

    fn new_substreams_client(
        &self,
    ) -> substreams::stream_client::StreamClient<
        InterceptedService<MetricsInterceptor<Channel>, impl tonic::service::Interceptor>,
    > {
        let metrics = MetricsInterceptor {
            metrics: self.endpoint_metrics.cheap_clone(),
            service: self.channel.cheap_clone(),
            host: self.host.clone(),
        };

        let mut client =
            substreams::stream_client::StreamClient::with_interceptor(metrics, self.auth.clone())
                .accept_compressed(CompressionEncoding::Gzip);

        if self.compression_enabled {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }

        client
    }

    pub async fn get_block<M>(
        &self,
        cursor: FirehoseCursor,
        logger: &Logger,
    ) -> Result<M, anyhow::Error>
    where
        M: prost::Message + BlockchainBlock + Default + 'static,
    {
        debug!(
            logger,
            "Connecting to firehose to retrieve block for cursor {}", cursor
        );

        let req = firehose::SingleBlockRequest {
            transforms: [].to_vec(),
            reference: Some(firehose::single_block_request::Reference::Cursor(
                firehose::single_block_request::Cursor {
                    cursor: cursor.to_string(),
                },
            )),
        };

        let mut client = self.new_client();
        match client.block(req).await {
            Ok(v) => Ok(M::decode(
                v.get_ref().block.as_ref().unwrap().value.as_ref(),
            )?),
            Err(e) => return Err(anyhow::format_err!("firehose error {}", e)),
        }
    }

    pub async fn genesis_block_ptr<M>(&self, logger: &Logger) -> Result<BlockPtr, anyhow::Error>
    where
        M: prost::Message + BlockchainBlock + Default + 'static,
    {
        info!(logger, "Requesting genesis block from firehose");

        // We use 0 here to mean the genesis block of the chain. Firehose
        // when seeing start block number 0 will always return the genesis
        // block of the chain, even if the chain's start block number is
        // not starting at block #0.
        self.block_ptr_for_number::<M>(logger, 0).await
    }

    pub async fn block_ptr_for_number<M>(
        &self,
        logger: &Logger,
        number: BlockNumber,
    ) -> Result<BlockPtr, anyhow::Error>
    where
        M: prost::Message + BlockchainBlock + Default + 'static,
    {
        debug!(
            logger,
            "Connecting to firehose to retrieve block for number {}", number
        );

        let mut client = self.new_stream_client();

        // The trick is the following.
        //
        // Firehose `start_block_num` and `stop_block_num` are both inclusive, so we specify
        // the block we are looking for in both.
        //
        // Now, the remaining question is how the block from the canonical chain is picked. We
        // leverage the fact that Firehose will always send the block in the longuest chain as the
        // last message of this request.
        //
        // That way, we either get the final block if the block is now in a final segment of the
        // chain (or probabilisticly if not finality concept exists for the chain). Or we get the
        // block that is in the longuest chain according to Firehose.
        let response_stream = client
            .blocks(firehose::Request {
                start_block_num: number as i64,
                stop_block_num: number as u64,
                final_blocks_only: false,
                ..Default::default()
            })
            .await?;

        let mut block_stream = response_stream.into_inner();

        debug!(logger, "Retrieving block(s) from firehose");

        let mut latest_received_block: Option<BlockPtr> = None;
        while let Some(message) = block_stream.next().await {
            match message {
                Ok(v) => {
                    let block = decode_firehose_block::<M>(&v)?.ptr();

                    match latest_received_block {
                        None => {
                            latest_received_block = Some(block);
                        }
                        Some(ref actual_ptr) => {
                            // We want to receive all events related to a specific block number,
                            // however, in some circumstances, it seems Firehose would not stop sending
                            // blocks (`start_block_num: 0 and stop_block_num: 0` on NEAR seems to trigger
                            // this).
                            //
                            // To prevent looping infinitely, we stop as soon as a new received block's
                            // number is higher than the latest received block's number, in which case it
                            // means it's an event for a block we are not interested in.
                            if block.number > actual_ptr.number {
                                break;
                            }

                            latest_received_block = Some(block);
                        }
                    }
                }
                Err(e) => return Err(anyhow::format_err!("firehose error {}", e)),
            };
        }

        match latest_received_block {
            Some(block_ptr) => Ok(block_ptr),
            None => Err(anyhow::format_err!(
                "Firehose should have returned at least one block for request"
            )),
        }
    }

    pub async fn stream_blocks(
        self: Arc<Self>,
        request: firehose::Request,
    ) -> Result<tonic::Streaming<firehose::Response>, anyhow::Error> {
        let mut client = self.new_stream_client();
        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }

    pub async fn substreams(
        self: Arc<Self>,
        request: substreams::Request,
    ) -> Result<tonic::Streaming<substreams::Response>, anyhow::Error> {
        let mut client = self.new_substreams_client();
        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }
}

#[derive(Clone, Debug)]
pub struct FirehoseEndpoints(Vec<Arc<FirehoseEndpoint>>);

impl FirehoseEndpoints {
    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    // selects the FirehoseEndpoint with the least amount of references, which will help with spliting
    // the load naively across the entire list.
    pub fn random(&self) -> anyhow::Result<Arc<FirehoseEndpoint>> {
        let endpoint = self
            .0
            .iter()
            .min_by_key(|x| Arc::strong_count(x))
            .ok_or(anyhow!("no available firehose endpoints"))?;
        if !endpoint.has_subgraph_capacity() {
            bail!("all connections saturated with {} connections, increase the firehose conn_pool_size or limit for the node", SUBGRAPHS_PER_CONN);
        }

        // Cloning here ensure we have the correct count at any given time, if we return a reference it can be cloned later
        // which could cause a high number of endpoints to be given away before accounting for them.
        Ok(endpoint.clone())
    }

    pub fn remove(&mut self, provider: &str) {
        self.0
            .retain(|network_endpoint| network_endpoint.provider != provider);
    }
}

impl From<Vec<Arc<FirehoseEndpoint>>> for FirehoseEndpoints {
    fn from(val: Vec<Arc<FirehoseEndpoint>>) -> Self {
        FirehoseEndpoints(val)
    }
}

#[derive(Clone, Debug)]
pub struct FirehoseNetworks {
    /// networks contains a map from chain id (`near-mainnet`, `near-testnet`, `solana-mainnet`, etc.)
    /// to a list of FirehoseEndpoint (type wrapper around `Arc<Vec<FirehoseEndpoint>>`).
    pub networks: BTreeMap<String, FirehoseEndpoints>,
}

impl FirehoseNetworks {
    pub fn new() -> FirehoseNetworks {
        FirehoseNetworks {
            networks: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, chain_id: String, endpoint: Arc<FirehoseEndpoint>) {
        let endpoints = self
            .networks
            .entry(chain_id)
            .or_insert_with(FirehoseEndpoints::new);

        endpoints.0.push(endpoint);
    }

    pub fn remove(&mut self, chain_id: &str, provider: &str) {
        if let Some(endpoints) = self.networks.get_mut(chain_id) {
            endpoints.remove(provider);
        }
    }

    /// Returns a `Vec` of tuples where the first element of the tuple is
    /// the chain's id and the second one is an endpoint for this chain.
    /// There can be mulitple tuple with the same chain id but with different
    /// endpoint where multiple providers exist for a single chain id.
    pub fn flatten(&self) -> Vec<(String, Arc<FirehoseEndpoint>)> {
        self.networks
            .iter()
            .flat_map(|(chain_id, firehose_endpoints)| {
                firehose_endpoints
                    .0
                    .iter()
                    .map(move |endpoint| (chain_id.clone(), endpoint.clone()))
            })
            .collect()
    }
}

#[cfg(test)]
mod test {
    use std::{mem, sync::Arc};

    use crate::{endpoint::EndpointMetrics, firehose::SubgraphLimit};

    use super::{FirehoseEndpoint, FirehoseEndpoints, SUBGRAPHS_PER_CONN};

    #[tokio::test]
    async fn firehose_endpoint_errors() {
        let endpoint = vec![Arc::new(FirehoseEndpoint::new(
            String::new(),
            "http://127.0.0.1".to_string(),
            None,
            false,
            false,
            SubgraphLimit::Unlimited,
            Arc::new(EndpointMetrics::noop()),
        ))];

        let mut endpoints = FirehoseEndpoints::from(endpoint);

        let mut keep = vec![];
        for _i in 0..SUBGRAPHS_PER_CONN {
            keep.push(endpoints.random().unwrap());
        }

        let err = endpoints.random().unwrap_err();
        assert!(err.to_string().contains("conn_pool_size"));

        mem::drop(keep);
        endpoints.random().unwrap();

        // Fails when empty too
        endpoints.remove("");

        let err = endpoints.random().unwrap_err();
        assert!(err.to_string().contains("no available firehose endpoints"));
    }

    #[tokio::test]
    async fn firehose_endpoint_with_limit() {
        let endpoint = vec![Arc::new(FirehoseEndpoint::new(
            String::new(),
            "http://127.0.0.1".to_string(),
            None,
            false,
            false,
            SubgraphLimit::Limit(2),
            Arc::new(EndpointMetrics::noop()),
        ))];

        let mut endpoints = FirehoseEndpoints::from(endpoint);

        let mut keep = vec![];
        for _ in 0..2 {
            keep.push(endpoints.random().unwrap());
        }

        let err = endpoints.random().unwrap_err();
        assert!(err.to_string().contains("conn_pool_size"));

        mem::drop(keep);
        endpoints.random().unwrap();

        // Fails when empty too
        endpoints.remove("");

        let err = endpoints.random().unwrap_err();
        assert!(err.to_string().contains("no available firehose endpoints"));
    }

    #[tokio::test]
    async fn firehose_endpoint_no_traffic() {
        let endpoint = vec![Arc::new(FirehoseEndpoint::new(
            String::new(),
            "http://127.0.0.1".to_string(),
            None,
            false,
            false,
            SubgraphLimit::Disabled,
            Arc::new(EndpointMetrics::noop()),
        ))];

        let mut endpoints = FirehoseEndpoints::from(endpoint);

        let err = endpoints.random().unwrap_err();
        assert!(err.to_string().contains("conn_pool_size"));

        // Fails when empty too
        endpoints.remove("");

        let err = endpoints.random().unwrap_err();
        assert!(err.to_string().contains("no available firehose endpoints"));
    }
}
