use crate::firehose::fetch_client::FetchClient;
use crate::firehose::interceptors::AuthInterceptor;
use crate::{
    blockchain::{
        block_stream::FirehoseCursor, Block as BlockchainBlock, BlockPtr, ChainIdentifier,
    },
    cheap_clone::CheapClone,
    components::store::BlockNumber,
    endpoint::{ConnectionType, EndpointMetrics, RequestLabels},
    env::ENV_VARS,
    firehose::decode_firehose_block,
    prelude::{anyhow, debug, DeploymentHash},
    substreams_rpc,
};
use async_trait::async_trait;
use futures03::StreamExt;
use http0::uri::{Scheme, Uri};
use itertools::Itertools;
use slog::Logger;
use std::{collections::HashMap, fmt::Display, ops::ControlFlow, sync::Arc, time::Duration};
use tokio::sync::OnceCell;
use tonic::codegen::InterceptedService;
use tonic::{
    codegen::CompressionEncoding,
    metadata::{Ascii, MetadataKey, MetadataValue},
    transport::{Channel, ClientTlsConfig},
    Request,
};

use super::{codec as firehose, interceptors::MetricsInterceptor, stream_client::StreamClient};
use crate::components::network_provider::ChainName;
use crate::components::network_provider::NetworkDetails;
use crate::components::network_provider::ProviderCheckStrategy;
use crate::components::network_provider::ProviderManager;
use crate::components::network_provider::ProviderName;

/// This is constant because we found this magic number of connections after
/// which the grpc connections start to hang.
/// For more details see: https://github.com/graphprotocol/graph-node/issues/3879
pub const SUBGRAPHS_PER_CONN: usize = 100;

const LOW_VALUE_THRESHOLD: usize = 10;
const LOW_VALUE_USED_PERCENTAGE: usize = 50;
const HIGH_VALUE_USED_PERCENTAGE: usize = 80;

#[derive(Debug)]
pub struct FirehoseEndpoint {
    pub provider: ProviderName,
    pub auth: AuthInterceptor,
    pub filters_enabled: bool,
    pub compression_enabled: bool,
    pub subgraph_limit: SubgraphLimit,
    endpoint_metrics: Arc<EndpointMetrics>,
    channel: Channel,

    /// The endpoint info is not intended to change very often, as it only contains the
    /// endpoint's metadata, so caching it avoids sending unnecessary network requests.
    info_response: OnceCell<crate::firehose::endpoint_info::InfoResponse>,
}

#[derive(Debug)]
pub struct ConnectionHeaders(HashMap<MetadataKey<Ascii>, MetadataValue<Ascii>>);

#[async_trait]
impl NetworkDetails for Arc<FirehoseEndpoint> {
    fn provider_name(&self) -> ProviderName {
        self.provider.clone()
    }

    async fn chain_identifier(&self) -> anyhow::Result<ChainIdentifier> {
        let genesis_block_ptr = self.clone().info().await?.genesis_block_ptr()?;

        Ok(ChainIdentifier {
            net_version: "0".to_string(),
            genesis_block_hash: genesis_block_ptr.hash,
        })
    }

    async fn provides_extended_blocks(&self) -> anyhow::Result<bool> {
        let info = self.clone().info().await?;

        Ok(info.block_features.iter().all(|x| x == "extended"))
    }
}

impl ConnectionHeaders {
    pub fn new() -> Self {
        Self(HashMap::new())
    }
    pub fn with_deployment(mut self, deployment: DeploymentHash) -> Self {
        if let Ok(deployment) = deployment.parse() {
            self.0
                .insert("x-deployment-id".parse().unwrap(), deployment);
        }
        self
    }
    pub fn add_to_request<T>(&self, request: T) -> Request<T> {
        let mut request = Request::new(request);
        self.0.iter().for_each(|(k, v)| {
            request.metadata_mut().insert(k, v.clone());
        });
        request
    }
}

#[derive(Clone, Debug, PartialEq, Ord, Eq, PartialOrd)]
pub enum AvailableCapacity {
    Unavailable,
    Low,
    High,
}

// TODO: Find a new home for this type.
#[derive(Clone, Debug, PartialEq, Ord, Eq, PartialOrd)]
pub enum SubgraphLimit {
    Disabled,
    Limit(usize),
    Unlimited,
}

impl SubgraphLimit {
    pub fn get_capacity(&self, current: usize) -> AvailableCapacity {
        match self {
            // Limit(0) should probably be Disabled but just in case
            SubgraphLimit::Disabled | SubgraphLimit::Limit(0) => AvailableCapacity::Unavailable,
            SubgraphLimit::Limit(total) => {
                let total = *total;
                if current >= total {
                    return AvailableCapacity::Unavailable;
                }

                let used_percent = current * 100 / total;

                // If total is low it can vary very quickly so we can consider 50% as the low threshold
                // to make selection more reliable
                let threshold_percent = if total <= LOW_VALUE_THRESHOLD {
                    LOW_VALUE_USED_PERCENTAGE
                } else {
                    HIGH_VALUE_USED_PERCENTAGE
                };

                if used_percent < threshold_percent {
                    return AvailableCapacity::High;
                }

                AvailableCapacity::Low
            }
            _ => AvailableCapacity::High,
        }
    }

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
        key: Option<String>,
        filters_enabled: bool,
        compression_enabled: bool,
        subgraph_limit: SubgraphLimit,
        endpoint_metrics: Arc<EndpointMetrics>,
    ) -> Self {
        let uri = url
            .as_ref()
            .parse::<Uri>()
            .expect("the url should have been validated by now, so it is a valid Uri");

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
                let bearer_token = format!("bearer {}", token);
                bearer_token.parse::<MetadataValue<Ascii>>().map(Some)
            })
            .expect("Firehose token is invalid");

        let key: Option<MetadataValue<Ascii>> = key
            .map_or(Ok(None), |key| {
                key.parse::<MetadataValue<Ascii>>().map(Some)
            })
            .expect("Firehose key is invalid");

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
            provider: provider.as_ref().into(),
            channel: endpoint.connect_lazy(),
            auth: AuthInterceptor { token, key },
            filters_enabled,
            compression_enabled,
            subgraph_limit,
            endpoint_metrics,
            info_response: OnceCell::new(),
        }
    }

    pub fn current_error_count(&self) -> u64 {
        self.endpoint_metrics.get_count(&self.provider)
    }

    // we need to -1 because there will always be a reference
    // inside FirehoseEndpoints that is not used (is always cloned).
    pub fn get_capacity(self: &Arc<Self>) -> AvailableCapacity {
        self.subgraph_limit
            .get_capacity(Arc::strong_count(self).saturating_sub(1))
    }

    fn metrics_interceptor(&self) -> MetricsInterceptor<Channel> {
        MetricsInterceptor {
            metrics: self.endpoint_metrics.cheap_clone(),
            service: self.channel.cheap_clone(),
            labels: RequestLabels {
                provider: self.provider.clone().into(),
                req_type: "unknown".into(),
                conn_type: ConnectionType::Firehose,
            },
        }
    }

    fn max_message_size(&self) -> usize {
        1024 * 1024 * ENV_VARS.firehose_grpc_max_decode_size_mb
    }

    fn new_fetch_client(
        &self,
    ) -> FetchClient<
        InterceptedService<MetricsInterceptor<Channel>, impl tonic::service::Interceptor>,
    > {
        let metrics = self.metrics_interceptor();

        let mut client = FetchClient::with_interceptor(metrics, self.auth.clone())
            .accept_compressed(CompressionEncoding::Gzip);

        if self.compression_enabled {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }

        client = client.max_decoding_message_size(self.max_message_size());

        client
    }

    fn new_stream_client(
        &self,
    ) -> StreamClient<
        InterceptedService<MetricsInterceptor<Channel>, impl tonic::service::Interceptor>,
    > {
        let metrics = self.metrics_interceptor();

        let mut client = StreamClient::with_interceptor(metrics, self.auth.clone())
            .accept_compressed(CompressionEncoding::Gzip);

        if self.compression_enabled {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }

        client = client.max_decoding_message_size(self.max_message_size());

        client
    }

    fn new_substreams_client(
        &self,
    ) -> substreams_rpc::stream_client::StreamClient<
        InterceptedService<MetricsInterceptor<Channel>, impl tonic::service::Interceptor>,
    > {
        let metrics = self.metrics_interceptor();

        let mut client = substreams_rpc::stream_client::StreamClient::with_interceptor(
            metrics,
            self.auth.clone(),
        )
        .accept_compressed(CompressionEncoding::Gzip);

        if self.compression_enabled {
            client = client.send_compressed(CompressionEncoding::Gzip);
        }

        client = client.max_decoding_message_size(self.max_message_size());

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
            "Connecting to firehose to retrieve block for cursor {}", cursor;
            "provider" => self.provider.as_str(),
        );

        let req = firehose::SingleBlockRequest {
            transforms: [].to_vec(),
            reference: Some(firehose::single_block_request::Reference::Cursor(
                firehose::single_block_request::Cursor {
                    cursor: cursor.to_string(),
                },
            )),
        };

        let mut client = self.new_fetch_client();
        match client.block(req).await {
            Ok(v) => Ok(M::decode(
                v.get_ref().block.as_ref().unwrap().value.as_ref(),
            )?),
            Err(e) => return Err(anyhow::format_err!("firehose error {}", e)),
        }
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
            "Connecting to firehose to retrieve block for number {}", number;
            "provider" => self.provider.as_str(),
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

        debug!(logger, "Retrieving block(s) from firehose";
               "provider" => self.provider.as_str());

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
        headers: &ConnectionHeaders,
    ) -> Result<tonic::Streaming<firehose::Response>, anyhow::Error> {
        let mut client = self.new_stream_client();
        let request = headers.add_to_request(request);
        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }

    pub async fn substreams(
        self: Arc<Self>,
        request: substreams_rpc::Request,
        headers: &ConnectionHeaders,
    ) -> Result<tonic::Streaming<substreams_rpc::Response>, anyhow::Error> {
        let mut client = self.new_substreams_client();
        let request = headers.add_to_request(request);
        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }

    pub async fn info(
        self: Arc<Self>,
    ) -> Result<crate::firehose::endpoint_info::InfoResponse, anyhow::Error> {
        let endpoint = self.cheap_clone();

        self.info_response
            .get_or_try_init(move || async move {
                let metrics = endpoint.metrics_interceptor();
                let auth = endpoint.auth.clone();

                let mut client = crate::firehose::endpoint_info::Client::new(metrics, auth);

                if endpoint.compression_enabled {
                    client = client.with_compression();
                }

                client = client.with_max_message_size(endpoint.max_message_size());

                client.info().await
            })
            .await
            .map(ToOwned::to_owned)
    }
}

#[derive(Debug)]
pub struct FirehoseEndpoints(ChainName, ProviderManager<Arc<FirehoseEndpoint>>);

impl FirehoseEndpoints {
    pub fn for_testing(adapters: Vec<Arc<FirehoseEndpoint>>) -> Self {
        let chain_name: ChainName = "testing".into();

        Self(
            chain_name.clone(),
            ProviderManager::new(
                crate::log::discard(),
                [(chain_name, adapters)],
                ProviderCheckStrategy::MarkAsValid,
            ),
        )
    }

    pub fn new(
        chain_name: ChainName,
        provider_manager: ProviderManager<Arc<FirehoseEndpoint>>,
    ) -> Self {
        Self(chain_name, provider_manager)
    }

    pub fn len(&self) -> usize {
        self.1.len(&self.0)
    }

    /// This function will attempt to grab an endpoint based on the Lowest error count
    //  with high capacity available. If an adapter cannot be found `endpoint` will
    // return an error.
    pub async fn endpoint(&self) -> anyhow::Result<Arc<FirehoseEndpoint>> {
        let endpoint = self
            .1
            .providers(&self.0)
            .await?
            .sorted_by_key(|x| x.current_error_count())
            .try_fold(None, |acc, adapter| {
                match adapter.get_capacity() {
                    AvailableCapacity::Unavailable => ControlFlow::Continue(acc),
                    AvailableCapacity::Low => match acc {
                        Some(_) => ControlFlow::Continue(acc),
                        None => ControlFlow::Continue(Some(adapter)),
                    },
                    // This means that if all adapters with low/no errors are low capacity
                    // we will retry the high capacity that has errors, at this point
                    // any other available with no errors are almost at their limit.
                    AvailableCapacity::High => ControlFlow::Break(Some(adapter)),
                }
            });

        match endpoint {
            ControlFlow::Continue(adapter)
            | ControlFlow::Break(adapter) =>
            adapter.cloned().ok_or(anyhow!("unable to get a connection, increase the firehose conn_pool_size or limit for the node"))
        }
    }
}

#[cfg(test)]
mod test {
    use std::{mem, sync::Arc};

    use slog::{o, Discard, Logger};

    use super::*;
    use crate::components::metrics::MetricsRegistry;
    use crate::endpoint::EndpointMetrics;
    use crate::firehose::SubgraphLimit;

    #[tokio::test]
    async fn firehose_endpoint_errors() {
        let endpoint = vec![Arc::new(FirehoseEndpoint::new(
            String::new(),
            "http://127.0.0.1".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Unlimited,
            Arc::new(EndpointMetrics::mock()),
        ))];

        let endpoints = FirehoseEndpoints::for_testing(endpoint);

        let mut keep = vec![];
        for _i in 0..SUBGRAPHS_PER_CONN {
            keep.push(endpoints.endpoint().await.unwrap());
        }

        let err = endpoints.endpoint().await.unwrap_err();
        assert!(err.to_string().contains("conn_pool_size"));

        mem::drop(keep);
        endpoints.endpoint().await.unwrap();

        let endpoints = FirehoseEndpoints::for_testing(vec![]);

        let err = endpoints.endpoint().await.unwrap_err();
        assert!(err.to_string().contains("unable to get a connection"));
    }

    #[tokio::test]
    async fn firehose_endpoint_with_limit() {
        let endpoint = vec![Arc::new(FirehoseEndpoint::new(
            String::new(),
            "http://127.0.0.1".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Limit(2),
            Arc::new(EndpointMetrics::mock()),
        ))];

        let endpoints = FirehoseEndpoints::for_testing(endpoint);

        let mut keep = vec![];
        for _ in 0..2 {
            keep.push(endpoints.endpoint().await.unwrap());
        }

        let err = endpoints.endpoint().await.unwrap_err();
        assert!(err.to_string().contains("conn_pool_size"));

        mem::drop(keep);
        endpoints.endpoint().await.unwrap();
    }

    #[tokio::test]
    async fn firehose_endpoint_no_traffic() {
        let endpoint = vec![Arc::new(FirehoseEndpoint::new(
            String::new(),
            "http://127.0.0.1".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Disabled,
            Arc::new(EndpointMetrics::mock()),
        ))];

        let endpoints = FirehoseEndpoints::for_testing(endpoint);

        let err = endpoints.endpoint().await.unwrap_err();
        assert!(err.to_string().contains("conn_pool_size"));
    }

    #[tokio::test]
    async fn firehose_endpoint_selection() {
        let logger = Logger::root(Discard, o!());
        let endpoint_metrics = Arc::new(EndpointMetrics::new(
            logger,
            &["high_error", "low availability", "high availability"],
            Arc::new(MetricsRegistry::mock()),
        ));

        let high_error_adapter1 = Arc::new(FirehoseEndpoint::new(
            "high_error".to_string(),
            "http://127.0.0.1".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Unlimited,
            endpoint_metrics.clone(),
        ));
        let high_error_adapter2 = Arc::new(FirehoseEndpoint::new(
            "high_error".to_string(),
            "http://127.0.0.1".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Unlimited,
            endpoint_metrics.clone(),
        ));
        let low_availability = Arc::new(FirehoseEndpoint::new(
            "low availability".to_string(),
            "http://127.0.0.2".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Limit(2),
            endpoint_metrics.clone(),
        ));
        let high_availability = Arc::new(FirehoseEndpoint::new(
            "high availability".to_string(),
            "http://127.0.0.3".to_string(),
            None,
            None,
            false,
            false,
            SubgraphLimit::Unlimited,
            endpoint_metrics.clone(),
        ));

        endpoint_metrics.report_for_test(&high_error_adapter1.provider, false);

        let endpoints = FirehoseEndpoints::for_testing(vec![
            high_error_adapter1.clone(),
            high_error_adapter2.clone(),
            low_availability.clone(),
            high_availability.clone(),
        ]);

        let res = endpoints.endpoint().await.unwrap();
        assert_eq!(res.provider, high_availability.provider);
        mem::drop(endpoints);

        // Removing high availability without errors should fallback to low availability
        let endpoints = FirehoseEndpoints::for_testing(
            vec![
                high_error_adapter1.clone(),
                high_error_adapter2,
                low_availability.clone(),
                high_availability.clone(),
            ]
            .into_iter()
            .filter(|a| a.provider_name() != high_availability.provider)
            .collect(),
        );

        // Ensure we're in a low capacity situation
        assert_eq!(low_availability.get_capacity(), AvailableCapacity::Low);

        // In the scenario where the only high level adapter has errors we keep trying that
        // because the others will be low or unavailable
        let res = endpoints.endpoint().await.unwrap();
        // This will match both high error adapters
        assert_eq!(res.provider, high_error_adapter1.provider);
    }

    #[test]
    fn subgraph_limit_calculates_availability() {
        #[derive(Debug)]
        struct Case {
            limit: SubgraphLimit,
            current: usize,
            capacity: AvailableCapacity,
        }

        let cases = vec![
            Case {
                limit: SubgraphLimit::Disabled,
                current: 20,
                capacity: AvailableCapacity::Unavailable,
            },
            Case {
                limit: SubgraphLimit::Limit(0),
                current: 20,
                capacity: AvailableCapacity::Unavailable,
            },
            Case {
                limit: SubgraphLimit::Limit(0),
                current: 0,
                capacity: AvailableCapacity::Unavailable,
            },
            Case {
                limit: SubgraphLimit::Limit(100),
                current: 80,
                capacity: AvailableCapacity::Low,
            },
            Case {
                limit: SubgraphLimit::Limit(2),
                current: 1,
                capacity: AvailableCapacity::Low,
            },
            Case {
                limit: SubgraphLimit::Limit(100),
                current: 19,
                capacity: AvailableCapacity::High,
            },
            Case {
                limit: SubgraphLimit::Limit(100),
                current: 100,
                capacity: AvailableCapacity::Unavailable,
            },
            Case {
                limit: SubgraphLimit::Limit(100),
                current: 99,
                capacity: AvailableCapacity::Low,
            },
            Case {
                limit: SubgraphLimit::Limit(100),
                current: 101,
                capacity: AvailableCapacity::Unavailable,
            },
            Case {
                limit: SubgraphLimit::Unlimited,
                current: 1000,
                capacity: AvailableCapacity::High,
            },
            Case {
                limit: SubgraphLimit::Unlimited,
                current: 0,
                capacity: AvailableCapacity::High,
            },
        ];

        for c in cases {
            let res = c.limit.get_capacity(c.current);
            assert_eq!(res, c.capacity, "{:#?}", c);
        }
    }

    #[test]
    fn available_capacity_ordering() {
        assert_eq!(
            AvailableCapacity::Unavailable < AvailableCapacity::Low,
            true
        );
        assert_eq!(
            AvailableCapacity::Unavailable < AvailableCapacity::High,
            true
        );
        assert_eq!(AvailableCapacity::Low < AvailableCapacity::High, true);
    }
}
