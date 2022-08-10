use crate::{
    blockchain::Block as BlockchainBlock,
    blockchain::BlockPtr,
    cheap_clone::CheapClone,
    components::store::BlockNumber,
    firehose::{decode_firehose_block, ForkStep},
    prelude::{debug, info},
    substreams,
};
use futures03::StreamExt;
use http::uri::{Scheme, Uri};
use rand::prelude::IteratorRandom;
use slog::Logger;
use std::{collections::BTreeMap, fmt::Display, iter, sync::Arc, time::Duration};
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
    Request,
};

use super::codec as firehose;

#[derive(Clone, Debug)]
pub struct FirehoseEndpoint {
    pub provider: String,
    pub token: Option<String>,
    pub filters_enabled: bool,
    channel: Channel,
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
        conn_pool_size: u16,
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
            .tcp_keepalive(Some(Duration::from_secs(15)));

        // Load balancing on a same endpoint is useful because it creates a connection pool.
        let channel = Channel::balance_list(iter::repeat(endpoint).take(conn_pool_size as usize));

        FirehoseEndpoint {
            provider: provider.as_ref().to_string(),
            channel,
            token,
            filters_enabled,
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
        let token_metadata = match self.token.clone() {
            Some(token) => Some(MetadataValue::from_str(token.as_str())?),
            None => None,
        };

        let mut client = firehose::stream_client::StreamClient::with_interceptor(
            self.channel.cheap_clone(),
            move |mut r: Request<()>| {
                if let Some(ref t) = token_metadata {
                    r.metadata_mut().insert("authorization", t.clone());
                }

                Ok(r)
            },
        );

        debug!(
            logger,
            "Connecting to firehose to retrieve block for number {}", number
        );

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
                fork_steps: vec![ForkStep::StepNew as i32, ForkStep::StepIrreversible as i32],
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
        let token_metadata = match self.token.clone() {
            Some(token) => Some(MetadataValue::from_str(token.as_str())?),
            None => None,
        };

        let mut client = firehose::stream_client::StreamClient::with_interceptor(
            self.channel.cheap_clone(),
            move |mut r: Request<()>| {
                if let Some(ref t) = token_metadata {
                    r.metadata_mut().insert("authorization", t.clone());
                }

                Ok(r)
            },
        );

        let response_stream = client.blocks(request).await?;
        let block_stream = response_stream.into_inner();

        Ok(block_stream)
    }

    pub async fn substreams(
        self: Arc<Self>,
        request: substreams::Request,
    ) -> Result<tonic::Streaming<substreams::Response>, anyhow::Error> {
        let token_metadata = match self.token.clone() {
            Some(token) => Some(MetadataValue::from_str(token.as_str())?),
            None => None,
        };

        let mut client = substreams::stream_client::StreamClient::with_interceptor(
            self.channel.cheap_clone(),
            move |mut r: Request<()>| {
                if let Some(ref t) = token_metadata {
                    r.metadata_mut().insert("authorization", t.clone());
                }

                Ok(r)
            },
        );

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

    pub fn random(&self) -> Option<&Arc<FirehoseEndpoint>> {
        // Select from the matching adapters randomly
        let mut rng = rand::thread_rng();
        self.0.iter().choose(&mut rng)
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
