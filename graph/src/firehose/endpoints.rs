use crate::{
    blockchain::Block as BlockchainBlock,
    blockchain::BlockPtr,
    cheap_clone::CheapClone,
    firehose::{decode_firehose_block, ForkStep},
    prelude::{debug, info},
};
use anyhow::Context;
use futures03::StreamExt;
use http::uri::{Scheme, Uri};
use rand::prelude::IteratorRandom;
use slog::Logger;
use std::{collections::BTreeMap, fmt::Display, sync::Arc};
use tonic::{
    metadata::MetadataValue,
    transport::{Channel, ClientTlsConfig},
    Request,
};

use super::codec as firehose;

#[derive(Clone, Debug)]
pub struct FirehoseEndpoint {
    pub provider: String,
    pub uri: String,
    pub token: Option<String>,
    channel: Channel,
    _logger: Logger,
}

impl Display for FirehoseEndpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self.uri.as_str(), f)
    }
}

impl FirehoseEndpoint {
    pub async fn new<S: AsRef<str>>(
        logger: Logger,
        provider: S,
        url: S,
        token: Option<String>,
    ) -> Result<Self, anyhow::Error> {
        let uri = url
            .as_ref()
            .parse::<Uri>()
            .expect("the url should have been validated by now, so it is a valid Uri");

        let endpoint = match uri.scheme().unwrap_or_else(|| &Scheme::HTTP).as_str() {
            "http" => Channel::builder(uri),
            "https" => Channel::builder(uri)
                .tls_config(ClientTlsConfig::new())
                .expect("TLS config on this host is invalid"),
            _ => panic!("invalid uri scheme for firehose endpoint"),
        };

        let uri = endpoint.uri().to_string();
        let channel = endpoint.connect_lazy().with_context(|| {
            format!(
                "unable to lazily connect to firehose provider {} (at {})",
                provider.as_ref(),
                url.as_ref()
            )
        })?;

        Ok(FirehoseEndpoint {
            provider: provider.as_ref().to_string(),
            uri,
            channel,
            token,
            _logger: logger,
        })
    }

    pub async fn genesis_block_ptr<M>(&self, logger: &Logger) -> Result<BlockPtr, anyhow::Error>
    where
        M: prost::Message + BlockchainBlock + Default + 'static,
    {
        let token_metadata = match self.token.clone() {
            Some(token) => Some(MetadataValue::from_str(token.as_str())?),
            None => None,
        };

        let mut client = firehose::stream_client::StreamClient::with_interceptor(
            self.channel.cheap_clone(),
            move |mut r: Request<()>| match token_metadata.as_ref() {
                Some(t) => {
                    r.metadata_mut().insert("authorization", t.clone());
                    Ok(r)
                }
                _ => Ok(r),
            },
        );

        debug!(logger, "Connecting to firehose to retrieve genesis block");
        let response_stream = client
            .blocks(firehose::Request {
                start_block_num: 0,
                fork_steps: vec![ForkStep::StepIrreversible as i32],
                ..Default::default()
            })
            .await?;

        let mut block_stream = response_stream.into_inner();

        info!(logger, "Requesting genesis block from firehose");
        let next = block_stream.next().await;

        match next {
            Some(Ok(v)) => Ok(decode_firehose_block::<M>(&v)?.ptr()),
            Some(Err(e)) => Err(anyhow::format_err!("firehose error {}", e)),
            None => Err(anyhow::format_err!(
                "firehose should have returned one block for genesis block request"
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
            move |mut r: Request<()>| match token_metadata.as_ref() {
                Some(t) => {
                    r.metadata_mut().insert("authorization", t.clone());
                    Ok(r)
                }
                _ => Ok(r),
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
        if self.0.len() == 0 {
            return None;
        }

        // Select from the matching adapters randomly
        let mut rng = rand::thread_rng();
        Some(&self.0.iter().choose(&mut rng).unwrap())
    }

    pub fn remove(&mut self, provider: &str) {
        self.0
            .retain(|network_endpoint| network_endpoint.provider != provider);
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
            .or_insert(FirehoseEndpoints::new());

        endpoints.0.push(endpoint.clone());
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
