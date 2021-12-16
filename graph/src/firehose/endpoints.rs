use crate::{
    blockchain::Block as BlockchainBlock,
    blockchain::BlockPtr,
    cheap_clone::CheapClone,
    firehose::decode_firehose_block,
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

use super::bstream;

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

        let mut client = bstream::block_stream_v2_client::BlockStreamV2Client::with_interceptor(
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
            .blocks(bstream::BlocksRequestV2 {
                start_block_num: 0,
                details: bstream::BlockDetails::Light as i32,
                fork_steps: vec![bstream::ForkStep::StepIrreversible as i32],
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
        request: bstream::BlocksRequestV2,
    ) -> Result<tonic::Streaming<bstream::BlockResponseV2>, anyhow::Error> {
        let token_metadata = match self.token.clone() {
            Some(token) => Some(MetadataValue::from_str(token.as_str())?),
            None => None,
        };

        let mut client = bstream::block_stream_v2_client::BlockStreamV2Client::with_interceptor(
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
pub struct FirehoseNetworkEndpoint {
    endpoint: Arc<FirehoseEndpoint>,
}

#[derive(Clone, Debug)]
pub struct FirehoseNetworkEndpoints {
    pub endpoints: Vec<FirehoseNetworkEndpoint>,
}

impl FirehoseNetworkEndpoints {
    pub fn new() -> Self {
        Self { endpoints: vec![] }
    }

    pub fn len(&self) -> usize {
        self.endpoints.len()
    }

    pub fn random(&self) -> Option<&Arc<FirehoseEndpoint>> {
        if self.endpoints.len() == 0 {
            return None;
        }

        // Select from the matching adapters randomly
        let mut rng = rand::thread_rng();
        Some(&self.endpoints.iter().choose(&mut rng).unwrap().endpoint)
    }

    pub fn remove(&mut self, provider: &str) {
        self.endpoints
            .retain(|network_endpoint| network_endpoint.endpoint.provider != provider);
    }
}

#[derive(Clone, Debug)]
pub struct FirehoseNetworks {
    pub networks: BTreeMap<String, FirehoseNetworkEndpoints>,
}

impl FirehoseNetworks {
    pub fn new() -> FirehoseNetworks {
        FirehoseNetworks {
            networks: BTreeMap::new(),
        }
    }

    pub fn insert(&mut self, name: String, endpoint: Arc<FirehoseEndpoint>) {
        let network_endpoints = self
            .networks
            .entry(name)
            .or_insert(FirehoseNetworkEndpoints { endpoints: vec![] });
        network_endpoints.endpoints.push(FirehoseNetworkEndpoint {
            endpoint: endpoint.clone(),
        });
    }

    pub fn remove(&mut self, name: &str, provider: &str) {
        if let Some(endpoints) = self.networks.get_mut(name) {
            endpoints.remove(provider);
        }
    }

    pub fn flatten(&self) -> Vec<(String, Arc<FirehoseEndpoint>)> {
        self.networks
            .iter()
            .flat_map(|(network_name, firehose_endpoints)| {
                firehose_endpoints
                    .endpoints
                    .iter()
                    .map(move |firehose_endpoint| {
                        (network_name.clone(), firehose_endpoint.endpoint.clone())
                    })
            })
            .collect()
    }
}
