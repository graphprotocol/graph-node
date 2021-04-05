use std::collections::HashMap;

use config::{Config, File};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Deserializer};

use graph::prelude::*;

use dirs_next as dirs;

fn deserialize_http_headers<'de, D>(deserializer: D) -> Result<HeaderMap, D::Error>
where
    D: Deserializer<'de>,
{
    let kvs: HashMap<String, String> = Deserialize::deserialize(deserializer)?;
    let mut headers = HeaderMap::new();
    for (k, v) in kvs.into_iter() {
        headers.insert(
            k.parse::<HeaderName>()
                .expect(&format!("invalid HTTP header name: {}", k)),
            v.parse::<HeaderValue>()
                .expect(&format!("knvalid HTTP header value: {}: {}", k, v)),
        );
    }
    Ok(headers)
}

#[derive(Deserialize, Debug)]
pub struct EthereumRpcConfig {
    #[serde(deserialize_with = "deserialize_http_headers")]
    pub http_headers: HeaderMap,
}

#[derive(Debug, Default, Deserialize)]
pub struct EthereumConfig {
    pub rpc: HashMap<String, EthereumRpcConfig>,
}

lazy_static! {
    pub static ref ETHEREUM_CONFIG: EthereumConfig = {
        let mut config = Config::default();
        config
            .merge(File::with_name("/etc/graph-node/ethereum.toml").required(false))
            .expect("invalid config file `/etc/graph-node/ethereum.toml`");

        if let Some(config_dir) = dirs::config_dir() {
            let filename = config_dir.join("graph-node/ethereum.toml");
            config
                .merge(File::from(filename.clone()).required(false))
                .expect(&format!("invalid config file `{}`", filename.display()));
        }

        // Handle an empty configuration without errors
        if format!("{}", config.cache) == "{}" {
            return EthereumConfig::default()
        } else {
            match config.try_into() {
                Ok(config) => config,
                Err(e) => {
                    panic!("invalid Ethereum config: {}", e);
                }
            }
        }
    };
}
