use graph::prelude::{
    anyhow::{anyhow, Context, Result},
    info, serde_json, Logger, NodeId,
};
use graph_chain_ethereum::CLEANUP_BLOCKS;
use graph_store_postgres::{DeploymentPlacer, Shard as ShardName, PRIMARY_SHARD};

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::read_to_string;
use url::Url;

const ANY_NAME: &str = ".*";

pub struct Opt {
    pub postgres_url: Option<String>,
    pub config: Option<String>,
    pub store_connection_pool_size: u32,
    pub postgres_secondary_hosts: Vec<String>,
    pub postgres_host_weights: Vec<usize>,
    pub disable_block_ingestor: bool,
    pub node_id: String,
}

impl Default for Opt {
    fn default() -> Self {
        Opt {
            postgres_url: None,
            config: None,
            store_connection_pool_size: 10,
            postgres_secondary_hosts: vec![],
            postgres_host_weights: vec![],
            disable_block_ingestor: true,
            node_id: "default".to_string(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(rename = "store")]
    pub stores: BTreeMap<String, Shard>,
    pub chains: ChainSection,
    pub deployment: Deployment,
}

fn validate_name(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(anyhow!("names must not be empty"));
    }
    if s.len() > 30 {
        return Err(anyhow!(
            "names can be at most 30 characters, but `{}` has {} characters",
            s,
            s.len()
        ));
    }

    if !s
        .chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(anyhow!(
            "name `{}` is invalid: names can only contain lowercase alphanumeric characters or '-'",
            s
        ));
    }
    Ok(())
}

impl Config {
    /// Check that the config is valid. Some defaults (like `pool_size`) will
    /// be filled in from `opt` at the same time.
    fn validate(&mut self, opt: &Opt) -> Result<()> {
        if !self.stores.contains_key(PRIMARY_SHARD.as_str()) {
            return Err(anyhow!("missing a primary store"));
        }
        if self.stores.len() > 1 && *CLEANUP_BLOCKS {
            // See 8b6ad0c64e244023ac20ced7897fe666
            return Err(anyhow!(
                "GRAPH_ETHEREUM_CLEANUP_BLOCKS can not be used with a sharded store"
            ));
        }
        for (key, shard) in self.stores.iter_mut() {
            ShardName::new(key.clone()).map_err(|e| anyhow!(e))?;
            shard.validate(opt)?;
        }
        self.deployment.validate()?;

        // Check that deployment rules only reference existing stores
        for (i, rule) in self.deployment.rules.iter().enumerate() {
            if !self.stores.contains_key(&rule.shard) {
                return Err(anyhow!(
                    "unknown shard {} in deployment rule {}",
                    rule.shard,
                    i
                ));
            }
        }

        // Check that chains only reference existing stores
        for (name, chain) in &self.chains.chains {
            if !self.stores.contains_key(&chain.shard) {
                return Err(anyhow!("unknown shard {} in chain {}", chain.shard, name));
            }
        }

        self.chains.validate()?;

        Ok(())
    }

    /// Load a configuration file if `opt.config` is set. If not, generate
    /// a config from the command line arguments in `opt`
    pub fn load(logger: &Logger, opt: &Opt) -> Result<Config> {
        if let Some(config) = &opt.config {
            info!(logger, "Reading configuration file `{}`", config);
            let config = read_to_string(config)?;
            let mut config: Config = toml::from_str(&config)?;
            config.validate(opt)?;
            Ok(config)
        } else {
            info!(
                logger,
                "Generating configuration from command line arguments"
            );
            Self::from_opt(opt)
        }
    }

    fn from_opt(opt: &Opt) -> Result<Config> {
        let deployment = Deployment::from_opt(opt);
        let mut stores = BTreeMap::new();
        let chains = ChainSection::from_opt(opt);
        stores.insert(PRIMARY_SHARD.to_string(), Shard::from_opt(opt)?);
        Ok(Config {
            stores,
            chains,
            deployment,
        })
    }

    /// Genrate a JSON representation of the config.
    pub fn to_json(&self) -> Result<String> {
        // It would be nice to produce a TOML representation, but that runs
        // into this error: https://github.com/alexcrichton/toml-rs/issues/142
        // and fixing it as described in the issue didn't fix it. Since serializing
        // this data isn't crucial and only needed for debugging, we'll
        // just stick with JSON
        Ok(serde_json::to_string_pretty(&self)?)
    }

    pub fn primary_store(&self) -> &Shard {
        self.stores
            .get(PRIMARY_SHARD.as_str())
            .expect("a validated config has a primary store")
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Shard {
    pub connection: String,
    #[serde(default = "one")]
    pub weight: usize,
    #[serde(default)]
    pub pool_size: u32,
    #[serde(default)]
    pub replicas: BTreeMap<String, Replica>,
}

fn check_pool_size(pool_size: u32, connection: &str) -> Result<()> {
    if pool_size < 2 {
        Err(anyhow!(
            "connection pool size must be at least 2, but is {} for {}",
            pool_size,
            connection
        ))
    } else {
        Ok(())
    }
}

impl Shard {
    fn validate(&mut self, opt: &Opt) -> Result<()> {
        self.connection = shellexpand::env(&self.connection)?.into_owned();
        if self.pool_size == 0 {
            self.pool_size = opt.store_connection_pool_size;
        }

        check_pool_size(self.pool_size, &self.connection)?;
        for (name, replica) in self.replicas.iter_mut() {
            validate_name(name).context("illegal replica name")?;
            replica.validate(opt)?;
        }
        Ok(())
    }

    fn from_opt(opt: &Opt) -> Result<Self> {
        let postgres_url = opt
            .postgres_url
            .as_ref()
            .expect("validation checked that postgres_url is set");
        check_pool_size(opt.store_connection_pool_size, &postgres_url)?;
        let mut replicas = BTreeMap::new();
        for (i, host) in opt.postgres_secondary_hosts.iter().enumerate() {
            let replica = Replica {
                connection: replace_host(&postgres_url, &host),
                weight: opt.postgres_host_weights.get(i + 1).cloned().unwrap_or(1),
                pool_size: opt.store_connection_pool_size,
            };
            replicas.insert(format!("replica{}", i + 1), replica);
        }
        Ok(Self {
            connection: postgres_url.clone(),
            weight: opt.postgres_host_weights.get(0).cloned().unwrap_or(1),
            pool_size: opt.store_connection_pool_size,
            replicas,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Replica {
    pub connection: String,
    #[serde(default = "one")]
    pub weight: usize,
    #[serde(default = "zero")]
    pub pool_size: u32,
}

impl Replica {
    fn validate(&mut self, opt: &Opt) -> Result<()> {
        self.connection = shellexpand::env(&self.connection)?.into_owned();
        if self.pool_size == 0 {
            self.pool_size = opt.store_connection_pool_size;
        }

        check_pool_size(self.pool_size, &self.connection)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChainSection {
    ingestor: String,
    #[serde(flatten)]
    chains: BTreeMap<String, Chain>,
}

impl ChainSection {
    fn validate(&self) -> Result<()> {
        NodeId::new(&self.ingestor)
            .map_err(|()| anyhow!("invalid node id for ingestor {}", &self.ingestor))?;
        for (_, chain) in &self.chains {
            chain.validate()?
        }
        Ok(())
    }

    fn from_opt(opt: &Opt) -> Self {
        // If we are not the block ingestor, set the node name
        // to something that is definitely not our node_id
        let ingestor = if opt.disable_block_ingestor {
            format!("{} is not ingesting", opt.node_id)
        } else {
            opt.node_id.clone()
        };
        Self {
            ingestor,
            chains: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chain {
    shard: String,
    #[serde(rename = "provider")]
    providers: Vec<Provider>,
}

impl Chain {
    fn validate(&self) -> Result<()> {
        // `Config` validates that `self.shard` references a configured shard

        for provider in &self.providers {
            provider.validate()?
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Provider {
    label: String,
    #[serde(default)]
    transport: Transport,
    url: String,
    features: Vec<String>,
}

const PROVIDER_FEATURES: [&str; 2] = ["traces", "archive "];

impl Provider {
    fn validate(&self) -> Result<()> {
        validate_name(&self.label).context("illegal provider name")?;

        for feature in &self.features {
            if !PROVIDER_FEATURES.contains(&feature.as_str()) {
                return Err(anyhow!(
                    "illegal feature `{}` for provider {}. Features must be one of {}",
                    feature,
                    self.label,
                    PROVIDER_FEATURES.join(", ")
                ));
            }
        }

        Url::parse(&self.url).map_err(|e| {
            anyhow!(
                "the url `{}` for provider {} is not a legal URL: {}",
                self.url,
                self.label,
                e
            )
        })?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum Transport {
    #[serde(rename = "rpc")]
    Rpc,
    #[serde(rename = "ws")]
    Ws,
    #[serde(rename = "ipc")]
    Ipc,
}

impl Default for Transport {
    fn default() -> Self {
        Self::Rpc
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Deployment {
    #[serde(rename = "rule")]
    rules: Vec<Rule>,
}

impl Deployment {
    fn validate(&self) -> Result<()> {
        if self.rules.is_empty() {
            return Err(anyhow!(
                "there must be at least one deployment rule".to_string()
            ));
        }
        let mut default_rule = false;
        for rule in &self.rules {
            rule.validate()?;
            if default_rule {
                return Err(anyhow!("rules after a default rule are useless"));
            }
            default_rule = rule.is_default();
        }
        if !default_rule {
            return Err(anyhow!(
                "the rules do not contain a default rule that matches everything"
            ));
        }
        Ok(())
    }

    fn from_opt(_: &Opt) -> Self {
        Self { rules: vec![] }
    }
}

impl DeploymentPlacer for Deployment {
    fn place(&self, name: &str, network: &str) -> Result<Option<(ShardName, Vec<NodeId>)>, String> {
        // Errors here are really programming errors. We should have validated
        // everything already so that the various conversions can't fail. We
        // still return errors so that they bubble up to the deployment request
        // rather than crashing the node and burying the crash in the logs
        let placement = match self.rules.iter().find(|rule| rule.matches(name, network)) {
            Some(rule) => {
                let shard = ShardName::new(rule.shard.clone()).map_err(|e| e.to_string())?;
                let indexers: Vec<_> = rule
                    .indexers
                    .iter()
                    .map(|idx| {
                        NodeId::new(idx.clone())
                            .map_err(|()| format!("{} is not a valid node name", idx))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Some((shard, indexers))
            }
            None => None,
        };
        Ok(placement)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Rule {
    #[serde(rename = "match", default)]
    pred: Predicate,
    #[serde(default = "primary_store")]
    shard: String,
    indexers: Vec<String>,
}

impl Rule {
    fn is_default(&self) -> bool {
        self.pred.matches_anything()
    }

    fn matches(&self, name: &str, network: &str) -> bool {
        self.pred.matches(name, network)
    }

    fn validate(&self) -> Result<()> {
        if self.indexers.is_empty() {
            return Err(anyhow!("useless rule without indexers"));
        }
        for indexer in &self.indexers {
            NodeId::new(indexer).map_err(|()| anyhow!("invalid node id {}", &indexer))?;
        }
        ShardName::new(self.shard.clone())
            .map_err(|e| anyhow!("illegal name for store shard `{}`: {}", &self.shard, e))?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Predicate {
    #[serde(with = "serde_regex", default = "any_name")]
    name: Regex,
    network: Option<String>,
}

impl Predicate {
    fn matches_anything(&self) -> bool {
        self.name.as_str() == ANY_NAME && self.network.is_none()
    }

    pub fn matches(&self, name: &str, network: &str) -> bool {
        if let Some(n) = &self.network {
            if n != network {
                return false;
            }
        }

        match self.name.find(name) {
            None => false,
            Some(m) => m.as_str() == name,
        }
    }
}

impl Default for Predicate {
    fn default() -> Self {
        Predicate {
            name: any_name(),
            network: None,
        }
    }
}

/// Replace the host portion of `url` and return a new URL with `host`
/// as the host portion
///
/// Panics if `url` is not a valid URL (which won't happen in our case since
/// we would have paniced before getting here as `url` is the connection for
/// the primary Postgres instance)
fn replace_host(url: &str, host: &str) -> String {
    let mut url = match Url::parse(url) {
        Ok(url) => url,
        Err(_) => panic!("Invalid Postgres URL {}", url),
    };
    if let Err(e) = url.set_host(Some(host)) {
        panic!("Invalid Postgres url {}: {}", url, e.to_string());
    }
    url.into_string()
}

// Various default functions for deserialization
fn any_name() -> Regex {
    Regex::new(ANY_NAME).unwrap()
}

fn primary_store() -> String {
    PRIMARY_SHARD.to_string()
}

fn one() -> usize {
    1
}

fn zero() -> u32 {
    0
}
