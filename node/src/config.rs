use graph::{
    components::ethereum::NodeCapabilities,
    prelude::{
        anyhow::{anyhow, bail, Context, Result},
        info, serde_json, Logger, NodeId,
    },
};
use graph_chain_ethereum::CLEANUP_BLOCKS;
use graph_store_postgres::{DeploymentPlacer, Shard as ShardName, PRIMARY_SHARD};

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::read_to_string;
use url::Url;

const ANY_NAME: &str = ".*";
/// A regular expression that matches nothing
const NO_NAME: &str = ".^";

pub struct Opt {
    pub postgres_url: Option<String>,
    pub config: Option<String>,
    // This is only used when we cosntruct a config purely from command
    // line options. When using a configuration file, pool sizes must be
    // set in the configuration file alone
    pub store_connection_pool_size: u32,
    pub postgres_secondary_hosts: Vec<String>,
    pub postgres_host_weights: Vec<usize>,
    pub disable_block_ingestor: bool,
    pub node_id: String,
    pub ethereum_rpc: Vec<String>,
    pub ethereum_ws: Vec<String>,
    pub ethereum_ipc: Vec<String>,
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
            ethereum_rpc: vec![],
            ethereum_ws: vec![],
            ethereum_ipc: vec![],
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub general: Option<GeneralSection>,
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
    /// Check that the config is valid.
    fn validate(&mut self) -> Result<()> {
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
            shard.validate(&key)?;
        }
        self.deployment.validate()?;

        // Check that deployment rules only reference existing stores and chains
        for (i, rule) in self.deployment.rules.iter().enumerate() {
            if !self.stores.contains_key(&rule.shard) {
                return Err(anyhow!(
                    "unknown shard {} in deployment rule {}",
                    rule.shard,
                    i
                ));
            }
            if let Some(networks) = &rule.pred.network {
                for network in networks.to_vec() {
                    if !self.chains.chains.contains_key(&network) {
                        return Err(anyhow!(
                            "unknown network {} in deployment rule {}",
                            network,
                            i
                        ));
                    }
                }
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
            config.validate()?;
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
        let chains = ChainSection::from_opt(opt)?;
        stores.insert(PRIMARY_SHARD.to_string(), Shard::from_opt(opt)?);
        Ok(Config {
            general: None,
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

    pub fn query_only(&self, node: &NodeId) -> bool {
        self.general
            .as_ref()
            .map(|g| match g.query.find(node.as_str()) {
                None => false,
                Some(m) => m.as_str() == node.as_str(),
            })
            .unwrap_or(false)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct GeneralSection {
    #[serde(with = "serde_regex", default = "no_name")]
    query: Regex,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Shard {
    pub connection: String,
    #[serde(default = "one")]
    pub weight: usize,
    #[serde(default)]
    pub pool_size: PoolSize,
    #[serde(default)]
    pub replicas: BTreeMap<String, Replica>,
}

impl Shard {
    fn validate(&mut self, name: &str) -> Result<()> {
        ShardName::new(name.to_string()).map_err(|e| anyhow!(e))?;

        self.connection = shellexpand::env(&self.connection)?.into_owned();

        if matches!(self.pool_size, PoolSize::None) {
            return Err(anyhow!("missing pool size definition for shard `{}`", name));
        }

        self.pool_size.validate(&self.connection)?;
        for (name, replica) in self.replicas.iter_mut() {
            validate_name(name).context("illegal replica name")?;
            replica.validate(&self.pool_size)?;
        }
        Ok(())
    }

    fn from_opt(opt: &Opt) -> Result<Self> {
        let postgres_url = opt
            .postgres_url
            .as_ref()
            .expect("validation checked that postgres_url is set");
        let pool_size = PoolSize::Fixed(opt.store_connection_pool_size);
        pool_size.validate(&postgres_url)?;
        let mut replicas = BTreeMap::new();
        for (i, host) in opt.postgres_secondary_hosts.iter().enumerate() {
            let replica = Replica {
                connection: replace_host(&postgres_url, &host),
                weight: opt.postgres_host_weights.get(i + 1).cloned().unwrap_or(1),
                pool_size: pool_size.clone(),
            };
            replicas.insert(format!("replica{}", i + 1), replica);
        }
        Ok(Self {
            connection: postgres_url.clone(),
            weight: opt.postgres_host_weights.get(0).cloned().unwrap_or(1),
            pool_size,
            replicas,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum PoolSize {
    None,
    Fixed(u32),
    Rule(Vec<PoolSizeRule>),
}

impl Default for PoolSize {
    fn default() -> Self {
        Self::None
    }
}

impl PoolSize {
    fn validate(&self, connection: &str) -> Result<()> {
        use PoolSize::*;

        let pool_size = match self {
            None => bail!("missing pool size for {}", connection),
            Fixed(s) => s.clone(),
            Rule(rules) => rules.iter().map(|rule| rule.size).min().unwrap_or(0u32),
        };

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

    pub fn size_for(&self, node: &NodeId, name: &str) -> Result<u32> {
        use PoolSize::*;
        match self {
            None => unreachable!("validation ensures we have a pool size"),
            Fixed(s) => Ok(s.clone()),
            Rule(rules) => rules
                .iter()
                .find(|rule| rule.matches(node.as_str()))
                .map(|rule| rule.size)
                .ok_or_else(|| {
                    anyhow!(
                        "no rule matches `{}` for the pool of shard {}",
                        node.as_str(),
                        name
                    )
                }),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PoolSizeRule {
    #[serde(with = "serde_regex", default = "any_name")]
    node: Regex,
    size: u32,
}

impl PoolSizeRule {
    fn matches(&self, name: &str) -> bool {
        match self.node.find(name) {
            None => false,
            Some(m) => m.as_str() == name,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Replica {
    pub connection: String,
    #[serde(default = "one")]
    pub weight: usize,
    #[serde(default)]
    pub pool_size: PoolSize,
}

impl Replica {
    fn validate(&mut self, pool_size: &PoolSize) -> Result<()> {
        self.connection = shellexpand::env(&self.connection)?.into_owned();
        if matches!(self.pool_size, PoolSize::None) {
            self.pool_size = pool_size.clone();
        }

        self.pool_size.validate(&self.connection)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ChainSection {
    pub ingestor: String,
    #[serde(flatten)]
    pub chains: BTreeMap<String, Chain>,
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

    fn from_opt(opt: &Opt) -> Result<Self> {
        // If we are not the block ingestor, set the node name
        // to something that is definitely not our node_id
        let ingestor = if opt.disable_block_ingestor {
            format!("{} is not ingesting", opt.node_id)
        } else {
            opt.node_id.clone()
        };
        let mut chains = BTreeMap::new();
        Self::parse_networks(&mut chains, Transport::Rpc, &opt.ethereum_rpc)?;
        Self::parse_networks(&mut chains, Transport::Ws, &opt.ethereum_ws)?;
        Self::parse_networks(&mut chains, Transport::Ipc, &opt.ethereum_ipc)?;
        Ok(Self { ingestor, chains })
    }

    fn parse_networks(
        chains: &mut BTreeMap<String, Chain>,
        transport: Transport,
        args: &Vec<String>,
    ) -> Result<()> {
        for (nr, arg) in args.iter().enumerate() {
            if arg.starts_with("wss://")
                || arg.starts_with("http://")
                || arg.starts_with("https://")
            {
                return Err(anyhow!(
                    "Is your Ethereum node string missing a network name? \
                     Try 'mainnet:' + the Ethereum node URL."
                ));
            } else {
                // Parse string (format is "NETWORK_NAME:NETWORK_CAPABILITIES:URL" OR
                // "NETWORK_NAME::URL" which will default to NETWORK_CAPABILITIES="archive,traces")
                let colon = arg.find(':').ok_or_else(|| {
                    return anyhow!(
                        "A network name must be provided alongside the \
                         Ethereum node location. Try e.g. 'mainnet:URL'."
                    );
                })?;

                let (name, rest_with_delim) = arg.split_at(colon);
                let rest = &rest_with_delim[1..];
                if name.is_empty() {
                    return Err(anyhow!("Ethereum network name cannot be an empty string"));
                }
                if rest.is_empty() {
                    return Err(anyhow!("Ethereum node URL cannot be an empty string"));
                }

                let colon = rest.find(":").ok_or_else(|| {
                    return anyhow!(
                        "A network name must be provided alongside the \
                         Ethereum node location. Try e.g. 'mainnet:URL'."
                    );
                })?;

                let (features, url_str) = rest.split_at(colon);
                let (url, features) = if vec!["http", "https", "ws", "wss"].contains(&features) {
                    (rest, DEFAULT_PROVIDER_FEATURES.to_vec())
                } else {
                    (&url_str[1..], features.split(',').collect())
                };
                let features = features.into_iter().map(|s| s.to_string()).collect();
                let provider = Provider {
                    label: format!("{}-{}-{}", name, transport, nr),
                    transport,
                    url: url.to_string(),
                    features,
                };
                let entry = chains.entry(name.to_string()).or_insert_with(|| Chain {
                    shard: PRIMARY_SHARD.to_string(),
                    providers: vec![],
                });
                entry.providers.push(provider);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Chain {
    pub shard: String,
    #[serde(rename = "provider")]
    pub providers: Vec<Provider>,
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
    pub label: String,
    #[serde(default)]
    pub transport: Transport,
    pub url: String,
    pub features: BTreeSet<String>,
}

const PROVIDER_FEATURES: [&str; 3] = ["traces", "archive", "no_eip1898"];
const DEFAULT_PROVIDER_FEATURES: [&str; 2] = ["traces", "archive"];

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

    pub fn node_capabilities(&self) -> NodeCapabilities {
        NodeCapabilities {
            archive: self.features.contains("archive"),
            traces: self.features.contains("traces"),
        }
    }
}

#[derive(Copy, Clone, Debug, Deserialize, Serialize)]
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

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use Transport::*;

        match self {
            Rpc => write!(f, "rpc"),
            Ws => write!(f, "ws"),
            Ipc => write!(f, "ipc"),
        }
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
    network: Option<NetworkPredicate>,
}

impl Predicate {
    fn matches_anything(&self) -> bool {
        self.name.as_str() == ANY_NAME && self.network.is_none()
    }

    pub fn matches(&self, name: &str, network: &str) -> bool {
        if let Some(n) = &self.network {
            if !n.matches(network) {
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
enum NetworkPredicate {
    Single(String),
    Many(Vec<String>),
}

impl NetworkPredicate {
    fn matches(&self, network: &str) -> bool {
        use NetworkPredicate::*;
        match self {
            Single(n) => n == network,
            Many(ns) => ns.iter().any(|n| n == network),
        }
    }

    fn to_vec(&self) -> Vec<String> {
        use NetworkPredicate::*;
        match self {
            Single(n) => vec![n.clone()],
            Many(ns) => ns.clone(),
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

fn no_name() -> Regex {
    Regex::new(NO_NAME).unwrap()
}

fn primary_store() -> String {
    PRIMARY_SHARD.to_string()
}

fn one() -> usize {
    1
}
