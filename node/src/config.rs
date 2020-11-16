use graph::prelude::{
    anyhow::{anyhow, Result},
    info, serde_json, Logger,
};

use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::read_to_string;
use url::Url;

const PRIMARY: &str = "primary";
const ANY_NAME: &str = ".*";

use crate::opt::Opt;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(rename = "store")]
    stores: BTreeMap<String, Shard>,
    deployment: Deployment,
    ingestor: Ingestor,
}

fn validate_name(s: &str) -> Result<()> {
    for c in s.chars() {
        if !c.is_ascii_alphanumeric() || c == '-' {
            return Err(anyhow!(
                "names can only contain alphanumeric characters or '-', but `{}` contains `{}`",
                s,
                c
            ));
        }
    }
    Ok(())
}

impl Config {
    /// Check that the config is valid. Some defaults (like `pool_size`) will
    /// be filled in from `opt` at the same time.
    fn validate(&mut self, opt: &Opt) -> Result<()> {
        if !self.stores.contains_key(PRIMARY) {
            return Err(anyhow!("missing a primary store"));
        }
        for (key, shard) in self.stores.iter_mut() {
            validate_name(key)?;
            shard.validate(opt)?;
        }
        self.deployment.validate()?;
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
        let ingestor = Ingestor::from_opt(opt);
        let deployment = Deployment::from_opt(opt);
        let mut stores = BTreeMap::new();
        stores.insert(PRIMARY.to_string(), Shard::from_opt(opt)?);
        Ok(Config {
            stores,
            deployment,
            ingestor,
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
            .get(PRIMARY)
            .expect("a validated config has a primary store")
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Shard {
    pub connection: String,
    #[serde(default = "one")]
    pub weight: usize,
    #[serde(default)]
    pub pool_size: u32,
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
            validate_name(name)?;
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

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
struct Deployment {
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

    // This needs to be moved to some sort of trait
    #[allow(dead_code)]
    fn place(&self, name: &str, network: &str, default: &str) -> Option<(&str, Vec<String>)> {
        if self.rules.is_empty() {
            // This can only happen if we have only command line arguments and no
            // configuration file
            Some((PRIMARY, vec![default.to_string()]))
        } else {
            self.rules
                .iter()
                .find(|rule| rule.matches(name, network))
                .map(|rule| (rule.store.as_str(), rule.indexers.clone()))
        }
    }

    fn from_opt(_: &Opt) -> Self {
        Self { rules: vec![] }
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Rule {
    #[serde(rename = "match", default)]
    pred: Predicate,
    #[serde(default = "primary_store")]
    store: String,
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
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
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

#[derive(Debug, Deserialize, Serialize)]
struct Ingestor {
    node: String,
}

impl Ingestor {
    fn from_opt(opt: &Opt) -> Self {
        // If we are not the block ingestor, set the node name
        // to something that is definitely not our node_id
        if opt.disable_block_ingestor {
            Ingestor {
                node: format!("{} is not ingesting", opt.node_id),
            }
        } else {
            Ingestor {
                node: opt.node_id.clone(),
            }
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
    PRIMARY.to_string()
}

fn one() -> usize {
    1
}

fn zero() -> u32 {
    0
}
