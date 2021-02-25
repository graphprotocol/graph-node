//! Entity types that contain the graph-node state.

use anyhow::{anyhow, Error};
use hex;
use lazy_static::lazy_static;
use rand::rngs::OsRng;
use rand::Rng;
use stable_hash::{SequenceNumber, StableHash, StableHasher};
use std::str::FromStr;
use std::{fmt, fmt::Display};
use web3::types::*;

use super::SubgraphDeploymentId;
use crate::components::{ethereum::EthereumBlockPointer, store::EntityType};
use crate::data::graphql::TryFromValue;
use crate::data::store::Value;
use crate::data::subgraph::SubgraphManifest;
use crate::prelude::*;

pub const POI_TABLE: &str = "poi2$";
lazy_static! {
    pub static ref POI_OBJECT: EntityType = EntityType::new("Poi$".to_string());
}

#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum SubgraphHealth {
    /// Syncing without errors.
    Healthy,

    /// Syncing but has errors.
    Unhealthy,

    /// No longer syncing due to fatal error.
    Failed,
}

impl SubgraphHealth {
    pub fn as_str(&self) -> &'static str {
        match self {
            SubgraphHealth::Healthy => "healthy",
            SubgraphHealth::Unhealthy => "unhealthy",
            SubgraphHealth::Failed => "failed",
        }
    }

    pub fn is_failed(&self) -> bool {
        match self {
            SubgraphHealth::Healthy => false,
            SubgraphHealth::Unhealthy => false,
            SubgraphHealth::Failed => true,
        }
    }
}

impl FromStr for SubgraphHealth {
    type Err = Error;

    fn from_str(s: &str) -> Result<SubgraphHealth, Error> {
        match s {
            "healthy" => Ok(SubgraphHealth::Healthy),
            "unhealthy" => Ok(SubgraphHealth::Unhealthy),
            "failed" => Ok(SubgraphHealth::Failed),
            _ => Err(anyhow!("failed to parse `{}` as SubgraphHealth", s)),
        }
    }
}

impl From<SubgraphHealth> for String {
    fn from(health: SubgraphHealth) -> String {
        health.as_str().to_string()
    }
}

impl From<SubgraphHealth> for Value {
    fn from(health: SubgraphHealth) -> Value {
        String::from(health).into()
    }
}

impl From<SubgraphHealth> for q::Value {
    fn from(health: SubgraphHealth) -> q::Value {
        q::Value::Enum(health.into())
    }
}

impl TryFromValue for SubgraphHealth {
    fn try_from_value(value: &q::Value) -> Result<SubgraphHealth, Error> {
        match value {
            q::Value::Enum(health) => SubgraphHealth::from_str(health),
            _ => Err(anyhow!(
                "cannot parse value as SubgraphHealth: `{:?}`",
                value
            )),
        }
    }
}

#[derive(Debug)]
pub struct SubgraphDeploymentEntity {
    pub manifest: SubgraphManifestEntity,
    pub failed: bool,
    pub health: SubgraphHealth,
    pub synced: bool,
    pub fatal_error: Option<SubgraphError>,
    pub non_fatal_errors: Vec<SubgraphError>,
    pub earliest_ethereum_block_hash: Option<H256>,
    pub earliest_ethereum_block_number: Option<u64>,
    pub latest_ethereum_block_hash: Option<H256>,
    pub latest_ethereum_block_number: Option<u64>,
    pub graft_base: Option<SubgraphDeploymentId>,
    pub graft_block_hash: Option<H256>,
    pub graft_block_number: Option<u64>,
    pub reorg_count: i32,
    pub current_reorg_depth: i32,
    pub max_reorg_depth: i32,
}

impl SubgraphDeploymentEntity {
    pub fn new(
        source_manifest: &SubgraphManifest,
        synced: bool,
        earliest_ethereum_block: Option<EthereumBlockPointer>,
    ) -> Self {
        Self {
            manifest: SubgraphManifestEntity::from(source_manifest),
            failed: false,
            health: SubgraphHealth::Healthy,
            synced,
            fatal_error: None,
            non_fatal_errors: vec![],
            earliest_ethereum_block_hash: earliest_ethereum_block.map(Into::into),
            earliest_ethereum_block_number: earliest_ethereum_block.map(Into::into),
            latest_ethereum_block_hash: earliest_ethereum_block.map(Into::into),
            latest_ethereum_block_number: earliest_ethereum_block.map(Into::into),
            graft_base: None,
            graft_block_hash: None,
            graft_block_number: None,
            reorg_count: 0,
            current_reorg_depth: 0,
            max_reorg_depth: 0,
        }
    }

    pub fn graft(mut self, base: Option<(SubgraphDeploymentId, EthereumBlockPointer)>) -> Self {
        if let Some((subgraph, ptr)) = base {
            self.graft_base = Some(subgraph);
            self.graft_block_hash = Some(ptr.hash);
            self.graft_block_number = Some(ptr.number);
            // When we graft, the block pointer is only set after copying
            // from the base subgraph finished successfully
            self.latest_ethereum_block_hash = None;
            self.latest_ethereum_block_number = None;
        }
        self
    }
}

#[derive(Debug)]
pub struct SubgraphManifestEntity {
    pub spec_version: String,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub features: Vec<String>,
    pub schema: String,
}

impl SubgraphManifestEntity {
    pub fn id(subgraph_id: &SubgraphDeploymentId) -> String {
        format!("{}-manifest", subgraph_id)
    }
}

impl<'a> From<&'a super::SubgraphManifest> for SubgraphManifestEntity {
    fn from(manifest: &'a super::SubgraphManifest) -> Self {
        Self {
            spec_version: manifest.spec_version.clone(),
            description: manifest.description.clone(),
            repository: manifest.repository.clone(),
            features: manifest.features.iter().map(|f| f.to_string()).collect(),
            schema: manifest.schema.document.clone().to_string(),
        }
    }
}

#[derive(Debug)]
pub struct SubgraphError {
    pub subgraph_id: SubgraphDeploymentId,
    pub message: String,
    pub block_ptr: Option<EthereumBlockPointer>,
    pub handler: Option<String>,

    // `true` if we are certain the error is deterministic. If in doubt, this is `false`.
    pub deterministic: bool,
}

impl Display for SubgraphError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "{}", self.message)?;
        if let Some(handler) = &self.handler {
            write!(f, " in handler `{}`", handler)?;
        }
        if let Some(block_ptr) = self.block_ptr {
            write!(f, " at block {}", block_ptr)?;
        }
        Ok(())
    }
}

impl StableHash for SubgraphError {
    fn stable_hash<H: StableHasher>(&self, mut sequence_number: H::Seq, state: &mut H) {
        let SubgraphError {
            subgraph_id,
            message,
            block_ptr,
            handler,
            deterministic,
        } = self;
        subgraph_id.stable_hash(sequence_number.next_child(), state);
        message.stable_hash(sequence_number.next_child(), state);
        block_ptr.stable_hash(sequence_number.next_child(), state);
        handler.stable_hash(sequence_number.next_child(), state);
        deterministic.stable_hash(sequence_number.next_child(), state);
    }
}

pub fn generate_entity_id() -> String {
    // Fast crypto RNG from operating system
    let mut rng = OsRng::new().unwrap();

    // 128 random bits
    let id_bytes: [u8; 16] = rng.gen();

    // 32 hex chars
    // Comparable to uuidv4, but without the hyphens,
    // and without spending bits on a version identifier.
    hex::encode(id_bytes)
}
