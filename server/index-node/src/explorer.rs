//! Functionality to support the explorer in the hosted service. Everything
//! in this file is private API and experimental and subject to change at
//! any time
use graphql_parser::query as q;
use http::{Response, StatusCode};
use hyper::Body;
use std::{
    collections::HashMap,
    env,
    str::FromStr,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use graph::{
    components::server::{index_node::VersionInfo, query::GraphQLServerError},
    data::subgraph::status,
    object,
    prelude::{lazy_static, serde_json, SerializableValue, Store},
};

lazy_static! {
    static ref TTL: Duration = {
        let ttl = env::var("GRAPH_EXPLORER_TTL")
            .ok()
            .map(|s| {
                u64::from_str(&s).unwrap_or_else(|_| {
                    panic!("GRAPH_EXPLORER_TTL must be a number, but is `{}`", s)
                })
            })
            .unwrap_or(10);
        Duration::from_secs(ttl)
    };
}

// Do not implement `Clone` for this; the IndexNode service puts the `Explorer`
// behind an `Arc` so we don't have to put each `Cache` into an `Arc`
//
// We cache responses for a fixed amount of time with the time given by
// `GRAPH_EXPLORER_TTL`
#[derive(Debug)]
pub struct Explorer<S> {
    store: Arc<S>,
    versions: Cache<q::Value>,
    version_infos: Cache<VersionInfo>,
    entity_counts: Cache<q::Value>,
}

impl<S> Explorer<S>
where
    S: Store,
{
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            versions: Cache::new(*TTL),
            version_infos: Cache::new(*TTL),
            entity_counts: Cache::new(*TTL),
        }
    }

    pub fn handle(&self, req: &[&str]) -> Result<Response<Body>, GraphQLServerError> {
        match req {
            ["subgraph-versions", subgraph_id] => self.handle_subgraph_versions(subgraph_id),
            ["subgraph-version", version] => self.handle_subgraph_version(version),
            ["subgraph-repo", version] => self.handle_subgraph_repo(version),
            ["entity-count", deployment] => self.handle_entity_count(deployment),
            _ => {
                return handle_not_found();
            }
        }
    }

    fn handle_subgraph_versions(
        &self,
        subgraph_id: &str,
    ) -> Result<Response<Body>, GraphQLServerError> {
        if let Some(value) = self.versions.get(subgraph_id) {
            return Ok(as_http_response(value.as_ref()));
        }

        let (current, pending) = self.store.versions_for_subgraph_id(subgraph_id)?;

        let value = object! {
            currentVersion: current,
            pendingVersion: pending
        };

        let resp = as_http_response(&value);
        self.versions.set(subgraph_id.to_string(), Arc::new(value));
        Ok(resp)
    }

    fn handle_subgraph_version(&self, version: &str) -> Result<Response<Body>, GraphQLServerError> {
        let vi = self.version_info(version)?;

        let value = object! {
            createdAt: vi.created_at.as_str(),
            deploymentId: vi.deployment_id.as_str(),
            latestEthereumBlockNumber: vi.latest_ethereum_block_number,
            totalEthereumBlocksCount: vi.total_ethereum_blocks_count,
            synced: vi.synced,
            failed: vi.failed,
            description: vi.description.as_ref().map(|s| s.as_str()),
            repository: vi.repository.as_ref().map(|s| s.as_str()),
            schema: vi.schema.document.to_string(),
            network: vi.network.as_ref().map(|s| s.as_str())
        };
        Ok(as_http_response(&value))
    }

    fn handle_subgraph_repo(&self, version: &str) -> Result<Response<Body>, GraphQLServerError> {
        let vi = self.version_info(version)?;

        let value = object! {
            createdAt: vi.created_at.as_str(),
            deploymentId: vi.deployment_id.as_str(),
            repository: vi.repository.as_ref().map(|s| s.as_str())
        };
        Ok(as_http_response(&value))
    }

    fn handle_entity_count(&self, deployment: &str) -> Result<Response<Body>, GraphQLServerError> {
        if let Some(value) = self.entity_counts.get(deployment) {
            return Ok(as_http_response(value.as_ref()));
        }

        let infos = self
            .store
            .status(status::Filter::Deployments(vec![deployment.to_string()]))?;
        let info = match infos.first() {
            Some(info) => info,
            None => {
                return handle_not_found();
            }
        };

        let value = object! {
            entityCount: info.entity_count
        };
        let resp = as_http_response(&value);
        self.entity_counts
            .set(deployment.to_string(), Arc::new(value));
        Ok(resp)
    }

    fn version_info(&self, version: &str) -> Result<Arc<VersionInfo>, GraphQLServerError> {
        match self.version_infos.get(version) {
            Some(vi) => Ok(vi),
            None => {
                let vi = Arc::new(self.store.version_info(version)?);
                self.version_infos.set(version.to_string(), vi.clone());
                Ok(vi)
            }
        }
    }
}

fn handle_not_found() -> Result<Response<Body>, GraphQLServerError> {
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not found"))
        .unwrap())
}

fn as_http_response(value: &q::Value) -> http::Response<Body> {
    let status_code = http::StatusCode::OK;
    let json = serde_json::to_string(&SerializableValue(value))
        .expect("Failed to serialize response to JSON");
    http::Response::builder()
        .status(status_code)
        .header("Access-Control-Allow-Origin", "*")
        .header("Access-Control-Allow-Headers", "Content-Type, User-Agent")
        .header("Access-Control-Allow-Methods", "GET, OPTIONS, POST")
        .header("Content-Type", "application/json")
        .body(Body::from(json))
        .unwrap()
}

/// Caching of values for a specified amount of time
#[derive(Debug)]
struct CacheEntry<T> {
    value: Arc<T>,
    expires: Instant,
}

/// A cache that keeps entries live for a fixed amount of time. It is assumed
/// that all that data that could possibly wind up in the cache is very small,
/// and that expired entries are replaced by an updated entry whenever expiry
/// is detected. In other words, the cache does not ever remove entries.
#[derive(Debug)]
struct Cache<T> {
    ttl: Duration,
    entries: RwLock<HashMap<String, CacheEntry<T>>>,
}

impl<T> Cache<T> {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Return the entry for `key` if it exists and is not expired yet, and
    /// return `None` otherwise. Note that expired entries stay in the cache
    /// as it is assumed that, after returning `None`, the caller will
    /// immediately overwrite that entry with a call to `set`
    fn get(&self, key: &str) -> Option<Arc<T>> {
        match self.entries.read().unwrap().get(key) {
            Some(CacheEntry { value, expires }) if *expires >= Instant::now() => {
                Some(value.clone())
            }
            _ => None,
        }
    }

    /// Associate `key` with `value` in the cache. The `value` will be
    /// valid for `self.ttl` duration
    fn set(&self, key: String, value: Arc<T>) {
        let entry = CacheEntry {
            value,
            expires: Instant::now() + self.ttl,
        };
        self.entries.write().unwrap().insert(key, entry);
    }
}

#[test]
fn cache() {
    const KEY: &str = "one";
    let cache = Cache::<String>::new(Duration::from_millis(10));
    cache.set(KEY.to_string(), Arc::new("value".to_string()));
    assert!(cache.get(KEY).is_some());
    std::thread::sleep(Duration::from_millis(15));
    assert!(cache.get(KEY).is_none())
}
