//! Functionality to support the explorer in the hosted service. Everything
//! in this file is private API and experimental and subject to change at
//! any time
use http::{Response, StatusCode};
use hyper::header::{
    ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
    CONTENT_TYPE,
};
use hyper::Body;
use std::{
    env,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use graph::{
    components::{
        server::{index_node::VersionInfo, query::GraphQLServerError},
        store::StatusStore,
    },
    data::subgraph::status,
    object,
    prelude::{lazy_static, q, serde_json, warn, Logger, SerializableValue},
    util::timed_cache::TimedCache,
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
    static ref LOCK_THRESHOLD: Duration = {
        let duration = env::var("GRAPH_EXPLORER_LOCK_THRESHOLD")
            .ok()
            .map(|s| {
                u64::from_str(&s).unwrap_or_else(|_| {
                    panic!(
                        "GRAPH_EXPLORER_LOCK_THRESHOLD must be a number, but is `{}`",
                        s
                    )
                })
            })
            .unwrap_or(100);
        Duration::from_millis(duration)
    };
    static ref QUERY_THRESHOLD: Duration = {
        let duration = env::var("GRAPH_EXPLORER_QUERY_THRESHOLD")
            .ok()
            .map(|s| {
                u64::from_str(&s).unwrap_or_else(|_| {
                    panic!(
                        "GRAPH_EXPLORER_QUERY_THRESHOLD must be a number, but is `{}`",
                        s
                    )
                })
            })
            .unwrap_or(500);
        Duration::from_millis(duration)
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
    versions: TimedCache<String, q::Value>,
    version_infos: TimedCache<String, VersionInfo>,
    entity_counts: TimedCache<String, q::Value>,
}

impl<S> Explorer<S>
where
    S: StatusStore,
{
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            versions: TimedCache::new(*TTL),
            version_infos: TimedCache::new(*TTL),
            entity_counts: TimedCache::new(*TTL),
        }
    }

    pub fn handle(
        &self,
        logger: &Logger,
        req: &[&str],
    ) -> Result<Response<Body>, GraphQLServerError> {
        match req {
            ["subgraph-versions", subgraph_id] => self.handle_subgraph_versions(subgraph_id),
            ["subgraph-version", version] => self.handle_subgraph_version(version),
            ["subgraph-repo", version] => self.handle_subgraph_repo(version),
            ["entity-count", deployment] => self.handle_entity_count(logger, deployment),
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

        let latest_ethereum_block_number = vi.latest_ethereum_block_number.map(|n| n as i32);
        let total_ethereum_blocks_count = vi.total_ethereum_blocks_count.map(|n| n as i32);
        let value = object! {
            createdAt: vi.created_at.as_str(),
            deploymentId: vi.deployment_id.as_str(),
            latestEthereumBlockNumber: latest_ethereum_block_number,
            totalEthereumBlocksCount: total_ethereum_blocks_count,
            synced: vi.synced,
            failed: vi.failed,
            description: vi.description.as_ref().map(|s| s.as_str()),
            repository: vi.repository.as_ref().map(|s| s.as_str()),
            schema: vi.schema.document.to_string(),
            network: vi.network.as_str()
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

    fn handle_entity_count(
        &self,
        logger: &Logger,
        deployment: &str,
    ) -> Result<Response<Body>, GraphQLServerError> {
        let start = Instant::now();
        let count = self.entity_counts.get(deployment);
        if start.elapsed() > *LOCK_THRESHOLD {
            let action = match count {
                Some(_) => "cache_hit",
                None => "cache_miss",
            };
            warn!(logger, "Getting entity_count takes too long";
                       "action" => action,
                       "deployment" => deployment,
                       "time_ms" => start.elapsed().as_millis());
        }

        if let Some(value) = count {
            return Ok(as_http_response(value.as_ref()));
        }

        let start = Instant::now();
        let infos = self
            .store
            .status(status::Filter::Deployments(vec![deployment.to_string()]))?;
        if start.elapsed() > *QUERY_THRESHOLD {
            warn!(logger, "Getting entity_count takes too long";
            "action" => "query_status",
            "deployment" => deployment,
            "time_ms" => start.elapsed().as_millis());
        }
        let info = match infos.first() {
            Some(info) => info,
            None => {
                return handle_not_found();
            }
        };

        let value = object! {
            entityCount: info.entity_count as i32
        };
        let start = Instant::now();
        let resp = as_http_response(&value);
        if start.elapsed() > *LOCK_THRESHOLD {
            warn!(logger, "Getting entity_count takes too long";
            "action" => "as_http_response",
            "deployment" => deployment,
            "time_ms" => start.elapsed().as_millis());
        }
        let start = Instant::now();
        self.entity_counts
            .set(deployment.to_string(), Arc::new(value));
        if start.elapsed() > *LOCK_THRESHOLD {
            warn!(logger, "Getting entity_count takes too long";
                "action" => "cache_set",
                "deployment" => deployment,
                "time_ms" => start.elapsed().as_millis());
        }
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
        .header(CONTENT_TYPE, "text/plain")
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .body(Body::from("Not found\n"))
        .unwrap())
}

fn as_http_response(value: &q::Value) -> http::Response<Body> {
    let status_code = http::StatusCode::OK;
    let json = serde_json::to_string(&SerializableValue(value))
        .expect("Failed to serialize response to JSON");
    http::Response::builder()
        .status(status_code)
        .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
        .header(ACCESS_CONTROL_ALLOW_HEADERS, "Content-Type, User-Agent")
        .header(ACCESS_CONTROL_ALLOW_METHODS, "GET, OPTIONS, POST")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap()
}
