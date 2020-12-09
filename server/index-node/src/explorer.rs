//! Functionality to support the explorer in the hosted service. Everything
//! in this file is private API and experimental and subject to change at
//! any time
use graphql_parser::query as q;
use http::{Response, StatusCode};
use hyper::Body;
use std::sync::Arc;

use graph::{
    components::server::query::GraphQLServerError,
    data::subgraph::status,
    object,
    prelude::{serde_json, SerializableValue, Store},
};

#[derive(Debug)]
pub struct Explorer<S> {
    store: Arc<S>,
}

impl<S> Clone for Explorer<S> {
    fn clone(&self) -> Self {
        Self {
            store: self.store.clone(),
        }
    }
}

impl<S> Explorer<S>
where
    S: Store,
{
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
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
        let (current, pending) = self.store.versions_for_subgraph_id(subgraph_id)?;

        let value = object! {
            currentVersion: current,
            pendingVersion: pending
        };

        Ok(as_http_response(value))
    }

    fn handle_subgraph_version(&self, version: &str) -> Result<Response<Body>, GraphQLServerError> {
        let vi = self.store.version_info(version)?;

        let value = object! {
            createdAt: vi.created_at,
            deploymentId: vi.deployment_id,
            latestEthereumBlockNumber: vi.latest_ethereum_block_number,
            totalEthereumBlocksCount: vi.total_ethereum_blocks_count,
            synced: vi.synced,
            failed: vi.failed,
            description: vi.description,
            repository: vi.repository,
            schema: vi.schema.document.to_string(),
            network: vi.network
        };
        Ok(as_http_response(value))
    }

    fn handle_subgraph_repo(&self, version: &str) -> Result<Response<Body>, GraphQLServerError> {
        let vi = self.store.version_info(version)?;

        let value = object! {
            createdAt: vi.created_at,
            deploymentId: vi.deployment_id,
            repository: vi.repository
        };
        Ok(as_http_response(value))
    }

    fn handle_entity_count(&self, deployment: &str) -> Result<Response<Body>, GraphQLServerError> {
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

        Ok(as_http_response(value))
    }
}

fn handle_not_found() -> Result<Response<Body>, GraphQLServerError> {
    Ok(Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(Body::from("Not found"))
        .unwrap())
}

fn as_http_response(value: q::Value) -> http::Response<Body> {
    let status_code = http::StatusCode::OK;
    let json = serde_json::to_string(&SerializableValue(&value))
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
