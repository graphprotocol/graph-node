#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate diesel;
extern crate diesel_dynamic_schema;
#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate diesel_derive_enum;
extern crate fallible_iterator;
extern crate futures;
extern crate graph;
extern crate graph_graphql;
extern crate graphql_parser;
extern crate inflector;
extern crate lazy_static;
extern crate lru_time_cache;
extern crate postgres;
extern crate serde;
extern crate uuid;

mod block_range;
mod block_store;
mod catalog;
mod chain_head_listener;
mod chain_store;
pub mod connection_pool;
mod db_schema;
mod deployment;
mod detail;
mod dynds;
mod entities;
mod functions;
mod jsonb;
mod network_store;
mod notification_listener;
mod primary;
pub mod query_store;
mod relational;
mod relational_queries;
mod sql_value;
mod store;
mod store_events;
mod subgraph_store;

#[cfg(debug_assertions)]
pub mod db_schema_for_tests {
    pub use crate::db_schema::ethereum_blocks;
    pub use crate::db_schema::ethereum_networks;
}

#[cfg(debug_assertions)]
pub mod layout_for_tests {
    pub use crate::block_range::*;
    pub use crate::entities::STRING_PREFIX_SIZE;
    pub use crate::primary::{Connection, Namespace, EVENT_TAP, EVENT_TAP_ENABLED};
    pub use crate::relational::*;
}

pub use self::block_store::BlockStore;
pub use self::chain_head_listener::ChainHeadUpdateListener;
pub use self::chain_store::ChainStore;
pub use self::detail::DeploymentDetail;
pub use self::network_store::NetworkStore;
pub use self::primary::UnusedDeployment;
pub use self::store_events::SubscriptionManager;
pub use self::subgraph_store::{unused, DeploymentPlacer, Shard, SubgraphStore, PRIMARY_SHARD};

/// This module is only meant to support command line tooling. It must not
/// be used in 'normal' graph-node code
pub mod command_support {
    pub mod catalog {
        pub use crate::primary::Connection;
        pub use crate::primary::{
            deployment_schemas, subgraph, subgraph_deployment_assignment, subgraph_version,
        };
    }
    pub use crate::entities::Connection;
    pub use crate::primary::Namespace;
    pub use crate::relational::{Catalog, Column, ColumnType, Layout};
}
