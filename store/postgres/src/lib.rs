#[macro_use]
extern crate derive_more;
#[macro_use]
extern crate diesel;
extern crate diesel_dynamic_schema;
#[macro_use]
extern crate diesel_migrations;
#[macro_use]
extern crate diesel_derive_enum;
extern crate failure;
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
mod catalog;
mod chain_head_listener;
pub mod connection_pool;
mod db_schema;
mod entities;
mod filter;
mod functions;
mod history_event;
mod jsonb;
mod jsonb_queries;
mod metadata;
mod notification_listener;
pub mod query_store;
pub mod relational;
mod relational_queries;
mod sql_value;
pub mod store;
mod store_events;

#[cfg(debug_assertions)]
pub mod db_schema_for_tests {
    pub use crate::db_schema::ethereum_blocks;
    pub use crate::db_schema::ethereum_networks;
}

#[cfg(debug_assertions)]
pub mod layout_for_tests {
    pub use crate::block_range::*;
    pub use crate::entities::STRING_PREFIX_SIZE;
    pub use crate::relational::*;
}

pub use self::chain_head_listener::ChainHeadUpdateListener;
pub use self::store::{Store, StoreConfig};
pub use self::store_events::SubscriptionManager;
