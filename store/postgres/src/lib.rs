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

mod chain_head_listener;
mod db_schema;
mod entities;
mod filter;
mod functions;
mod jsonb;
mod mapping;
mod mapping_sql;
mod notification_listener;
mod sql_value;
pub mod store;
mod store_events;

#[cfg(debug_assertions)]
pub mod db_schema_for_tests {
    pub use crate::db_schema::ethereum_blocks;
    pub use crate::db_schema::ethereum_networks;
}

#[cfg(debug_assertions)]
pub mod mapping_for_tests {
    pub use crate::mapping::*;
}

pub use self::chain_head_listener::ChainHeadUpdateListener;
pub use self::store::{Store, StoreConfig};
