extern crate ethabi;
extern crate ethereum_types;
extern crate futures;
extern crate graphql_parser;
extern crate hex;
extern crate num_bigint;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
#[macro_use]
extern crate slog;
extern crate failure;
extern crate ipfs_api;
extern crate parity_wasm;
extern crate slog_async;
extern crate slog_term;
extern crate tiny_keccak;
pub extern crate tokio;
extern crate web3;

/// Traits and types for all system components.
pub mod components;

/// Common data types used throughout The Graph.
pub mod data;

/// Utilities.
pub mod util;

/// A prelude that makes all system component traits and data types available.
///
/// Add the following code to import all traits and data types listed below at once.
///
/// ```
/// use graph::prelude::*;
/// ```
pub mod prelude {
    pub use tokio;
    pub use tokio::prelude::*;

    pub use std::sync::Arc;

    pub use components::ethereum::EthereumAdapter;
    pub use components::link_resolver::LinkResolver;
    pub use components::query::QueryRunner;
    pub use components::schema::{SchemaProvider, SchemaProviderEvent};
    pub use components::server::GraphQLServer;
    pub use components::store::{
        BasicStore, Store, StoreEvent, StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange,
    };
    pub use components::subgraph::{
        RuntimeHost, RuntimeHostBuilder, RuntimeHostEvent, RuntimeManager, SubgraphProvider,
        SubgraphProviderEvent,
    };
    pub use components::{EventConsumer, EventProducer};

    pub use data::query::{
        Query, QueryError, QueryExecutionError, QueryResult, QueryVariableValue, QueryVariables,
    };
    pub use data::schema::Schema;
    pub use data::store::{Attribute, Entity, Value};
    pub use data::subgraph::{DataSource, Link, SubgraphManifest, SubgraphManifestResolveError};
    pub use util::stream::StreamError;
}
