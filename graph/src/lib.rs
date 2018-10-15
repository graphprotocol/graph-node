extern crate backtrace;
extern crate diesel;
pub extern crate ethabi;
extern crate futures;
extern crate graphql_parser;
extern crate hex;
extern crate num_bigint;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
#[macro_use]
pub extern crate slog;
#[macro_use]
extern crate failure;
extern crate ipfs_api;
extern crate parity_wasm;
pub extern crate serde_json;
extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_term;
extern crate tiny_keccak;
pub extern crate tokio;
extern crate tokio_retry;
pub extern crate web3;

/// Traits and types for all system components.
pub mod components;

/// Common data types used throughout The Graph.
pub mod data;

/// Utilities.
pub mod util;

/// Extension traits for external types.
pub mod ext;

/// A prelude that makes all system component traits and data types available.
///
/// Add the following code to import all traits and data types listed below at once.
///
/// ```
/// use graph::prelude::*;
/// ```
pub mod prelude {
    // Glob import from `slog` to re-export the macros, but prevent
    // `slog::Result` from shadowing `Result`. Rust 2018 will have proper macro
    // imports then we can remove `slog::*` in favor of something fine-grained.
    pub use failure::{Error, SyncFailure};
    pub use slog;
    pub use slog::*;
    pub use std::result::Result;
    pub use tokio;
    pub use tokio::prelude::*;

    pub use std::sync::Arc;

    pub use components::ethereum::{
        BlockStream, BlockStreamBuilder, ChainHeadUpdate, ChainHeadUpdateListener, EthereumAdapter,
        EthereumBlock, EthereumBlockData, EthereumBlockPointer, EthereumEventData,
        EthereumLogFilter, EthereumNetworkIdentifier, EthereumTransactionData,
    };
    pub use components::graphql::{GraphQlRunner, QueryResultFuture, SubscriptionResultFuture};
    pub use components::link_resolver::LinkResolver;
    pub use components::server::admin::JsonRpcServer;
    pub use components::server::query::GraphQLServer;
    pub use components::server::subscription::SubscriptionServer;
    pub use components::store::{
        ChainStore, EntityChange, EntityChangeOperation, EntityChangeStream, EntityOperation,
        EventSource, Store, StoreFilter, StoreKey, StoreOrder, StoreQuery, StoreRange,
        SubgraphEntityPair,
    };
    pub use components::subgraph::{
        RuntimeHost, RuntimeHostBuilder, SchemaEvent, SubgraphInstance, SubgraphInstanceManager,
        SubgraphProvider, SubgraphProviderEvent, SubgraphRegistry,
    };
    pub use components::{EventConsumer, EventProducer};

    pub use data::graphql::SerializableValue;
    pub use data::query::{
        Query, QueryError, QueryExecutionError, QueryResult, QueryVariableValue, QueryVariables,
    };
    pub use data::schema::Schema;
    pub use data::store::{Attribute, Entity, Value};
    pub use data::subgraph::{
        DataSource, Link, MappingABI, MappingEventHandler, SubgraphId, SubgraphManifest,
        SubgraphManifestResolveError, SubgraphProviderError,
    };
    pub use data::subscription::{
        QueryResultStream, Subscription, SubscriptionError, SubscriptionResult,
    };
    pub use ext::futures::{
        CancelGuard, CancelHandle, CancelableError, FutureExtension, SharedCancelGuard,
        StreamExtension,
    };
    pub use util::futures::{with_retry, with_retry_log_after};
}
