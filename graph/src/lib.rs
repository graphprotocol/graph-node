extern crate backtrace;
extern crate diesel;
pub extern crate ethabi;
extern crate futures;
extern crate graphql_parser;
extern crate hex;
#[macro_use]
extern crate lazy_static;
extern crate num_bigint;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_yaml;
pub extern crate slog;
#[macro_use]
pub extern crate failure;
extern crate ipfs_api;
extern crate parity_wasm;
extern crate rand;
pub extern crate serde_json;
pub extern crate slog_async;
extern crate slog_envlogger;
extern crate slog_term;
extern crate tiny_keccak;
pub extern crate tokio;
pub extern crate tokio_executor;
extern crate tokio_retry;
pub extern crate tokio_timer;
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
    pub use failure::{self, bail, err_msg, format_err, Error, Fail, SyncFailure};
    pub use serde_derive::{Deserialize, Serialize};
    pub use slog::{self, crit, debug, error, info, o, trace, warn, Logger};
    pub use std::fmt::Debug;
    pub use std::sync::Arc;
    pub use tokio;
    pub use tokio::prelude::*;

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
        ChainStore, EntityChange, EntityChangeOperation, EntityChangeStream, EntityFilter,
        EntityKey, EntityOperation, EntityOrder, EntityQuery, EntityRange, EventSource, Store,
        StoreError, SubgraphDeploymentStore, TransactionAbortError,
    };
    pub use components::subgraph::{
        RuntimeHost, RuntimeHostBuilder, SubgraphAssignmentProvider, SubgraphInstance,
        SubgraphInstanceManager, SubgraphRegistrar,
    };
    pub use components::{EventConsumer, EventProducer};

    pub use data::graphql::SerializableValue;
    pub use data::query::{Query, QueryError, QueryExecutionError, QueryResult, QueryVariables};
    pub use data::schema::Schema;
    pub use data::store::scalar::{BigInt, BigIntSign};
    pub use data::store::{
        Attribute, AssignmentEvent, Entity, NodeId, SubgraphEntityPair, Value, ValueType,
    };
    pub use data::subgraph::schema::TypedEntity;
    pub use data::subgraph::{
        CreateSubgraphResult, DataSource, Link, MappingABI, MappingEventHandler,
        SubgraphAssignmentProviderError, SubgraphAssignmentProviderEvent, SubgraphId,
        SubgraphManifest, SubgraphManifestResolveError, SubgraphName, SubgraphRegistrarError,
    };
    pub use data::subscription::{
        QueryResultStream, Subscription, SubscriptionError, SubscriptionResult,
    };
    pub use ext::futures::{
        CancelGuard, CancelHandle, CancelableError, FutureExtension, SharedCancelGuard,
        StreamExtension,
    };
    pub use util::futures::retry;
}
