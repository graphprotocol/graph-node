extern crate backtrace;
pub extern crate bigdecimal;
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
extern crate semver;
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
    pub use std::iter::FromIterator;
    pub use std::sync::Arc;
    pub use tokio;
    pub use tokio::prelude::*;

    pub use crate::components::ethereum::{
        BlockStream, BlockStreamBuilder, ChainHeadUpdate, ChainHeadUpdateListener, EthereumAdapter,
        EthereumAdapterError, EthereumBlock, EthereumBlockData, EthereumBlockPointer,
        EthereumEventData, EthereumLogFilter, EthereumNetworkIdentifier, EthereumTransactionData,
    };
    pub use crate::components::graphql::{
        GraphQlRunner, QueryResultFuture, SubscriptionResultFuture,
    };
    pub use crate::components::link_resolver::LinkResolver;
    pub use crate::components::server::admin::JsonRpcServer;
    pub use crate::components::server::query::GraphQLServer;
    pub use crate::components::server::subscription::SubscriptionServer;
    pub use crate::components::store::{
        AttributeIndexDefinition, ChainStore, EntityChange, EntityChangeOperation, EntityFilter,
        EntityKey, EntityOperation, EntityOrder, EntityQuery, EntityRange, EventSource, Store,
        StoreError, StoreEvent, StoreEventStream, StoreEventStreamBox, SubgraphDeploymentStore,
        TransactionAbortError, SUBSCRIPTION_THROTTLE_INTERVAL,
    };
    pub use crate::components::subgraph::{
        BlockState, DataSourceLoader, DataSourceTemplateInfo, RuntimeHost, RuntimeHostBuilder,
        SubgraphAssignmentProvider, SubgraphInstance, SubgraphInstanceManager, SubgraphRegistrar,
        SubgraphVersionSwitchingMode,
    };
    pub use crate::components::{EventConsumer, EventProducer};

    pub use crate::data::graphql::{SerializableValue, TryFromValue, ValueMap};
    pub use crate::data::query::{
        Query, QueryError, QueryExecutionError, QueryResult, QueryVariables,
    };
    pub use crate::data::schema::Schema;
    pub use crate::data::store::scalar::{BigDecimal, BigInt, BigIntSign};
    pub use crate::data::store::{
        AssignmentEvent, Attribute, Entity, NodeId, SubgraphEntityPair, SubgraphVersionSummary,
        Value, ValueType,
    };
    pub use crate::data::subgraph::schema::{SubgraphDeploymentEntity, TypedEntity};
    pub use crate::data::subgraph::{
        CreateSubgraphResult, DataSource, DataSourceTemplate, Link, MappingABI,
        MappingEventHandler, SubgraphAssignmentProviderError, SubgraphAssignmentProviderEvent,
        SubgraphDeploymentId, SubgraphManifest, SubgraphManifestResolveError, SubgraphName,
        SubgraphRegistrarError,
    };
    pub use crate::data::subscription::{
        QueryResultStream, Subscription, SubscriptionError, SubscriptionResult,
    };
    pub use crate::ext::futures::{
        CancelGuard, CancelHandle, CancelableError, FutureExtension, SharedCancelGuard,
        StreamExtension,
    };
    pub use crate::util::futures::retry;
}
