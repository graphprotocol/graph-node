/// Traits and types for all system components.
pub mod components;

/// Common data types used throughout The Graph.
pub mod data;

/// Utilities.
pub mod util;

/// Extension traits for external types.
pub mod ext;

/// Logging utilities
pub mod log;

/// `CheapClone` trait.
pub mod cheap_clone;

/// Module with mocks for different parts of the system.
pub mod mock {
    pub use crate::components::ethereum::MockEthereumAdapter;
    pub use crate::components::store::MockStore;
}

/// Wrapper for spawning tasks that abort on panic, which is our default.
mod task_spawn;
pub use task_spawn::{
    block_on, spawn, spawn_allow_panic, spawn_blocking, spawn_blocking_allow_panic,
};

pub use bytes;
pub use url;

/// A prelude that makes all system component traits and data types available.
///
/// Add the following code to import all traits and data types listed below at once.
///
/// ```
/// use graph::prelude::*;
/// ```
pub mod prelude {
    pub use super::entity;
    pub use anyhow::{self, Context as _};
    pub use async_trait::async_trait;
    pub use bigdecimal;
    pub use ethabi;
    pub use failure::{self, err_msg, format_err, Error, Fail, SyncFailure};
    pub use futures::future;
    pub use futures::prelude::*;
    pub use futures::stream;
    pub use futures03;
    pub use futures03::compat::{Future01CompatExt, Sink01CompatExt, Stream01CompatExt};
    pub use futures03::future::{FutureExt as _, TryFutureExt};
    pub use futures03::sink::SinkExt as _;
    pub use futures03::stream::{StreamExt as _, TryStreamExt};
    pub use hex;
    pub use lazy_static::lazy_static;
    pub use reqwest;
    pub use serde_derive::{Deserialize, Serialize};
    pub use serde_json;
    pub use slog::{self, crit, debug, error, info, o, trace, warn, Logger};
    pub use std::convert::TryFrom;
    pub use std::fmt::Debug;
    pub use std::iter::FromIterator;
    pub use std::pin::Pin;
    pub use std::sync::Arc;
    pub use std::time::Duration;
    pub use thiserror;
    pub use tiny_keccak;
    pub use tokio;
    pub use web3;

    pub type DynTryFuture<'a, Ok = (), Err = Error> =
        Pin<Box<dyn futures03::Future<Output = Result<Ok, Err>> + Send + 'a>>;

    pub use crate::components::ethereum::{
        BlockFinality, BlockStream, BlockStreamBuilder, BlockStreamEvent, BlockStreamMetrics,
        ChainHeadUpdate, ChainHeadUpdateListener, ChainHeadUpdateStream, EthereumAdapter,
        EthereumAdapterError, EthereumBlock, EthereumBlockData, EthereumBlockFilter,
        EthereumBlockPointer, EthereumBlockTriggerType, EthereumBlockWithCalls,
        EthereumBlockWithTriggers, EthereumCall, EthereumCallData, EthereumCallFilter,
        EthereumContractCall, EthereumContractCallError, EthereumEventData, EthereumLogFilter,
        EthereumNetworkIdentifier, EthereumTransactionData, EthereumTrigger, LightEthereumBlock,
        LightEthereumBlockExt, ProviderEthRpcMetrics, SubgraphEthRpcMetrics,
    };
    pub use crate::components::graphql::{
        GraphQlRunner, QueryLoadManager, SubscriptionResultFuture,
    };
    pub use crate::components::link_resolver::{JsonStreamValue, JsonValueStream, LinkResolver};
    pub use crate::components::metrics::{
        aggregate::Aggregate, stopwatch::StopwatchMetrics, Collector, Counter, CounterVec, Gauge,
        GaugeVec, Histogram, HistogramOpts, HistogramVec, MetricsRegistry, Opts, PrometheusError,
        Registry,
    };
    pub use crate::components::server::admin::JsonRpcServer;
    pub use crate::components::server::index_node::IndexNodeServer;
    pub use crate::components::server::metrics::MetricsServer;
    pub use crate::components::server::query::GraphQLServer;
    pub use crate::components::server::subscription::SubscriptionServer;
    pub use crate::components::store::{
        BlockNumber, ChainStore, ChildMultiplicity, EntityCache, EntityChange,
        EntityChangeOperation, EntityCollection, EntityFilter, EntityKey, EntityLink,
        EntityModification, EntityOperation, EntityOrder, EntityQuery, EntityRange, EntityWindow,
        EthereumCallCache, MetadataOperation, ParentLink, PoolWaitStats, QueryStore, Store,
        StoreError, StoreEvent, StoreEventStream, StoreEventStreamBox, SubgraphDeploymentStore,
        TransactionAbortError, WindowAttribute, BLOCK_NUMBER_MAX, SUBSCRIPTION_THROTTLE_INTERVAL,
    };
    pub use crate::components::subgraph::{
        BlockState, DataSourceLoader, DataSourceTemplateInfo, HostMetrics, RuntimeHost,
        RuntimeHostBuilder, SubgraphAssignmentProvider, SubgraphInstance, SubgraphInstanceManager,
        SubgraphRegistrar, SubgraphVersionSwitchingMode,
    };
    pub use crate::components::{EventConsumer, EventProducer};

    pub use crate::cheap_clone::CheapClone;
    pub use crate::data::graphql::{
        shape_hash::shape_hash, SerializableValue, TryFromValue, ValueMap,
    };
    pub use crate::data::query::{
        Query, QueryError, QueryExecutionError, QueryResult, QueryVariables,
    };
    pub use crate::data::schema::{ApiSchema, Schema};
    pub use crate::data::store::ethereum::*;
    pub use crate::data::store::scalar::{BigDecimal, BigInt, BigIntSign};
    pub use crate::data::store::{
        AssignmentEvent, Attribute, Entity, NodeId, SubgraphEntityPair, ToEntityId, ToEntityKey,
        TryIntoEntity, Value, ValueType,
    };
    pub use crate::data::subgraph::schema::{SubgraphDeploymentEntity, TypedEntity};
    pub use crate::data::subgraph::{
        BlockHandlerFilter, CreateSubgraphResult, DataSource, DataSourceContext,
        DataSourceTemplate, DeploymentState, Link, MappingABI, MappingBlockHandler,
        MappingCallHandler, MappingEventHandler, SubgraphAssignmentProviderError,
        SubgraphAssignmentProviderEvent, SubgraphDeploymentId, SubgraphFeatures, SubgraphManifest,
        SubgraphManifestResolveError, SubgraphManifestValidationError, SubgraphName,
        SubgraphRegistrarError, UnvalidatedSubgraphManifest,
    };
    pub use crate::data::subscription::{
        QueryResultStream, Subscription, SubscriptionError, SubscriptionResult,
    };
    pub use crate::ext::futures::{
        CancelGuard, CancelHandle, CancelToken, CancelableError, FutureExtension,
        SharedCancelGuard, StreamExtension,
    };
    pub use crate::impl_slog_value;
    pub use crate::log::codes::LogCode;
    pub use crate::log::elastic::{elastic_logger, ElasticDrainConfig, ElasticLoggingConfig};
    pub use crate::log::factory::{
        ComponentLoggerConfig, ElasticComponentLoggerConfig, LoggerFactory,
    };
    pub use crate::log::split::split_logger;
    pub use crate::util::cache_weight::CacheWeight;
    pub use crate::util::error::CompatErr;
    pub use crate::util::futures::{retry, TimeoutError};
    pub use crate::util::stats::MovingStats;
}
